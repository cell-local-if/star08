"""Tests for explicit reference release (DELETE /v1/blobs/{digest}/refs) and on-demand GC (POST /v1/gc)."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import BlobNotFound, DigestConflict, InvalidRequest, Store


class StoreReleaseUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def test_release_decrements_and_reports_remaining(self) -> None:
        blob = self.store.put(b"data")
        self.store.put(b"data")
        self.assertEqual(self.store.release(blob.digest), 1)
        self.assertEqual(self.store.listing()[0]["refs"], 1)
        self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 4, "puts": 1})

    def test_release_to_zero_keeps_content_until_gc(self) -> None:
        blob = self.store.put(b"data")
        self.assertEqual(self.store.release(blob.digest), 0)
        self.assertEqual(self.store.get(blob.digest), b"data")
        self.assertEqual(self.store.head(blob.digest).digest, blob.digest)
        self.assertEqual(self.store.listing()[0]["refs"], 0)
        self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 4, "puts": 0})

    def test_release_at_zero_is_a_conflict_and_count_stays(self) -> None:
        blob = self.store.put(b"data")
        self.store.release(blob.digest)
        with self.assertRaises(DigestConflict):
            self.store.release(blob.digest)
        self.assertEqual(self.store.listing()[0]["refs"], 0)

    def test_release_invalid_and_unknown_digest(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.store.release("not-a-digest")
        with self.assertRaises(InvalidRequest):
            self.store.release("A" * 64)
        with self.assertRaises(BlobNotFound):
            self.store.release("0" * 64)

    def test_gc_deletes_only_zero_ref_blobs(self) -> None:
        keep = self.store.put(b"keep")
        drop = self.store.put(b"drop")
        self.store.release(drop.digest)
        result = self.store.gc()
        self.assertEqual(result["deleted"], [drop.digest])
        self.assertEqual(result["stats"], {"blobs": 1, "bytes": 4, "puts": 1})
        self.assertEqual(self.store.get(keep.digest), b"keep")
        with self.assertRaises(BlobNotFound):
            self.store.get(drop.digest)

    def test_gc_with_nothing_to_delete(self) -> None:
        self.store.put(b"keep")
        result = self.store.gc()
        self.assertEqual(result["deleted"], [])
        self.assertEqual(result["stats"], {"blobs": 1, "bytes": 4, "puts": 1})

    def test_put_after_gc_rebuilds_blob_with_refs_one(self) -> None:
        blob = self.store.put(b"data")
        self.store.release(blob.digest)
        self.store.gc()
        again = self.store.put(b"data")
        self.assertEqual(again.digest, blob.digest)
        self.assertEqual(self.store.listing()[0]["refs"], 1)
        self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 4, "puts": 1})


class ReleaseAndGcHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from artifacts import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def setUp(self) -> None:
        with self.server.store._lock:
            self.server.store._blobs.clear()
            self.server.store._meta.clear()
            self.server.store._refs.clear()

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def put(self, payload: bytes) -> str:
        status, _, headers = self.call("PUT", "/v1/blobs", payload)
        self.assertEqual(status, 201)
        return headers["X-Blob-Digest"]

    def release(self, digest: str):
        return self.call("DELETE", f"/v1/blobs/{digest}/refs")

    def test_release_happy_path_and_zero_ref_visibility(self) -> None:
        digest = self.put(b"http-data")
        self.put(b"http-data")
        status, body, _ = self.release(digest)
        self.assertEqual((status, json.loads(body)), (200, {"digest": digest, "refs": 1}))
        status, body, _ = self.release(digest)
        self.assertEqual((status, json.loads(body)), (200, {"digest": digest, "refs": 0}))
        # content still served at refs 0, listed with refs 0
        self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[1], b"http-data")
        self.assertEqual(self.call("HEAD", f"/v1/blobs/{digest}")[0], 200)
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = json.loads(body)
        self.assertEqual(listing["blobs"][0]["refs"], 0)
        self.assertEqual(listing["stats"], {"blobs": 1, "bytes": 9, "puts": 0})

    def test_release_error_mapping(self) -> None:
        status, body, _ = self.release("zz")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.release("0" * 64)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (404, "not_found"))
        digest = self.put(b"once")
        self.release(digest)
        status, body, _ = self.release(digest)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
        _, body, _ = self.call("GET", "/v1/blobs")
        self.assertEqual(json.loads(body)["blobs"][0]["refs"], 0)

    def test_gc_deletes_zero_ref_blobs_and_reports_sorted_deleted(self) -> None:
        keep = self.put(b"keep-me")
        dropped = [self.put(payload) for payload in (b"drop-1", b"drop-2", b"drop-3")]
        for digest in dropped:
            self.release(digest)
        status, body, _ = self.call("POST", "/v1/gc", b"{}")
        self.assertEqual(status, 200)
        result = json.loads(body)
        self.assertEqual(result["deleted"], sorted(dropped))
        self.assertEqual(result["stats"], {"blobs": 1, "bytes": 7, "puts": 1})
        self.assertEqual(self.call("GET", f"/v1/blobs/{keep}")[0], 200)
        for digest in dropped:
            self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[0], 404)
            self.assertEqual(self.call("HEAD", f"/v1/blobs/{digest}")[0], 404)
        # stats in the gc response match the plain listing
        _, body, _ = self.call("GET", "/v1/blobs")
        self.assertEqual(json.loads(body)["stats"], result["stats"])

    def test_gc_with_nothing_to_delete_returns_empty_list(self) -> None:
        self.put(b"still-referenced")
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(status, 200)
        result = json.loads(body)
        self.assertEqual(result["deleted"], [])
        self.assertEqual(result["stats"], {"blobs": 1, "bytes": 16, "puts": 1})

    def test_put_after_gc_rebuilds_blob(self) -> None:
        digest = self.put(b"rebuild-me")
        self.release(digest)
        self.call("POST", "/v1/gc")
        self.assertEqual(self.put(b"rebuild-me"), digest)
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = json.loads(body)
        self.assertEqual(listing["blobs"][0]["refs"], 1)
        self.assertEqual(listing["stats"], {"blobs": 1, "bytes": 10, "puts": 1})

    def test_concurrent_puts_and_releases_preserve_ref_count(self) -> None:
        payload = b"hot-blob"
        digest = self.put(payload)  # refs = 1
        put_threads, release_threads = 4, 4
        puts, releases = 20, 10  # per thread

        def do_puts() -> None:
            for _ in range(puts):
                self.assertEqual(self.call("PUT", "/v1/blobs", payload)[0], 201)

        def do_releases() -> None:
            remaining = releases
            while remaining:
                status, _, _ = self.release(digest)
                if status == 200:
                    remaining -= 1
                else:
                    # refs momentarily 0: a put will arrive; retry until it lands
                    self.assertEqual(status, 409)

        threads = ([threading.Thread(target=do_puts) for _ in range(put_threads)]
                   + [threading.Thread(target=do_releases) for _ in range(release_threads)])
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        expected = 1 + puts * put_threads - releases * release_threads
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = json.loads(body)
        self.assertEqual(listing["blobs"][0]["refs"], expected)
        self.assertEqual(listing["stats"]["puts"], expected)

    def test_concurrent_gc_and_put_never_loses_the_blob(self) -> None:
        payload = b"racy"
        digest = self.put(payload)
        self.release(digest)  # refs = 0, content retained
        stop = threading.Event()
        errors: list[int] = []

        def gc_loop() -> None:
            while not stop.is_set():
                self.call("POST", "/v1/gc")

        def put_loop() -> None:
            for _ in range(50):
                status, _, _ = self.call("PUT", "/v1/blobs", payload)
                if status != 201:
                    errors.append(status)
                self.release(digest)

        gc_thread = threading.Thread(target=gc_loop)
        put_thread = threading.Thread(target=put_loop)
        gc_thread.start()
        put_thread.start()
        put_thread.join()
        stop.set()
        gc_thread.join()
        self.assertEqual(errors, [])
        # every PUT landed and every release matched one: refs back to 0, blob
        # either retained (refs 0) or already reclaimed; a final PUT must rebuild it
        self.assertEqual(self.put(payload), digest)
        _, body, _ = self.call("GET", "/v1/blobs")
        self.assertEqual(json.loads(body)["blobs"][0]["refs"], 1)


if __name__ == "__main__":
    unittest.main()
