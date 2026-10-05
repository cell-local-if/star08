"""Explicit reference release and on-demand garbage collection."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import BlobNotFound, DigestConflict, InvalidRequest, Store, digest_of


class ReleaseUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def test_release_decrements_and_reports_remaining_refs(self) -> None:
        blob = self.store.put(b"shared")
        self.store.put(b"shared")
        self.store.put(b"shared")
        self.assertEqual(self.store.release(blob.digest), 2)
        self.assertEqual(self.store.release(blob.digest), 1)
        self.assertEqual(self.store.listing()[0]["refs"], 1)

    def test_release_to_zero_keeps_bytes_until_gc(self) -> None:
        blob = self.store.put(b"keep-me")
        self.assertEqual(self.store.release(blob.digest), 0)
        # Content and metadata stay live; listing/stats still show the blob with refs 0.
        self.assertEqual(self.store.get(blob.digest), b"keep-me")
        self.assertEqual(self.store.head(blob.digest).digest, blob.digest)
        self.assertEqual(self.store.listing(),
                         [{"digest": blob.digest, "size": 7,
                           "media_type": "application/octet-stream", "refs": 0}])
        self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 7, "puts": 0})

    def test_release_at_zero_conflicts_without_changing_count(self) -> None:
        blob = self.store.put(b"once")
        self.assertEqual(self.store.release(blob.digest), 0)
        with self.assertRaises(DigestConflict):
            self.store.release(blob.digest)
        self.assertEqual(self.store.stats()["puts"], 0)

    def test_release_errors(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.store.release("ZZ")
        with self.assertRaises(InvalidRequest):
            self.store.release("a" * 63)
        with self.assertRaises(BlobNotFound):
            self.store.release("0" * 64)


class GcUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def test_gc_reaps_only_zero_ref_blobs_and_returns_sorted_digests(self) -> None:
        keep = self.store.put(b"keep")
        self.store.put(b"keep")  # refs 2
        dead_a = self.store.put(b"dead-a")
        dead_b = self.store.put(b"dead-b")
        live = self.store.put(b"live")
        self.assertEqual(self.store.release(dead_a.digest), 0)
        self.assertEqual(self.store.release(dead_b.digest), 0)
        self.assertEqual(self.store.release(keep.digest), 1)

        result = self.store.gc()
        self.assertEqual(result["deleted"], sorted([dead_a.digest, dead_b.digest]))
        self.assertEqual(result["stats"], {"blobs": 2, "bytes": len(b"keep") + len(b"live"), "puts": 2})
        self.assertEqual(result["stats"], self.store.stats())
        # Referenced content is untouched.
        self.assertEqual(self.store.get(keep.digest), b"keep")
        self.assertEqual(self.store.get(live.digest), b"live")
        for gone in (dead_a.digest, dead_b.digest):
            with self.assertRaises(BlobNotFound):
                self.store.get(gone)
            with self.assertRaises(BlobNotFound):
                self.store.head(gone)

    def test_gc_with_nothing_reapable_returns_empty_list(self) -> None:
        self.assertEqual(self.store.gc(), {"deleted": [], "stats": {"blobs": 0, "bytes": 0, "puts": 0}})
        self.store.put(b"x")
        self.assertEqual(self.store.gc()["deleted"], [])

    def test_put_after_gc_rebuilds_blob_with_one_ref(self) -> None:
        blob = self.store.put(b"reborn")
        self.store.release(blob.digest)
        self.store.gc()
        rebuilt = self.store.put(b"reborn")
        self.assertEqual(rebuilt.digest, blob.digest)
        self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 6, "puts": 1})
        self.assertEqual(self.store.listing()[0]["refs"], 1)

    def test_upload_complete_counts_as_a_write_for_release_and_gc(self) -> None:
        data = b"session-bytes"
        digest = digest_of(data)
        # Plain PUT then a session commit of the same bytes: refs 2.
        self.store.put(data)
        self.store.put_completed(data, None, "application/octet-stream")
        self.assertEqual(self.store.stats()["puts"], 2)
        self.assertEqual(self.store.release(digest), 1)
        self.assertEqual(self.store.release(digest), 0)
        self.store.gc()
        with self.assertRaises(BlobNotFound):
            self.store.get(digest)
        # A fresh session of the same bytes rebuilds the blob.
        rebuilt = self.store.put_completed(data, digest, "application/octet-stream")
        self.assertEqual((rebuilt.digest, self.store.stats()["puts"]), (digest, 1))


class HttpSurfaceTests(unittest.TestCase):
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
        store = self.server.store
        with store._lock:
            store._blobs.clear()
            store._meta.clear()
            store._refs.clear()

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def put(self, payload: bytes = b"payload") -> str:
        status, body, headers = self.call("PUT", "/v1/blobs", payload)
        self.assertEqual(status, 201)
        return json.loads(body)["digest"]

    def test_release_endpoint_statuses_and_body(self) -> None:
        digest = self.put(b"abc")
        self.put(b"abc")
        status, body, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"digest": digest, "refs": 1})
        status, body, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.assertEqual((status, json.loads(body)), (200, {"digest": digest, "refs": 0}))

        status, body, _ = self.call("DELETE", "/v1/blobs/ZZ/refs")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.call("DELETE", f"/v1/blobs/{'0' * 64}/refs")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (404, "not_found"))
        status, body, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))

    def test_zero_refs_visible_before_gc_gone_after(self) -> None:
        digest = self.put(b"visible-until-gc")
        self.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[0], 200)
        self.assertEqual(self.call("HEAD", f"/v1/blobs/{digest}")[0], 200)
        listing = json.loads(self.call("GET", "/v1/blobs")[1])
        self.assertEqual([(b["digest"], b["refs"]) for b in listing["blobs"]], [(digest, 0)])
        self.assertEqual(listing["stats"], {"blobs": 1, "bytes": 16, "puts": 0})
        # min_refs filter reflects the released state as well.
        hidden = json.loads(self.call("GET", "/v1/blobs?min_refs=1")[1])["blobs"]
        self.assertEqual(hidden, [])
        shown = json.loads(self.call("GET", "/v1/blobs?min_refs=0")[1])["blobs"]
        self.assertEqual(len(shown), 1)

        status, body, _ = self.call("POST", "/v1/gc", b'{"ignored": true}')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body),
                         {"deleted": [digest], "stats": {"blobs": 0, "bytes": 0, "puts": 0}})
        self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[0], 404)
        self.assertEqual(self.call("HEAD", f"/v1/blobs/{digest}")[0], 404)
        # Releasing a reaped digest is now not_found.
        self.assertEqual(self.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 404)

        # Re-PUT rebuilds as a fresh blob with one ref.
        rebuilt = self.put(b"visible-until-gc")
        self.assertEqual(rebuilt, digest)
        listing = json.loads(self.call("GET", "/v1/blobs")[1])
        self.assertEqual(listing["blobs"][0]["refs"], 1)
        self.assertEqual(listing["stats"], {"blobs": 1, "bytes": 16, "puts": 1})

    def test_gc_keeps_referenced_blobs_and_sorts_deleted(self) -> None:
        digests = []
        for payload in (b"p0", b"p1", b"p2", b"p3"):
            digest = self.put(payload)
            digests.append(digest)
        # Release p0 and p3 to refs 0; p1 stays refs 1; p2 gets to refs 2 then back to 1.
        self.put(b"p2")
        for digest in (digests[0], digests[3]):
            self.assertEqual(self.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
        self.call("DELETE", f"/v1/blobs/{digests[2]}/refs")
        status, body, _ = self.call("POST", "/v1/gc", b"")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload["deleted"], sorted([digests[0], digests[3]]))
        self.assertEqual(payload["stats"]["blobs"], 2)
        self.assertEqual(payload["stats"]["puts"], 2)
        # GC stats match the no-param listing caliber.
        listing = json.loads(self.call("GET", "/v1/blobs")[1])
        self.assertEqual(payload["stats"], listing["stats"])
        # A second sweep has nothing to do.
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual((status, json.loads(body)["deleted"]), (200, []))

    def test_concurrent_puts_completes_releases_and_gc_never_lose_refs(self) -> None:
        payload = b"hot-blob" * 8
        digest = digest_of(payload)
        writes = {"n": 0}      # successful PUTs + session completes
        releases = {"n": 0}    # successful reference releases
        counter_lock = threading.Lock()
        stop = threading.Event()

        def count(bucket: dict, ok: bool) -> None:
            if ok:
                with counter_lock:
                    bucket["n"] += 1

        def put_writer() -> None:
            for _ in range(10):
                count(writes, self.call("PUT", "/v1/blobs", payload)[0] == 201)

        def session_writer() -> None:
            for _ in range(5):
                body = json.dumps({"size": len(payload),
                                   "media_type": "application/octet-stream"}).encode()
                status, raw, _ = self.call("POST", "/v1/uploads", body,
                                           {"Content-Type": "application/json"})
                if status != 201:
                    continue
                upload_id = json.loads(raw)["upload_id"]
                # Two chunks; any losing race (409/404) means this write simply did not commit.
                half = len(payload) // 2
                s1, _, _ = self.call("PUT", f"/v1/uploads/{upload_id}", payload[:half],
                                     {"X-Upload-Offset": "0"})
                if s1 != 200:
                    continue
                s2, _, _ = self.call("PUT", f"/v1/uploads/{upload_id}", payload[half:],
                                     {"X-Upload-Offset": str(half)})
                if s2 != 200:
                    continue
                status, _, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
                count(writes, status == 201)

        def releaser() -> None:
            # Bounded attempts: a write can be released at most once, and 404/409 never decrement.
            for _ in range(12):
                status, _, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
                count(releases, status == 200)

        def sweeper() -> None:
            while not stop.is_set():
                self.call("POST", "/v1/gc")

        workers = ([threading.Thread(target=put_writer) for _ in range(3)]
                   + [threading.Thread(target=session_writer) for _ in range(2)]
                   + [threading.Thread(target=releaser) for _ in range(4)]
                   + [threading.Thread(target=sweeper)])
        for thread in workers:
            thread.start()
        for thread in workers[:-1]:
            thread.join()
        stop.set()
        workers[-1].join()
        self.call("POST", "/v1/gc")  # quiesce: reap a zero-ref blob if one remains

        with self.server.store._lock:
            present = digest in self.server.store._blobs
            refs = self.server.store._refs.get(digest, 0)

        # No release can outnumber the writes that actually committed.
        self.assertLessEqual(releases["n"], writes["n"])
        expected = writes["n"] - releases["n"]
        if expected > 0:
            self.assertTrue(present)
            self.assertEqual(refs, expected)
            self.assertEqual(self.store_stats()["puts"], expected)
            self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[0], 200)
        else:
            # Every committed write was released: final sweep removed the bytes.
            self.assertFalse(present)
            self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[0], 404)
            self.assertEqual(self.call("HEAD", f"/v1/blobs/{digest}")[0], 404)

    def store_stats(self) -> dict:
        return json.loads(self.call("GET", "/v1/blobs")[1])["stats"]


if __name__ == "__main__":
    unittest.main()
