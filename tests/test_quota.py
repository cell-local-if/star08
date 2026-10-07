"""Tests for the optional service-level store byte quota (--max-store-bytes)."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import QuotaExceeded, Store, digest_of, serve

QUOTA_BODY = {"error": {"code": "quota_exceeded", "message": "store byte quota exceeded"}}


class StoreQuotaUnitTests(unittest.TestCase):
    def test_invalid_quota_values_rejected(self) -> None:
        for bad in (0, -1, 1.5, True, "1024"):
            with self.assertRaises(ValueError):
                Store(max_store_bytes=bad)  # type: ignore[arg-type]

    def test_put_fills_exactly_to_cap_then_413(self) -> None:
        store = Store(max_store_bytes=10)
        store.put(b"AAAA")
        store.put(b"BBBBBB")  # exactly at the cap: allowed
        with self.assertRaises(QuotaExceeded):
            store.put(b"C")
        self.assertEqual(store.stats(), {"blobs": 2, "bytes": 10, "puts": 2})

    def test_repeat_put_never_counts_against_quota(self) -> None:
        store = Store(max_store_bytes=4)
        store.put(b"AAAA")
        for _ in range(5):
            store.put(b"AAAA")  # dedup: refs grow, bytes do not
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 4, "puts": 6})

    def test_zero_ref_blob_still_occupies_quota_until_gc(self) -> None:
        store = Store(max_store_bytes=5)
        blob = store.put(b"AAAA")
        store.release(blob.digest)  # refs 0, content retained
        with self.assertRaises(QuotaExceeded):
            store.put(b"BB")
        result = store.gc()
        self.assertEqual(result["deleted"], [blob.digest])
        store.put(b"BB")  # quota freed by gc
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 2, "puts": 1})

    def test_put_validation_errors_precede_quota(self) -> None:
        from artifacts import DigestConflict, InvalidRequest

        store = Store(max_store_bytes=1)
        store.put(b"x")  # cap fully used
        with self.assertRaises(InvalidRequest):
            store.put(b"")  # empty body: 400 before quota
        with self.assertRaises(InvalidRequest):
            store.put(b"y" * (1048576 + 1))  # over single-blob cap: 400 before quota
        with self.assertRaises(InvalidRequest):
            store.put(b"y", declared_digest="zz")
        with self.assertRaises(DigestConflict):
            store.put(b"y", declared_digest=digest_of(b"z"))

    def test_put_completed_quota_and_dedup(self) -> None:
        store = Store(max_store_bytes=4)
        store.put(b"AAAA")
        with self.assertRaises(QuotaExceeded):
            store.put_completed(b"BB", None, "text/plain")
        blob = store.put_completed(b"AAAA", None, "text/plain")  # dedup: no new bytes
        self.assertEqual(blob.digest, digest_of(b"AAAA"))
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 4, "puts": 2})

    def test_absorb_is_all_or_nothing_under_quota(self) -> None:
        store = Store(max_store_bytes=5)
        store.put(b"AAAA")  # 4 of 5 used
        fetched = [(digest_of(b"xx"), b"xx", "text/plain"),
                   (digest_of(b"yy"), b"yy", "text/plain")]
        with self.assertRaises(QuotaExceeded):
            store.absorb(fetched, sorted(d for d, _, _ in fetched))
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 4, "puts": 1})
        # a batch that fits commits whole
        small = [(digest_of(b"z"), b"z", "text/plain")]
        result = store.absorb(small, [digest_of(b"z")])
        self.assertEqual(result["synced"], [digest_of(b"z")])
        self.assertEqual(store.stats(), {"blobs": 2, "bytes": 5, "puts": 2})

    def test_absorb_skips_bytes_of_digests_already_stored(self) -> None:
        store = Store(max_store_bytes=5)
        store.put(b"AAAA")
        # "AAAA" is already local: only the 1 new byte counts against the cap
        fetched = [(digest_of(b"AAAA"), b"AAAA", "text/plain"),
                   (digest_of(b"z"), b"z", "text/plain")]
        result = store.absorb(fetched, sorted(d for d, _, _ in fetched))
        self.assertEqual(result["existing"], [digest_of(b"AAAA")])
        self.assertEqual(result["synced"], [digest_of(b"z")])
        self.assertEqual(store.stats(), {"blobs": 2, "bytes": 5, "puts": 2})

    def test_no_quota_by_default(self) -> None:
        store = Store()
        for i in range(50):
            store.put(bytes([i]) * 1000)
        self.assertEqual(store.stats()["bytes"], 50000)


class QuotaHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = serve(port=0, max_store_bytes=10)
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

    def put(self, payload: bytes, headers: dict | None = None):
        return self.call("PUT", "/v1/blobs", payload, headers)

    def stats(self) -> dict:
        _, body, _ = self.call("GET", "/v1/blobs")
        return json.loads(body)["stats"]

    def test_put_over_quota_is_413_with_fixed_body_and_no_side_effects(self) -> None:
        self.assertEqual(self.put(b"AAAA")[0], 201)
        self.assertEqual(self.put(b"BBBBBB")[0], 201)  # exactly at the cap of 10
        status, body, _ = self.put(b"C")
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body), QUOTA_BODY)
        # no visible storage change; reads of the stored blobs are unaffected
        self.assertEqual(self.stats(), {"blobs": 2, "bytes": 10, "puts": 2})
        self.assertEqual(self.call("GET", f"/v1/blobs/{digest_of(b'AAAA')}")[0], 200)

    def test_repeat_put_succeeds_at_full_quota(self) -> None:
        self.assertEqual(self.put(b"AAAA")[0], 201)
        self.assertEqual(self.put(b"BBBBBB")[0], 201)
        status, _, _ = self.put(b"AAAA")
        self.assertEqual(status, 201)
        self.assertEqual(self.stats(), {"blobs": 2, "bytes": 10, "puts": 3})

    def test_existing_400_and_409_precede_quota(self) -> None:
        self.assertEqual(self.put(b"AAAAAAAAAA")[0], 201)  # cap fully used
        self.assertEqual(self.put(b"")[0], 400)  # empty body
        status, _, _ = self.put(b"B", {"X-Blob-Digest": "zz"})
        self.assertEqual(status, 400)  # bad digest header shape
        status, _, _ = self.put(b"B", {"X-Blob-Digest": digest_of(b"other")})
        self.assertEqual(status, 409)  # declared digest mismatch
        self.assertEqual(self.stats(), {"blobs": 1, "bytes": 10, "puts": 1})

    def test_release_then_gc_frees_quota(self) -> None:
        self.assertEqual(self.put(b"AAAA")[0], 201)
        self.assertEqual(self.put(b"BBBBBB")[0], 201)
        self.assertEqual(self.call("DELETE", f"/v1/blobs/{digest_of(b'BBBBBB')}/refs")[0], 200)
        # refs 0 but not yet collected: still occupies quota
        self.assertEqual(self.put(b"C")[0], 413)
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["deleted"], [digest_of(b"BBBBBB")])
        self.assertEqual(self.put(b"C")[0], 201)
        self.assertEqual(self.stats(), {"blobs": 2, "bytes": 5, "puts": 2})

    def test_complete_over_quota_leaves_session_uncommitted(self) -> None:
        self.assertEqual(self.put(b"AAAA")[0], 201)  # 4 of 10 used... use most of it
        self.assertEqual(self.put(b"BBBBB")[0], 201)  # 9 of 10 used
        status, body, _ = self.call("POST", "/v1/uploads",
                                    json.dumps({"size": 2, "media_type": "text/plain"}).encode())
        self.assertEqual(status, 201)
        upload_id = json.loads(body)["upload_id"]
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", b"CC",
                                   {"X-Upload-Offset": "0"})[0], 200)
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body), QUOTA_BODY)
        # session stays uncommitted and resumable; nothing stored
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["status"], "uncommitted")
        self.assertEqual(self.stats(), {"blobs": 2, "bytes": 9, "puts": 2})
        # freeing quota lets the same session complete
        self.call("DELETE", f"/v1/blobs/{digest_of(b'BBBBB')}/refs")
        self.call("POST", "/v1/gc")
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(body)["digest"], digest_of(b"CC"))

    def test_complete_existing_errors_precede_quota(self) -> None:
        self.assertEqual(self.put(b"AAAAAAAAAA")[0], 201)  # cap fully used
        # unknown session: 404 before quota
        self.assertEqual(self.call("POST", f"/v1/uploads/{'0' * 32}/complete")[0], 404)
        # incomplete session: 409 before quota
        _, body, _ = self.call("POST", "/v1/uploads",
                               json.dumps({"size": 4, "media_type": "text/plain"}).encode())
        upload_id = json.loads(body)["upload_id"]
        self.assertEqual(self.call("POST", f"/v1/uploads/{upload_id}/complete")[0], 409)
        # declared digest mismatch: 409 before quota
        _, body, _ = self.call("POST", "/v1/uploads",
                               json.dumps({"size": 1, "media_type": "text/plain",
                                           "digest": digest_of(b"other")}).encode())
        upload_id = json.loads(body)["upload_id"]
        self.call("PUT", f"/v1/uploads/{upload_id}", b"Z", {"X-Upload-Offset": "0"})
        self.assertEqual(self.call("POST", f"/v1/uploads/{upload_id}/complete")[0], 409)

    def test_complete_of_already_stored_content_succeeds_at_full_quota(self) -> None:
        self.assertEqual(self.put(b"AAAAAAAAAA")[0], 201)  # cap fully used
        _, body, _ = self.call("POST", "/v1/uploads",
                               json.dumps({"size": 10, "media_type": "text/plain"}).encode())
        upload_id = json.loads(body)["upload_id"]
        self.call("PUT", f"/v1/uploads/{upload_id}", b"AAAAAAAAAA", {"X-Upload-Offset": "0"})
        status, _, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)  # dedup: no new bytes, only refs +1
        self.assertEqual(self.stats(), {"blobs": 1, "bytes": 10, "puts": 2})

    def test_concurrent_puts_never_exceed_cap(self) -> None:
        self.assertEqual(self.put(b"AAAA")[0], 201)  # 4 of 10 used, room for 6
        results: list[int] = []
        lock = threading.Lock()

        def put(marker: bytes) -> None:
            status, _, _ = self.put(marker * 6)
            with lock:
                results.append(status)

        threads = [threading.Thread(target=put, args=(m,)) for m in (b"p", b"q", b"r", b"s")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results.count(201), 1)
        self.assertEqual(results.count(413), 3)
        self.assertEqual(self.stats()["bytes"], 10)


class QuotaMirrorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.remote = serve(port=0)
        cls.remote_port = cls.remote.server_address[1]
        threading.Thread(target=cls.remote.serve_forever, daemon=True).start()
        cls.local = serve(port=0, max_store_bytes=10)
        cls.local_port = cls.local.server_address[1]
        threading.Thread(target=cls.local.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.remote.shutdown()
        cls.local.shutdown()

    def setUp(self) -> None:
        for server in (self.remote, self.local):
            with server.store._lock:
                server.store._blobs.clear()
                server.store._meta.clear()
                server.store._refs.clear()

    def call(self, port: int, method: str, path: str, data: bytes | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def pull(self, digests: list[str]):
        body = json.dumps({"base_url": f"http://127.0.0.1:{self.remote_port}",
                           "digests": digests}).encode()
        return self.call(self.local_port, "POST", "/v1/mirror/pull", body)

    def test_pull_over_quota_stores_nothing(self) -> None:
        for payload in (b"xxxx", b"yyyy"):
            self.call(self.remote_port, "PUT", "/v1/blobs", payload)
        self.call(self.local_port, "PUT", "/v1/blobs", b"AAAA")  # 4 of 10 used
        digests = sorted([digest_of(b"xxxx"), digest_of(b"yyyy")])
        status, body = self.pull(digests)
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body), QUOTA_BODY)
        # all-or-nothing: no part of the batch landed, local state untouched
        _, listing = self.call(self.local_port, "GET", "/v1/blobs")
        self.assertEqual(json.loads(listing)["stats"], {"blobs": 1, "bytes": 4, "puts": 1})
        for digest in digests:
            self.assertEqual(self.call(self.local_port, "GET", f"/v1/blobs/{digest}")[0], 404)

    def test_pull_that_fits_commits_whole_batch(self) -> None:
        for payload in (b"xx", b"yy"):
            self.call(self.remote_port, "PUT", "/v1/blobs", payload)
        self.call(self.local_port, "PUT", "/v1/blobs", b"AAAAAA")  # 6 of 10 used
        digests = sorted([digest_of(b"xx"), digest_of(b"yy")])
        status, body = self.pull(digests)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["synced"], digests)
        _, listing = self.call(self.local_port, "GET", "/v1/blobs")
        self.assertEqual(json.loads(listing)["stats"], {"blobs": 3, "bytes": 10, "puts": 3})

    def test_pull_of_already_stored_digests_needs_no_quota(self) -> None:
        self.call(self.remote_port, "PUT", "/v1/blobs", b"same")
        self.call(self.local_port, "PUT", "/v1/blobs", b"same")
        self.call(self.local_port, "PUT", "/v1/blobs", b"AAAAAA")  # cap of 10 fully used
        status, body = self.pull([digest_of(b"same")])
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["existing"], [digest_of(b"same")])


class QuotaUploadStateTests(unittest.TestCase):
    """With --upload-state, a 413 complete must leave the session uncommitted and unpersisted."""

    def setUp(self) -> None:
        import tempfile

        self.dir = tempfile.TemporaryDirectory()
        self.state_path = f"{self.dir.name}/uploads.json"
        self.server = serve(port=0, upload_state=self.state_path, max_store_bytes=4)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.dir.cleanup()

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def test_quota_complete_is_not_persisted(self) -> None:
        self.call("PUT", "/v1/blobs", b"AAAA")  # cap of 4 fully used
        _, body = self.call("POST", "/v1/uploads",
                            json.dumps({"size": 1, "media_type": "text/plain"}).encode())
        upload_id = json.loads(body)["upload_id"]
        self.call("PUT", f"/v1/uploads/{upload_id}", b"C", {"X-Upload-Offset": "0"})
        status, body = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body), QUOTA_BODY)
        _, body = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["status"], "uncommitted")
        # restart against the same state file: session comes back uncommitted
        self.server.shutdown()
        self.server.server_close()
        self.server = serve(port=0, upload_state=self.state_path, max_store_bytes=4)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        status, body = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "uncommitted")


if __name__ == "__main__":
    unittest.main()
