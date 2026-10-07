"""Tests for the optional store byte quota (--max-store-bytes / serve(max_store_bytes=...))."""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import DigestConflict, InvalidRequest, QuotaExceeded, Store, digest_of


class StoreQuotaUnitTests(unittest.TestCase):
    def test_no_quota_by_default(self) -> None:
        store = Store()
        store.put(b"x" * 1000)
        store.put(b"y" * 1000)
        self.assertEqual(store.stats()["bytes"], 2000)

    def test_invalid_quota_values_rejected(self) -> None:
        for bad in (0, -1, True, 1.5, "100"):
            with self.assertRaises(ValueError):
                Store(bad)

    def test_put_within_and_beyond_quota(self) -> None:
        store = Store(max_store_bytes=10)
        store.put(b"aaaaaa")
        store.put(b"cccc")  # exactly at the cap
        with self.assertRaises(QuotaExceeded) as caught:
            store.put(b"d")
        self.assertEqual(str(caught.exception), "store byte quota exceeded")
        self.assertEqual(caught.exception.code, "quota_exceeded")
        self.assertEqual(caught.exception.status, 413)
        self.assertEqual(store.stats(), {"blobs": 2, "bytes": 10, "puts": 2})

    def test_repeat_put_needs_no_new_bytes(self) -> None:
        store = Store(max_store_bytes=4)
        store.put(b"aaaa")
        blob = store.put(b"aaaa")  # refs +1 at full quota: still fine
        self.assertEqual(store.listing()[0]["refs"], 2)
        self.assertEqual(blob.digest, digest_of(b"aaaa"))

    def test_zero_ref_blob_still_counts_until_gc(self) -> None:
        store = Store(max_store_bytes=8)
        blob = store.put(b"aaaa")
        store.release(blob.digest)
        with self.assertRaises(QuotaExceeded):
            store.put(b"bbbbb")  # 4 (refs 0) + 5 > 8
        store.gc()
        store.put(b"bbbbb")
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 5, "puts": 1})

    def test_put_validation_precedes_quota(self) -> None:
        store = Store(max_store_bytes=1)
        store.put(b"a")
        with self.assertRaises(InvalidRequest):
            store.put(b"")
        with self.assertRaises(InvalidRequest):
            store.put(b"x" * (1024 * 1024 + 1))
        with self.assertRaises(DigestConflict):
            store.put(b"bb", declared_digest=digest_of(b"cc"))
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 1, "puts": 1})

    def test_put_completed_quota(self) -> None:
        store = Store(max_store_bytes=3)
        store.put(b"aaa")
        with self.assertRaises(QuotaExceeded):
            store.put_completed(b"bb", None, "text/plain")
        # digest conflict is still checked before the quota
        with self.assertRaises(DigestConflict):
            store.put_completed(b"bb", digest_of(b"cc"), "text/plain")
        self.assertEqual(store.stats(), {"blobs": 1, "bytes": 3, "puts": 1})

    def test_absorb_is_all_or_nothing_under_quota(self) -> None:
        store = Store(max_store_bytes=5)
        fetched = [("a" * 64, b"aaa", "text/plain"), ("b" * 64, b"bbb", "text/plain")]
        with self.assertRaises(QuotaExceeded):
            store.absorb(fetched, ["a" * 64, "b" * 64])
        self.assertEqual(store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        result = store.absorb([fetched[0]], ["a" * 64])
        self.assertEqual(result["synced"], ["a" * 64])
        # the already-stored digest needs no new bytes, but the second one still overflows
        with self.assertRaises(QuotaExceeded):
            store.absorb(fetched, ["a" * 64, "b" * 64])
        result = store.absorb([fetched[0]], ["a" * 64])
        self.assertEqual(result["existing"], ["a" * 64])
        self.assertEqual(store.listing()[0]["refs"], 1)


class QuotaHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from artifacts import serve

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

    def put(self, payload: bytes, expected: int = 201) -> str:
        status, _, headers = self.call("PUT", "/v1/blobs", payload)
        self.assertEqual(status, expected)
        return headers.get("X-Blob-Digest")

    def stats(self) -> dict:
        _, body, _ = self.call("GET", "/v1/blobs")
        return json.loads(body)["stats"]

    def test_put_413_body_and_no_visible_change(self) -> None:
        self.put(b"aaaaaa")
        status, body, _ = self.call("PUT", "/v1/blobs", b"bbbbb")
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body), {"error": {"code": "quota_exceeded",
                                                      "message": "store byte quota exceeded"}})
        self.assertEqual(self.stats(), {"blobs": 1, "bytes": 6, "puts": 1})
        self.put(b"cccc")  # exactly to the cap
        self.assertEqual(self.stats(), {"blobs": 2, "bytes": 10, "puts": 2})

    def test_put_error_ordering(self) -> None:
        self.put(b"aaaaaaaaaa")
        status, body, _ = self.call("PUT", "/v1/blobs", b"")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.call("PUT", "/v1/blobs", b"zz",
                                    {"X-Blob-Digest": digest_of(b"yy")})
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
        # repeat of stored content still succeeds at full quota
        self.put(b"aaaaaaaaaa")
        self.assertEqual(self.stats(), {"blobs": 1, "bytes": 10, "puts": 2})

    def test_release_gc_frees_quota(self) -> None:
        digest = self.put(b"aaaaaa")
        self.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.put(b"bbbbb", expected=413)  # refs-0 blob still occupies the quota
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["deleted"], [digest])
        self.put(b"bbbbb")
        self.assertEqual(self.stats(), {"blobs": 1, "bytes": 5, "puts": 1})

    def test_complete_quota_and_recovery(self) -> None:
        self.put(b"aaaaaaaa")
        status, body, _ = self.call("POST", "/v1/uploads",
                                    json.dumps({"size": 4, "media_type": "text/plain"}).encode())
        upload_id = json.loads(body)["upload_id"]
        status, _, _ = self.call("PUT", f"/v1/uploads/{upload_id}", b"zzzz",
                                 {"X-Upload-Offset": "0"})
        self.assertEqual(status, 200)
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (413, "quota_exceeded"))
        # session stays resumable; nothing was stored
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        view = json.loads(body)
        self.assertEqual((view["status"], view["received"]), ("uncommitted", 4))
        self.assertEqual(self.stats(), {"blobs": 1, "bytes": 8, "puts": 1})
        # freeing space lets the same session complete
        _, listing, _ = self.call("GET", "/v1/blobs")
        self.call("DELETE", f"/v1/blobs/{json.loads(listing)['blobs'][0]['digest']}/refs")
        self.call("POST", "/v1/gc")
        status, _, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)

    def test_complete_error_ordering(self) -> None:
        self.put(b"aaaaaaaaaa")
        # unknown session: 404 before quota
        status, body, _ = self.call("POST", f"/v1/uploads/{'0' * 32}/complete")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (404, "not_found"))
        # incomplete session: 409 before quota
        _, body, _ = self.call("POST", "/v1/uploads",
                               json.dumps({"size": 5, "media_type": "x"}).encode())
        upload_id = json.loads(body)["upload_id"]
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))


class QuotaMirrorHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from artifacts import serve

        cls.remote = serve(port=0)
        cls.local = serve(port=0, max_store_bytes=5)
        cls.remote_port = cls.remote.server_address[1]
        cls.local_port = cls.local.server_address[1]
        threading.Thread(target=cls.remote.serve_forever, daemon=True).start()
        threading.Thread(target=cls.local.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.remote.shutdown()
        cls.local.shutdown()

    def setUp(self) -> None:
        with self.local.store._lock:
            self.local.store._blobs.clear()
            self.local.store._meta.clear()
            self.local.store._refs.clear()

    def call(self, port: int, method: str, path: str, data: bytes | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def remote_put(self, payload: bytes) -> str:
        status, body = self.call(self.remote_port, "PUT", "/v1/blobs", payload)
        self.assertEqual(status, 201)
        return json.loads(body)["digest"]

    def pull(self, digests: list[str]):
        return self.call(self.local_port, "POST", "/v1/mirror/pull",
                         json.dumps({"base_url": f"http://127.0.0.1:{self.remote_port}",
                                     "digests": digests}).encode())

    def local_stats(self) -> dict:
        _, body = self.call(self.local_port, "GET", "/v1/blobs")
        return json.loads(body)["stats"]

    def test_pull_all_or_nothing_under_quota(self) -> None:
        big = self.remote_put(b"12345")
        small = self.remote_put(b"678")
        status, body = self.pull([big, small])  # 5 + 3 > 5
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body), {"error": {"code": "quota_exceeded",
                                                      "message": "store byte quota exceeded"}})
        self.assertEqual(self.local_stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        status, body = self.pull([big])
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["synced"], [big])
        # the existing blob needs no new bytes, but the 3-byte one still overflows
        status, _ = self.pull([big, small])
        self.assertEqual(status, 413)
        self.assertEqual(self.local_stats(), {"blobs": 1, "bytes": 5, "puts": 1})


class QuotaUploadStateTests(unittest.TestCase):
    """A quota refusal on complete must roll back the persisted session too."""

    def setUp(self) -> None:
        from artifacts import serve

        handle, self.state_path = tempfile.mkstemp()
        os.close(handle)
        os.unlink(self.state_path)
        self.server = serve(port=0, upload_state=self.state_path, max_store_bytes=4)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        if os.path.exists(self.state_path):
            os.unlink(self.state_path)

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def test_quota_refusal_rolls_back_persisted_commit(self) -> None:
        status, _ = self.call("PUT", "/v1/blobs", b"aaaa")
        self.assertEqual(status, 201)
        _, body = self.call("POST", "/v1/uploads",
                            json.dumps({"size": 2, "media_type": "x"}).encode())
        upload_id = json.loads(body)["upload_id"]
        self.call("PUT", f"/v1/uploads/{upload_id}", b"zz", {"X-Upload-Offset": "0"})
        status, body = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 413)
        # in-memory view and the persisted file agree: uncommitted, bytes kept
        _, body = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["status"], "uncommitted")
        with open(self.state_path, "rb") as handle:
            doc = json.loads(handle.read())
        (entry,) = doc["sessions"]
        self.assertFalse(entry["committed"])
        self.assertIsNone(entry["final_digest"])
        self.assertEqual(entry["received"], 2)
        self.assertEqual(len(entry["chunks"]), 1)


if __name__ == "__main__":
    unittest.main()
