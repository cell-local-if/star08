"""Baseline tests for the content-addressed artifact store."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import BlobNotFound, DigestConflict, InvalidRequest, Store, digest_of


class StoreUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def test_put_get_roundtrip_and_digest_matches_sha256(self) -> None:
        import hashlib

        blob = self.store.put(b"hello world", media_type="text/plain")
        self.assertEqual(blob.digest, hashlib.sha256(b"hello world").hexdigest())
        self.assertEqual(self.store.get(blob.digest), b"hello world")
        self.assertEqual(blob.media_type, "text/plain")

    def test_identical_bytes_are_stored_once(self) -> None:
        first = self.store.put(b"same")
        second = self.store.put(b"same")
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 4, "puts": 2})
        self.assertEqual(self.store.listing()[0]["refs"], 2)

    def test_declared_digest_mismatch_is_a_conflict_not_a_write(self) -> None:
        with self.assertRaises(DigestConflict):
            self.store.put(b"payload", declared_digest=digest_of(b"other"))
        self.assertEqual(self.store.stats()["blobs"], 0)

    def test_invalid_inputs_are_rejected(self) -> None:
        for call in [lambda: self.store.put(b""), lambda: self.store.put(b"x", declared_digest="ZZ"),
                     lambda: self.store.put("not-bytes"), lambda: self.store.get("short"),
                     lambda: self.store.put(b"x" * (1_048_576 + 1))]:
            with self.assertRaises(InvalidRequest):
                call()

    def test_unknown_digest_is_not_found(self) -> None:
        with self.assertRaises(BlobNotFound):
            self.store.get("0" * 64)


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

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def test_put_head_get_list(self) -> None:
        status, body, headers = self.call("PUT", "/v1/blobs", b"artifact-bytes",
                                          {"Content-Type": "application/octet-stream"})
        self.assertEqual(status, 201)
        digest = headers["X-Blob-Digest"]
        self.assertEqual(json.loads(body)["digest"], digest)
        status, raw, headers = self.call("HEAD", f"/v1/blobs/{digest}")
        self.assertEqual((status, headers["X-Blob-Digest"]), (200, digest))
        self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[1], b"artifact-bytes")
        self.assertEqual(json.loads(self.call("GET", "/v1/blobs")[1])["stats"]["blobs"], 1)

    def test_digest_mismatch_and_unknown_paths(self) -> None:
        status, body, _ = self.call("PUT", "/v1/blobs", b"payload", {"X-Blob-Digest": "a" * 64})
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
        self.assertEqual(self.call("GET", f"/v1/blobs/{'0' * 64}")[0], 404)
        self.assertEqual(self.call("PUT", "/v1/nope", b"x")[0], 404)


if __name__ == "__main__":
    unittest.main()
