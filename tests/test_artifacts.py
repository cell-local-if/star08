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


class ListQueryTests(unittest.TestCase):
    """GET /v1/blobs metadata filtering and deterministic pagination."""

    @classmethod
    def setUpClass(cls) -> None:
        from artifacts import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.store = cls.server.store
        # distinct sizes and refs so every filter has something to select
        cls.payloads = [b"aa", b"bbbb", b"cccccc", b"dddddddd", b"eeeeeeeeee"]
        cls.size_of = {}
        for payload in cls.payloads:
            cls.size_of[cls.store.put(payload).digest] = len(payload)
        cls.store.put(b"aa")  # second ref for the first blob only
        cls.refs_of = {digest: 1 for digest in cls.size_of}
        cls.refs_of[digest_of(b"aa")] = 2
        cls.digests = sorted(cls.size_of)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, path: str):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", method="GET")
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())

    def test_no_params_keeps_baseline_shape_without_cursor(self) -> None:
        status, body = self.call("/v1/blobs")
        self.assertEqual(status, 200)
        self.assertNotIn("next_cursor", body)
        self.assertEqual([b["digest"] for b in body["blobs"]], self.digests)
        self.assertEqual(body["stats"], {"blobs": 5, "bytes": 30, "puts": 6})

    def test_filters_combine_and_stats_stay_global(self) -> None:
        status, body = self.call("/v1/blobs?min_size=4&max_size=8")
        self.assertEqual(status, 200)
        expected = [d for d in self.digests if 4 <= self.size_of[d] <= 8]
        self.assertEqual([b["digest"] for b in body["blobs"]], expected)
        self.assertEqual(len(expected), 3)
        self.assertEqual(body["stats"], {"blobs": 5, "bytes": 30, "puts": 6})
        status, body = self.call("/v1/blobs?min_refs=2")
        self.assertEqual([b["digest"] for b in body["blobs"]], [digest_of(b"aa")])
        self.assertEqual(body["blobs"][0]["refs"], 2)
        prefix = self.digests[2][:8]
        status, body = self.call(f"/v1/blobs?digest_prefix={prefix}&min_size=1")
        self.assertEqual([b["digest"] for b in body["blobs"]], [self.digests[2]])

    def test_limit_and_after_page_through_every_blob_once(self) -> None:
        seen, cursor = [], None
        for _ in range(6):
            query = f"limit=2{'&after=' + cursor if cursor else ''}"
            status, body = self.call(f"/v1/blobs?{query}")
            self.assertEqual(status, 200)
            digests = [b["digest"] for b in body["blobs"]]
            self.assertLessEqual(len(digests), 2)
            seen.extend(digests)
            cursor = body["next_cursor"]
            if cursor is None:
                break
            self.assertEqual(cursor, digests[-1])
        self.assertEqual(seen, self.digests)
        self.assertIsNone(cursor)

    def test_cursor_is_strictly_after_and_filter_scoped(self) -> None:
        status, body = self.call(f"/v1/blobs?limit=1&after={self.digests[0]}")
        self.assertEqual([b["digest"] for b in body["blobs"]], [self.digests[1]])
        big = [d for d in self.digests if self.size_of[d] >= 6 and d > self.digests[1]]
        status, body = self.call(f"/v1/blobs?min_size=6&limit=1&after={self.digests[1]}")
        self.assertEqual([b["digest"] for b in body["blobs"]], big[:1])
        # after beyond the largest digest yields an empty page and null cursor
        status, body = self.call(f"/v1/blobs?limit=1&after={'f' * 64}")
        self.assertEqual((body["blobs"], body["next_cursor"]), ([], None))

    def test_invalid_queries_are_400_with_error_shape(self) -> None:
        bad = [
            "/v1/blobs?bogus=1",                      # unknown parameter
            "/v1/blobs?limit=1&limit=2",              # duplicate parameter
            "/v1/blobs?limit=",                       # missing value
            "/v1/blobs?limit",                        # missing '='
            "/v1/blobs?limit=0",
            "/v1/blobs?limit=101",
            "/v1/blobs?limit=-1",
            "/v1/blobs?limit=1.5",
            "/v1/blobs?digest_prefix=",               # empty prefix
            "/v1/blobs?digest_prefix=AB",             # uppercase hex
            "/v1/blobs?digest_prefix=" + "a" * 65,    # too long
            "/v1/blobs?min_size=-1",
            "/v1/blobs?min_size=1048577",             # above 1 MiB cap
            "/v1/blobs?max_size=1048577",
            "/v1/blobs?min_size=9&max_size=3",        # inverted bounds
            "/v1/blobs?min_refs=x",
            "/v1/blobs?after=" + "a" * 64,            # after without limit
            "/v1/blobs?limit=1&after=zz",             # malformed cursor
        ]
        for path in bad:
            with self.subTest(path=path):
                status, body = self.call(path)
                self.assertEqual(status, 400)
                self.assertEqual(body["error"]["code"], "invalid_request")
                self.assertIn("message", body["error"])

    def test_valid_edge_values_are_accepted(self) -> None:
        status, _ = self.call("/v1/blobs?min_size=0&max_size=1048576&min_refs=0&limit=100")
        self.assertEqual(status, 200)
        status, body = self.call(f"/v1/blobs?digest_prefix={self.digests[0]}")  # full 64-char prefix
        self.assertEqual([b["digest"] for b in body["blobs"]], [self.digests[0]])


if __name__ == "__main__":
    unittest.main()
