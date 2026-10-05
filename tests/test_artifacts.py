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


class BlobQueryTests(unittest.TestCase):
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
        self.server.store._blobs.clear()
        self.server.store._meta.clear()
        self.server.store._refs.clear()
        # distinct sizes so size filters are observable; "aa" is PUT twice for refs
        self.digests = {}
        for name, payload, media in [("aa", b"aa", "text/plain"), ("bb", b"bbbb", "text/plain"),
                                     ("cc", b"cccccc", "application/json"), ("dd", b"dddddddd", None)]:
            _, _, headers = self.call("PUT", "/v1/blobs", payload,
                                      {"Content-Type": media} if media else {})
            self.digests[name] = headers["X-Blob-Digest"]
        self.call("PUT", "/v1/blobs", b"aa", {"Content-Type": "text/plain"})  # refs(aa) = 2
        self.ordered = sorted(self.digests.values())

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def get_json(self, path: str):
        status, body, _ = self.call("GET", path)
        return status, json.loads(body)

    def test_no_params_keeps_baseline_shape_without_cursor(self) -> None:
        status, body = self.get_json("/v1/blobs")
        self.assertEqual(status, 200)
        self.assertNotIn("next_cursor", body)
        self.assertEqual([b["digest"] for b in body["blobs"]], self.ordered)
        self.assertEqual(body["stats"], {"blobs": 4, "bytes": 20, "puts": 5})

    def test_limit_paginates_in_digest_order_with_cursor(self) -> None:
        status, page1 = self.get_json("/v1/blobs?limit=3")
        self.assertEqual(status, 200)
        self.assertEqual([b["digest"] for b in page1["blobs"]], self.ordered[:3])
        self.assertEqual(page1["next_cursor"], self.ordered[2])
        status, page2 = self.get_json(f"/v1/blobs?limit=3&after={page1['next_cursor']}")
        self.assertEqual([b["digest"] for b in page2["blobs"]], self.ordered[3:])
        self.assertIsNone(page2["next_cursor"])
        self.assertEqual(page2["stats"], {"blobs": 4, "bytes": 20, "puts": 5})

    def test_after_is_strict_and_cursor_is_not_a_position(self) -> None:
        status, page = self.get_json(f"/v1/blobs?limit=100&after={self.ordered[1]}")
        self.assertEqual([b["digest"] for b in page["blobs"]], self.ordered[2:])

    def test_filters_combine(self) -> None:
        prefix = self.digests["cc"][:8]
        status, body = self.get_json(
            f"/v1/blobs?digest_prefix={prefix}&min_size=6&max_size=6&min_refs=1&limit=10")
        self.assertEqual(status, 200)
        self.assertEqual([b["digest"] for b in body["blobs"]], [self.digests["cc"]])
        self.assertIsNone(body["next_cursor"])

    def test_min_refs_selects_repeated_put(self) -> None:
        status, body = self.get_json("/v1/blobs?min_refs=2")
        self.assertEqual([b["digest"] for b in body["blobs"]], [self.digests["aa"]])
        self.assertEqual(body["blobs"][0]["refs"], 2)

    def test_stats_ignore_filters_and_pagination(self) -> None:
        _, body = self.get_json("/v1/blobs?min_size=999999&limit=1")
        self.assertEqual(body["stats"], {"blobs": 4, "bytes": 20, "puts": 5})
        self.assertEqual(body["blobs"], [])
        self.assertIsNone(body["next_cursor"])

    def test_invalid_queries_are_400_invalid_request(self) -> None:
        bad = [
            "/v1/blobs?nope=1",                                  # unknown parameter
            "/v1/blobs?limit=2&limit=3",                         # duplicate parameter
            "/v1/blobs?digest_prefix=",                          # empty prefix
            "/v1/blobs?digest_prefix=ZZ",                        # non-hex prefix
            "/v1/blobs?digest_prefix=" + "a" * 65,               # prefix too long
            "/v1/blobs?min_size=-1",                             # negative
            "/v1/blobs?min_size=1.5",                            # not an integer
            "/v1/blobs?min_size=",                               # missing value
            "/v1/blobs?min_size=1048577",                        # above 1 MiB cap
            "/v1/blobs?max_size=1048577",
            "/v1/blobs?min_size=10&max_size=5",                  # inverted range
            "/v1/blobs?min_refs=abc",
            "/v1/blobs?limit=0",
            "/v1/blobs?limit=101",
            "/v1/blobs?limit=two",
            "/v1/blobs?after=" + "a" * 64,                       # after without limit
            "/v1/blobs?limit=2&after=abc",                       # malformed cursor
            "/v1/blobs?limit=2&after=" + "A" * 64,               # uppercase cursor
        ]
        for path in bad:
            status, body = self.get_json(path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request", path)

    def test_concurrent_puts_do_not_tear_a_page(self) -> None:
        stop = threading.Event()

        def writer() -> None:
            counter = 0
            while not stop.is_set():
                counter += 1
                self.call("PUT", "/v1/blobs", f"concurrent-{counter}".encode())

        threads = [threading.Thread(target=writer) for _ in range(4)]
        for thread in threads:
            thread.start()
        try:
            for _ in range(20):
                status, body = self.get_json("/v1/blobs?limit=100")
                self.assertEqual(status, 200)
                digests = [b["digest"] for b in body["blobs"]]
                self.assertEqual(digests, sorted(digests))
                self.assertEqual(len(digests), len(set(digests)))
                for blob in body["blobs"]:
                    self.assertEqual(set(blob), {"digest", "size", "media_type", "refs"})
                self.assertGreaterEqual(body["stats"]["blobs"], len(digests))
        finally:
            stop.set()
            for thread in threads:
                thread.join()


if __name__ == "__main__":
    unittest.main()
