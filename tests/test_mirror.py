"""Tests for POST /v1/mirror/pull: request validation, remote presence/GET
verification, atomic commit semantics, refs handling and error mapping."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from artifacts import InvalidRequest, Store, digest_of, parse_mirror_pull

D = lambda text: digest_of(text.encode())  # noqa: E731


class ParseMirrorPullTests(unittest.TestCase):
    def valid(self, n: int = 2) -> bytes:
        return json.dumps({"base_url": "http://127.0.0.1:9000",
                           "digests": [f"{i:064x}" for i in range(n)]}).encode()

    def test_accepts_site_root_with_or_without_slash_and_https(self) -> None:
        for url in ("http://127.0.0.1:9000", "http://127.0.0.1:9000/",
                    "https://example.com", "http://localhost"):
            base_url, digests = parse_mirror_pull(
                json.dumps({"base_url": url, "digests": ["a" * 64]}).encode())
            self.assertEqual(base_url, url.rstrip("/"))
            self.assertEqual(digests, ["a" * 64])

    def test_rejects_bad_shapes(self) -> None:
        bad = [
            b"not json",
            b"[1, 2]",
            b"{}",
            b'{"base_url": "http://x"}',                                  # digests missing
            b'{"digests": ["' + b"a" * 64 + b'"]}',                       # base_url missing
            b'{"base_url": 5, "digests": ["' + b"a" * 64 + b'"]}',        # base_url not a string
            b'{"base_url": "http://x", "digests": ["' + b"a" * 64 + b'"], "z": 1}',
            self.valid(0).replace(b'"digests":[', b'"digests":['),        # placeholder, replaced below
        ]
        bad[-1] = json.dumps({"base_url": "http://x", "digests": []}).encode()
        bad.append(json.dumps({"base_url": "http://x",
                               "digests": [f"{i:064x}" for i in range(101)]}).encode())
        bad.append(json.dumps({"base_url": "http://x",
                               "digests": ["a" * 64, "a" * 64]}).encode())
        bad.append(json.dumps({"base_url": "http://x", "digests": ["A" * 64]}).encode())
        bad.append(json.dumps({"base_url": "http://x", "digests": ["a" * 63]}).encode())
        bad.append(json.dumps({"base_url": "http://x", "digests": "not-an-array"}).encode())
        for body in bad:
            with self.subTest(body=body[:60]):
                with self.assertRaises(InvalidRequest):
                    parse_mirror_pull(body)

    def test_rejects_bad_base_urls(self) -> None:
        bad_urls = [
            "ftp://example.com",                      # wrong scheme
            "example.com",                            # no scheme
            "http://",                                # no host
            "http://user@example.com",                # userinfo
            "http://user:pass@example.com",           # userinfo with password
            "http://example.com/path",                # not the site root
            "http://example.com/api/",                # not the site root
            "http://example.com/?x=1",                # query
            "http://example.com/#frag",               # fragment
            "http://example.com:notaport",            # bad port
        ]
        for url in bad_urls:
            body = json.dumps({"base_url": url, "digests": ["a" * 64]}).encode()
            with self.subTest(url=url):
                with self.assertRaises(InvalidRequest):
                    parse_mirror_pull(body)


class Remote:
    """Programmable in-process mirror remote (presence + blob GET only)."""

    def __init__(self) -> None:
        self.blobs: dict[str, tuple[bytes, str | None]] = {}
        self.presence_override = None  # callable(handler, body) -> None
        self.blob_override = None      # callable(handler, digest) -> None
        self.log: list[tuple[str, str, bytes | None]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:
                return

            def reply(self, status: int, body: bytes = b"", headers: dict | None = None) -> None:
                self.send_response(status)
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                outer.log.append(("POST", self.path, body))
                if self.path == "/v1/blobs/presence":
                    if outer.presence_override is not None:
                        return outer.presence_override(self, body)
                    digests = json.loads(body.decode("utf-8"))["digests"]
                    present = sorted(d for d in digests if d in outer.blobs)
                    missing = sorted(d for d in digests if d not in outer.blobs)
                    self.reply(200, json.dumps({"present": present, "missing": missing}).encode(),
                               {"Content-Type": "application/json"})
                else:
                    self.reply(404, b'{"error":{"code":"not_found"}}')

            def do_GET(self) -> None:  # noqa: N802
                outer.log.append(("GET", self.path, None))
                digest = self.path.rsplit("/", 1)[-1]
                if self.path == f"/v1/blobs/{digest}":
                    if outer.blob_override is not None:
                        return outer.blob_override(self, digest)
                    if digest in outer.blobs:
                        data, media_type = outer.blobs[digest]
                        headers = {"X-Blob-Digest": digest}
                        if media_type is not None:
                            headers["Content-Type"] = media_type
                        self.reply(200, data, headers)
                    else:
                        self.reply(404, b'{"error":{"code":"not_found"}}')
                else:
                    self.reply(404, b'{"error":{"code":"not_found"}}')

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base_url = f"http://127.0.0.1:{self.port}"

    def add(self, payload: bytes, media_type: str | None = None) -> str:
        digest = digest_of(payload)
        self.blobs[digest] = (payload, media_type)
        return digest

    def gets(self) -> list[str]:
        return [path.rsplit("/", 1)[-1] for method, path, _ in self.log if method == "GET"]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class MirrorPullHttpTests(unittest.TestCase):
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
        store._blobs.clear()
        store._meta.clear()
        store._refs.clear()
        self.remote = Remote()

    def tearDown(self) -> None:
        self.remote.close()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def pull(self, base_url: str, digests: list[str]):
        body = json.dumps({"base_url": base_url, "digests": digests}).encode()
        return self.call("POST", "/v1/mirror/pull", body,
                         {"Content-Type": "application/json"})

    def put(self, payload: bytes, media_type: str | None = None) -> str:
        headers = {"Content-Type": media_type} if media_type else {}
        status, _, headers_out = self.call("PUT", "/v1/blobs", payload, headers)
        self.assertEqual(status, 201)
        return headers_out["X-Blob-Digest"]

    def listing(self) -> dict[str, dict]:
        status, raw, _ = self.call("GET", "/v1/blobs")
        self.assertEqual(status, 200)
        return {entry["digest"]: entry for entry in json.loads(raw)["blobs"]}

    def test_syncs_remote_blobs_and_partitions_the_request(self) -> None:
        remote_a = self.remote.add(b"remote-a", "text/plain")
        remote_b = self.remote.add(b"remote-b")  # no Content-Type on the wire
        local = self.put(b"local-only", "text/plain")
        nowhere = "e" * 64
        status, raw, _ = self.pull(self.remote.base_url,
                                   [nowhere, remote_b, local, remote_a])
        self.assertEqual(status, 200)
        body = json.loads(raw)
        self.assertEqual(set(body), {"synced", "existing", "missing"})
        self.assertEqual(body["synced"], sorted([remote_a, remote_b]))
        self.assertEqual(body["existing"], [local])
        self.assertEqual(body["missing"], [nowhere])
        blobs = self.listing()
        self.assertEqual(blobs[remote_a]["refs"], 1)
        self.assertEqual(blobs[remote_a]["media_type"], "text/plain")
        self.assertEqual(blobs[remote_a]["size"], 8)
        self.assertEqual(blobs[remote_b]["media_type"], "application/octet-stream")
        self.assertEqual(blobs[local]["refs"], 1)  # untouched by the pull
        status, data, headers = self.call("GET", f"/v1/blobs/{remote_a}")
        self.assertEqual((status, data), (200, b"remote-a"))

    def test_presence_request_is_sorted_and_local_digests_are_not_fetched(self) -> None:
        local = self.put(b"already-here")
        remote_only = self.remote.add(b"fetch-me")
        self.remote.add(b"already-here")  # remote has it too; must not be fetched
        status, raw, _ = self.pull(self.remote.base_url, [remote_only, local])
        self.assertEqual(status, 200)
        posts = [entry for entry in self.remote.log if entry[0] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][1], "/v1/blobs/presence")
        self.assertEqual(json.loads(posts[0][2])["digests"], sorted([remote_only, local]))
        self.assertEqual(self.remote.gets(), [remote_only])  # no GET for the local digest
        body = json.loads(raw)
        self.assertEqual(body["synced"], [remote_only])
        self.assertEqual(body["existing"], [local])

    def test_retry_reports_existing_and_never_adds_refs(self) -> None:
        digest = self.remote.add(b"pull-me-twice")
        _, first, _ = self.pull(self.remote.base_url, [digest])
        self.assertEqual(json.loads(first)["synced"], [digest])
        _, second, _ = self.pull(self.remote.base_url, [digest])
        body = json.loads(second)
        self.assertEqual(body["synced"], [])
        self.assertEqual(body["existing"], [digest])
        self.assertEqual(body["missing"], [])
        self.assertEqual(self.listing()[digest]["refs"], 1)

    def test_remote_missing_but_local_is_existing(self) -> None:
        local = self.put(b"only-local")
        status, raw, _ = self.pull(self.remote.base_url, [local])
        self.assertEqual(status, 200)
        body = json.loads(raw)
        self.assertEqual(body["existing"], [local])
        self.assertEqual(body["missing"], [])

    def test_bad_request_shapes_are_400(self) -> None:
        bad = [
            b"not json",
            b"{}",
            b'{"base_url": "http://x"}',
            b'{"digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "ftp://x", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://u@x", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x/p", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x/?q=1", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x", "digests": []}',
            b'{"base_url": "http://x", "digests": ["zz"], "extra": 1}',
        ]
        for body in bad:
            status, raw, _ = self.call("POST", "/v1/mirror/pull", body)
            self.assertEqual(status, 400, body)
            self.assertEqual(json.loads(raw)["error"]["code"], "invalid_request", body)
        self.assertEqual(self.remote.log, [])  # validation precedes any remote call

    def test_unknown_paths_are_404(self) -> None:
        status, _, _ = self.call("POST", "/v1/mirror", b"{}")
        self.assertEqual(status, 404)
        status, _, _ = self.call("GET", "/v1/mirror/pull")
        self.assertEqual(status, 404)

    def assert_mirror_error(self, base_url: str, digests: list[str]) -> None:
        status, raw, _ = self.pull(base_url, digests)
        self.assertEqual(status, 502)
        self.assertEqual(json.loads(raw),
                         {"error": {"code": "mirror_error", "message": "remote sync failed"}})

    def test_connection_failure_is_502(self) -> None:
        self.assert_mirror_error("http://127.0.0.1:1", ["a" * 64])

    def test_presence_failures_are_502_and_store_nothing(self) -> None:
        digest = self.remote.add(b"presence-games")
        cases = {
            "non-200": lambda h, b: h.reply(500, b"oops"),
            "not json": lambda h, b: h.reply(200, b"nope"),
            "extra field": lambda h, b: h.reply(
                200, json.dumps({"present": [digest], "missing": [], "stats": {}}).encode()),
            "missing field": lambda h, b: h.reply(200, b'{"present": []}'),
            "unsorted": lambda h, b: h.reply(
                200, json.dumps({"present": [], "missing": [digest, "0" * 64]}).encode()),
            "incomplete coverage": lambda h, b: h.reply(
                200, json.dumps({"present": [digest], "missing": []}).encode()),
            "extra digest": lambda h, b: h.reply(
                200, json.dumps({"present": [digest, "f" * 64], "missing": ["0" * 64]}).encode()),
            "overlap": lambda h, b: h.reply(
                200, json.dumps({"present": [digest], "missing": [digest, "0" * 64]}).encode()),
            "bad element": lambda h, b: h.reply(
                200, json.dumps({"present": ["ZZ"], "missing": ["0" * 64, digest]}).encode()),
        }
        for name, override in cases.items():
            with self.subTest(case=name):
                self.remote.presence_override = override
                self.assert_mirror_error(self.remote.base_url, [digest, "0" * 64])
                self.assertEqual(self.listing(), {})
                self.remote.presence_override = None

    def test_get_failures_are_502_and_store_nothing(self) -> None:
        digest = self.remote.add(b"get-games")
        wrong = D("something-else")
        cases = {
            "non-200": lambda h, d: h.reply(404, b"nope"),
            "redirect": lambda h, d: h.reply(302, b"", {"Location": "/v1/blobs/" + digest}),
            "no digest header": lambda h, d: h.reply(200, b"get-games"),
            "wrong digest header": lambda h, d: h.reply(
                200, b"get-games", {"X-Blob-Digest": wrong}),
            "hash mismatch": lambda h, d: h.reply(
                200, b"tampered-bytes", {"X-Blob-Digest": digest}),
            "empty body": lambda h, d: h.reply(200, b"", {"X-Blob-Digest": digest}),
            "media type too long": lambda h, d: h.reply(
                200, b"get-games", {"X-Blob-Digest": digest, "Content-Type": "x" * 201}),
        }
        for name, override in cases.items():
            with self.subTest(case=name):
                self.remote.blob_override = override
                self.assert_mirror_error(self.remote.base_url, [digest])
                self.assertEqual(self.listing(), {})
                self.remote.blob_override = None

    def test_oversized_blob_is_502(self) -> None:
        from artifacts import MAX_BLOB

        digest = digest_of(b"x" * (MAX_BLOB + 1))
        self.remote.blobs[digest] = (b"x" * (MAX_BLOB + 1), None)
        self.assert_mirror_error(self.remote.base_url, [digest])
        self.assertEqual(self.listing(), {})

    def test_no_partial_commit_when_a_later_get_fails(self) -> None:
        good = self.remote.add(b"good-blob")
        bad = self.remote.add(b"bad-blob")
        first, second = sorted([good, bad])

        def override(handler, digest):
            if digest == second:
                return handler.reply(200, b"corrupted", {"X-Blob-Digest": second})
            data, media_type = self.remote.blobs[digest]
            handler.reply(200, data, {"X-Blob-Digest": digest})

        self.remote.blob_override = override
        self.assert_mirror_error(self.remote.base_url, [good, bad])
        self.assertEqual(self.listing(), {})  # the validated first blob was not stored

    def test_existing_digest_skips_a_failing_remote_get(self) -> None:
        local = self.put(b"local-copy")
        self.remote.blobs[local] = (b"local-copy", None)  # remote claims to have it
        self.remote.blob_override = lambda h, d: h.reply(500, b"broken")
        status, raw, _ = self.pull(self.remote.base_url, [local])
        self.assertEqual(status, 200)  # never fetched: local at commit time
        self.assertEqual(json.loads(raw)["existing"], [local])
        self.assertEqual(self.remote.gets(), [])


class MirrorCommitTests(unittest.TestCase):
    def test_commit_is_insert_if_absent_and_partitions(self) -> None:
        store = Store()
        blob = store.put(b"pre-existing")
        new_digest = D("brand-new")
        absent = "0" * 64
        fetched = [(new_digest, b"brand-new", "text/plain"),
                   (blob.digest, b"pre-existing", "text/plain")]  # lost the race
        result = store.mirror_commit(fetched, [absent, blob.digest, new_digest])
        self.assertEqual(result, {"synced": [new_digest], "existing": [blob.digest],
                                  "missing": [absent]})
        self.assertEqual(store.head(new_digest).size, 9)
        self.assertEqual(store.head(blob.digest).size, len(b"pre-existing"))
        # The pre-existing blob kept its single reference; the fetched copy was dropped.
        listing = {entry["digest"]: entry["refs"] for entry in store.listing()}
        self.assertEqual(listing[blob.digest], 1)
        self.assertEqual(listing[new_digest], 1)


if __name__ == "__main__":
    unittest.main()
