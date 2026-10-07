"""Tests for POST /v1/mirror/pull: request validation, remote presence/GET
verification, all-or-nothing atomic commit, classification semantics and
compatibility with the rest of the surface."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from artifacts import MAX_BLOB, InvalidRequest, Store, digest_of, parse_mirror_pull, serve

D = digest_of  # bytes -> 64-char lowercase hex


def post_json(port: int, path: str, payload: bytes):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=payload, method="POST")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


class ParseMirrorPullTests(unittest.TestCase):
    def valid(self) -> bytes:
        return json.dumps({"base_url": "http://127.0.0.1:9000",
                           "digests": ["a" * 64]}).encode()

    def test_accepts_http_and_https_roots_with_optional_port_and_slash(self) -> None:
        for url in ("http://example.com", "https://example.com/",
                    "http://127.0.0.1:8080", "https://[::1]:443/"):
            base_url, digests = parse_mirror_pull(
                json.dumps({"base_url": url, "digests": ["b" * 64]}).encode())
            self.assertEqual(base_url, url.rstrip("/"))
            self.assertEqual(digests, ["b" * 64])

    def test_rejects_every_bad_shape_with_invalid_request(self) -> None:
        bad_bodies = [
            b"not json at all",
            b"\xff\xfe{}",
            b"[1, 2]",
            b"42",
            b"null",
            b"{}",                                          # both fields missing
            b'{"base_url": "http://x"}',                    # digests missing
            b'{"digests": ["' + b"a" * 64 + b'"]}',         # base_url missing
            b'{"base_url": 5, "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": null, "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "ftp://x", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://user@x", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://user:pw@x", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x/path", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x/?q=1", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x?", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x#frag", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x:bad", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x y", "digests": ["' + b"a" * 64 + b'"]}',
            b'{"base_url": "http://x", "digests": []}',
            b'{"base_url": "http://x", "digests": "not-an-array"}',
            b'{"base_url": "http://x", "digests": ["' + b"A" * 64 + b'"]}',
            b'{"base_url": "http://x", "digests": ["' + b"a" * 63 + b'"]}',
            b'{"base_url": "http://x", "digests": [123]}',
            b'{"base_url": "http://x", "digests": ["' + b"a" * 64 + b'"], "extra": 1}',
            b'{"base_url": "http://x", "digests": ["' + b"a" * 64 + b'"]} trailing',
        ]
        dup = json.dumps({"base_url": "http://x",
                          "digests": ["a" * 64, "a" * 64]}).encode()
        bad_bodies.append(dup)
        too_many = json.dumps({"base_url": "http://x",
                               "digests": [f"{i:064x}" for i in range(101)]}).encode()
        bad_bodies.append(too_many)
        for body in bad_bodies:
            with self.subTest(body=body[:70]):
                with self.assertRaises(InvalidRequest):
                    parse_mirror_pull(body)

    def test_accepts_one_and_one_hundred_digests(self) -> None:
        for n in (1, 100):
            body = json.dumps({"base_url": "http://x",
                               "digests": [f"{i:064x}" for i in range(n)]}).encode()
            self.assertEqual(len(parse_mirror_pull(body)[1]), n)


class MirrorServer:
    """A controllable remote: serves presence and blob GETs from a table."""

    def __init__(self) -> None:
        self.blobs: dict[str, tuple[bytes, str | None]] = {}
        self.presence_response: tuple[int, object] | None = None
        self.get_overrides: dict[str, tuple[int, dict[str, str], bytes]] = {}
        self.get_requests: list[str] = []
        self.presence_requests: list[list[str]] = []

        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:
                return

            def _send(self, status: int, body: bytes, headers: dict[str, str]) -> None:
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                if self.path == "/v1/blobs/presence":
                    length = int(self.headers.get("Content-Length") or 0)
                    payload = json.loads(self.rfile.read(length) or b"{}")
                    outer.presence_requests.append(list(payload.get("digests", [])))
                    if outer.presence_response is not None:
                        status, body = outer.presence_response
                        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                        return self._send(status, raw, {"Content-Type": "application/json"})
                    requested = set(payload.get("digests", []))
                    present = [{"digest": d, "size": len(outer.blobs[d][0]),
                                "media_type": outer.blobs[d][1] or "application/octet-stream",
                                "refs": 1}
                               for d in sorted(requested) if d in outer.blobs]
                    missing = sorted(requested - set(outer.blobs))
                    body = json.dumps({"present": present, "missing": missing,
                                       "stats": {"blobs": 0, "bytes": 0, "puts": 0}}).encode()
                    return self._send(200, body, {"Content-Type": "application/json"})
                self._send(404, b"{}", {"Content-Type": "application/json"})

            def do_GET(self) -> None:  # noqa: N802
                if self.path.startswith("/v1/blobs/"):
                    digest = self.path.rsplit("/", 1)[1]
                    outer.get_requests.append(digest)
                    if digest in outer.get_overrides:
                        status, headers, body = outer.get_overrides[digest]
                        return self._send(status, body, headers)
                    if digest in outer.blobs:
                        data, media_type = outer.blobs[digest]
                        headers = {"X-Blob-Digest": digest}
                        if media_type is not None:
                            headers["Content-Type"] = media_type
                        return self._send(200, data, headers)
                    return self._send(404, b"{}", {"Content-Type": "application/json"})
                self._send(404, b"{}", {"Content-Type": "application/json"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def add(self, text: bytes, media_type: str | None = "application/octet-stream") -> str:
        digest = digest_of(text)
        self.blobs[digest] = (text, media_type)
        return digest

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class MirrorPullHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        with self.server.store._lock:
            self.server.store._blobs.clear()
            self.server.store._meta.clear()
            self.server.store._refs.clear()
        self.remote = MirrorServer()
        self.addCleanup(self.remote.close)

    def pull(self, digests: list[str], base_url: str | None = None):
        body = json.dumps({"base_url": base_url or self.remote.base_url,
                           "digests": digests}).encode()
        return post_json(self.port, "/v1/mirror/pull", body)

    def test_syncs_missing_blobs_and_classifies_all_three_buckets(self) -> None:
        new1 = self.remote.add(b"remote one", "text/plain")
        new2 = self.remote.add(b"remote two")  # no content type -> octet-stream
        local = D(b"local already")
        self.server.store.put(b"local already", media_type="text/html")
        remote_also_has_local = self.remote.add(b"local already", "text/html")
        self.assertEqual(remote_also_has_local, local)
        nowhere = "0" * 64

        status, body = self.pull([nowhere, new2, local, new1])
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"synced", "existing", "missing"})
        self.assertEqual(body["synced"], sorted([new1, new2]))
        self.assertEqual(body["existing"], [local])
        self.assertEqual(body["missing"], [nowhere])
        self.assertEqual(sorted(body["synced"] + body["existing"] + body["missing"]),
                         sorted([nowhere, new2, local, new1]))

        # New blobs landed with refs 1 and the remote media types.
        listing = {entry["digest"]: entry for entry in self.server.store.listing()}
        self.assertEqual(listing[new1]["refs"], 1)
        self.assertEqual(listing[new1]["media_type"], "text/plain")
        self.assertEqual(listing[new2]["refs"], 1)
        self.assertEqual(listing[new2]["media_type"], "application/octet-stream")
        # The pre-existing blob was not re-referenced or re-typed.
        self.assertEqual(listing[local]["refs"], 1)
        self.assertEqual(listing[local]["media_type"], "text/html")
        # Presence was asked for the sorted request; no GET for the local blob.
        self.assertEqual(self.remote.presence_requests,
                         [sorted([nowhere, new2, local, new1])])
        self.assertEqual(sorted(self.remote.get_requests), sorted([new1, new2]))

    def test_bytes_are_readable_after_sync(self) -> None:
        digest = self.remote.add(b"synced bytes")
        status, _ = self.pull([digest])
        self.assertEqual(status, 200)
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/v1/blobs/{digest}") as response:
            self.assertEqual(response.read(), b"synced bytes")

    def test_retry_syncs_nothing_and_adds_no_references(self) -> None:
        digest = self.remote.add(b"once only")
        status, body = self.pull([digest])
        self.assertEqual((status, body["synced"]), (200, [digest]))
        status, body = self.pull([digest])
        self.assertEqual(status, 200)
        self.assertEqual(body, {"synced": [], "existing": [digest], "missing": []})
        listing = {entry["digest"]: entry for entry in self.server.store.listing()}
        self.assertEqual(listing[digest]["refs"], 1)

    def test_all_missing_is_a_quiet_200(self) -> None:
        absent = ["1" * 64, "2" * 64]
        status, body = self.pull(absent)
        self.assertEqual(status, 200)
        self.assertEqual(body, {"synced": [], "existing": [], "missing": absent})
        self.assertEqual(self.remote.get_requests, [])

    def test_remote_content_type_absent_defaults_to_octet_stream(self) -> None:
        digest = self.remote.add(b"no type", None)
        status, body = self.pull([digest])
        self.assertEqual((status, body["synced"]), (200, [digest]))
        self.assertEqual(self.server.store.head(digest).media_type,
                         "application/octet-stream")

    def test_bad_request_shape_is_400_and_never_touches_the_remote(self) -> None:
        for payload in (b"{}", b'{"base_url": "http://x"}', b"not json",
                        b'{"base_url": "http://user@x", "digests": ["' + b"a" * 64 + b'"]}'):
            status, body = post_json(self.port, "/v1/mirror/pull", payload)
            self.assertEqual(status, 400)
            self.assertEqual(body["error"]["code"], "invalid_request")
        self.assertEqual(self.remote.presence_requests, [])

    def test_unknown_paths_still_404(self) -> None:
        status, body = post_json(self.port, "/v1/mirror", b"{}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        status, body = post_json(self.port, "/v1/mirror/pull/extra", b"{}")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{self.port}/v1/mirror/pull")
            self.fail("GET on the mirror route must not exist")
        except urllib.error.HTTPError as error:
            self.assertEqual(error.code, 404)

    def assert_mirror_error(self, status: int, body: dict) -> None:
        self.assertEqual(status, 502)
        self.assertEqual(body, {"error": {"code": "mirror_error",
                                          "message": "remote sync failed"}})

    def test_connection_failure_is_502(self) -> None:
        # Nothing listens on this port.
        status, body = self.pull(["a" * 64], base_url="http://127.0.0.1:1")
        self.assert_mirror_error(status, body)

    def test_presence_failures_are_502_and_store_nothing(self) -> None:
        digest = self.remote.add(b"candidate")
        cases = [
            (500, {"present": [], "missing": [digest]}),          # non-200
            (200, b"not json"),                                   # bad JSON
            (200, [1, 2]),                                        # not an object
            (200, {"missing": [digest]}),                         # present missing
            (200, {"present": [], "missing": "nope"}),            # wrong type
            (200, {"present": [], "missing": []}),                # incomplete cover
            (200, {"present": [digest], "missing": [digest]}),    # overlap
            (200, {"present": ["0" * 64], "missing": [digest]}),  # extra digest
            (200, {"present": ["zz" * 32], "missing": [digest]}), # bad digest shape
        ]
        for response in cases:
            with self.subTest(response=response):
                self.remote.presence_response = response
                status, body = self.pull([digest])
                self.assert_mirror_error(status, body)
                self.assertEqual(self.server.store.stats()["blobs"], 0)
        self.remote.presence_response = None

    def test_unsorted_presence_lists_are_502(self) -> None:
        d1 = self.remote.add(b"one")
        d2 = self.remote.add(b"two")
        high, low = max(d1, d2), min(d1, d2)
        self.remote.presence_response = (200, {"present": [high, low], "missing": []})
        status, body = self.pull([d1, d2])
        self.assert_mirror_error(status, body)
        self.assertEqual(self.server.store.stats()["blobs"], 0)

    def test_get_failures_are_502_and_store_nothing(self) -> None:
        good = self.remote.add(b"good bytes")
        cases = {
            "status": (404, {"X-Blob-Digest": "x"}, b""),
            "header": (200, {"X-Blob-Digest": "0" * 64}, b"good bytes"),
            "no_header": (200, {}, b"good bytes"),
            "hash": (200, {"X-Blob-Digest": "placeholder"}, b"tampered bytes"),
            "empty": (200, {"X-Blob-Digest": "placeholder"}, b""),
            "oversize": (200, {"X-Blob-Digest": "placeholder"}, b"x" * (MAX_BLOB + 1)),
            "media_type": (200, {"X-Blob-Digest": "placeholder",
                                 "Content-Type": "x" * 201}, b"good bytes"),
        }
        for name, override in cases.items():
            with self.subTest(case=name):
                target = self.remote.add(b"target " + name.encode())
                headers = dict(override[1])
                if headers.get("X-Blob-Digest") == "placeholder":
                    headers["X-Blob-Digest"] = target
                self.remote.get_overrides[target] = (override[0], headers, override[2])
                status, body = self.pull([good, target])
                self.assert_mirror_error(status, body)
                # All-or-nothing: even the fetchable blob was not stored.
                self.assertEqual(self.server.store.stats()["blobs"], 0)
                del self.remote.get_overrides[target]

    def test_existing_entries_unaffected_by_mirror_error(self) -> None:
        local = D(b"untouched")
        self.server.store.put(b"untouched")
        self.server.store.put(b"untouched")  # refs == 2
        self.remote.presence_response = (500, b"")
        status, body = self.pull([local, "1" * 64])
        self.assert_mirror_error(status, body)
        listing = {entry["digest"]: entry for entry in self.server.store.listing()}
        self.assertEqual(listing[local]["refs"], 2)


class MirrorRealRemoteTests(unittest.TestCase):
    """Pull from a second full instance of this same service."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.remote_server = serve(port=0)
        cls.remote_port = cls.remote_server.server_address[1]
        threading.Thread(target=cls.remote_server.serve_forever, daemon=True).start()
        cls.local_server = serve(port=0)
        cls.local_port = cls.local_server.server_address[1]
        threading.Thread(target=cls.local_server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.remote_server.shutdown()
        cls.remote_server.server_close()
        cls.local_server.shutdown()
        cls.local_server.server_close()

    def test_pulls_from_a_same_contract_instance(self) -> None:
        payload = b"blob living on the real remote"
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.remote_port}/v1/blobs", data=payload,
            method="PUT")
        request.add_header("Content-Type", "text/plain")
        with urllib.request.urlopen(request) as response:
            digest = json.loads(response.read())["digest"]

        body = json.dumps({"base_url": f"http://127.0.0.1:{self.remote_port}",
                           "digests": [digest, "f" * 64]}).encode()
        status, result = post_json(self.local_port, "/v1/mirror/pull", body)
        self.assertEqual(status, 200)
        self.assertEqual(result, {"synced": [digest], "existing": [],
                                  "missing": ["f" * 64]})
        blob, data = self.local_server.store.read(digest)
        self.assertEqual(data, payload)
        self.assertEqual(blob.media_type, "text/plain")
        # The remote was only read: its refs did not change.
        remote_listing = {e["digest"]: e for e in self.remote_server.store.listing()}
        self.assertEqual(remote_listing[digest]["refs"], 1)


if __name__ == "__main__":
    unittest.main()
