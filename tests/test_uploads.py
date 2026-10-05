"""Public-behaviour tests for resumable upload sessions (POST/GET/PUT/DELETE /v1/uploads)."""
from __future__ import annotations

import hashlib
import http.client
import json
import re
import threading
import unittest
import urllib.error
import urllib.request

MAX_CHUNK = 262_144


class UploadTestBase(unittest.TestCase):
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

    def create(self, size: int, media_type: str = "application/octet-stream", digest: str | None = None):
        spec: dict = {"size": size, "media_type": media_type}
        if digest is not None:
            spec["digest"] = digest
        status, body, _ = self.call("POST", "/v1/uploads", json.dumps(spec).encode(),
                                    {"Content-Type": "application/json"})
        self.assertEqual(status, 201, body)
        return json.loads(body)

    def put_chunk(self, upload_id: str, offset, chunk: bytes, extra_headers: dict | None = None):
        headers = {"X-Upload-Offset": str(offset)}
        headers.update(extra_headers or {})
        return self.call("PUT", f"/v1/uploads/{upload_id}", chunk, headers)

    def upload_fully(self, upload_id: str, data: bytes) -> None:
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + MAX_CHUNK]
            status, body, _ = self.put_chunk(upload_id, offset, chunk)
            self.assertEqual(status, 200, body)
            offset += len(chunk)


class CreateTests(UploadTestBase):
    def test_create_success_shape(self) -> None:
        body = self.create(10, "text/plain")
        self.assertEqual(set(body), {"upload_id", "size", "received", "status"})
        self.assertEqual((body["size"], body["received"], body["status"]), (10, 0, "uncommitted"))
        self.assertIsNotNone(re.fullmatch(r"[0-9a-f]{32}", body["upload_id"]))

    def test_create_then_get_describes_session_for_resume(self) -> None:
        upload_id = self.create(7, "text/plain", digest=hashlib.sha256(b"1234567").hexdigest())["upload_id"]
        status, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(status, 200)
        description = json.loads(body)
        self.assertEqual(description["size"], 7)
        self.assertEqual(description["received"], 0)
        self.assertEqual(description["media_type"], "text/plain")
        self.assertEqual(description["status"], "uncommitted")
        self.assertEqual(description["digest"], hashlib.sha256(b"1234567").hexdigest())

    def test_get_omits_digest_when_not_declared(self) -> None:
        upload_id = self.create(3)["upload_id"]
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertNotIn("digest", json.loads(body))

    def test_create_rejects_invalid_specs(self) -> None:
        bad_bodies = [
            b"",                                        # empty body
            b"not json",                                # not JSON
            b"[1, 2]",                                  # not an object
            b"{}",                                      # missing size and media_type
            json.dumps({"media_type": "a/b"}).encode(),                     # missing size
            json.dumps({"size": 10}).encode(),                              # missing media_type
            json.dumps({"size": 0, "media_type": "a/b"}).encode(),          # size too small
            json.dumps({"size": 1048577, "media_type": "a/b"}).encode(),    # size too large
            json.dumps({"size": 1.5, "media_type": "a/b"}).encode(),        # non-integer size
            json.dumps({"size": "10", "media_type": "a/b"}).encode(),       # string size
            json.dumps({"size": True, "media_type": "a/b"}).encode(),       # bool size
            json.dumps({"size": 10, "media_type": 42}).encode(),            # non-string media_type
            json.dumps({"size": 10, "media_type": "x" * 201}).encode(),     # media_type too long
            json.dumps({"size": 10, "media_type": "a/b", "digest": "zz"}).encode(),
            json.dumps({"size": 10, "media_type": "a/b", "digest": "A" * 64}).encode(),
            json.dumps({"size": 10, "media_type": "a/b", "nope": 1}).encode(),  # unknown field
        ]
        for raw in bad_bodies:
            status, body, _ = self.call("POST", "/v1/uploads", raw, {"Content-Type": "application/json"})
            self.assertEqual(status, 400, raw)
            self.assertEqual(json.loads(body)["error"]["code"], "invalid_request", raw)

    def test_create_boundary_sizes(self) -> None:
        for size in (1, 1_048_576):
            body = self.create(size)
            self.assertEqual(body["size"], size)


class ChunkTests(UploadTestBase):
    def test_chunked_upload_roundtrip(self) -> None:
        payload = bytes(range(256)) * 1200  # 307200 bytes -> two chunks
        upload_id = self.create(len(payload), "application/x-test")["upload_id"]
        status, body, _ = self.put_chunk(upload_id, 0, payload[:MAX_CHUNK])
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"received": MAX_CHUNK, "status": "uncommitted"})
        status, body, _ = self.put_chunk(upload_id, MAX_CHUNK, payload[MAX_CHUNK:])
        self.assertEqual(json.loads(body)["received"], len(payload))
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)
        result = json.loads(body)
        self.assertEqual(result["digest"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(result["size"], len(payload))
        self.assertEqual(result["media_type"], "application/x-test")
        status, raw, headers = self.call("GET", f"/v1/blobs/{result['digest']}")
        self.assertEqual(status, 200)
        self.assertEqual(raw, payload)
        self.assertEqual(headers["Content-Type"], "application/x-test")

    def test_resume_via_get_received(self) -> None:
        payload = b"resume-me-please"
        upload_id = self.create(len(payload))["upload_id"]
        self.put_chunk(upload_id, 0, payload[:6])
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        received = json.loads(body)["received"]
        self.assertEqual(received, 6)
        status, _, _ = self.put_chunk(upload_id, received, payload[received:])
        self.assertEqual(status, 200)
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(body)["digest"], hashlib.sha256(payload).hexdigest())

    def test_offset_must_equal_received(self) -> None:
        upload_id = self.create(10)["upload_id"]
        status, body, _ = self.put_chunk(upload_id, 5, b"abc")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "conflict")
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["received"], 0)  # failed chunk left no hole

    def test_chunk_beyond_declared_size_is_conflict(self) -> None:
        upload_id = self.create(4)["upload_id"]
        status, body, _ = self.put_chunk(upload_id, 0, b"12345")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "conflict")

    def test_offset_header_validation(self) -> None:
        upload_id = self.create(10)["upload_id"]
        status, _, _ = self.call("PUT", f"/v1/uploads/{upload_id}", b"abc")  # header missing
        self.assertEqual(status, 400)
        for bad in ("abc", "-1", "1.5", "1e3", ""):
            status, body, _ = self.put_chunk(upload_id, bad, b"abc")
            self.assertEqual(status, 400, bad)
            self.assertEqual(json.loads(body)["error"]["code"], "invalid_request", bad)

    def test_chunk_size_limits(self) -> None:
        upload_id = self.create(1_048_576)["upload_id"]
        status, body, _ = self.put_chunk(upload_id, 0, b"")
        self.assertEqual(status, 400)  # empty chunk
        status, body, _ = self.put_chunk(upload_id, 0, b"x" * (MAX_CHUNK + 1))
        self.assertEqual(status, 400)  # oversized chunk
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_request")
        status, body, _ = self.put_chunk(upload_id, 0, b"x" * MAX_CHUNK)
        self.assertEqual(status, 200)  # exactly the cap is fine
        self.assertEqual(json.loads(body)["received"], MAX_CHUNK)

    def test_missing_content_length_is_400(self) -> None:
        upload_id = self.create(10)["upload_id"]
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.putrequest("PUT", f"/v1/uploads/{upload_id}", skip_host=True, skip_accept_encoding=True)
        connection.endheaders()
        response = connection.getresponse()
        response.read()
        self.assertEqual(response.status, 400)
        connection.close()


class SessionStateTests(UploadTestBase):
    def test_delete_removes_session(self) -> None:
        upload_id = self.create(10)["upload_id"]
        status, _, _ = self.call("DELETE", f"/v1/uploads/{upload_id}")
        self.assertEqual(status, 204)
        for method, path in [("GET", f"/v1/uploads/{upload_id}"), ("DELETE", f"/v1/uploads/{upload_id}"),
                             ("POST", f"/v1/uploads/{upload_id}/complete")]:
            status, body, _ = self.call(method, path)
            self.assertEqual(status, 404, (method, path))
            self.assertEqual(json.loads(body)["error"]["code"], "not_found")
        status, body, _ = self.put_chunk(upload_id, 0, b"abc")
        self.assertEqual(status, 404)

    def test_malformed_upload_id_is_400_unknown_is_404(self) -> None:
        for bad_id in ("xyz", "A" * 32, "a" * 31, "a" * 33, "g" * 32, ""):
            status, body, _ = self.call("GET", f"/v1/uploads/{bad_id}")
            self.assertEqual(status, 400 if bad_id else 404, bad_id)  # empty id = different route
        status, body, _ = self.call("GET", f"/v1/uploads/{'0' * 32}")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["code"], "not_found")

    def test_complete_requires_all_bytes(self) -> None:
        upload_id = self.create(10)["upload_id"]
        self.put_chunk(upload_id, 0, b"12345")
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "conflict")

    def test_declared_digest_mismatch_is_conflict_and_writes_nothing(self) -> None:
        payload = b"actual bytes"
        wrong = hashlib.sha256(b"other bytes").hexdigest()
        upload_id = self.create(len(payload), digest=wrong)["upload_id"]
        self.upload_fully(upload_id, payload)
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "conflict")
        # neither the declared nor the computed digest was stored
        self.assertEqual(self.call("GET", f"/v1/blobs/{wrong}")[0], 404)
        self.assertEqual(self.call("GET", f"/v1/blobs/{hashlib.sha256(payload).hexdigest()}")[0], 404)
        # the session survives, still uncommitted
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["status"], "uncommitted")

    def test_committed_session_is_immutable(self) -> None:
        payload = b"done"
        upload_id = self.create(len(payload))["upload_id"]
        self.upload_fully(upload_id, payload)
        self.assertEqual(self.call("POST", f"/v1/uploads/{upload_id}/complete")[0], 201)
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["status"], "committed")
        for method, path, data, headers in [
            ("POST", f"/v1/uploads/{upload_id}/complete", None, None),   # repeat complete
            ("DELETE", f"/v1/uploads/{upload_id}", None, None),          # delete committed
        ]:
            status, body, _ = self.call(method, path, data, headers)
            self.assertEqual(status, 409, method)
            self.assertEqual(json.loads(body)["error"]["code"], "conflict")
        status, body, _ = self.put_chunk(upload_id, len(payload), b"x")  # write committed
        self.assertEqual(status, 409)

    def test_identical_bytes_stored_once_with_one_extra_ref(self) -> None:
        payload = b"dedup me"
        digest = hashlib.sha256(payload).hexdigest()
        for _ in range(2):
            upload_id = self.create(len(payload))["upload_id"]
            self.upload_fully(upload_id, payload)
            status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
            self.assertEqual(status, 201)
            self.assertEqual(json.loads(body)["digest"], digest)
        _, body, _ = self.call("GET", f"/v1/blobs?digest_prefix={digest}")
        listing = json.loads(body)
        self.assertEqual(len(listing["blobs"]), 1)
        self.assertEqual(listing["blobs"][0]["refs"], 2)

    def test_routing_is_unchanged_for_other_paths(self) -> None:
        self.assertEqual(self.call("GET", "/health")[0], 200)
        self.assertEqual(self.call("POST", "/v1/blobs", b"x")[0], 404)
        self.assertEqual(self.call("GET", "/v1/uploads")[0], 404)
        self.assertEqual(self.call("PUT", "/v1/uploads", b"x")[0], 404)
        self.assertEqual(self.call("DELETE", "/v1/blobs")[0], 404)
        self.assertEqual(self.call("POST", f"/v1/uploads/{'0' * 32}", b"x")[0], 404)
        self.assertEqual(self.call("POST", f"/v1/uploads/{'0' * 32}/complete/extra")[0], 404)


class ConcurrencyTests(UploadTestBase):
    def test_concurrent_appends_leave_no_overlap_or_hole(self) -> None:
        payload = b"concurrent!"
        upload_id = self.create(len(payload))["upload_id"]
        results: list[int] = []
        guard = threading.Lock()

        def writer() -> None:
            status, _, _ = self.put_chunk(upload_id, 0, payload)
            with guard:
                results.append(status)

        threads = [threading.Thread(target=writer) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), [200] + [409] * 7)
        _, body, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(body)["received"], len(payload))
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(body)["digest"], hashlib.sha256(payload).hexdigest())

    def test_concurrent_complete_commits_exactly_once(self) -> None:
        payload = b"complete me once"
        digest = hashlib.sha256(payload).hexdigest()
        upload_id = self.create(len(payload))["upload_id"]
        self.upload_fully(upload_id, payload)
        results: list[int] = []
        guard = threading.Lock()

        def completer() -> None:
            status, _, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
            with guard:
                results.append(status)

        threads = [threading.Thread(target=completer) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), [201] + [409] * 5)
        _, body, _ = self.call("GET", f"/v1/blobs?digest_prefix={digest}")
        self.assertEqual(json.loads(body)["blobs"][0]["refs"], 1)


if __name__ == "__main__":
    unittest.main()
