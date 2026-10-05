"""Public-behavior assertions for resumable upload sessions."""
from __future__ import annotations

import hashlib
import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import CHUNK_MAX, MAX_BLOB, InvalidRequest, UploadManager

HEX32 = "0123456789abcdef" * 2  # 32 lowercase hex characters, syntactically valid but unknown


class ParseCreateTests(unittest.TestCase):
    def parse(self, payload: str | bytes) -> None:
        UploadManager.parse_create(payload if isinstance(payload, bytes) else payload.encode())

    def test_valid_bodies(self) -> None:
        self.assertEqual(
            UploadManager.parse_create(json.dumps({"size": 1, "media_type": "text/plain"}).encode()),
            (1, "text/plain", None))
        self.assertEqual(
            UploadManager.parse_create(
                json.dumps({"size": MAX_BLOB, "media_type": "application/octet-stream",
                            "digest": "a" * 64}).encode()),
            (MAX_BLOB, "application/octet-stream", "a" * 64))

    def test_invalid_bodies_raise_invalid_request(self) -> None:
        bad: list[bytes] = [
            b"", b"not json", b"[1,2,3]", b'"string"', b"null",
        ]
        bad.extend(json.dumps(payload).encode() for payload in [
            {"media_type": "text/plain"},                    # size missing
            {"size": 1},                                     # media_type missing
            {"size": 0, "media_type": "x"},                  # size below 1
            {"size": MAX_BLOB + 1, "media_type": "x"},       # size above 1 MiB
            {"size": -1, "media_type": "x"},
            {"size": "10", "media_type": "x"},               # size not an integer
            {"size": 1.5, "media_type": "x"},
            {"size": True, "media_type": "x"},               # bool is not an integer
            {"size": None, "media_type": "x"},
            {"size": 1, "media_type": ""},                   # empty media_type
            {"size": 1, "media_type": 2},
            {"size": 1, "media_type": "x" * 201},
            {"size": 1, "media_type": "x", "digest": "Z" * 64},   # uppercase digest
            {"size": 1, "media_type": "x", "digest": "a" * 63},   # short digest
            {"size": 1, "media_type": "x", "digest": 7},
            {"size": 1, "media_type": "x", "extra": 1},      # unknown field
        ])
        for body in bad:
            with self.assertRaises(InvalidRequest, msg=body):
                self.parse(body)


class UploadHttpTests(unittest.TestCase):
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
        self.server.uploads._sessions.clear()

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def create(self, size: int, media_type: str = "application/octet-stream", digest: str | None = None):
        body = {"size": size, "media_type": media_type}
        if digest is not None:
            body["digest"] = digest
        status, raw, _ = self.call("POST", "/v1/uploads", json.dumps(body).encode())
        return status, json.loads(raw)

    def put_chunk(self, upload_id: str, offset: int, chunk: bytes):
        status, raw, _ = self.call("PUT", f"/v1/uploads/{upload_id}", chunk,
                                   {"X-Upload-Offset": str(offset)})
        return status, json.loads(raw) if raw else {}

    def get_session(self, upload_id: str):
        status, raw, _ = self.call("GET", f"/v1/uploads/{upload_id}")
        return status, json.loads(raw) if raw else {}

    def complete(self, upload_id: str, data: bytes | None = b""):
        status, raw, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete", data)
        return status, json.loads(raw) if raw else {}

    # ---- creation -------------------------------------------------------

    def test_create_returns_201_with_zero_received_and_hex_id(self) -> None:
        status, body = self.create(128, "text/plain")
        self.assertEqual(status, 201)
        self.assertEqual(set(body), {"upload_id", "size", "received", "status"})
        self.assertEqual((body["size"], body["received"], body["status"]), (128, 0, "uncommitted"))
        self.assertRegex(body["upload_id"], r"^[0-9a-f]{32}$")

    def test_create_echoes_optional_digest_on_followup_get(self) -> None:
        _, body = self.create(8, digest="a" * 64)
        status, view = self.get_session(body["upload_id"])
        self.assertEqual(status, 200)
        self.assertEqual(view, {"size": 8, "received": 0, "media_type": "application/octet-stream",
                                "digest": "a" * 64, "status": "uncommitted"})

    def test_size_boundaries(self) -> None:
        self.assertEqual(self.create(1)[0], 201)
        self.assertEqual(self.create(MAX_BLOB)[0], 201)
        for payload in [
            {"media_type": "x"}, {"size": 0, "media_type": "x"},
            {"size": MAX_BLOB + 1, "media_type": "x"}, {"size": "10", "media_type": "x"},
            {"size": 1.5, "media_type": "x"}, {"size": True, "media_type": "x"},
            {"size": 1, "media_type": "x", "digest": "A" * 64},
            {"size": 1, "media_type": "x", "nope": 1},
        ]:
            status, raw, _ = self.call("POST", "/v1/uploads", json.dumps(payload).encode())
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (400, "invalid_request"), payload)
        for raw in [b"", b"{", b"[]", b"null"]:
            status, body, _ = self.call("POST", "/v1/uploads", raw)
            self.assertEqual((status, json.loads(body)["error"]["code"]), (400, "invalid_request"))

    # ---- chunked upload --------------------------------------------------

    def test_chunked_roundtrip_commits_blob_and_dedupes_refs(self) -> None:
        payload = b"".join(bytes([i % 256]) * 1000 for i in range(10))  # 10000 bytes
        _, created = self.create(len(payload), "application/test")
        upload_id = created["upload_id"]
        for offset in range(0, len(payload), 3000):
            chunk = payload[offset:offset + 3000]
            status, body = self.put_chunk(upload_id, offset, chunk)
            self.assertEqual(status, 200)
            self.assertEqual((body["received"], body["status"]), (offset + len(chunk), "uncommitted"))
            status, view = self.get_session(upload_id)
            self.assertEqual((status, view["received"]), (200, offset + len(chunk)))
        status, body = self.complete(upload_id)
        digest = hashlib.sha256(payload).hexdigest()
        self.assertEqual(status, 201)
        self.assertEqual(body, {"digest": digest, "size": len(payload), "media_type": "application/test"})
        self.assertEqual(self.call("GET", f"/v1/blobs/{digest}")[1], payload)
        # A second session uploading identical bytes stores one blob, two refs.
        _, other = self.create(len(payload), "application/test")
        for offset in range(0, len(payload), 3000):
            self.put_chunk(other["upload_id"], offset, payload[offset:offset + 3000])
        self.assertEqual(self.complete(other["upload_id"])[1]["digest"], digest)
        status, listing, _ = self.call("GET", "/v1/blobs")
        stats = json.loads(listing)["stats"]
        self.assertEqual(stats, {"blobs": 1, "bytes": len(payload), "puts": 2})

    def test_full_one_mebibyte_upload_in_max_sized_chunks(self) -> None:
        size = MAX_BLOB
        _, created = self.create(size)
        upload_id = created["upload_id"]
        pieces = []
        for offset in range(0, size, CHUNK_MAX):
            piece = bytes(((offset + i) % 256) for i in range(min(CHUNK_MAX, size - offset)))
            pieces.append(piece)
            status, body = self.put_chunk(upload_id, offset, piece)
            self.assertEqual(status, 200)
            self.assertEqual(body["received"], offset + len(piece))
        payload = b"".join(pieces)
        status, body = self.complete(upload_id)
        self.assertEqual(status, 201)
        self.assertEqual(body["digest"], hashlib.sha256(payload).hexdigest())

    def test_resume_after_misaligned_put_uses_get_then_continues(self) -> None:
        _, created = self.create(11)
        upload_id = created["upload_id"]
        self.put_chunk(upload_id, 0, b"hello")
        # Client retried with a stale offset; the byte stream is untouched.
        status, body = self.put_chunk(upload_id, 0, b"XXXXX")
        self.assertEqual((status, body["error"]["code"]), (409, "conflict"))
        status, view = self.get_session(upload_id)
        self.assertEqual((view["received"], view["status"]), (5, "uncommitted"))
        self.assertEqual(self.put_chunk(upload_id, 5, b" world")[0], 200)
        status, body = self.complete(upload_id)
        self.assertEqual(status, 201)
        self.assertEqual(self.call("GET", f"/v1/blobs/{body['digest']}")[1], b"hello world")

    def test_chunk_validation_errors(self) -> None:
        _, created = self.create(10)
        upload_id = created["upload_id"]
        # missing offset header
        status, raw, _ = self.call("PUT", f"/v1/uploads/{upload_id}", b"abc")
        self.assertEqual((status, json.loads(raw)["error"]["code"]), (400, "invalid_request"))
        # non-decimal, signed, fractional, prefixed offsets (leading whitespace is
        # stripped by HTTP header parsing, so it cannot be expressed on the wire)
        for bad_offset in ["abc", "-1", "1.0", "0x1", "+1", ""]:
            status, raw, _ = self.call("PUT", f"/v1/uploads/{upload_id}", b"abc",
                                       {"X-Upload-Offset": bad_offset})
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (400, "invalid_request"),
                             bad_offset)
        # empty chunk with an otherwise valid request
        status, body = self.put_chunk(upload_id, 0, b"")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # chunk exceeding the 256 KiB cap is rejected while reading Content-Length
        status, body = self.put_chunk(upload_id, 0, b"x" * (CHUNK_MAX + 1))
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # nothing was appended
        self.assertEqual(self.get_session(upload_id)[1]["received"], 0)

    def test_chunk_past_declared_size_is_conflict(self) -> None:
        _, created = self.create(4)
        upload_id = created["upload_id"]
        status, body = self.put_chunk(upload_id, 0, b"12345")  # within chunk cap, over session size
        self.assertEqual((status, body["error"]["code"]), (409, "conflict"))
        self.assertEqual(self.get_session(upload_id)[1]["received"], 0)

    def test_unknown_and_deleted_and_malformed_sessions(self) -> None:
        # malformed upload_id is a 400 on every verb
        for method, path, data in [
            ("GET", "/v1/uploads/zzz", None),
            ("PUT", "/v1/uploads/zzz", b"x"),
            ("DELETE", "/v1/uploads/zzz", None),
            ("POST", "/v1/uploads/zzz/complete", b""),
            ("GET", "/v1/uploads/" + "a" * 31, None),
            ("GET", "/v1/uploads/" + "a" * 33, None),
        ]:
            status, raw, _ = self.call(method, path, data,
                                       {"X-Upload-Offset": "0"} if method == "PUT" else None)
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (400, "invalid_request"),
                             (method, path))
        # well-formed but unknown id is a 404
        for method, path, data in [
            ("GET", f"/v1/uploads/{HEX32}", None),
            ("PUT", f"/v1/uploads/{HEX32}", b"x"),
            ("DELETE", f"/v1/uploads/{HEX32}", None),
            ("POST", f"/v1/uploads/{HEX32}/complete", b""),
        ]:
            status, raw, _ = self.call(method, path, data,
                                       {"X-Upload-Offset": "0"} if method == "PUT" else None)
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (404, "not_found"),
                             (method, path))
        # DELETE makes the id invisible forever
        _, created = self.create(4)
        upload_id = created["upload_id"]
        self.put_chunk(upload_id, 0, b"ab")
        self.assertEqual(self.call("DELETE", f"/v1/uploads/{upload_id}")[0], 204)
        for method, path, data in [
            ("GET", f"/v1/uploads/{upload_id}", None),
            ("PUT", f"/v1/uploads/{upload_id}", b"cd"),
            ("DELETE", f"/v1/uploads/{upload_id}", None),
            ("POST", f"/v1/uploads/{upload_id}/complete", b""),
        ]:
            status, raw, _ = self.call(method, path, data,
                                       {"X-Upload-Offset": "2"} if method == "PUT" else None)
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (404, "not_found"),
                             (method, path))

    def test_complete_requires_full_size_and_is_repeatable_only_as_conflict(self) -> None:
        _, created = self.create(5)
        upload_id = created["upload_id"]
        self.put_chunk(upload_id, 0, b"ab")
        status, body = self.complete(upload_id)
        self.assertEqual((status, body["error"]["code"]), (409, "conflict"))
        self.put_chunk(upload_id, 2, b"cde")
        status, committed = self.complete(upload_id)
        self.assertEqual(status, 201)
        # committed session: visible via GET, but writes/deletes/re-complete all conflict
        status, view = self.get_session(upload_id)
        self.assertEqual((status, view["status"], view["digest"], view["received"]),
                         (200, "committed", committed["digest"], 5))
        self.assertEqual((self.put_chunk(upload_id, 5, b"x")[0], 409)[0], 409)
        self.assertEqual(self.put_chunk(upload_id, 5, b"x")[1]["error"]["code"], "conflict")
        self.assertEqual(self.call("DELETE", f"/v1/uploads/{upload_id}")[0], 409)
        status, body = self.complete(upload_id)
        self.assertEqual((status, body["error"]["code"]), (409, "conflict"))
        # blob survives the failed delete
        self.assertEqual(self.call("GET", f"/v1/blobs/{committed['digest']}")[0], 200)

    def test_declared_digest_mismatch_conflicts_without_creating_blob(self) -> None:
        payload = b"truthful bytes"
        _, created = self.create(len(payload), digest=hashlib.sha256(b"something else").hexdigest())
        upload_id = created["upload_id"]
        for offset in range(0, len(payload), 7):
            self.put_chunk(upload_id, offset, payload[offset:offset + 7])
        status, body = self.complete(upload_id)
        self.assertEqual((status, body["error"]["code"]), (409, "conflict"))
        self.assertEqual(json.loads(self.call("GET", "/v1/blobs")[1])["stats"]["blobs"], 0)
        # the session is unchanged and still resumable/visible
        status, view = self.get_session(upload_id)
        self.assertEqual((status, view["status"], view["received"]), (200, "uncommitted", len(payload)))
        self.assertEqual(self.complete(upload_id)[0], 409)
        # it can be abandoned via DELETE
        self.assertEqual(self.call("DELETE", f"/v1/uploads/{upload_id}")[0], 204)

    def test_routing_and_baseline_surface_unchanged(self) -> None:
        self.assertEqual(self.call("GET", "/health")[0], 200)
        self.assertEqual(self.call("POST", "/v1/uploads/x/other", b"")[0], 404)
        self.assertEqual(self.call("PUT", "/v1/nope", b"x")[0], 404)
        # baseline blob PUT still works and is not a session route
        status, body, _ = self.call("PUT", "/v1/blobs", b"plain", {"Content-Type": "text/plain"})
        self.assertEqual(status, 201)

    # ---- concurrency ----------------------------------------------------

    def test_concurrent_same_offset_writes_have_one_winner_and_no_tearing(self) -> None:
        length = 16
        _, created = self.create(length)
        upload_id = created["upload_id"]
        candidates = [bytes([i]) * length for i in range(8)]  # distinct uniform-byte chunks
        barrier = threading.Barrier(len(candidates))
        statuses: list[int] = []
        lock = threading.Lock()

        def worker(chunk: bytes) -> None:
            barrier.wait()
            status, _ = self.put_chunk(upload_id, 0, chunk)
            with lock:
                statuses.append(status)

        threads = [threading.Thread(target=worker, args=(chunk,)) for chunk in candidates]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(statuses.count(200), 1)
        self.assertEqual(statuses.count(409), len(candidates) - 1)
        status, view = self.get_session(upload_id)
        self.assertEqual((status, view["received"]), (200, length))
        status, body = self.complete(upload_id)
        self.assertEqual(status, 201)
        stored = self.call("GET", f"/v1/blobs/{body['digest']}")[1]
        self.assertIn(stored, candidates)  # exactly one whole chunk, never interleaved bytes

    def test_concurrent_distinct_offsets_only_append_at_received(self) -> None:
        step, count = 4, 8
        size = step * count
        _, created = self.create(size)
        upload_id = created["upload_id"]
        barrier = threading.Barrier(count)
        outcomes: list[tuple[int, int]] = []
        lock = threading.Lock()

        def worker(index: int) -> None:
            barrier.wait()
            chunk = bytes([65 + index]) * step
            status, _ = self.put_chunk(upload_id, index * step, chunk)
            with lock:
                outcomes.append((index, status))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = sorted(index for index, status in outcomes if status == 200)
        # The accepted offsets form a contiguous prefix starting at 0: no gap, no overlap.
        self.assertEqual(winners, list(range(len(winners))))
        self.assertEqual(self.get_session(upload_id)[1]["received"], step * len(winners))
        # The losers retry serially in offset order; bytes assemble exactly in order.
        for index in range(len(winners), count):
            self.assertEqual(self.put_chunk(upload_id, index * step, bytes([65 + index]) * step)[0], 200)
        status, body = self.complete(upload_id)
        self.assertEqual(status, 201)
        stored = self.call("GET", f"/v1/blobs/{body['digest']}")[1]
        self.assertEqual(stored, b"".join(bytes([65 + i]) * step for i in range(count)))


if __name__ == "__main__":
    unittest.main()
