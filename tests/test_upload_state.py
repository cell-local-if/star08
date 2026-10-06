"""Optional on-disk persistence for resumable upload sessions (--upload-state)."""
from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from artifacts import (
    CHUNK_MAX,
    MAX_BLOB,
    UploadState,
    UploadStateError,
    UploadStateInvalid,
    make_handler,
    serve,
)
from artifacts import Store, UploadManager


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class PersistedServer:
    """A serve()-like server whose lifecycle a test can cycle against one state path."""

    def __init__(self, state_path: str | None, store: Store | None = None,
                 manager: UploadManager | None = None) -> None:
        self.state_path = state_path
        self.httpd: ThreadingHTTPServer | None = None
        self._own_lifecycle = manager is None
        if manager is None:
            self.server = serve(port=0, upload_state=state_path)
        else:
            assert store is not None
            self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store, manager))
            self.server.store = store
            self.server.uploads = manager
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


class StateFileRoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "uploads.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def call(self, server: PersistedServer, method: str, path: str,
             data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.port}{path}", data=data, method=method,
            headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def create(self, server: PersistedServer, body: dict) -> tuple[int, dict]:
        status, raw = self.call(server, "POST", "/v1/uploads", json.dumps(body).encode())
        return status, json.loads(raw)

    def test_uncommitted_session_resumes_across_restart_with_bytes_and_metadata(self) -> None:
        server = PersistedServer(self.path)
        try:
            payload = b"0123456789abcdef" * 4  # 64 bytes, two chunks
            declared = hashlib.sha256(payload).hexdigest()
            _, created = self.create(server, {"size": len(payload),
                                              "media_type": "application/test",
                                              "digest": declared})
            upload_id = created["upload_id"]
            first = payload[:30]
            status, body = self.call(server, "PUT", f"/v1/uploads/{upload_id}", first,
                                     {"X-Upload-Offset": "0"})
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), {"received": 30, "status": "uncommitted"})
        finally:
            server.stop()

        # Restart with the same state path; blobs are process-memory and gone.
        server = PersistedServer(self.path)
        try:
            status, raw = self.call(server, "GET", f"/v1/uploads/{upload_id}")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(raw), {
                "size": 64, "received": 30, "media_type": "application/test",
                "digest": declared, "status": "uncommitted"})
            # Resuming at the old offset is a conflict; GET's received is the truth.
            status, raw = self.call(server, "PUT", f"/v1/uploads/{upload_id}", payload[30:60],
                                    {"X-Upload-Offset": "0"})
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (409, "conflict"))
            status, _ = self.call(server, "PUT", f"/v1/uploads/{upload_id}", payload[30:],
                                  {"X-Upload-Offset": "30"})
            self.assertEqual(status, 200)
            status, raw = self.call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 201)
            self.assertEqual(json.loads(raw), {"digest": declared, "size": 64,
                                               "media_type": "application/test"})
            status, raw = self.call(server, "GET", f"/v1/blobs/{declared}")
            self.assertEqual(status, 200)
            self.assertEqual(raw, payload)
        finally:
            server.stop()

    def test_committed_session_keeps_state_and_digest_after_restart(self) -> None:
        server = PersistedServer(self.path)
        try:
            payload = b"committed bytes here"
            _, created = self.create(server, {"size": len(payload), "media_type": "text/plain"})
            upload_id = created["upload_id"]
            self.assertEqual(
                self.call(server, "PUT", f"/v1/uploads/{upload_id}", payload,
                          {"X-Upload-Offset": "0"})[0], 200)
            status, raw = self.call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")
            digest = json.loads(raw)["digest"]
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            status, raw = self.call(server, "GET", f"/v1/uploads/{upload_id}")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(raw), {
                "size": len(payload), "received": len(payload), "media_type": "text/plain",
                "digest": digest, "status": "committed"})
            # Repeated complete stays a conflict after restart.
            status, raw = self.call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (409, "conflict"))
            # Writes and deletes on a committed session stay conflicts.
            status, raw = self.call(server, "PUT", f"/v1/uploads/{upload_id}", b"x",
                                    {"X-Upload-Offset": str(len(payload))})
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (409, "conflict"))
            status, raw = self.call(server, "DELETE", f"/v1/uploads/{upload_id}")
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (409, "conflict"))
            # The blob itself is process-memory and was not persisted.
            self.assertEqual(self.call(server, "GET", f"/v1/blobs/{digest}")[0], 404)
        finally:
            server.stop()

    def test_tombstone_is_invisible_after_restart(self) -> None:
        server = PersistedServer(self.path)
        try:
            _, created = self.create(server, {"size": 4, "media_type": "x"})
            upload_id = created["upload_id"]
            self.assertEqual(
                self.call(server, "PUT", f"/v1/uploads/{upload_id}", b"ab",
                          {"X-Upload-Offset": "0"})[0], 200)
            self.assertEqual(self.call(server, "DELETE", f"/v1/uploads/{upload_id}")[0], 204)
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            for method, path, data, headers in [
                ("GET", f"/v1/uploads/{upload_id}", None, None),
                ("PUT", f"/v1/uploads/{upload_id}", b"cd", {"X-Upload-Offset": "2"}),
                ("DELETE", f"/v1/uploads/{upload_id}", None, None),
                ("POST", f"/v1/uploads/{upload_id}/complete", b"", None),
            ]:
                status, raw = self.call(server, method, path, data, headers)
                self.assertEqual((status, json.loads(raw)["error"]["code"]), (404, "not_found"),
                                 method)
            # A brand-new session id never collides with the restored tombstone.
            _, fresh = self.create(server, {"size": 1, "media_type": "x"})
            self.assertNotEqual(fresh["upload_id"], upload_id)
        finally:
            server.stop()

    def test_multiple_chunks_full_size_round_trip_after_restart(self) -> None:
        server = PersistedServer(self.path)
        size = CHUNK_MAX + 10
        try:
            _, created = self.create(server, {"size": size, "media_type": "application/octet-stream"})
            upload_id = created["upload_id"]
            first = b"a" * CHUNK_MAX
            self.assertEqual(
                self.call(server, "PUT", f"/v1/uploads/{upload_id}", first,
                          {"X-Upload-Offset": "0"})[0], 200)
        finally:
            server.stop()
        server = PersistedServer(self.path)
        try:
            rest = b"b" * 10
            self.assertEqual(
                self.call(server, "PUT", f"/v1/uploads/{upload_id}", rest,
                          {"X-Upload-Offset": str(CHUNK_MAX)})[0], 200)
            status, raw = self.call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 201)
            self.assertEqual(json.loads(raw)["digest"],
                             hashlib.sha256(first + rest).hexdigest())
        finally:
            server.stop()


class StateFileValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_state(self, content: bytes | str | None) -> str:
        path = os.path.join(self.tmp.name, "state.json")
        if content is not None:
            mode = "wb" if isinstance(content, bytes) else "w"
            with open(path, mode) as handle:
                handle.write(content)
        return path

    def assert_invalid(self, content: bytes | str | None) -> None:
        path = self.write_state(content)
        with self.assertRaises(UploadStateInvalid):
            serve(port=0, upload_state=path).server_close()

    def test_empty_file_is_invalid(self) -> None:
        self.assert_invalid(b"")

    def test_corrupt_json_is_invalid(self) -> None:
        self.assert_invalid(b"{not json")
        self.assert_invalid(b"\xff\xfe not utf8")

    def test_wrong_shape_is_invalid(self) -> None:
        self.assert_invalid(json.dumps([]))
        self.assert_invalid(json.dumps({}))
        self.assert_invalid(json.dumps({"version": 1}))
        self.assert_invalid(json.dumps({"sessions": []}))
        self.assert_invalid(json.dumps({"version": 1, "sessions": {}, "extra": 1}))
        self.assert_invalid(json.dumps({"version": "1", "sessions": []}))

    def test_wrong_version_is_invalid(self) -> None:
        self.assert_invalid(json.dumps({"version": 2, "sessions": []}))
        self.assert_invalid(json.dumps({"version": 0, "sessions": []}))

    def test_bad_session_entries_are_invalid(self) -> None:
        good = {
            "upload_id": "a" * 32, "size": 4, "media_type": "x",
            "declared_digest": None, "chunks": [], "received": 0,
            "committed": False, "deleted": False, "final_digest": None,
        }
        bad_docs: list[object] = [
            {"version": 1, "sessions": [[]]},
            {"version": 1, "sessions": [{**good, "upload_id": "z" * 32}]},
            {"version": 1, "sessions": [{**good, "size": 0}]},
            {"version": 1, "sessions": [{**good, "size": MAX_BLOB + 1}]},
            {"version": 1, "sessions": [{**good, "received": 5}]},
            {"version": 1, "sessions": [{**good, "media_type": ""}]},
            {"version": 1, "sessions": [{**good, "declared_digest": "Z" * 64}]},
            {"version": 1, "sessions": [{**good, "extra": 1}]},
            {"version": 1, "sessions": [{k: v for k, v in good.items()
                                         if k != "final_digest"}]},
            {"version": 1, "sessions": [{**good, "committed": True, "received": 0}]},
            {"version": 1, "sessions": [{**good, "committed": True,
                                         "final_digest": "a" * 64}]},
            {"version": 1, "sessions": [{**good, "chunks": ["not base64!!"]}]},
            {"version": 1, "sessions": [{**good, "chunks": [""]}]},
        ]
        for doc in bad_docs:
            self.assert_invalid(json.dumps(doc))

    def test_duplicate_upload_ids_are_invalid(self) -> None:
        entry = {
            "upload_id": "a" * 32, "size": 4, "media_type": "x",
            "declared_digest": None, "chunks": [], "received": 0,
            "committed": False, "deleted": False, "final_digest": None,
        }
        self.assert_invalid(json.dumps({"version": 1, "sessions": [entry, entry]}))

    def test_missing_state_file_starts_with_no_sessions(self) -> None:
        path = os.path.join(self.tmp.name, "absent.json")
        server = serve(port=0, upload_state=path)
        try:
            self.assertEqual(server.uploads._sessions, {})
        finally:
            server.server_close()

    def test_cli_exits_nonzero_with_stderr_message(self) -> None:
        path = self.write_state(b"broken")
        proc = subprocess.run(
            [sys.executable, "-m", "artifacts.app", "--port", str(_free_port()),
             "--upload-state", path],
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "src"},
            capture_output=True, text=True, timeout=20)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stderr.strip(), f"upload state invalid: {path}")
        self.assertEqual(proc.stdout, "")


class WriteFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "uploads.json")
        self.store = Store()
        self.state = UploadState(self.path)
        self.uploads = UploadManager(self.store, self.state, self.state.load())
        self.server = PersistedServer(self.path, self.store, self.uploads)

    def tearDown(self) -> None:
        self.server.stop()
        self.tmp.cleanup()

    def call(self, method: str, url_path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.server.port}{url_path}", data=data, method=method,
            headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def test_unwritable_path_makes_create_return_500_with_exact_body(self) -> None:
        # Point the state at a path inside a nonexistent directory: load treated it
        # as absent, but every write fails.
        bad_directory = tempfile.mkdtemp(dir=self.tmp.name)
        os.rmdir(bad_directory)
        state = UploadState(os.path.join(bad_directory, "nested", "state.json"))
        uploads = UploadManager(Store(), state, {})
        server = PersistedServer(None, Store(), uploads)
        try:
            status, raw = self.call_with(server, "POST", "/v1/uploads",
                                         json.dumps({"size": 1, "media_type": "x"}).encode())
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "upload state write failed"}})
        finally:
            server.stop()

    @staticmethod
    def call_with(server: PersistedServer, method: str, path: str,
                  data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.port}{path}",
            data=data, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def test_failed_append_is_not_confirmed_and_restart_keeps_last_good_state(self) -> None:
        status, raw = self.call("POST", "/v1/uploads",
                                json.dumps({"size": 10, "media_type": "x"}).encode())
        upload_id = json.loads(raw)["upload_id"]
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", b"hello",
                                   {"X-Upload-Offset": "0"})[0], 200)

        real_save = self.state.save
        failures = {"count": 0}

        def fail_once(sessions):
            failures["count"] += 1
            raise UploadStateError()

        self.state.save = fail_once  # type: ignore[method-assign]
        try:
            status, raw = self.call("PUT", f"/v1/uploads/{upload_id}", b"world",
                                    {"X-Upload-Offset": "5"})
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "upload state write failed"}})
        finally:
            self.state.save = real_save  # type: ignore[method-assign]

        # The unconfirmed append was rolled back: received is still 5 and the
        # bytes were not appended.
        status, raw = self.call("GET", f"/v1/uploads/{upload_id}")
        self.assertEqual(json.loads(raw)["received"], 5)
        # Client retries at the persisted offset and succeeds.
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", b"world",
                                   {"X-Upload-Offset": "5"})[0], 200)
        self.assertEqual(failures["count"], 1)

        # The file on disk never held the failed write: a fresh process sees 10 bytes.
        self.server.stop()
        restored = UploadState(self.path).load()
        session = restored[upload_id]
        self.assertEqual((session.received, session.size), (10, 10))
        self.assertEqual(b"".join(session.chunks), b"helloworld")

    def test_failed_delete_is_not_confirmed(self) -> None:
        status, raw = self.call("POST", "/v1/uploads",
                                json.dumps({"size": 2, "media_type": "x"}).encode())
        upload_id = json.loads(raw)["upload_id"]
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", b"ab",
                                   {"X-Upload-Offset": "0"})[0], 200)

        def fail(sessions):
            raise UploadStateError()

        self.state.save = fail  # type: ignore[method-assign]
        try:
            status, _ = self.call("DELETE", f"/v1/uploads/{upload_id}")
            self.assertEqual(status, 500)
        finally:
            del self.state.save
        # Session is still visible and resumable.
        status, raw = self.call("GET", f"/v1/uploads/{upload_id}")
        view = json.loads(raw)
        self.assertEqual((status, view["status"], view["received"]), (200, "uncommitted", 2))

    def test_failed_complete_leaves_session_resumable_and_creates_no_blob(self) -> None:
        payload = b"finish me!"
        status, raw = self.call(
            "POST", "/v1/uploads",
            json.dumps({"size": len(payload), "media_type": "text/plain"}).encode())
        upload_id = json.loads(raw)["upload_id"]
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", payload,
                                   {"X-Upload-Offset": "0"})[0], 200)

        def fail(sessions):
            raise UploadStateError()

        self.state.save = fail  # type: ignore[method-assign]
        try:
            status, raw = self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 500)
        finally:
            del self.state.save
        status, raw = self.call("GET", f"/v1/uploads/{upload_id}")
        view = json.loads(raw)
        self.assertEqual((view["status"], view["received"]), ("uncommitted", len(payload)))
        self.assertNotIn("digest", view)
        # Nothing entered the content-addressed store.
        self.assertEqual(self.store.stats()["blobs"], 0)
        # Retry after the persistence problem clears works normally.
        status, raw = self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(raw)["digest"], hashlib.sha256(payload).hexdigest())


class NoStatePathTests(unittest.TestCase):
    """Without --upload-state nothing is written and behavior is purely in-memory."""

    def test_serve_without_state_has_no_backing_file(self) -> None:
        server = serve(port=0)
        try:
            self.assertIsNone(server.uploads._state)
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main()
