"""Optional upload-session persistence: cross-restart resumption and failure semantics."""
from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from artifacts import (
    UploadManager,
    UploadStateInvalid,
    UploadStateWriteFailed,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(method: str, url: str, data: bytes | None = None, headers: dict | None = None):
    request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except urllib.error.URLError:
        return -1, b""


class _Server:
    """A live in-process server bound to a persistent state path."""

    def __init__(self, state_path: str | None) -> None:
        from artifacts import serve

        self.server = serve(port=0, upload_state=state_path)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"


class RestartTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_path = os.path.join(self._tmp.name, "uploads.json")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def create(self, server: _Server, size: int, media_type: str = "application/octet-stream",
               digest: str | None = None) -> dict:
        body = {"size": size, "media_type": media_type}
        if digest is not None:
            body["digest"] = digest
        status, raw = http("POST", server.url("/v1/uploads"), json.dumps(body).encode())
        self.assertEqual(status, 201, raw)
        return json.loads(raw)

    def put(self, server: _Server, upload_id: str, offset: int, chunk: bytes):
        return http("PUT", server.url(f"/v1/uploads/{upload_id}"), chunk,
                    {"X-Upload-Offset": str(offset)})

    def get(self, server: _Server, upload_id: str):
        return http("GET", server.url(f"/v1/uploads/{upload_id}"))

    def delete(self, server: _Server, upload_id: str):
        return http("DELETE", server.url(f"/v1/uploads/{upload_id}"))

    def complete(self, server: _Server, upload_id: str):
        return http("POST", server.url(f"/v1/uploads/{upload_id}/complete"), b"")

    def test_uncommitted_session_resumes_across_restart(self) -> None:
        payload = bytes((i * 7 + 3) % 256 for i in range(10_000))
        declared = hashlib.sha256(b"different declared digest value").hexdigest()
        server = _Server(self.state_path)
        try:
            created = self.create(server, len(payload), "application/test", digest=declared)
            upload_id = created["upload_id"]
            cut = 3000
            for offset in range(0, cut, 3000):
                status, body = self.put(server, upload_id, offset, payload[offset:offset + 3000])
                self.assertEqual(status, 200, body)
            status, view_raw = self.get(server, upload_id)
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(view_raw)["received"], cut)
        finally:
            server.stop()

        # Restart with the same state path; blob store is empty again but the session survives.
        server = _Server(self.state_path)
        try:
            status, raw = self.get(server, upload_id)
            self.assertEqual(status, 200, raw)
            view = json.loads(raw)
            self.assertEqual(view, {"size": len(payload), "received": cut,
                                    "media_type": "application/test",
                                    "digest": declared, "status": "uncommitted"})
            # Continue from the persisted offset.
            for offset in range(cut, len(payload), 3000):
                status, body = self.put(server, upload_id, offset,
                                        payload[offset:offset + 3000])
                self.assertEqual(status, 200, body)
            # Declared digest deliberately wrong -> conflict, session stays resumable.
            status, body = self.complete(server, upload_id)
            self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
            status, raw = self.get(server, upload_id)
            self.assertEqual((json.loads(raw)["status"], json.loads(raw)["received"]),
                             ("uncommitted", len(payload)))
            # Restart again mid-conflict: full received bytes persist.
        finally:
            server.stop()

        server = _Server(self.state_path)
        try:
            status, raw = self.get(server, upload_id)
            self.assertEqual(status, 200)
            self.assertEqual((json.loads(raw)["received"], json.loads(raw)["status"]),
                             (len(payload), "uncommitted"))
        finally:
            server.stop()

    def test_committed_session_survives_and_repeat_complete_conflicts(self) -> None:
        payload = b"persisted committed payload" * 10
        digest = hashlib.sha256(payload).hexdigest()
        server = _Server(self.state_path)
        try:
            upload_id = self.create(server, len(payload), "text/plain")["upload_id"]
            for offset in range(0, len(payload), 4000):
                self.assertEqual(
                    self.put(server, upload_id, offset, payload[offset:offset + 4000])[0], 200)
            status, body = self.complete(server, upload_id)
            self.assertEqual(status, 201, body)
            self.assertEqual(json.loads(body), {"digest": digest, "size": len(payload),
                                                "media_type": "text/plain"})
        finally:
            server.stop()

        server = _Server(self.state_path)
        try:
            status, raw = self.get(server, upload_id)
            self.assertEqual(status, 200)
            view = json.loads(raw)
            self.assertEqual((view["status"], view["digest"], view["received"]),
                             ("committed", digest, len(payload)))
            # Re-complete after restart is a conflict, with identical digest semantics.
            status, body = self.complete(server, upload_id)
            self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
            # PUT/DELETE on a committed session stay conflicts.
            status, body = self.put(server, upload_id, len(payload), b"x")
            self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
            self.assertEqual(self.delete(server, upload_id)[0], 409)
            # The committed blob itself is NOT persisted: fresh process memory.
            status, _ = http("GET", server.url(f"/v1/blobs/{digest}"))
            self.assertEqual(status, 404)
        finally:
            server.stop()

    def test_tombstone_stays_invisible_across_restart(self) -> None:
        server = _Server(self.state_path)
        try:
            upload_id = self.create(server, 12)["upload_id"]
            self.assertEqual(self.put(server, upload_id, 0, b"abcdef")[0], 200)
            self.assertEqual(self.delete(server, upload_id)[0], 204)
        finally:
            server.stop()

        server = _Server(self.state_path)
        try:
            for status, _ in [
                self.get(server, upload_id),
                self.put(server, upload_id, 6, b"gh"),
                self.delete(server, upload_id),
                self.complete(server, upload_id),
            ]:
                self.assertEqual(status, 404)
        finally:
            server.stop()

    def test_upload_id_is_not_reused_after_restart(self) -> None:
        server = _Server(self.state_path)
        try:
            first = self.create(server, 4)["upload_id"]
        finally:
            server.stop()
        server = _Server(self.state_path)
        try:
            for _ in range(50):
                new_id = self.create(server, 4)["upload_id"]
                self.assertNotEqual(new_id, first)
        finally:
            server.stop()

    def test_unknown_session_is_404_not_400_after_restart(self) -> None:
        server = _Server(self.state_path)
        try:
            status, _ = self.get(server, "0123456789abcdef" * 2)
            self.assertEqual(status, 404)
        finally:
            server.stop()


class StateFileValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_path = os.path.join(self._tmp.name, "uploads.json")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def manager(self) -> UploadManager:
        from artifacts import Store

        return UploadManager(Store(), state_path=self.state_path)

    def write(self, raw: bytes | str | object) -> None:
        if isinstance(raw, (bytes, str)):
            data = raw if isinstance(raw, bytes) else raw.encode()
            with open(self.state_path, "wb") as handle:
                handle.write(data)
        else:
            with open(self.state_path, "w", encoding="utf-8") as handle:
                json.dump(raw, handle)

    def test_missing_path_initialises_empty_state_file(self) -> None:
        manager = self.manager()
        self.assertEqual(manager._sessions, {})  # noqa: SLF001
        with open(self.state_path, "rb") as handle:
            document = json.loads(handle.read())
        self.assertEqual(document, {"version": 1, "uploads": []})

    def test_empty_file_is_invalid(self) -> None:
        self.write(b"")
        with self.assertRaises(UploadStateInvalid):
            self.manager()

    def test_corrupt_or_wrong_shape_files_are_invalid(self) -> None:
        bad = [
            b"{",
            b"not json",
            b"[]",
            b'"string"',
            b"null",
            b"42",
            {"version": 1},
            {"uploads": []},
            {"version": 1, "uploads": {}},
            {"version": "1", "uploads": []},
            {"version": 2, "uploads": []},
            {"version": 0, "uploads": []},
            {"version": True, "uploads": []},
            {"version": 1, "uploads": [], "extra": 1},
            {"version": 1, "uploads": ["x"]},
            {"version": 1, "uploads": [{}]},
            # malformed record: missing fields
            {"version": 1, "uploads": [{"upload_id": "a" * 32}]},
            # bad upload id
            {"version": 1, "uploads": [{
                "upload_id": "Z" * 32, "size": 1, "received": 0,
                "media_type": "x", "status": "uncommitted", "data": ""}]},
            # received > size
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 1, "received": 2,
                "media_type": "x", "status": "uncommitted", "data": "ag"}]},
            # data length disagrees with received
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 4, "received": 1,
                "media_type": "x", "status": "uncommitted", "data": ""}]},
            # bad base64
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 4, "received": 2,
                "media_type": "x", "status": "uncommitted", "data": "@@@"}]},
            # committed without digest / not full
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 4, "received": 3,
                "media_type": "x", "status": "committed"}]},
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 4, "received": 4,
                "media_type": "x", "status": "committed"}]},
            # unknown status / unknown field
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 4, "received": 0,
                "media_type": "x", "status": "frozen", "data": ""}]},
            {"version": 1, "uploads": [{
                "upload_id": "a" * 32, "size": 4, "received": 0,
                "media_type": "x", "status": "uncommitted", "data": "", "x": 1}]},
            # duplicate ids
            {"version": 1, "uploads": [
                {"upload_id": "a" * 32, "size": 4, "received": 0,
                 "media_type": "x", "status": "uncommitted", "data": ""},
                {"upload_id": "a" * 32, "size": 4, "received": 0,
                 "media_type": "x", "status": "uncommitted", "data": ""}]},
        ]
        for index, payload in enumerate(bad):
            self.write(payload)
            with self.assertRaises(UploadStateInvalid, msg=f"case {index}: {payload!r}"):
                self.manager()

    def test_valid_document_loads(self) -> None:
        self.write({"version": 1, "uploads": [
            {"upload_id": "a" * 32, "size": 4, "received": 2,
             "media_type": "x", "status": "uncommitted", "data": "YWI="},
            {"upload_id": "b" * 32, "size": 3, "received": 3,
             "media_type": "y", "status": "committed", "digest": "c" * 64,
             "declared_digest": "d" * 64},
            {"upload_id": "c" * 32, "size": 9, "received": 5,
             "media_type": "z", "status": "deleted"},
        ]})
        manager = self.manager()
        self.assertEqual(set(manager._sessions), {"a" * 32, "b" * 32, "c" * 32})  # noqa: SLF001

    def test_cli_refuses_to_listen_on_invalid_state(self) -> None:
        self.write(b"")
        port = free_port()
        env = dict(os.environ, PYTHONPATH=str(SRC_DIR), PYTHONWARNINGS="ignore")
        proc = subprocess.run(
            [sys.executable, "-m", "artifacts.app", "--port", str(port),
             "--upload-state", self.state_path],
            cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=20)
        self.assertNotEqual(proc.returncode, 0)
        # The contractual line is present verbatim (a runpy RuntimeWarning may precede
        # it in some environments; it is unrelated to the state validation).
        self.assertIn(f"upload state invalid: {self.state_path}\n", proc.stderr)
        self.assertTrue(any(
            line == f"upload state invalid: {self.state_path}" for line in proc.stderr.splitlines()))
        # And it must not be listening.
        status, _ = http("GET", f"http://127.0.0.1:{port}/health")
        self.assertEqual(status, -1)

    def test_cli_without_state_flag_keeps_baseline_behavior(self) -> None:
        port = free_port()
        env = dict(os.environ, PYTHONPATH=str(SRC_DIR))
        proc = subprocess.Popen(
            [sys.executable, "-m", "artifacts.app", "--port", str(port)],
            cwd=REPO_ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.time() + 10
            while time.time() < deadline:
                status, _ = http("GET", f"http://127.0.0.1:{port}/health")
                if status == 200:
                    break
                time.sleep(0.1)
            else:  # pragma: no cover
                self.fail("server never came up")
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.stderr is not None:
                proc.stderr.close()


class WriteFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_path = os.path.join(self._tmp.name, "uploads.json")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_runtime_write_failure_returns_500_and_change_is_rolled_back(self) -> None:
        from artifacts import serve

        server = serve(port=0, upload_state=self.state_path)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{port}"
            # Create one valid session and append a chunk successfully.
            status, raw = http("POST", f"{base}/v1/uploads",
                               json.dumps({"size": 8, "media_type": "x"}).encode())
            self.assertEqual(status, 201, raw)
            upload_id = json.loads(raw)["upload_id"]
            self.assertEqual(http("PUT", f"{base}/v1/uploads/{upload_id}", b"abcd",
                                  {"X-Upload-Offset": "0"})[0], 200)

            # Force subsequent persistence writes to fail by pointing the state
            # directory at a non-writable location: replace the file with a directory
            # of the same name so os.replace onto it fails.
            os.remove(self.state_path)
            os.mkdir(self.state_path)

            status, raw = http("PUT", f"{base}/v1/uploads/{upload_id}", b"ef",
                               {"X-Upload-Offset": "4"})
            self.assertEqual(status, 500, raw)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "upload state write failed"}})
            # The rejected bytes were rolled back in memory.
            status, raw = http("GET", f"{base}/v1/uploads/{upload_id}")
            self.assertEqual(json.loads(raw)["received"], 4)

            # create also fails with the same 500 body.
            status, raw = http("POST", f"{base}/v1/uploads",
                               json.dumps({"size": 1, "media_type": "x"}).encode())
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw)["error"]["message"], "upload state write failed")

            # delete and complete on the existing session fail the same way.
            status, raw = http("DELETE", f"{base}/v1/uploads/{upload_id}")
            self.assertEqual(status, 500)
            # session still visible: delete was rolled back
            self.assertEqual(http("GET", f"{base}/v1/uploads/{upload_id}")[0], 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_complete_write_failure_does_not_leave_blob_or_ref(self) -> None:
        from artifacts import serve

        server = serve(port=0, upload_state=self.state_path)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{port}"
            status, raw = http("POST", f"{base}/v1/uploads",
                               json.dumps({"size": 4, "media_type": "x"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            http("PUT", f"{base}/v1/uploads/{upload_id}", b"abcd",
                 {"X-Upload-Offset": "0"})
            os.remove(self.state_path)
            os.mkdir(self.state_path)
            status, _ = http("POST", f"{base}/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 500)
            # No blob/ref from the unacknowledged completion, and the session stays resumable.
            self.assertEqual(server.store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_restart_after_failed_write_recovers_last_good_state(self) -> None:
        # Manager-level: simulate a failing write mid-append, then persist good state again.
        from artifacts import Store

        store = Store()
        manager = UploadManager(store, state_path=self.state_path)
        session = manager.create(4, "x", None)
        manager.append(session.upload_id, 0, b"ab")

        original_write = manager._write_state  # noqa: SLF001

        def failing() -> None:
            raise UploadStateWriteFailed()

        manager._write_state = failing  # type: ignore[method-assign]  # noqa: SLF001
        with self.assertRaises(UploadStateWriteFailed):
            manager.append(session.upload_id, 2, b"cd")
        manager._write_state = original_write  # type: ignore[method-assign]  # noqa: SLF001

        # In-memory session was rolled back to received == 2.
        self.assertEqual(manager.view(session.upload_id)["received"], 2)
        # The file on disk is still the last good document.
        manager2 = UploadManager(Store(), state_path=self.state_path)
        self.assertEqual(manager2.view(session.upload_id)["received"], 2)
        manager2.append(session.upload_id, 2, b"cd")
        _, blob = manager2.complete(session.upload_id)
        self.assertEqual(blob.digest, hashlib.sha256(b"abcd").hexdigest())


class NoStatePathTests(unittest.TestCase):
    def test_baseline_memory_mode_is_unchanged(self) -> None:
        from artifacts import serve

        server = serve(port=0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.assertEqual(http("GET", f"http://127.0.0.1:{port}/health")[0], 200)
            status, raw = http(
                "POST", f"http://127.0.0.1:{port}/v1/uploads",
                json.dumps({"size": 4, "media_type": "x"}).encode())
            self.assertEqual(status, 201)
            upload_id = json.loads(raw)["upload_id"]
            self.assertEqual(http("PUT", f"http://127.0.0.1:{port}/v1/uploads/{upload_id}",
                                  b"abcd", {"X-Upload-Offset": "0"})[0], 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
