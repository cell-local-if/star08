"""Optional durable audit history (--audit-state) for GET /v1/events."""
from __future__ import annotations

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
    AuditState,
    AuditStateInvalid,
    StoreState,
    UploadState,
    digest_of,
    make_handler,
    serve,
)

D = digest_of


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RunningServer:
    """A serve()-like server a test can cycle against the same audit path."""

    def __init__(self, audit_path: str | None = None, store_path: str | None = None,
                 upload_path: str | None = None, max_store_bytes: int | None = None) -> None:
        self.server = serve(port=0, audit_state=audit_path, store_state=store_path,
                            upload_state=upload_path, max_store_bytes=max_store_bytes)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @classmethod
    def with_store(cls, store) -> "RunningServer":
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store))
        server.store = store
        server.audit = store.audit
        instance = cls.__new__(cls)
        instance.server = server
        instance.port = server.server_address[1]
        instance.thread = threading.Thread(target=server.serve_forever, daemon=True)
        instance.thread.start()
        return instance

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def events(self, query: str = "") -> tuple[int, dict]:
        status, raw = self.call("GET", f"/v1/events{query}")
        return status, json.loads(raw)

    def put_blob(self, payload: bytes, media_type: str | None = None) -> tuple[int, dict]:
        headers = {"Content-Type": media_type} if media_type is not None else {}
        status, raw = self.call("PUT", "/v1/blobs", payload, headers)
        return status, json.loads(raw)


class AuditStateRoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "audit.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def drive_history(self, server: RunningServer) -> list[tuple[str, dict]]:
        """Exercise every event-emitting op once; return the expected (op, result) pairs."""
        expected: list[tuple[str, dict]] = []

        _, body = server.put_blob(b"alpha", "text/plain")
        alpha = body["digest"]
        expected.append(("blob_put", {"digest": alpha, "size": 5,
                                      "media_type": "text/plain", "refs": 1}))

        status, raw = server.call("POST", "/v1/uploads",
                                  json.dumps({"size": 4, "media_type": "text/plain"}).encode())
        upload_id = json.loads(raw)["upload_id"]
        expected.append(("upload_create", {"upload_id": upload_id, "size": 4,
                                           "received": 0, "status": "uncommitted"}))

        status, _ = server.call("PUT", f"/v1/uploads/{upload_id}", b"beta",
                                {"X-Upload-Offset": "0"})
        self.assertEqual(status, 200)
        expected.append(("upload_append", {"upload_id": upload_id, "received": 4,
                                           "status": "uncommitted"}))

        status, raw = server.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
        self.assertEqual(status, 201)
        beta = json.loads(raw)["digest"]
        expected.append(("upload_complete", {"upload_id": upload_id, "digest": beta,
                                             "size": 4, "media_type": "text/plain"}))

        status, raw = server.call("DELETE", f"/v1/blobs/{alpha}/refs")
        self.assertEqual(status, 200)
        expected.append(("blob_release", {"digest": alpha, "refs": 0}))

        status, raw = server.call("POST", "/v1/gc", b"")
        self.assertEqual(status, 200)
        gc_body = json.loads(raw)
        self.assertEqual(gc_body["deleted"], [alpha])
        expected.append(("gc", gc_body))

        # A second upload session, then deleted: exercises upload_delete.
        status, raw = server.call("POST", "/v1/uploads",
                                  json.dumps({"size": 1, "media_type": "application/x"}).encode())
        second_id = json.loads(raw)["upload_id"]
        expected.append(("upload_create", {"upload_id": second_id, "size": 1,
                                           "received": 0, "status": "uncommitted"}))
        status, _ = server.call("DELETE", f"/v1/uploads/{second_id}")
        self.assertEqual(status, 204)
        expected.append(("upload_delete", {"upload_id": second_id}))

        # mirror_pull from a tiny remote carrying one fresh blob.
        remote = RunningServer()
        try:
            _, remote_body = remote.put_blob(b"mirrored", "text/plain")
            mirrored = remote_body["digest"]
            status, raw = server.call("POST", "/v1/mirror/pull", json.dumps({
                "base_url": f"http://127.0.0.1:{remote.port}",
                "digests": [mirrored],
            }).encode())
            self.assertEqual(status, 200)
            mirror_body = json.loads(raw)
            expected.append(("mirror_pull", mirror_body))
        finally:
            remote.stop()
        return expected

    def test_full_history_survives_restart_with_continuous_seq(self) -> None:
        server = RunningServer(self.path)
        try:
            expected = self.drive_history(server)
        finally:
            server.stop()

        # The file holds one versioned snapshot containing every event.
        with open(self.path, encoding="utf-8") as handle:
            doc = json.load(handle)
        self.assertEqual(doc["version"], 1)
        self.assertEqual([e["seq"] for e in doc["events"]],
                         list(range(1, len(expected) + 1)))

        server = RunningServer(self.path)
        try:
            status, page = server.events("?limit=100")
            self.assertEqual(status, 200)
            events = page["events"]
            self.assertEqual([(e["seq"], e["op"]) for e in events],
                             [(i + 1, op) for i, (op, _result) in enumerate(expected)])
            for event, (_op, result) in zip(events, expected):
                self.assertEqual(event["result"], result)
            self.assertEqual(page["next_after"], len(expected))
            self.assertFalse(page["has_more"])

            # Empty page past the end keeps next_after at `after`.
            status, tail = server.events(f"?after={len(expected)}")
            self.assertEqual(tail, {"events": [], "next_after": len(expected),
                                    "has_more": False})

            # New commits continue right after the largest seq; no repeat or gap.
            _, body = server.put_blob(b"after restart")
            status, tail = server.events(f"?after={len(expected)}")
            event = tail["events"][0]
            self.assertEqual(event["seq"], len(expected) + 1)
            self.assertEqual(event["op"], "blob_put")
            self.assertEqual(event["result"]["digest"], body["digest"])
        finally:
            server.stop()

    def test_pagination_semantics_unchanged_after_recovery(self) -> None:
        server = RunningServer(self.path)
        try:
            for text in (b"one", b"two", b"three"):
                server.put_blob(text)
        finally:
            server.stop()

        server = RunningServer(self.path)
        try:
            status, first = server.events("?after=0&limit=2")
            self.assertEqual(status, 200)
            self.assertEqual([e["seq"] for e in first["events"]], [1, 2])
            self.assertEqual(first["next_after"], 2)
            self.assertTrue(first["has_more"])
            status, second = server.events("?after=2&limit=2")
            self.assertEqual([e["seq"] for e in second["events"]], [3])
            self.assertEqual(second["next_after"], 3)
            self.assertFalse(second["has_more"])
        finally:
            server.stop()

    def test_failed_requests_leave_no_events_before_or_after_restart(self) -> None:
        server = RunningServer(self.path, max_store_bytes=10)
        try:
            _, body = server.put_blob(b"ok")  # 2 bytes, fits
            digest = body["digest"]
            # 400: empty body; 404: unknown blob; 409: release the only ref twice.
            self.assertEqual(server.call("PUT", "/v1/blobs", b"")[0], 400)
            self.assertEqual(server.call("GET", f"/v1/blobs/{'a' * 64}")[0], 404)
            self.assertEqual(server.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
            self.assertEqual(server.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 409)
            # 413: quota rejection; 502: unreachable mirror.
            self.assertEqual(server.put_blob(b"x" * 11)[0], 413)
            status, _ = server.call("POST", "/v1/mirror/pull", json.dumps({
                "base_url": "http://127.0.0.1:1",
                "digests": ["a" * 64],
            }).encode())
            self.assertEqual(status, 502)
        finally:
            server.stop()

        server = RunningServer(self.path, max_store_bytes=10)
        try:
            _, page = server.events("?limit=100")
            self.assertEqual([(e["seq"], e["op"]) for e in page["events"]],
                             [(1, "blob_put"), (2, "blob_release")])
        finally:
            server.stop()

    def test_audit_path_alone_persists_history_without_blob_or_session_state(self) -> None:
        server = RunningServer(self.path)
        try:
            _, body = server.put_blob(b"lonely")
            digest = body["digest"]
            status, raw = server.call("POST", "/v1/uploads",
                                      json.dumps({"size": 1, "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
        finally:
            server.stop()

        # Restart with only the audit path: blobs and sessions are gone, but the
        # audit history is complete and new events continue its sequence.
        server = RunningServer(self.path)
        try:
            self.assertEqual(server.call("GET", f"/v1/blobs/{digest}")[0], 404)
            self.assertEqual(server.call("GET", f"/v1/uploads/{upload_id}")[0], 404)
            _, page = server.events("?limit=100")
            self.assertEqual([(e["seq"], e["op"]) for e in page["events"]],
                             [(1, "blob_put"), (2, "upload_create")])
            self.assertEqual(page["events"][1]["result"]["upload_id"], upload_id)
            self.assertEqual(server.put_blob(b"later")[0], 201)
            _, page = server.events("?after=2")
            self.assertEqual([e["seq"] for e in page["events"]], [3])
        finally:
            server.stop()

    def test_all_three_state_paths_restore_together(self) -> None:
        store_path = os.path.join(self.tmp.name, "store.json")
        upload_path = os.path.join(self.tmp.name, "uploads.json")
        payload = b"triple state"
        server = RunningServer(self.path, store_path=store_path, upload_path=upload_path)
        try:
            status, raw = server.call("POST", "/v1/uploads",
                                      json.dumps({"size": len(payload),
                                                  "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            server.call("PUT", f"/v1/uploads/{upload_id}", payload,
                        {"X-Upload-Offset": "0"})
            self.assertEqual(
                server.call("POST", f"/v1/uploads/{upload_id}/complete", b"")[0], 201)
        finally:
            server.stop()

        server = RunningServer(self.path, store_path=store_path, upload_path=upload_path)
        try:
            status, raw = server.call("GET", f"/v1/uploads/{upload_id}")
            self.assertEqual(json.loads(raw)["status"], "committed")
            self.assertEqual(server.call("GET", f"/v1/blobs/{D(payload)}")[0], 200)
            _, page = server.events("?limit=100")
            self.assertEqual([e["op"] for e in page["events"]],
                             ["upload_create", "upload_append", "upload_complete"])
        finally:
            server.stop()

    def test_missing_file_starts_empty_and_is_created_on_first_commit(self) -> None:
        self.assertFalse(os.path.exists(self.path))
        server = RunningServer(self.path)
        try:
            self.assertEqual(server.events()[1]["events"], [])
            self.assertEqual(server.put_blob(b"first")[0], 201)
            self.assertTrue(os.path.exists(self.path))
        finally:
            server.stop()

    def test_concurrent_commits_keep_a_single_continuous_durable_sequence(self) -> None:
        server = RunningServer(self.path)
        try:
            payloads = [f"blob-{i}".encode() for i in range(20)]

            def put(payload: bytes) -> None:
                status, _ = server.put_blob(payload)
                self.assertEqual(status, 201)

            threads = [threading.Thread(target=put, args=(payload,)) for payload in payloads]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            server.stop()

        server = RunningServer(self.path)
        try:
            _, page = server.events("?limit=100")
            events = page["events"]
            self.assertEqual([e["seq"] for e in events], list(range(1, 21)))
            self.assertEqual({e["result"]["digest"] for e in events},
                             {D(payload) for payload in payloads})
            self.assertEqual(len({e["seq"] for e in events}), 20)
        finally:
            server.stop()


class AuditStateValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_state(self, content: bytes | str | None) -> str:
        path = os.path.join(self.tmp.name, "audit.json")
        if content is not None:
            mode = "wb" if isinstance(content, bytes) else "w"
            with open(path, mode) as handle:
                handle.write(content)
        return path

    def assert_invalid(self, content: bytes | str) -> None:
        path = self.write_state(content)
        with self.assertRaises(AuditStateInvalid):
            serve(port=0, audit_state=path).server_close()

    @staticmethod
    def event(seq: int, op: str = "blob_put", result: object = None) -> dict:
        return {"seq": seq, "op": op,
                "result": result if result is not None else {"digest": "a" * 64}}

    def test_empty_file_is_invalid(self) -> None:
        self.assert_invalid(b"")

    def test_corrupt_json_is_invalid(self) -> None:
        self.assert_invalid(b"{not json")
        self.assert_invalid(b"\xff\xfe not utf8")

    def test_wrong_shape_is_invalid(self) -> None:
        self.assert_invalid(json.dumps([]))
        self.assert_invalid(json.dumps({}))
        self.assert_invalid(json.dumps({"version": 1}))
        self.assert_invalid(json.dumps({"events": []}))
        self.assert_invalid(json.dumps({"version": 1, "events": [], "extra": 1}))
        self.assert_invalid(json.dumps({"version": "1", "events": []}))
        self.assert_invalid(json.dumps({"version": 1, "events": {}}))

    def test_wrong_version_is_invalid(self) -> None:
        self.assert_invalid(json.dumps({"version": 2, "events": []}))
        self.assert_invalid(json.dumps({"version": 0, "events": []}))
        self.assert_invalid(json.dumps({"version": True, "events": []}))

    def test_bad_event_entries_are_invalid(self) -> None:
        good_doc = {"version": 1, "events": [self.event(1)]}
        bad_docs: list[object] = [
            {"version": 1, "events": [[]]},
            {"version": 1, "events": [{}]},
            {"version": 1, "events": [{"seq": 1, "op": "blob_put"}]},
            {"version": 1, "events": [{"seq": 1, "op": "blob_put",
                                       "result": {}, "extra": 1}]},
            {"version": 1, "events": [self.event("1")]},
            {"version": 1, "events": [self.event(0)]},
            {"version": 1, "events": [self.event(2)]},
            {"version": 1, "events": [self.event(1, op="")]},
            {"version": 1, "events": [self.event(1, op="nonsense")]},
            {"version": 1, "events": [self.event(1, result=[])]},
            # The sequence must be continuous: duplicates and gaps are rejected.
            {"version": 1, "events": [self.event(1), self.event(1)]},
            {"version": 1, "events": [self.event(1), self.event(3)]},
            {"version": 1, "events": [self.event(1), self.event(2), self.event(2)]},
        ]
        for doc in bad_docs:
            self.assert_invalid(json.dumps(doc))
        # Sanity: the matching good document loads.
        path = self.write_state(json.dumps(good_doc))
        server = serve(port=0, audit_state=path)
        try:
            self.assertEqual([e["seq"] for e in server.audit.query(0, 50)["events"]], [1])
        finally:
            server.server_close()

    def test_cli_exits_nonzero_with_stderr_message(self) -> None:
        path = self.write_state(b"broken")
        proc = subprocess.run(
            [sys.executable, "-m", "artifacts.app", "--port", str(_free_port()),
             "--audit-state", path],
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "src"},
            capture_output=True, text=True, timeout=20)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stderr.strip(), f"audit state invalid: {path}")
        self.assertEqual(proc.stdout, "")


class AuditWriteFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.audit_path = os.path.join(self.tmp.name, "audit.json")
        self.store_path = os.path.join(self.tmp.name, "store.json")
        self.upload_path = os.path.join(self.tmp.name, "uploads.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def break_audit_save(self, server: RunningServer) -> None:
        def fail(events) -> None:
            raise OSError("simulated audit disk failure")
        server.server.store.audit._state.save = fail  # type: ignore[method-assign]

    def heal_audit_save(self, server: RunningServer) -> None:
        del server.server.store.audit._state.save  # type: ignore[attr-defined]

    def test_audit_only_failure_returns_fixed_500_and_confirms_nothing(self) -> None:
        server = RunningServer(self.audit_path)
        try:
            self.assertEqual(server.put_blob(b"grounded")[0], 201)
            self.break_audit_save(server)
            status, raw = server.call("PUT", "/v1/blobs", b"unconfirmed")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "audit write failed"}})
            self.heal_audit_save(server)
            # The failed change was rolled back in memory.
            self.assertEqual(server.call("GET", f"/v1/blobs/{D(b'unconfirmed')}")[0], 404)
            # No event was recorded: the next success continues at seq 2.
            _, page = server.events("?limit=100")
            self.assertEqual([e["seq"] for e in page["events"]], [1])
            self.assertEqual(server.put_blob(b"after")[0], 201)
            _, page = server.events("?after=1")
            self.assertEqual([e["seq"] for e in page["events"]], [2])
        finally:
            server.stop()
        # Restart confirms the file never held the rejected event.
        server = RunningServer(self.audit_path)
        try:
            _, page = server.events("?limit=100")
            self.assertEqual([e["result"]["digest"] for e in page["events"]],
                             [D(b"grounded"), D(b"after")])
        finally:
            server.stop()

    def test_store_state_is_rolled_back_to_request_start_snapshot(self) -> None:
        server = RunningServer(self.audit_path, store_path=self.store_path)
        try:
            self.assertEqual(server.put_blob(b"durable")[0], 201)
            self.break_audit_save(server)
            try:
                status, raw = server.call("PUT", "/v1/blobs", b"ghost")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(raw)["error"]["message"], "audit write failed")
                # A repeat PUT of the existing digest also fails without bumping refs.
                self.assertEqual(server.call("PUT", "/v1/blobs", b"durable")[0], 500)
            finally:
                self.heal_audit_save(server)
            # Memory, store file and audit file all show only the first commit.
            self.assertEqual(server.call("GET", f"/v1/blobs/{D(b'ghost')}")[0], 404)
            restored = StoreState(self.store_path).load()
            self.assertEqual([(d, data, refs) for d, data, _mt, refs in restored],
                             [(D(b"durable"), b"durable", 1)])
            events = AuditState(self.audit_path).load()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["result"]["refs"], 1)
        finally:
            server.stop()

    def test_failed_release_and_gc_confirm_nothing(self) -> None:
        server = RunningServer(self.audit_path, store_path=self.store_path)
        try:
            _, body = server.put_blob(b"pinned")
            digest = body["digest"]
            self.break_audit_save(server)
            try:
                self.assertEqual(server.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 500)
                self.assertEqual(server.call("POST", "/v1/gc", b"")[0], 500)
            finally:
                self.heal_audit_save(server)
            status, raw = server.call("GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
            self.assertEqual(StoreState(self.store_path).load()[0][3], 1)
            self.assertEqual(len(AuditState(self.audit_path).load()), 1)
        finally:
            server.stop()

    def test_upload_create_failure_restores_upload_state_and_skips_event(self) -> None:
        server = RunningServer(self.audit_path, upload_path=self.upload_path)
        try:
            self.break_audit_save(server)
            try:
                status, raw = server.call("POST", "/v1/uploads",
                                          json.dumps({"size": 1,
                                                      "media_type": "text/plain"}).encode())
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(raw)["error"]["message"], "audit write failed")
            finally:
                self.heal_audit_save(server)
            # No session ever confirmed: the upload state file stays absent and
            # the audit history stays empty.
            self.assertFalse(os.path.exists(self.upload_path))
            self.assertEqual(AuditState(self.audit_path).load(), [])
        finally:
            server.stop()

    def test_failed_complete_with_all_three_states_restores_everything(self) -> None:
        payload = b"atomic boundary"
        server = RunningServer(self.audit_path, store_path=self.store_path,
                               upload_path=self.upload_path)
        try:
            status, raw = server.call("POST", "/v1/uploads",
                                      json.dumps({"size": len(payload),
                                                  "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            server.call("PUT", f"/v1/uploads/{upload_id}", payload,
                        {"X-Upload-Offset": "0"})
            self.break_audit_save(server)
            try:
                status, raw = server.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(raw)["error"]["message"], "audit write failed")
            finally:
                self.heal_audit_save(server)
            # Nothing confirmed on any side: no blob, session still uncommitted
            # with its bytes, no upload_complete event.
            self.assertEqual(server.call("GET", f"/v1/blobs/{D(payload)}")[0], 404)
            self.assertEqual(StoreState(self.store_path).load(), [])
            session = UploadState(self.upload_path).load()[upload_id]
            self.assertFalse(session.committed)
            self.assertEqual(b"".join(session.chunks), payload)
            ops = [e["op"] for e in AuditState(self.audit_path).load()]
            self.assertEqual(ops, ["upload_create", "upload_append"])
            # A retry once the audit path heals completes exactly once.
            self.assertEqual(
                server.call("POST", f"/v1/uploads/{upload_id}/complete", b"")[0], 201)
        finally:
            server.stop()

    def test_unwritable_audit_directory_makes_commits_fail_with_fixed_500(self) -> None:
        bad_directory = tempfile.mkdtemp(dir=self.tmp.name)
        os.rmdir(bad_directory)
        from artifacts import AuditLog, AuditState as _AuditState, Store
        store = Store(audit=AuditLog(state=_AuditState(
            os.path.join(bad_directory, "nested", "audit.json"))))
        server = RunningServer.with_store(store)
        try:
            status, raw = server.call("PUT", "/v1/blobs", b"data")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "audit write failed"}})
            self.assertEqual(store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.stop()


class NoAuditStatePathTests(unittest.TestCase):
    """Without --audit-state the trail stays purely in-memory, as before."""

    def test_serve_without_audit_state_has_no_backing_state(self) -> None:
        server = serve(port=0)
        try:
            self.assertIsNone(server.store.audit._state)
            server.store.put(b"ephemeral")
            self.assertEqual(server.store.audit.query(0, 50)["events"][0]["seq"], 1)
        finally:
            server.server_close()
        # A fresh process-equivalent starts the sequence back at 1.
        server = serve(port=0)
        try:
            self.assertEqual(server.store.audit.query(0, 50)["events"], [])
            server.store.put(b"ephemeral")
            self.assertEqual(server.store.audit.query(0, 50)["events"][0]["seq"], 1)
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main()
