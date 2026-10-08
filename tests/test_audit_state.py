"""Optional on-disk persistence for the audit history (--audit-state)."""
from __future__ import annotations

import http.client
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

from artifacts import (
    AuditError,
    AuditState,
    AuditStateInvalid,
    StoreState,
    UploadState,
    digest_of,
    serve,
)

D = digest_of  # bytes -> 64-char lowercase hex


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RunningServer:
    """A server whose lifecycle a test can cycle against state paths."""

    def __init__(self, audit_path: str | None = None,
                 store_path: str | None = None,
                 upload_path: str | None = None,
                 max_store_bytes: int | None = None) -> None:
        kwargs = {"port": 0, "audit_state": audit_path,
                  "store_state": store_path, "upload_state": upload_path}
        if max_store_bytes is not None:
            kwargs["max_store_bytes"] = max_store_bytes
        self.server = serve(**kwargs)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

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

    def put_blob(self, payload: bytes, media_type: str | None = None) -> tuple[int, dict]:
        headers = {"Content-Type": media_type} if media_type is not None else {}
        status, raw = self.call("PUT", "/v1/blobs", payload, headers)
        return status, json.loads(raw)

    def events(self, query: str = "", limit: int | None = None) -> dict:
        if limit is not None:
            query = f"limit={limit}" + (f"&{query}" if query else "")
        suffix = f"?{query}" if query else ""
        status, raw = self.call("GET", f"/v1/events{suffix}")
        assert status == 200, (status, raw)
        return json.loads(raw)


def create_upload(server: RunningServer, size: int, media_type: str = "text/plain") -> str:
    status, raw = server.call("POST", "/v1/uploads",
                              json.dumps({"size": size, "media_type": media_type}).encode())
    assert status == 201, (status, raw)
    return json.loads(raw)["upload_id"]


class AuditRestartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "audit.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_full_history_replays_and_seq_continues_after_restart(self) -> None:
        server = RunningServer(audit_path=self.path)
        try:
            _, first = server.put_blob(b"alpha", "text/plain")
            upload_id = create_upload(server, 5)
            self.assertEqual(server.call(
                "PUT", f"/v1/uploads/{upload_id}", b"alpha",
                {"X-Upload-Offset": "0"})[0], 200)
            self.assertEqual(server.call(
                "POST", f"/v1/uploads/{upload_id}/complete", b"")[0], 201)
            self.assertEqual(server.call(
                "DELETE", f"/v1/blobs/{first['digest']}/refs")[0], 200)
            self.assertEqual(server.call(
                "DELETE", f"/v1/blobs/{first['digest']}/refs")[0], 200)
            status, raw = server.call("POST", "/v1/gc", b"")
            self.assertEqual(status, 200)
            ops = [e["op"] for e in server.events()["events"]]
            self.assertEqual(ops, [
                "blob_put", "upload_create", "upload_append",
                "upload_complete", "blob_release", "blob_release", "gc",
            ])
            self.assertEqual([e["seq"] for e in server.events()["events"]],
                             list(range(1, 8)))
        finally:
            server.stop()

        # Same path: the whole history replays; the next event continues at max seq + 1.
        server = RunningServer(audit_path=self.path)
        try:
            page = server.events()
            self.assertEqual([e["seq"] for e in page["events"]], list(range(1, 8)))
            self.assertEqual(page["next_after"], 7)
            self.assertFalse(page["has_more"])
            self.assertEqual(page["events"][0]["result"]["digest"], D(b"alpha"))
            _, body = server.put_blob(b"beta")
            page = server.events("after=7")
            self.assertEqual(len(page["events"]), 1)
            self.assertEqual(page["events"][0]["seq"], 8)
            self.assertEqual(page["events"][0]["op"], "blob_put")
            self.assertEqual(page["events"][0]["result"]["digest"], body["digest"])
        finally:
            server.stop()

        # And again: 8 events survive the second restart too, seq continues at 9.
        server = RunningServer(audit_path=self.path)
        try:
            self.assertEqual(len(server.events(limit=100)["events"]), 8)
            server.put_blob(b"gamma")
            self.assertEqual(server.events("after=8")["events"][0]["seq"], 9)
        finally:
            server.stop()

    def test_pagination_semantics_unchanged_after_restore(self) -> None:
        server = RunningServer(audit_path=self.path)
        try:
            for index in range(5):
                server.put_blob(f"blob-{index}".encode())
        finally:
            server.stop()
        server = RunningServer(audit_path=self.path)
        try:
            page = server.events("after=0&limit=2")
            self.assertEqual([e["seq"] for e in page["events"]], [1, 2])
            self.assertEqual(page["next_after"], 2)
            self.assertTrue(page["has_more"])
            page = server.events("after=4&limit=10")
            self.assertEqual([e["seq"] for e in page["events"]], [5])
            self.assertEqual(page["next_after"], 5)
            self.assertFalse(page["has_more"])
            page = server.events("after=5")
            self.assertEqual(page["events"], [])
            self.assertEqual(page["next_after"], 5)
            self.assertFalse(page["has_more"])
        finally:
            server.stop()

    def test_audit_only_keeps_events_when_blobs_are_gone(self) -> None:
        # --audit-state alone: blobs remain in-memory and disappear on restart,
        # but the audit history survives unchanged.
        server = RunningServer(audit_path=self.path)
        try:
            server.put_blob(b"memory only")
        finally:
            server.stop()
        server = RunningServer(audit_path=self.path)
        try:
            status, _ = server.call("GET", f"/v1/blobs/{D(b'memory only')}")
            self.assertEqual(status, 404)
            events = server.events()["events"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["op"], "blob_put")
        finally:
            server.stop()

    def test_missing_file_starts_with_empty_history(self) -> None:
        missing = os.path.join(self.tmp.name, "absent.json")
        server = serve(port=0, audit_state=missing)
        try:
            self.assertEqual(server.audit.query(0, 50)["events"], [])
        finally:
            server.server_close()

    def test_all_three_states_replay_together(self) -> None:
        upload_path = os.path.join(self.tmp.name, "uploads.json")
        store_path = os.path.join(self.tmp.name, "store.json")
        payload = b"triple state payload"
        server = RunningServer(audit_path=self.path, store_path=store_path,
                               upload_path=upload_path)
        try:
            upload_id = create_upload(server, len(payload))
            self.assertEqual(server.call(
                "PUT", f"/v1/uploads/{upload_id}", payload,
                {"X-Upload-Offset": "0"})[0], 200)
            self.assertEqual(server.call(
                "POST", f"/v1/uploads/{upload_id}/complete", b"")[0], 201)
        finally:
            server.stop()
        server = RunningServer(audit_path=self.path, store_path=store_path,
                               upload_path=upload_path)
        try:
            events = server.events(limit=100)["events"]
            self.assertEqual([e["op"] for e in events],
                             ["upload_create", "upload_append", "upload_complete"])
            self.assertEqual([e["seq"] for e in events], [1, 2, 3])
            status, raw = server.call("GET", f"/v1/blobs/{D(payload)}")
            self.assertEqual((status, raw), (200, payload))
            status, raw = server.call("GET", f"/v1/uploads/{upload_id}")
            self.assertEqual(json.loads(raw)["status"], "committed")
            # The next commit carries seq 4 across all three files atomically.
            server.put_blob(b"after restart")
            self.assertEqual(server.events("after=3")["events"][0]["seq"], 4)
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

    def assert_invalid(self, content: bytes | str | None) -> None:
        path = self.write_state(content)
        with self.assertRaises(AuditStateInvalid):
            serve(port=0, audit_state=path).server_close()

    @staticmethod
    def event(seq: int = 1, op: str = "gc", result: object = None) -> dict:
        return {"seq": seq, "op": op, "result": result if result is not None else {}}

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
        good = self.event()
        bad_docs: list[object] = [
            {"version": 1, "events": [[]]},
            {"version": 1, "events": [{}]},
            {"version": 1, "events": [{**good, "extra": 1}]},
            {"version": 1, "events": [{k: v for k, v in good.items() if k != "op"}]},
            {"version": 1, "events": [{**good, "seq": 0}]},
            {"version": 1, "events": [{**good, "seq": "1"}]},
            {"version": 1, "events": [{**good, "seq": True}]},
            {"version": 1, "events": [{**good, "seq": 2}]},
            {"version": 1, "events": [self.event(1), self.event(1)]},
            {"version": 1, "events": [self.event(1), self.event(3)]},
            {"version": 1, "events": [{**good, "op": ""}]},
            {"version": 1, "events": [{**good, "op": "bogus_op"}]},
            {"version": 1, "events": [{**good, "op": 5}]},
            {"version": 1, "events": [{**good, "result": []}]},
            {"version": 1, "events": [{**good, "result": "ok"}]},
        ]
        for doc in bad_docs:
            self.assert_invalid(json.dumps(doc))

    def test_empty_event_list_is_valid(self) -> None:
        path = self.write_state(json.dumps({"version": 1, "events": []}))
        server = serve(port=0, audit_state=path)
        try:
            self.assertEqual(server.audit.query(0, 50)["events"], [])
            server.store.put(b"x")
            events = server.audit.query(0, 50)["events"]
            self.assertEqual([event["seq"] for event in events], [1])
        finally:
            server.server_close()

    def test_consecutive_history_is_restored(self) -> None:
        doc = {"version": 1, "events": [
            self.event(1, "blob_put", {"digest": "a" * 64, "size": 1,
                                      "media_type": "text/plain", "refs": 1}),
            self.event(2, "gc", {"deleted": [],
                                 "stats": {"blobs": 0, "bytes": 0, "puts": 0}}),
        ]}
        path = self.write_state(json.dumps(doc))
        server = serve(port=0, audit_state=path)
        try:
            events = server.audit.query(0, 50)["events"]
            self.assertEqual([event["seq"] for event in events], [1, 2])
            server.store.put(b"y")
            self.assertEqual(server.audit.query(2, 50)["events"][0]["seq"], 3)
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
        self.server = RunningServer(audit_path=self.audit_path)

    def tearDown(self) -> None:
        self.server.stop()
        self.tmp.cleanup()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        return self.server.call(method, path, data, headers)

    def break_audit_save(self) -> None:
        def fail(events):
            raise AuditError()
        self.server.server.audit._state.save = fail  # type: ignore[method-assign]

    def heal_audit_save(self) -> None:
        del self.server.server.audit._state.save  # type: ignore[attr-defined]

    def test_unwritable_audit_path_makes_put_return_500_exact_body(self) -> None:
        bad_directory = tempfile.mkdtemp(dir=self.tmp.name)
        os.rmdir(bad_directory)
        server = RunningServer(audit_path=os.path.join(bad_directory, "nested", "audit.json"))
        try:
            status, raw = server.call("PUT", "/v1/blobs", b"data")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "audit write failed"}})
            self.assertEqual(server.server.store.stats(),
                             {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.stop()

    def test_failed_audit_append_rolls_put_back_and_keeps_seq_free(self) -> None:
        status, _ = self.server.put_blob(b"first")
        self.assertEqual(status, 201)
        self.break_audit_save()
        try:
            status, raw = self.call("PUT", "/v1/blobs", b"second")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "audit write failed"}})
        finally:
            self.heal_audit_save()
        # The mutation was not confirmed: no blob, no event, disk unchanged.
        self.assertEqual(self.call("GET", f"/v1/blobs/{D(b'second')}")[0], 404)
        events = self.server.events(limit=100)["events"]
        self.assertEqual([event["op"] for event in events], ["blob_put"])
        on_disk = AuditState(self.audit_path).load()
        self.assertEqual(len(on_disk), 1)
        # A retry after healing reuses the unconsumed seq: no gap.
        self.assertEqual(self.call("PUT", "/v1/blobs", b"second")[0], 201)
        events = self.server.events(limit=100)["events"]
        self.assertEqual([event["seq"] for event in events], [1, 2])

    def test_failed_audit_append_rolls_store_and_upload_states_back(self) -> None:
        upload_path = os.path.join(self.tmp.name, "uploads.json")
        store_path = os.path.join(self.tmp.name, "store.json")
        self.server.stop()
        self.server = RunningServer(audit_path=self.audit_path, store_path=store_path,
                                    upload_path=upload_path)
        self.server.put_blob(b"pinned")
        digest = D(b"pinned")
        self.break_audit_save()
        try:
            # A release that cannot record its event is not confirmed anywhere.
            status, raw = self.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual((status, json.loads(raw)),
                             (500, {"error": {"code": "internal_error",
                                              "message": "audit write failed"}}))
            # An upload create is likewise fully rolled back.
            status, raw = self.call("POST", "/v1/uploads",
                                    json.dumps({"size": 3, "media_type": "text/plain"}).encode())
            self.assertEqual(status, 500)
        finally:
            self.heal_audit_save()
        # Memory: refs unchanged; no session.
        listing = json.loads(self.call("GET", "/v1/blobs")[1])
        self.assertEqual(listing["blobs"][0]["refs"], 1)
        # Store state file restored to the pre-request snapshot.
        restored = StoreState(store_path).load()
        self.assertEqual([(d, refs) for d, _b, _mt, refs in restored], [(digest, 1)])
        # Upload state file never gained the session.
        self.assertEqual(UploadState(upload_path).load(), {})
        # Audit file holds only the original successful put.
        self.assertEqual([event["op"] for event in AuditState(self.audit_path).load()],
                         ["blob_put"])
        # After healing, the next confirmed event is still seq 2 (no hole).
        self.assertEqual(self.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
        self.assertEqual(self.server.events("after=1")["events"][0]["seq"], 2)

    def test_failed_complete_with_three_states_creates_nothing(self) -> None:
        upload_path = os.path.join(self.tmp.name, "uploads.json")
        store_path = os.path.join(self.tmp.name, "store.json")
        self.server.stop()
        self.server = RunningServer(audit_path=self.audit_path, store_path=store_path,
                                    upload_path=upload_path)
        payload = b"complete under broken audit"
        upload_id = create_upload(self.server, len(payload))
        self.call("PUT", f"/v1/uploads/{upload_id}", payload, {"X-Upload-Offset": "0"})
        self.break_audit_save()
        try:
            status, raw = self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw)["error"]["message"], "audit write failed")
        finally:
            self.heal_audit_save()
        # No blob in memory or on disk; the session stays uncommitted and resumable.
        self.assertEqual(self.call("GET", f"/v1/blobs/{D(payload)}")[0], 404)
        self.assertEqual(StoreState(store_path).load(), [])
        session = UploadState(upload_path).load()[upload_id]
        self.assertFalse(session.committed)
        self.assertEqual(b"".join(session.chunks), payload)
        # Retried complete succeeds and records the next contiguous seq.
        self.assertEqual(self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")[0], 201)
        ops = [event["op"] for event in self.server.events(limit=100)["events"]]
        self.assertEqual(ops, ["upload_create", "upload_append", "upload_complete"])


class AuditContentTests(unittest.TestCase):
    """Only confirmed successes produce events; ops/results keep their baseline shape."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "audit.json")
        self.server = RunningServer(audit_path=self.path)

    def tearDown(self) -> None:
        self.server.stop()
        self.tmp.cleanup()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        return self.server.call(method, path, data, headers)

    def ops(self) -> list[str]:
        return [event["op"] for event in self.server.events(limit=100)["events"]]

    def test_failures_quota_and_mirror_errors_record_nothing(self) -> None:
        server = RunningServer(audit_path=os.path.join(self.tmp.name, "quota.json"),
                               max_store_bytes=10)
        try:
            self.assertEqual(server.call("PUT", "/v1/blobs", b"")[0], 400)
            self.assertEqual(server.call("PUT", "/v1/blobs", b"x",
                                         {"X-Blob-Digest": "a" * 64})[0], 409)
            self.assertEqual(server.call("DELETE", f"/v1/blobs/{'a' * 64}/refs")[0], 404)
            self.assertEqual(server.call("GET", f"/v1/blobs/{'b' * 64}")[0], 404)
            self.assertEqual(server.call("PUT", "/v1/blobs", b"z" * 20)[0], 413)
            # A mirror pointed at a dead port fails at transport: 502, no event.
            body = json.dumps({"base_url": f"http://127.0.0.1:{_free_port()}",
                               "digests": ["c" * 64]}).encode()
            self.assertEqual(server.call("POST", "/v1/mirror/pull", body)[0], 502)
            # Unknown route and malformed events query are not audited either.
            self.assertEqual(server.call("GET", "/v1/nope")[0], 404)
            self.assertEqual(server.call("GET", "/v1/events?limit=0")[0], 400)
            self.assertEqual(server.events()["events"], [])
        finally:
            server.stop()

    def test_successful_mirror_pull_records_one_event(self) -> None:
        remote = RunningServer(audit_path=os.path.join(self.tmp.name, "remote.json"))
        try:
            _, one = remote.put_blob(b"mirror one", "text/plain")
            _, two = remote.put_blob(b"mirror two")
            digests = sorted([one["digest"], two["digest"], D(b"absent")])
            status, raw = self.call("POST", "/v1/mirror/pull", json.dumps({
                "base_url": f"http://127.0.0.1:{remote.port}",
                "digests": digests,
            }).encode())
            self.assertEqual(status, 200)
            body = json.loads(raw)
        finally:
            remote.stop()
        events = self.server.events(limit=100)["events"]
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["seq"], 1)
        self.assertEqual(event["op"], "mirror_pull")
        self.assertEqual(event["result"], body)

    def test_upload_lifecycle_events_match_contract(self) -> None:
        upload_id = create_upload(self.server, 5)
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", b"hello",
                                   {"X-Upload-Offset": "0"})[0], 200)
        # A bad append (offset mismatch) produces no event.
        self.assertEqual(self.call("PUT", f"/v1/uploads/{upload_id}", b"zz",
                                   {"X-Upload-Offset": "9"})[0], 409)
        # An unfinished session cannot complete: 409, no event.
        partial = create_upload(self.server, 10)
        self.assertEqual(self.call("PUT", f"/v1/uploads/{partial}", b"five5",
                                   {"X-Upload-Offset": "0"})[0], 200)
        self.assertEqual(self.call("POST", f"/v1/uploads/{partial}/complete",
                                   b"")[0], 409)
        events = self.server.events(limit=100)["events"]
        self.assertEqual([e["op"] for e in events],
                         ["upload_create", "upload_append", "upload_create", "upload_append"])
        first = events[0]
        self.assertEqual(first["result"],
                         {"upload_id": upload_id, "size": 5,
                          "received": 0, "status": "uncommitted"})
        self.assertEqual(events[1]["result"],
                         {"upload_id": upload_id, "received": 5,
                          "status": "uncommitted"})
        # Now complete the finished session; deleting a committed session is 409 (no event).
        self.assertEqual(self.call("POST", f"/v1/uploads/{upload_id}/complete",
                                   b"")[0], 201)
        self.assertEqual(self.call("DELETE", f"/v1/uploads/{upload_id}")[0], 409)
        events = self.server.events(limit=100)["events"]
        complete_event = events[4]
        self.assertEqual(complete_event["op"], "upload_complete")
        self.assertEqual(complete_event["result"],
                         {"upload_id": upload_id, "digest": D(b"hello"),
                          "size": 5, "media_type": "text/plain"})
        # A fresh session deleted while uncommitted records upload_delete.
        other = create_upload(self.server, 3)
        self.assertEqual(self.call("DELETE", f"/v1/uploads/{other}")[0], 204)
        events = self.server.events(limit=100)["events"]
        self.assertEqual(events[-2]["op"], "upload_create")
        self.assertEqual(events[-1], {"seq": events[-1]["seq"], "op": "upload_delete",
                                      "result": {"upload_id": other}})


class AuditConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "audit.json")
        self.server = RunningServer(audit_path=self.path)

    def tearDown(self) -> None:
        self.server.stop()
        self.tmp.cleanup()

    def test_concurrent_commits_get_contiguous_seqs_in_commit_order(self) -> None:
        count = 32
        # Pre-establish every connection: a ThreadingHTTPServer hands each
        # accepted socket its own handler thread (which blocks reading), so
        # the barrier-released sends do not pile onto the listen backlog and
        # no request is retried after a possible commit.
        connections = [http.client.HTTPConnection("127.0.0.1", self.server.port)
                       for _ in range(count)]
        for connection in connections:
            connection.connect()
        barrier = threading.Barrier(count)
        statuses: list[int] = []
        digests: list[str] = []
        lock = threading.Lock()

        def worker(index: int, connection: http.client.HTTPConnection) -> None:
            data = f"concurrent-{index}".encode()
            barrier.wait()
            connection.request("PUT", "/v1/blobs", body=data,
                               headers={"Content-Type": "application/octet-stream"})
            response = connection.getresponse()
            body = json.loads(response.read())
            with lock:
                statuses.append(response.status)
                digests.append(body["digest"])
            connection.close()

        threads = [threading.Thread(target=worker, args=(i, connection))
                   for i, connection in enumerate(connections)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(statuses, [201] * count)
        self.assertEqual(len(set(digests)), count)
        events = self.server.events(limit=100)["events"]
        self.assertEqual([event["seq"] for event in events], list(range(1, count + 1)))
        self.assertEqual(len({event["result"]["digest"] for event in events}), count)
        # The persisted file tells the same story: nothing swapped or gapped.
        on_disk = AuditState(self.path).load()
        self.assertEqual([event["seq"] for event in on_disk], list(range(1, count + 1)))


class NoAuditStatePathTests(unittest.TestCase):
    """Without --audit-state the trail stays purely in-memory (baseline behavior)."""

    def test_serve_without_audit_state_has_no_backing_file(self) -> None:
        server = serve(port=0)
        try:
            self.assertIsNone(server.audit._state)
        finally:
            server.server_close()

    def test_memory_history_restarts_seq_at_one(self) -> None:
        server = serve(port=0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/v1/blobs", data=b"ephemeral", method="PUT")
            with urllib.request.urlopen(request) as response:
                self.assertEqual(response.status, 201)
            self.assertEqual(len(server.audit.query(0, 50)["events"]), 1)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()
        # A fresh server starts the sequence over: no state file was involved.
        server = serve(port=0)
        try:
            self.assertEqual(server.audit.query(0, 50)["events"], [])
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main()
