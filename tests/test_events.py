"""In-process audit trail: GET /v1/events."""
from __future__ import annotations

import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from artifacts import (
    AuditError,
    AuditLog,
    Store,
    digest_of,
    make_handler,
    serve,
)

D = digest_of  # bytes -> 64-char lowercase hex


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class BrokenAudit(AuditLog):
    """An audit trail whose event write always fails."""

    def record(self, op, result):
        raise AuditError()


class RunningServer:
    """A serve()-like server a test can stop and restart."""

    def __init__(self, store: Store | None = None, **serve_kwargs) -> None:
        if store is None:
            self.server = serve(port=0, **serve_kwargs)
        else:
            self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store))
            self.server.store = store
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
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method)
        for name, value in (headers or {}).items():
            request.add_header(name, value)
        try:
            with urllib.request.urlopen(request) as response:
                raw = response.read()
                return response.status, self._decode(raw)
        except urllib.error.HTTPError as error:
            return error.code, self._decode(error.read())

    @staticmethod
    def _decode(raw: bytes):
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw  # e.g. the raw bytes of GET /v1/blobs/{digest}

    def put_blob(self, data: bytes, media_type: str = "application/octet-stream"):
        return self.call("PUT", "/v1/blobs", data=data,
                         headers={"Content-Type": media_type})

    def events(self, query: str = ""):
        status, body = self.call("GET", f"/v1/events{query}")
        self.assert_ok(status)
        return body

    def assert_ok(self, status: int) -> None:
        if status >= 400:
            raise AssertionError(f"unexpected status {status}")


class EventsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = RunningServer()
        cls.port = cls.server.port

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def setUp(self) -> None:
        # Each test starts from a clean store and a clean trail.
        self.server.call("POST", "/v1/gc")
        status, body = self.server.call("GET", "/v1/blobs")
        for entry in body["blobs"]:
            for _ in range(entry["refs"]):
                self.server.call("DELETE", f"/v1/blobs/{entry['digest']}/refs")
        self.server.call("POST", "/v1/gc")
        # Drain the events the cleanup itself recorded (paging to the end).
        after = 0
        while True:
            status, body = self.server.call("GET", f"/v1/events?after={after}&limit=100")
            if not body["has_more"]:
                self._base = body["next_after"]
                break
            after = body["next_after"]

    def tearDown(self) -> None:
        pass

    def ops(self, query: str = "") -> list[dict]:
        status, body = self.server.call(
            "GET", f"/v1/events?after={self._base}{'&' + query if query else ''}")
        self.assertEqual(status, 200)
        return body["events"]

    def test_empty_trail_shape(self) -> None:
        status, body = self.server.call("GET", f"/v1/events?after={self._base}")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"events": [], "next_after": self._base, "has_more": False})

    def test_put_release_gc_events(self) -> None:
        status, blob = self.server.put_blob(b"hello")
        self.assertEqual(status, 201)
        digest = blob["digest"]
        self.server.put_blob(b"hello")  # repeat: refs 2, recorded as such
        self.server.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.server.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.server.call("POST", "/v1/gc")
        events = self.ops()
        self.assertEqual([e["op"] for e in events],
                         ["blob_put", "blob_put", "blob_release", "blob_release", "gc"])
        self.assertEqual([e["seq"] for e in events],
                         [self._base + i for i in range(1, 6)])
        self.assertEqual(events[0]["result"], {
            "digest": digest, "size": 5,
            "media_type": "application/octet-stream", "refs": 1})
        self.assertEqual(events[1]["result"]["refs"], 2)  # repeat records actual refs
        self.assertEqual(events[2]["result"], {"digest": digest, "refs": 1})
        self.assertEqual(events[3]["result"], {"digest": digest, "refs": 0})
        self.assertEqual(events[4]["result"]["deleted"], [digest])
        self.assertIn("stats", events[4]["result"])

    def test_upload_lifecycle_events(self) -> None:
        status, created = self.server.call(
            "POST", "/v1/uploads",
            data=json.dumps({"size": 3, "media_type": "text/plain"}).encode())
        self.assertEqual(status, 201)
        upload_id = created["upload_id"]
        self.server.call("PUT", f"/v1/uploads/{upload_id}", data=b"abc",
                         headers={"X-Upload-Offset": "0"})
        status, completed = self.server.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)
        # A second session that is created and then deleted.
        status, other = self.server.call(
            "POST", "/v1/uploads",
            data=json.dumps({"size": 1, "media_type": "text/plain"}).encode())
        self.assertEqual(status, 201)
        self.server.call("DELETE", f"/v1/uploads/{other['upload_id']}")
        events = self.ops()
        self.assertEqual([e["op"] for e in events],
                         ["upload_create", "upload_append", "upload_complete",
                          "upload_create", "upload_delete"])
        self.assertEqual(events[0]["result"], {
            "upload_id": upload_id, "size": 3, "received": 0, "status": "uncommitted"})
        self.assertEqual(events[1]["result"], {
            "upload_id": upload_id, "received": 3, "status": "uncommitted"})
        self.assertEqual(events[2]["result"], {
            "upload_id": upload_id, "digest": D(b"abc"), "size": 3,
            "media_type": "text/plain"})
        self.assertEqual(events[4]["result"], {"upload_id": other["upload_id"]})

    def test_read_only_and_failed_requests_not_recorded(self) -> None:
        self.server.put_blob(b"data")
        digest = D(b"data")
        before = len(self.ops())
        # Read-only requests.
        self.server.call("GET", f"/v1/blobs/{digest}")
        self.server.call("HEAD", f"/v1/blobs/{digest}")
        self.server.call("GET", "/v1/blobs")
        self.server.call("GET", "/v1/events")
        self.server.call("POST", "/v1/blobs/presence",
                         data=json.dumps({"digests": [digest]}).encode())
        # Failed requests of every recorded op kind.
        self.server.call("PUT", "/v1/blobs", data=b"")                       # 400
        self.server.call("PUT", "/v1/blobs", data=b"x" * (1048576 + 1))      # 400
        self.server.call("PUT", "/v1/blobs", data=b"data",
                         headers={"X-Blob-Digest": D(b"other")})             # 409
        self.server.call("DELETE", f"/v1/blobs/{D(b'ghost')}/refs")          # 404
        self.server.call("DELETE", "/v1/blobs/nothex/refs")                  # 400
        status, _ = self.server.call(
            "POST", "/v1/uploads", data=json.dumps({"size": 0, "media_type": "a/b"}).encode())
        self.assertEqual(status, 400)
        status, _ = self.server.call("POST", "/v1/uploads/" + "0" * 32 + "/complete")
        self.assertEqual(status, 404)
        self.server.call("PUT", "/v1/uploads/" + "0" * 32, data=b"x",
                         headers={"X-Upload-Offset": "0"})                   # 404
        self.server.call("DELETE", "/v1/uploads/" + "0" * 32)                # 404
        self.assertEqual(len(self.ops()), before)

    def test_pagination(self) -> None:
        for i in range(5):
            self.server.put_blob(bytes([i]))
        status, page = self.server.call("GET", f"/v1/events?after={self._base}&limit=2")
        self.assertEqual([e["seq"] for e in page["events"]],
                         [self._base + 1, self._base + 2])
        self.assertEqual(page["next_after"], self._base + 2)
        self.assertTrue(page["has_more"])
        status, page = self.server.call(
            "GET", f"/v1/events?after={page['next_after']}&limit=2")
        self.assertEqual([e["seq"] for e in page["events"]],
                         [self._base + 3, self._base + 4])
        self.assertTrue(page["has_more"])
        status, page = self.server.call(
            "GET", f"/v1/events?after={page['next_after']}&limit=2")
        self.assertEqual([e["seq"] for e in page["events"]], [self._base + 5])
        self.assertEqual(page["next_after"], self._base + 5)
        self.assertFalse(page["has_more"])
        # Empty page beyond the last event: next_after echoes after.
        status, page = self.server.call("GET", f"/v1/events?after={self._base + 5}")
        self.assertEqual(page, {"events": [], "next_after": self._base + 5,
                                "has_more": False})

    def test_query_param_validation(self) -> None:
        for query in ("?foo=1", "?after=1&after=2", "?limit=1&limit=2",
                      "?after=-1", "?after=1.5", "?after=abc", "?after=",
                      "?limit=0", "?limit=101", "?limit=abc", "?limit=", "?limit=1.5"):
            status, body = self.server.call("GET", f"/v1/events{query}")
            self.assertEqual(status, 400, query)
            self.assertEqual(body["error"]["code"], "invalid_request", query)
        # Legal boundaries.
        for query in ("", "?after=0", "?limit=1", "?limit=100", "?after=0007"):
            status, _ = self.server.call("GET", f"/v1/events{query}")
            self.assertEqual(status, 200, query)

    def test_concurrent_commits_do_not_tear(self) -> None:
        def worker(index: int) -> None:
            self.server.put_blob(bytes([index]) * 8)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20, 40)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        events = self.ops()
        self.assertEqual(len(events), 20)
        self.assertEqual([e["seq"] for e in events],
                         [self._base + i for i in range(1, 21)])
        self.assertTrue(all(e["op"] == "blob_put" for e in events))
        self.assertTrue(all(set(e) == {"seq", "op", "result"} for e in events))


class MirrorEventsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.remote = RunningServer()
        cls.local = RunningServer()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.remote.stop()
        cls.local.stop()

    def test_multi_digest_pull_records_one_event(self) -> None:
        self.remote.put_blob(b"alpha")
        self.remote.put_blob(b"beta")
        self.local.put_blob(b"alpha")  # already local -> existing
        status, pulled = self.local.call(
            "POST", "/v1/mirror/pull",
            data=json.dumps({
                "base_url": f"http://127.0.0.1:{self.remote.port}",
                "digests": [D(b"alpha"), D(b"beta"), D(b"ghost")],
            }).encode())
        self.assertEqual(status, 200)
        status, body = self.local.call("GET", "/v1/events")
        pulls = [e for e in body["events"] if e["op"] == "mirror_pull"]
        self.assertEqual(len(pulls), 1)  # one event for the whole batch
        self.assertEqual(pulls[0]["result"], {
            "synced": [D(b"beta")], "existing": [D(b"alpha")],
            "missing": [D(b"ghost")]})
        # A failed pull records nothing.
        status, _ = self.local.call(
            "POST", "/v1/mirror/pull",
            data=json.dumps({"base_url": "http://127.0.0.1:1",
                             "digests": [D(b"gamma")]}).encode())
        self.assertEqual(status, 502)
        status, body = self.local.call("GET", "/v1/events")
        self.assertEqual(len([e for e in body["events"] if e["op"] == "mirror_pull"]), 1)


class AuditRestartTest(unittest.TestCase):
    def test_restart_clears_trail_and_seq(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            store_path = f"{directory}/store.json"
            server = RunningServer(store_state=store_path)
            try:
                server.put_blob(b"persisted")
                status, body = server.call("GET", "/v1/events")
                self.assertEqual(len(body["events"]), 1)
            finally:
                server.stop()
            server = RunningServer(store_state=store_path)
            try:
                # The blob survived; the trail did not, and seq restarts at 1.
                status, _ = server.call("GET", f"/v1/blobs/{D(b'persisted')}")
                self.assertEqual(status, 200)
                status, body = server.call("GET", "/v1/events")
                self.assertEqual(body, {"events": [], "next_after": 0, "has_more": False})
                server.put_blob(b"fresh")
                status, body = server.call("GET", "/v1/events")
                self.assertEqual([e["seq"] for e in body["events"]], [1])
                # The persisted state file carries no audit data.
                with open(store_path, "rb") as handle:
                    state = json.load(handle)
                self.assertEqual(set(state), {"version", "blobs"})
            finally:
                server.stop()


class AuditFailureTest(unittest.TestCase):
    def test_failed_event_write_blocks_the_commit(self) -> None:
        store = Store(audit=BrokenAudit())
        server = RunningServer(store=store)
        try:
            status, body = server.put_blob(b"blocked")
            self.assertEqual(status, 500)
            self.assertEqual(body, {"error": {"code": "internal_error",
                                              "message": "audit write failed"}})
            # The change was not committed.
            status, _ = server.call("GET", f"/v1/blobs/{D(b'blocked')}")
            self.assertEqual(status, 404)
            status, body = server.call("GET", "/v1/events")
            self.assertEqual(body["events"], [])
            # Upload create is blocked the same way.
            status, body = server.call(
                "POST", "/v1/uploads",
                data=json.dumps({"size": 1, "media_type": "a/b"}).encode())
            self.assertEqual(status, 500)
            self.assertEqual(body["error"]["message"], "audit write failed")
        finally:
            server.stop()
        self.assertEqual(store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})

    def test_failed_event_write_rolls_back_release_and_gc(self) -> None:
        store = Store()
        digest = store.put(b"keep").digest
        server = RunningServer(store=store)
        try:
            status, _ = server.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual(status, 200)
            store._audit = BrokenAudit()  # reads resolve lazily; store ops use it too
            status, body = server.call("DELETE", f"/v1/blobs/{digest}/refs")
            # refs is already 0: the 409 fires before any audit write.
            self.assertEqual(status, 409)
            status, body = server.call("POST", "/v1/gc")
            self.assertEqual(status, 500)
            self.assertEqual(body["error"]["message"], "audit write failed")
            # The gc was rolled back: the refs-0 blob is still there.
            status, _ = server.call("GET", f"/v1/blobs/{digest}")
            self.assertEqual(status, 200)
        finally:
            server.stop()

    def test_failed_event_write_rolls_back_complete(self) -> None:
        import tempfile
        from artifacts import UploadManager, UploadState
        with tempfile.TemporaryDirectory() as directory:
            store_path = f"{directory}/store.json"
            upload_path = f"{directory}/uploads.json"
            from artifacts import StoreState
            store = Store(state=StoreState(store_path))
            manager = UploadManager(store, UploadState(upload_path))
            server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store, manager))
            server.store = store
            runner = RunningServer.__new__(RunningServer)
            runner.server = server
            runner.port = server.server_address[1]
            runner.thread = threading.Thread(target=server.serve_forever, daemon=True)
            runner.thread.start()
            try:
                status, created = runner.call(
                    "POST", "/v1/uploads",
                    data=json.dumps({"size": 3, "media_type": "text/plain"}).encode())
                self.assertEqual(status, 201)
                upload_id = created["upload_id"]
                runner.call("PUT", f"/v1/uploads/{upload_id}", data=b"abc",
                            headers={"X-Upload-Offset": "0"})
                manager._audit = BrokenAudit()
                status, body = runner.call("POST", f"/v1/uploads/{upload_id}/complete")
                self.assertEqual(status, 500)
                self.assertEqual(body["error"]["message"], "audit write failed")
                # Nothing committed: no blob, session still uncommitted and resumable.
                status, _ = runner.call("GET", f"/v1/blobs/{D(b'abc')}")
                self.assertEqual(status, 404)
                status, view = runner.call("GET", f"/v1/uploads/{upload_id}")
                self.assertEqual(view["status"], "uncommitted")
                self.assertEqual(view["received"], 3)
                self.assertEqual(store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
                # Both state files agree with the rolled-back memory.
                with open(store_path, "rb") as handle:
                    self.assertEqual(json.load(handle)["blobs"], [])
                with open(upload_path, "rb") as handle:
                    sessions = json.load(handle)["sessions"]
                self.assertEqual(len(sessions), 1)
                self.assertFalse(sessions[0]["committed"])
                # Once the audit write recovers, the same session completes normally.
                manager._audit = AuditLog()
                status, completed = runner.call("POST", f"/v1/uploads/{upload_id}/complete")
                self.assertEqual(status, 201)
                self.assertEqual(completed["digest"], D(b"abc"))
            finally:
                runner.stop()


if __name__ == "__main__":
    unittest.main()
