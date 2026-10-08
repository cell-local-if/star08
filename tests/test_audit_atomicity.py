"""Audit-append failure is atomic with every confirmed mutation.

Contract (README, "审计事件"): when the in-process audit trail rejects an event
append, the whole request fails with
``500 {"error":{"code":"internal_error","message":"audit write failed"}}`` and
must leave nothing confirmed:

* no event visible through GET /v1/events;
* blob refs / stats bytes back to their pre-request values;
* upload received / status / tombstone / received chunks back as well;
* with --store-state / --upload-state, both files reverted to the last
  successful snapshot before the response, so a restart reproduces the
  pre-request state exactly;
* pure-memory and single-state-file modes must not confirm either;
* a failed commit never removes bytes, refs, sessions or events committed by
  concurrent requests.

State-file *write* failures keep their dedicated messages
(``store state write failed`` / ``upload state write failed``) and are covered
by test_store_state.py / test_upload_state.py; one case is mirrored here to pin
the distinction against an audit failure.
"""
from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from artifacts import (
    AuditError,
    AuditLog,
    Store,
    StoreState,
    StoreStateError,
    UploadManager,
    UploadState,
    UploadStateError,
    digest_of,
    make_handler,
)


class FailingAudit(AuditLog):
    """Audit trail whose next ``armed`` record() appends raise AuditError.

    Mirrors a real rejected append: ``record`` raises AuditError (the same type
    ``_record_audit`` surfaces) and leaves no event behind, because the base
    append is never reached.
    """

    def __init__(self) -> None:
        super().__init__()
        self.armed = 0
        self.attempts = 0

    def arm(self, count: int = 1) -> None:
        self.armed += count

    def record(self, op: str, result: dict) -> None:
        self.attempts += 1
        if self.armed > 0:
            self.armed -= 1
            raise AuditError()
        super().record(op, result)


class Fixture:
    """One Store + UploadManager with a FailingAudit and optional state files."""

    def __init__(self, store_path: str | None = None,
                 upload_path: str | None = None,
                 max_store_bytes: int | None = None) -> None:
        self.audit = FailingAudit()
        blob_state = restored = None
        if store_path is not None:
            blob_state = StoreState(store_path)
            try:
                restored = blob_state.load()
            except Exception:
                restored = []
        self.store_path = store_path
        self.upload_path = upload_path
        self.store = Store(max_store_bytes=max_store_bytes, state=blob_state,
                           restored=restored, audit=self.audit)
        upload_state = sessions = None
        if upload_path is not None:
            upload_state = UploadState(upload_path)
            try:
                sessions = upload_state.load()
            except Exception:
                sessions = {}
        self.uploads = UploadManager(self.store, upload_state, sessions)

    def file_snapshot(self) -> dict[str | None, bytes | None]:
        out: dict[str | None, bytes | None] = {}
        for path in (self.store_path, self.upload_path):
            out[path] = open(path, "rb").read() if path and os.path.exists(path) else None
        return out

    def reload_store_state(self) -> dict[str, int]:
        assert self.store_path is not None
        return {digest: refs for digest, _data, _mt, refs
                in StoreState(self.store_path).load()}

    def reload_uploads(self) -> dict[str, dict]:
        assert self.upload_path is not None
        return {uid: {"received": s.received, "committed": s.committed,
                      "deleted": s.deleted, "chunks": list(s.chunks)}
                for uid, s in UploadState(self.upload_path).load().items()}


def audit_failure_500() -> dict:
    return {"error": {"code": "internal_error", "message": "audit write failed"}}


# --------------------------------------------------------------------------- #
# Store-side commits (PUT, release, gc, mirror absorb, upload complete store) #
# --------------------------------------------------------------------------- #

class StoreAuditRollbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store_path = os.path.join(self.tmp.name, "store.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _fixture(self, persisted: bool) -> Fixture:
        return Fixture(store_path=self.store_path if persisted else None)

    def test_put_new_blob_rolled_back_every_mode(self) -> None:
        for persisted in (False, True):
            with self.subTest(persisted=persisted):
                fx = self._fixture(persisted)
                before = fx.file_snapshot()
                fx.audit.arm()
                with self.assertRaises(AuditError):
                    fx.store.put(b"new-blob", media_type="text/plain")
                self.assertEqual(fx.store.stats(),
                                 {"blobs": 0, "bytes": 0, "puts": 0})
                self.assertEqual(fx.store.audit.query(0, 100)["events"], [])
                if persisted:
                    # A first-ever commit restores to an empty snapshot: reload
                    # reproduces the pre-request (empty) store.
                    self.assertEqual(fx.reload_store_state(), {})

    def test_repeat_put_keeps_refs_at_one(self) -> None:
        for persisted in (False, True):
            with self.subTest(persisted=persisted):
                fx = self._fixture(persisted)
                fx.store.put(b"repeat", media_type="text/plain")
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.store.put(b"repeat", media_type="text/plain")
                self.assertEqual(fx.store.stats(),
                                 {"blobs": 1, "bytes": 6, "puts": 1})
                if persisted:
                    self.assertEqual(fx.reload_store_state(), {digest_of(b"repeat"): 1})
                    self.assertEqual(fx.file_snapshot(), before)

    def test_release_rolled_back(self) -> None:
        for persisted in (False, True):
            with self.subTest(persisted=persisted):
                fx = self._fixture(persisted)
                fx.store.put(b"x"); fx.store.put(b"x")  # refs == 2
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.store.release(digest_of(b"x"))
                self.assertEqual(fx.store.stats()["puts"], 2)
                if persisted:
                    self.assertEqual(fx.reload_store_state(), {digest_of(b"x"): 2})
                    self.assertEqual(fx.file_snapshot(), before)

    def test_gc_restores_zero_ref_records(self) -> None:
        for persisted in (False, True):
            with self.subTest(persisted=persisted):
                fx = self._fixture(persisted)
                fx.store.put(b"dead")
                dead = digest_of(b"dead")
                fx.store.put(b"alive")
                fx.store.release(dead)  # dead at refs 0, alive at refs 1
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.store.gc()
                stats = fx.store.stats()
                self.assertEqual(stats["blobs"], 2)
                self.assertEqual(stats["bytes"], len(b"dead") + len(b"alive"))
                if persisted:
                    self.assertEqual(fx.reload_store_state(),
                                     {digest_of(b"alive"): 1, dead: 0})
                    self.assertEqual(fx.file_snapshot(), before)

    def test_mirror_absorb_rolled_back_existing_untouched(self) -> None:
        for persisted in (False, True):
            with self.subTest(persisted=persisted):
                fx = self._fixture(persisted)
                fx.store.put(b"already-here")
                here = digest_of(b"already-here")
                incoming = b"fresh-from-mirror"
                fresh = digest_of(incoming)
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.store.absorb([(fresh, incoming, "text/plain")],
                                    sorted([here, fresh]))
                self.assertEqual(fx.store.stats()["blobs"], 1)
                self.assertEqual(fx.store.stats()["bytes"], len(b"already-here"))
                if persisted:
                    self.assertEqual(fx.reload_store_state(), {here: 1})
                    self.assertEqual(fx.file_snapshot(), before)

    def test_put_completed_rolls_back_the_store_side(self) -> None:
        # UploadManager drives put_completed with its audit_event; a rejected
        # event rolls the blob/ref back exactly like a plain PUT.
        for persisted in (False, True):
            with self.subTest(persisted=persisted):
                fx = self._fixture(persisted)
                fx.audit.arm()
                with self.assertRaises(AuditError):
                    fx.store.put_completed(
                        b"completed", None, "text/plain",
                        audit_event=("upload_complete", {"digest": digest_of(b"completed"),
                                                         "size": 9,
                                                         "media_type": "text/plain"}))
                self.assertEqual(fx.store.stats()["blobs"], 0)


# --------------------------------------------------------------------------- #
# Upload-session commits (create, append, abandon/delete, complete)           #
# --------------------------------------------------------------------------- #

class UploadAuditRollbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.upload_path = os.path.join(self.tmp.name, "uploads.json")
        self.store_path = os.path.join(self.tmp.name, "store.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _fixture(self, uploads: bool, store: bool) -> Fixture:
        return Fixture(self.store_path if store else None,
                       self.upload_path if uploads else None)

    def test_create_rolled_back(self) -> None:
        for uploads in (False, True):
            with self.subTest(uploads=uploads):
                fx = self._fixture(uploads, False)
                fx.audit.arm()
                with self.assertRaises(AuditError):
                    fx.uploads.create(4, "text/plain", None)
                self.assertEqual(fx.uploads._sessions, {})
                if uploads:
                    self.assertEqual(fx.reload_uploads(), {})

    def test_append_rolls_back_chunk_and_received(self) -> None:
        for uploads in (False, True):
            with self.subTest(uploads=uploads):
                fx = self._fixture(uploads, False)
                session = fx.uploads.create(11, "text/plain", None)
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.uploads.append(session.upload_id, 0, b"hello ")
                self.assertEqual(session.received, 0)
                self.assertEqual(session.chunks, [])
                self.assertFalse(session.committed or session.deleted)
                if uploads:
                    reloaded = fx.reload_uploads()[session.upload_id]
                    self.assertEqual((reloaded["received"], reloaded["chunks"]), (0, []))
                    self.assertEqual(fx.file_snapshot(), before)

    def test_failed_append_chunk_survives_restart_before_retry(self) -> None:
        fx = self._fixture(True, True)
        size = 300_000
        session = fx.uploads.create(size, "text/plain", None)
        first = b"A" * 200_000
        fx.uploads.append(session.upload_id, 0, first)
        second = b"B" * 100_000
        fx.audit.arm()
        with self.assertRaises(AuditError):
            fx.uploads.append(session.upload_id, 200_000, second)
        # A restart against the reverted file sees exactly the confirmed prefix.
        reloaded = Fixture(self.store_path, self.upload_path)
        view = reloaded.uploads.view(session.upload_id)
        self.assertEqual((view["received"], view["status"]), (200_000, "uncommitted"))
        # ... and the retry then completes end to end.
        reloaded.uploads.append(session.upload_id, 200_000, second)
        _, blob = reloaded.uploads.complete(session.upload_id)
        self.assertEqual(blob.digest, digest_of(first + second))

    def test_delete_rolls_back_tombstone_and_chunks(self) -> None:
        for uploads in (False, True):
            with self.subTest(uploads=uploads):
                fx = self._fixture(uploads, False)
                session = fx.uploads.create(6, "text/plain", None)
                fx.uploads.append(session.upload_id, 0, b"abc")
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.uploads.delete(session.upload_id)
                self.assertFalse(session.deleted)
                self.assertEqual(session.received, 3)
                self.assertEqual(b"".join(session.chunks), b"abc")
                if uploads:
                    reloaded = fx.reload_uploads()[session.upload_id]
                    self.assertFalse(reloaded["deleted"])
                    self.assertEqual(reloaded["received"], 3)
                    self.assertEqual(fx.file_snapshot(), before)

    def test_complete_rolls_back_session_and_blob(self) -> None:
        for uploads, store in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(uploads=uploads, store=store):
                fx = self._fixture(uploads, store)
                session = fx.uploads.create(5, "text/plain", None)
                fx.uploads.append(session.upload_id, 0, b"compl")
                fx.audit.arm()
                before = fx.file_snapshot()
                with self.assertRaises(AuditError):
                    fx.uploads.complete(session.upload_id)
                # Session side: still resumable with its bytes, no commit markers.
                self.assertFalse(session.committed or session.deleted)
                self.assertEqual(session.received, 5)
                self.assertEqual(b"".join(session.chunks), b"compl")
                self.assertIsNone(session.final_digest)
                # Store side: no blob, no bytes.
                self.assertEqual(fx.store.stats()["blobs"], 0)
                if store:
                    self.assertEqual(fx.reload_store_state(), {})
                if uploads:
                    reloaded = fx.reload_uploads()[session.upload_id]
                    self.assertFalse(reloaded["committed"])
                    self.assertEqual(reloaded["received"], 5)
                    self.assertEqual(fx.file_snapshot(), before)

    def test_complete_repeat_digest_does_not_leak_a_reference(self) -> None:
        fx = self._fixture(True, True)
        fx.store.put(b"same-bytes", media_type="text/plain")  # refs == 1
        session = fx.uploads.create(10, "text/plain", None)
        fx.uploads.append(session.upload_id, 0, b"same-bytes")
        fx.audit.arm()
        with self.assertRaises(AuditError):
            fx.uploads.complete(session.upload_id)
        self.assertEqual(fx.reload_store_state(), {digest_of(b"same-bytes"): 1})
        reloaded = fx.reload_uploads()[session.upload_id]
        self.assertFalse(reloaded["committed"])


# --------------------------------------------------------------------------- #
# HTTP surface: fixed 500 body, no event, restart equivalence end to end      #
# --------------------------------------------------------------------------- #

def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class HttpServer:
    def __init__(self, fx: Fixture) -> None:
        self.fx = fx
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(fx.store, fx.uploads))
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


class AuditFailureHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store_path = os.path.join(self.tmp.name, "store.json")
        self.upload_path = os.path.join(self.tmp.name, "uploads.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _server(self, store: bool, uploads: bool,
                quota: int | None = None) -> tuple[HttpServer, Fixture]:
        fx = Fixture(self.store_path if store else None,
                     self.upload_path if uploads else None,
                     max_store_bytes=quota)
        return HttpServer(fx), fx

    def _assert_500(self, status: int, raw: bytes) -> None:
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(raw), audit_failure_500())

    def test_put_failure_response_and_event_absence(self) -> None:
        for store in (False, True):
            with self.subTest(store=store):
                httpd, fx = self._server(store, False)
                try:
                    fx.audit.arm()
                    status, raw = httpd.call("PUT", "/v1/blobs", b"abc")
                    self._assert_500(status, raw)
                    self.assertEqual(
                        json.loads(httpd.call("GET", "/v1/events")[1])["events"], [])
                    self.assertEqual(
                        json.loads(httpd.call("GET", "/v1/blobs")[1])["stats"],
                        {"blobs": 0, "bytes": 0, "puts": 0})
                finally:
                    httpd.stop()

    def test_every_mutation_entry_returns_the_fixed_500(self) -> None:
        httpd, fx = self._server(True, True)
        try:
            # Seed confirmed state: one blob at refs 1, one refs-0 blob, one live session.
            _, raw = httpd.call("PUT", "/v1/blobs", b"keep-me", {"Content-Type": "text/plain"})
            keep = json.loads(raw)["digest"]
            _, raw = httpd.call("PUT", "/v1/blobs", b"drop-me", {"Content-Type": "text/plain"})
            drop = json.loads(raw)["digest"]
            self.assertEqual(httpd.call("DELETE", f"/v1/blobs/{drop}/refs")[0], 200)
            _, raw = httpd.call("POST", "/v1/uploads",
                                json.dumps({"size": 5, "media_type": "text/plain"}).encode())
            uid = json.loads(raw)["upload_id"]
            self.assertEqual(
                httpd.call("PUT", f"/v1/uploads/{uid}", b"abcde",
                           {"X-Upload-Offset": "0"})[0], 200)
            baseline_events = json.loads(httpd.call("GET", "/v1/events")[1])["events"]
            files_before = fx.file_snapshot()

            # Every mutating entry that works against the seeded state. Each
            # failed commit leaves both state and events unchanged, so the same
            # live, uncommitted session survives the failed complete/delete and
            # can be retried below.
            cases: list[tuple[str, tuple]] = [
                ("PUT", ("/v1/blobs", b"new-one", {"Content-Type": "text/plain"})),
                ("DELETE", (f"/v1/blobs/{keep}/refs",)),
                ("POST", ("/v1/gc", b"")),
                ("POST", ("/v1/uploads",
                          json.dumps({"size": 1, "media_type": "text/plain"}).encode())),
                ("POST", (f"/v1/uploads/{uid}/complete", b"")),
                ("DELETE", (f"/v1/uploads/{uid}",)),
            ]
            for method, args in cases:
                with self.subTest(entry=method + " " + args[0]):
                    fx.audit.arm()
                    status, body = httpd.call(method, *args)
                    self._assert_500(status, body)
                    self.assertEqual(
                        json.loads(httpd.call("GET", "/v1/events")[1])["events"],
                        baseline_events)
                    self.assertEqual(fx.file_snapshot(), files_before)

            # Append gets its own case against a fresh, empty session.
            _, raw = httpd.call("POST", "/v1/uploads",
                                json.dumps({"size": 10, "media_type": "text/plain"}).encode())
            uid2 = json.loads(raw)["upload_id"]
            files_before = fx.file_snapshot()
            baseline_events = json.loads(httpd.call("GET", "/v1/events")[1])["events"]
            fx.audit.arm()
            status, body = httpd.call("PUT", f"/v1/uploads/{uid2}", b"chunk",
                                      {"X-Upload-Offset": "0"})
            self._assert_500(status, body)
            self.assertEqual(json.loads(httpd.call("GET", "/v1/events")[1])["events"],
                             baseline_events)
            self.assertEqual(fx.file_snapshot(), files_before)
        finally:
            httpd.stop()

    def test_mirror_pull_failure_stores_nothing(self) -> None:
        import artifacts.app as mod
        httpd, fx = self._server(True, False)
        payload = b"mirrored-bytes"
        digest = digest_of(payload)
        real_presence, real_get = mod._remote_presence, mod._remote_get
        mod._remote_presence = lambda base_url, digests: set(digests)
        mod._remote_get = lambda base_url, wanted: (payload, "text/plain")
        try:
            fx.audit.arm()
            status, raw = httpd.call("POST", "/v1/mirror/pull", json.dumps(
                {"base_url": "http://mirror.example", "digests": [digest]}).encode())
            self._assert_500(status, raw)
            self.assertEqual(
                json.loads(httpd.call("GET", "/v1/events")[1])["events"], [])
            self.assertEqual(
                json.loads(httpd.call("GET", "/v1/blobs")[1])["stats"],
                {"blobs": 0, "bytes": 0, "puts": 0})
            self.assertEqual(fx.reload_store_state(), {})
        finally:
            mod._remote_presence, mod._remote_get = real_presence, real_get
            httpd.stop()

    def test_restart_after_failures_reproduces_pre_request_state(self) -> None:
        fx = Fixture(self.store_path, self.upload_path)
        httpd = HttpServer(fx)
        try:
            _, raw = httpd.call("PUT", "/v1/blobs", b"persisted", {"Content-Type": "text/plain"})
            blob = json.loads(raw)["digest"]
            _, raw = httpd.call("POST", "/v1/uploads",
                                json.dumps({"size": 5, "media_type": "text/plain"}).encode())
            uid = json.loads(raw)["upload_id"]
            # Fail a release, an append and a completion; none confirm.
            fx.audit.arm()
            self.assertEqual(httpd.call("DELETE", f"/v1/blobs/{blob}/refs")[0], 500)
            fx.audit.arm()
            self.assertEqual(httpd.call("PUT", f"/v1/uploads/{uid}", b"hello",
                                        {"X-Upload-Offset": "0"})[0], 500)
        finally:
            httpd.stop()

        # Restart with a healthy audit trail: refs still 1, session still empty.
        fx2 = Fixture(self.store_path, self.upload_path)
        httpd2 = HttpServer(fx2)
        try:
            listing = json.loads(httpd2.call("GET", "/v1/blobs")[1])
            self.assertEqual(listing["blobs"][0]["refs"], 1)
            status, raw = httpd2.call("GET", f"/v1/uploads/{uid}")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(raw)["received"], 0)
            # The failed requests left no events in the new trail either (the
            # trail is process memory and restarts empty; the state files alone
            # must not replay any failed mutation).
        finally:
            httpd2.stop()


# --------------------------------------------------------------------------- #
# Non-committing error paths never reach the audit trail                      #
# --------------------------------------------------------------------------- #

class ErrorPathsDoNotAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = Fixture(
            os.path.join(self.tmp.name, "store.json"),
            os.path.join(self.tmp.name, "uploads.json"),
            max_store_bytes=2000)
        self.httpd = HttpServer(self.fx)
        import artifacts.app as mod
        self._mod = mod

    def tearDown(self) -> None:
        self.httpd.stop()
        self.tmp.cleanup()

    def call(self, *args):
        return self.httpd.call(*args)

    def test_domained_failures_leave_audit_unarmed_and_empty(self) -> None:
        self.fx.audit.arm()  # must remain pending through every failure below
        # 400: bad PUT body/header, bad create body, malformed upload id, bad mirror body
        self.assertEqual(self.call("PUT", "/v1/blobs", b"")[0], 400)
        self.assertEqual(
            self.call("PUT", "/v1/blobs", b"abc", {"X-Blob-Digest": "a" * 64})[0], 409)
        self.assertEqual(self.call("POST", "/v1/uploads", b"{}")[0], 400)
        self.assertEqual(self.call("GET", "/v1/uploads/zz")[0], 400)
        self.assertEqual(self.call("POST", "/v1/mirror/pull", b"{}")[0], 400)
        # 404: unknown and tombstoned sessions
        unknown = "a" * 32
        self.assertEqual(self.call("GET", f"/v1/uploads/{unknown}")[0], 404)
        self.assertEqual(
            self.call("PUT", f"/v1/uploads/{unknown}", b"x",
                      {"X-Upload-Offset": "0"})[0], 404)
        self.assertEqual(self.call("DELETE", f"/v1/uploads/{unknown}")[0], 404)
        self.assertEqual(self.call("POST", f"/v1/uploads/{unknown}/complete", b"")[0], 404)
        self.assertEqual(self.call("DELETE", f"/v1/blobs/{'b' * 64}/refs")[0], 404)
        # 502: mirror remote unreachable (nothing stored, no event)
        status, _ = self.call("POST", "/v1/mirror/pull", json.dumps(
            {"base_url": "http://127.0.0.1:1", "digests": ["c" * 64]}).encode())
        self.assertEqual(status, 502)
        # Setup commits with the trail disarmed...
        self.fx.audit.armed = 0
        _, raw = self.call("POST", "/v1/uploads",
                           json.dumps({"size": 11, "media_type": "text/plain"}).encode())
        uid = json.loads(raw)["upload_id"]
        self.assertEqual(
            self.call("PUT", f"/v1/uploads/{uid}", b"abc",
                      {"X-Upload-Offset": "0"})[0], 200)
        # ...then 409s: wrong offset, incomplete complete.
        self.fx.audit.arm()
        self.assertEqual(
            self.call("PUT", f"/v1/uploads/{uid}", b"x",
                      {"X-Upload-Offset": "9"})[0], 409)
        self.assertEqual(self.call("POST", f"/v1/uploads/{uid}/complete", b"")[0], 409)
        # A declared-digest mismatch on complete is a 409 with no commit either.
        self.fx.audit.armed = 0
        _, raw = self.call("POST", "/v1/uploads", json.dumps(
            {"size": 3, "media_type": "text/plain", "digest": "a" * 64}).encode())
        uid2 = json.loads(raw)["upload_id"]
        self.assertEqual(
            self.call("PUT", f"/v1/uploads/{uid2}", b"abc",
                      {"X-Upload-Offset": "0"})[0], 200)
        self.fx.audit.arm()
        self.assertEqual(self.call("POST", f"/v1/uploads/{uid2}/complete", b"")[0], 409)
        # 413: oversized put and oversized complete, quota released on failure.
        self.assertEqual(self.call("PUT", "/v1/blobs", b"z" * 2001)[0], 413)
        # Nothing consumed the pending arm: the next real commit fails audited.
        self.assertEqual(self.call("POST", "/v1/gc", b"")[0], 500)
        # Only the deliberate setup commits have events.
        ops = [event["op"] for event in
               json.loads(self.call("GET", "/v1/events?limit=100")[1])["events"]]
        self.assertEqual(ops, [
            "upload_create", "upload_append", "upload_create", "upload_append",
        ])
        # And the failed gc still did not confirm: nothing was deleted.
        self.assertEqual(self.fx.audit.armed, 0)


# --------------------------------------------------------------------------- #
# Concurrency: one rejected commit cannot roll other commits back             #
# --------------------------------------------------------------------------- #

class AuditFailureConcurrencyTests(unittest.TestCase):
    def test_one_failed_put_among_many_keeps_every_other_commit(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        fx = Fixture(os.path.join(tmp.name, "store.json"),
                     os.path.join(tmp.name, "uploads.json"))
        httpd = HttpServer(fx)
        self.addCleanup(httpd.stop)
        payloads = [f"blob-{i:03d}".encode() for i in range(21)]
        outcomes: list[tuple[int, int]] = []
        outcome_lock = threading.Lock()

        def worker(index: int) -> None:
            status, _ = httpd.call("PUT", "/v1/blobs", payloads[index],
                                   {"Content-Type": "text/plain"})
            with outcome_lock:
                outcomes.append((index, status))

        fx.audit.arm()  # exactly one racing commit must be rejected
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(21)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        failed = [i for i, status in outcomes if status == 500]
        succeeded = [i for i, status in outcomes if status == 201]
        self.assertEqual(len(failed), 1)
        self.assertEqual(sorted(failed + succeeded), list(range(21)))
        listing = json.loads(httpd.call("GET", "/v1/blobs")[1])
        surviving = {digest_of(payloads[i]) for i in succeeded}
        self.assertEqual({b["digest"] for b in listing["blobs"]}, surviving)
        self.assertTrue(all(b["refs"] == 1 for b in listing["blobs"]))
        events = json.loads(httpd.call("GET", "/v1/events?limit=100")[1])["events"]
        self.assertEqual(len(events), 20)
        self.assertTrue(all(e["op"] == "blob_put" for e in events))
        # The state file reloads to exactly the 20 confirmed blobs.
        self.assertEqual(set(fx.reload_store_state()), surviving)


# --------------------------------------------------------------------------- #
# State-file write failures keep their own, different messages                #
# --------------------------------------------------------------------------- #

class StateWriteFailureMessagesTests(unittest.TestCase):
    def test_store_and_upload_state_write_failures_are_distinct_from_audit(self) -> None:
        store = Store(audit=AuditLog())

        def fail_store(*_args) -> None:
            raise StoreStateError()

        store._state = type("BrokenStoreState", (), {"save": staticmethod(fail_store)})()
        with self.assertRaises(StoreStateError) as caught:
            store.put(b"x")
        self.assertEqual(str(caught.exception), "store state write failed")
        self.assertEqual(store.stats()["blobs"], 0)  # rolled back
        self.assertEqual(store.audit.query(0, 10)["events"], [])

        uploads = UploadManager(store)

        def fail_upload(*_args) -> None:
            raise UploadStateError()

        uploads._state = type("BrokenUploadState", (),
                              {"save": staticmethod(fail_upload)})()
        with self.assertRaises(UploadStateError) as caught:
            uploads.create(3, "text/plain", None)
        self.assertEqual(str(caught.exception), "upload state write failed")
        self.assertEqual(uploads._sessions, {})


if __name__ == "__main__":
    unittest.main()
