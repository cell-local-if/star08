"""Optional on-disk persistence for the blob store itself (--store-state)."""
from __future__ import annotations

import base64
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
    MAX_BLOB,
    Store,
    StoreState,
    StoreStateError,
    StoreStateInvalid,
    UploadManager,
    UploadState,
    digest_of,
    make_handler,
    serve,
)

D = digest_of  # bytes -> 64-char lowercase hex


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RunningServer:
    """A serve()-like server whose lifecycle a test can cycle against one state path."""

    def __init__(self, store_path: str | None = None, upload_path: str | None = None,
                 store: Store | None = None) -> None:
        if store is None:
            self.server = serve(port=0, store_state=store_path, upload_state=upload_path)
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


class StoreStateRoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "store.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_put_blobs_survive_restart_with_bytes_media_type_and_refs(self) -> None:
        server = RunningServer(store_path=self.path)
        try:
            status, body = server.put_blob(b"alpha", "text/plain")
            self.assertEqual(status, 201)
            alpha = body["digest"]
            self.assertEqual(alpha, D(b"alpha"))
            status, body = server.put_blob(b"beta", "application/octet-stream")
            beta = body["digest"]
            self.assertEqual(body["media_type"], "application/octet-stream")
            # A repeat stores nothing new and adds a reference.
            status, _ = server.put_blob(b"alpha", "text/plain")
            self.assertEqual(status, 201)
        finally:
            server.stop()

        server = RunningServer(store_path=self.path)
        try:
            status, raw = server.call("GET", f"/v1/blobs/{alpha}")
            self.assertEqual((status, raw), (200, b"alpha"))
            status, raw = server.call("GET", f"/v1/blobs/{beta}")
            self.assertEqual((status, raw), (200, b"beta"))
            status, raw = server.call("GET", "/v1/blobs")
            listing = json.loads(raw)
            self.assertEqual(listing["blobs"], [
                {"digest": alpha, "size": 5, "media_type": "text/plain", "refs": 2},
                {"digest": beta, "size": 4, "media_type": "application/octet-stream", "refs": 1},
            ] if alpha < beta else [
                {"digest": beta, "size": 4, "media_type": "application/octet-stream", "refs": 1},
                {"digest": alpha, "size": 5, "media_type": "text/plain", "refs": 2},
            ])
            self.assertEqual(listing["stats"], {"blobs": 2, "bytes": 9, "puts": 3})
        finally:
            server.stop()

    def test_zero_refs_record_survives_restart_until_next_gc(self) -> None:
        server = RunningServer(store_path=self.path)
        try:
            _, body = server.put_blob(b"orphan")
            digest = body["digest"]
            status, raw = server.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual(json.loads(raw)["refs"], 0)
        finally:
            server.stop()

        # The refs-0 record is restored too: still readable, still listed.
        server = RunningServer(store_path=self.path)
        try:
            status, raw = server.call("GET", f"/v1/blobs/{digest}")
            self.assertEqual((status, raw), (200, b"orphan"))
            status, raw = server.call("GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 0)
            # Only the next gc reclaims it, reported in digest order.
            status, raw = server.call("POST", "/v1/gc", b"")
            self.assertEqual(json.loads(raw)["deleted"], [digest])
            self.assertEqual(server.call("GET", f"/v1/blobs/{digest}")[0], 404)
        finally:
            server.stop()

        # The gc itself was persisted: the blob stays gone.
        server = RunningServer(store_path=self.path)
        try:
            self.assertEqual(server.call("GET", f"/v1/blobs/{digest}")[0], 404)
            status, raw = server.call("GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"], [])
        finally:
            server.stop()

    def test_release_decrement_survives_restart(self) -> None:
        server = RunningServer(store_path=self.path)
        try:
            _, body = server.put_blob(b"counted")
            digest = body["digest"]
            server.put_blob(b"counted")
            status, raw = server.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual(json.loads(raw)["refs"], 1)
        finally:
            server.stop()

        server = RunningServer(store_path=self.path)
        try:
            status, raw = server.call("GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
            # Releasing the restored reference reaches 0, and a further release
            # is a conflict exactly as in memory.
            self.assertEqual(server.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
            status, raw = server.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (409, "conflict"))
        finally:
            server.stop()

    def test_completed_upload_blob_persists_with_store_state_alone(self) -> None:
        payload = b"uploaded and persisted"
        server = RunningServer(store_path=self.path)
        try:
            status, raw = server.call("POST", "/v1/uploads",
                                      json.dumps({"size": len(payload),
                                                  "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            self.assertEqual(server.call("PUT", f"/v1/uploads/{upload_id}", payload,
                                         {"X-Upload-Offset": "0"})[0], 200)
            status, raw = server.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 201)
            digest = json.loads(raw)["digest"]
        finally:
            server.stop()

        # No --upload-state: the session is gone, but the blob it committed is not.
        server = RunningServer(store_path=self.path)
        try:
            self.assertEqual(server.call("GET", f"/v1/uploads/{upload_id}")[0], 404)
            status, raw = server.call("GET", f"/v1/blobs/{digest}")
            self.assertEqual((status, raw), (200, payload))
        finally:
            server.stop()

    def test_both_state_paths_together_restore_sessions_and_blobs(self) -> None:
        upload_path = os.path.join(self.tmp.name, "uploads.json")
        payload = b"both kinds of state"
        server = RunningServer(store_path=self.path, upload_path=upload_path)
        try:
            status, raw = server.call("POST", "/v1/uploads",
                                      json.dumps({"size": len(payload),
                                                  "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            server.call("PUT", f"/v1/uploads/{upload_id}", payload, {"X-Upload-Offset": "0"})
            status, raw = server.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
            digest = json.loads(raw)["digest"]
        finally:
            server.stop()

        server = RunningServer(store_path=self.path, upload_path=upload_path)
        try:
            status, raw = server.call("GET", f"/v1/uploads/{upload_id}")
            self.assertEqual(json.loads(raw)["status"], "committed")
            status, raw = server.call("GET", f"/v1/blobs/{digest}")
            self.assertEqual((status, raw), (200, payload))
        finally:
            server.stop()

    def test_mirror_pull_persists_atomically_across_restart(self) -> None:
        remote = RunningServer()
        _, one = remote.put_blob(b"mirror one", "text/plain")
        _, two = remote.put_blob(b"mirror two")
        digests = sorted([one["digest"], two["digest"], D(b"nowhere")])
        server = RunningServer(store_path=self.path)
        try:
            status, raw = server.call("POST", "/v1/mirror/pull", json.dumps({
                "base_url": f"http://127.0.0.1:{remote.port}",
                "digests": digests,
            }).encode())
            self.assertEqual(status, 200)
            body = json.loads(raw)
            self.assertEqual(body["synced"], sorted([one["digest"], two["digest"]]))
            self.assertEqual(body["missing"], [D(b"nowhere")])
        finally:
            server.stop()
            remote.stop()

        # The remote is gone; the pulled bytes must come from the state file.
        server = RunningServer(store_path=self.path)
        try:
            status, raw = server.call("GET", f"/v1/blobs/{one['digest']}")
            self.assertEqual((status, raw), (200, b"mirror one"))
            status, raw = server.call("GET", f"/v1/blobs/{two['digest']}")
            self.assertEqual((status, raw), (200, b"mirror two"))
        finally:
            server.stop()

    def test_missing_state_file_starts_with_empty_store(self) -> None:
        path = os.path.join(self.tmp.name, "absent.json")
        server = serve(port=0, store_state=path)
        try:
            self.assertEqual(server.store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.server_close()


class StoreStateValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_state(self, content: bytes | str | None) -> str:
        path = os.path.join(self.tmp.name, "store.json")
        if content is not None:
            mode = "wb" if isinstance(content, bytes) else "w"
            with open(path, mode) as handle:
                handle.write(content)
        return path

    def assert_invalid(self, content: bytes | str | None) -> None:
        path = self.write_state(content)
        with self.assertRaises(StoreStateInvalid):
            serve(port=0, store_state=path).server_close()

    @staticmethod
    def good_entry() -> dict:
        return {"digest": D(b"hello"), "media_type": "text/plain", "refs": 1,
                "data": base64.b64encode(b"hello").decode("ascii")}

    def test_empty_file_is_invalid(self) -> None:
        self.assert_invalid(b"")

    def test_corrupt_json_is_invalid(self) -> None:
        self.assert_invalid(b"{not json")
        self.assert_invalid(b"\xff\xfe not utf8")

    def test_wrong_shape_is_invalid(self) -> None:
        self.assert_invalid(json.dumps([]))
        self.assert_invalid(json.dumps({}))
        self.assert_invalid(json.dumps({"version": 1}))
        self.assert_invalid(json.dumps({"blobs": []}))
        self.assert_invalid(json.dumps({"version": 1, "blobs": [], "extra": 1}))
        self.assert_invalid(json.dumps({"version": "1", "blobs": []}))
        self.assert_invalid(json.dumps({"version": 1, "blobs": {}}))

    def test_wrong_version_is_invalid(self) -> None:
        self.assert_invalid(json.dumps({"version": 2, "blobs": []}))
        self.assert_invalid(json.dumps({"version": 0, "blobs": []}))
        self.assert_invalid(json.dumps({"version": True, "blobs": []}))

    def test_bad_blob_entries_are_invalid(self) -> None:
        good = self.good_entry()
        bad_docs: list[object] = [
            {"version": 1, "blobs": [[]]},
            {"version": 1, "blobs": [{**good, "digest": "Z" * 64}]},
            {"version": 1, "blobs": [{**good, "refs": -1}]},
            {"version": 1, "blobs": [{**good, "refs": True}]},
            {"version": 1, "blobs": [{**good, "refs": "1"}]},
            {"version": 1, "blobs": [{**good, "media_type": ""}]},
            {"version": 1, "blobs": [{**good, "media_type": "x" * 201}]},
            {"version": 1, "blobs": [{**good, "data": "not base64!!"}]},
            {"version": 1, "blobs": [{**good, "data": ""}]},
            {"version": 1, "blobs": [{**good, "data": 5}]},
            {"version": 1, "blobs": [{**good, "extra": 1}]},
            {"version": 1, "blobs": [{k: v for k, v in good.items() if k != "refs"}]},
            # The digest must honestly name the bytes.
            {"version": 1, "blobs": [{**good, "digest": "a" * 64}]},
            {"version": 1, "blobs": [
                {**good, "data": base64.b64encode(b"x" * (MAX_BLOB + 1)).decode("ascii"),
                 "digest": D(b"x" * (MAX_BLOB + 1))}]},
        ]
        for doc in bad_docs:
            self.assert_invalid(json.dumps(doc))

    def test_duplicate_digests_are_invalid(self) -> None:
        entry = self.good_entry()
        self.assert_invalid(json.dumps({"version": 1, "blobs": [entry, entry]}))

    def test_zero_refs_entry_is_valid_and_restored(self) -> None:
        entry = {**self.good_entry(), "refs": 0}
        path = self.write_state(json.dumps({"version": 1, "blobs": [entry]}))
        server = serve(port=0, store_state=path)
        try:
            self.assertEqual(server.store.listing()[0]["refs"], 0)
            self.assertEqual(server.store.get(D(b"hello")), b"hello")
        finally:
            server.server_close()

    def test_cli_exits_nonzero_with_stderr_message(self) -> None:
        path = self.write_state(b"broken")
        proc = subprocess.run(
            [sys.executable, "-m", "artifacts.app", "--port", str(_free_port()),
             "--store-state", path],
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "src"},
            capture_output=True, text=True, timeout=20)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stderr.strip(), f"store state invalid: {path}")
        self.assertEqual(proc.stdout, "")


class StoreWriteFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "store.json")
        self.server = RunningServer(store_path=self.path)
        self.state = self.server.server.store._state

    def tearDown(self) -> None:
        self.server.stop()
        self.tmp.cleanup()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        return self.server.call(method, path, data, headers)

    def break_save(self):
        def fail(blobs, meta, refs):
            raise StoreStateError()
        self.state.save = fail  # type: ignore[method-assign]

    def heal_save(self) -> None:
        del self.state.save  # type: ignore[attr-defined]

    def test_unwritable_path_makes_put_return_500_with_exact_body(self) -> None:
        bad_directory = tempfile.mkdtemp(dir=self.tmp.name)
        os.rmdir(bad_directory)
        store = Store(state=StoreState(os.path.join(bad_directory, "nested", "store.json")))
        server = RunningServer(store=store)
        try:
            status, raw = server.call("PUT", "/v1/blobs", b"data")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "store state write failed"}})
            # Nothing was stored: the change was rolled back.
            self.assertEqual(store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.stop()

    def test_failed_put_is_rolled_back_and_disk_keeps_last_good_state(self) -> None:
        status, _ = self.server.put_blob(b"first")
        self.assertEqual(status, 201)
        self.break_save()
        try:
            status, raw = self.call("PUT", "/v1/blobs", b"second")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "store state write failed"}})
            # A repeat of an existing digest also fails and keeps refs unchanged.
            status, _ = self.call("PUT", "/v1/blobs", b"first")
            self.assertEqual(status, 500)
        finally:
            self.heal_save()
        self.assertEqual(self.call("GET", f"/v1/blobs/{D(b'second')}")[0], 404)
        status, raw = self.call("GET", "/v1/blobs")
        self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
        # The file on disk never held the failed writes.
        restored = StoreState(self.path).load()
        self.assertEqual([(d, data, refs) for d, data, _mt, refs in restored],
                         [(D(b"first"), b"first", 1)])
        # After healing, writes work again.
        self.assertEqual(self.call("PUT", "/v1/blobs", b"second")[0], 201)

    def test_failed_release_is_rolled_back(self) -> None:
        self.server.put_blob(b"pinned")
        digest = D(b"pinned")
        self.break_save()
        try:
            status, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual(status, 500)
        finally:
            self.heal_save()
        status, raw = self.call("GET", "/v1/blobs")
        self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
        self.assertEqual(StoreState(self.path).load()[0][3], 1)

    def test_failed_gc_is_rolled_back(self) -> None:
        self.server.put_blob(b"garbage")
        digest = D(b"garbage")
        self.assertEqual(self.call("DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
        self.break_save()
        try:
            status, _ = self.call("POST", "/v1/gc", b"")
            self.assertEqual(status, 500)
        finally:
            self.heal_save()
        # The blob is still there at refs 0, in memory and on disk.
        status, raw = self.call("GET", f"/v1/blobs/{digest}")
        self.assertEqual((status, raw), (200, b"garbage"))
        restored = StoreState(self.path).load()
        self.assertEqual([(d, refs) for d, _data, _mt, refs in restored], [(digest, 0)])
        # Retried gc reclaims it for good.
        status, raw = self.call("POST", "/v1/gc", b"")
        self.assertEqual(json.loads(raw)["deleted"], [digest])

    def test_failed_complete_creates_no_blob_and_session_stays_resumable(self) -> None:
        payload = b"complete me"
        status, raw = self.call("POST", "/v1/uploads",
                                json.dumps({"size": len(payload),
                                            "media_type": "text/plain"}).encode())
        upload_id = json.loads(raw)["upload_id"]
        self.call("PUT", f"/v1/uploads/{upload_id}", payload, {"X-Upload-Offset": "0"})
        self.break_save()
        try:
            status, raw = self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "store state write failed"}})
        finally:
            self.heal_save()
        self.assertEqual(self.call("GET", f"/v1/blobs/{D(payload)}")[0], 404)
        status, raw = self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(raw)["digest"], D(payload))

    def test_failed_complete_with_both_states_rolls_session_back_on_disk(self) -> None:
        upload_path = os.path.join(self.tmp.name, "uploads.json")
        self.server.stop()
        self.server = RunningServer(store_path=self.path, upload_path=upload_path)
        self.state = self.server.server.store._state
        payload = b"double state complete"
        status, raw = self.call("POST", "/v1/uploads",
                                json.dumps({"size": len(payload),
                                            "media_type": "text/plain"}).encode())
        upload_id = json.loads(raw)["upload_id"]
        self.call("PUT", f"/v1/uploads/{upload_id}", payload, {"X-Upload-Offset": "0"})
        self.break_save()
        try:
            status, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual(status, 500)
        finally:
            self.heal_save()
        # The upload state file was restored too: a fresh process sees the
        # session uncommitted with its bytes, and no blob in the store.
        session = UploadState(upload_path).load()[upload_id]
        self.assertFalse(session.committed)
        self.assertEqual(b"".join(session.chunks), payload)
        self.assertEqual(StoreState(self.path).load(), [])


class NoStoreStatePathTests(unittest.TestCase):
    """Without --store-state nothing is written and behavior is purely in-memory."""

    def test_serve_without_store_state_has_no_backing_file(self) -> None:
        server = serve(port=0)
        try:
            self.assertIsNone(server.store._state)
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main()
