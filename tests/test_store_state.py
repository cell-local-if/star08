"""Optional on-disk persistence for the artifact store itself (--store-state)."""
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

from artifacts import (
    MAX_BLOB,
    Store,
    StoreState,
    StoreStateError,
    StoreStateInvalid,
    digest_of,
    serve,
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class PersistedServer:
    """A serve() instance whose lifecycle a test can cycle against one state path."""

    def __init__(self, store_state: str | None, upload_state: str | None = None) -> None:
        self.server = serve(port=0, store_state=store_state, upload_state=upload_state)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


def call(server: PersistedServer, method: str, path: str,
         data: bytes | None = None, headers: dict | None = None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.port}{path}", data=data, method=method,
        headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def put_blob(server: PersistedServer, data: bytes, media_type: str | None = None):
    headers = {"Content-Type": media_type} if media_type is not None else {}
    return call(server, "PUT", "/v1/blobs", data, headers)


class StoreStateRoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "store.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_put_round_trip_restores_bytes_media_type_and_refs(self) -> None:
        alpha, beta = b"alpha bytes", b"beta bytes with more"
        alpha_digest, beta_digest = digest_of(alpha), digest_of(beta)
        server = PersistedServer(self.path)
        try:
            self.assertEqual(put_blob(server, alpha, "text/plain")[0], 201)
            self.assertEqual(put_blob(server, alpha, "text/plain")[0], 201)  # repeat: refs 2
            # urllib adds a form Content-Type when none is given; set the default explicitly.
            self.assertEqual(put_blob(server, beta, "application/octet-stream")[0], 201)
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            status, raw = call(server, "GET", f"/v1/blobs/{alpha_digest}")
            self.assertEqual((status, raw), (200, alpha))
            status, raw = call(server, "GET", "/v1/blobs")
            listing = json.loads(raw)
            self.assertEqual(listing["blobs"], [
                {"digest": alpha_digest, "size": len(alpha),
                 "media_type": "text/plain", "refs": 2},
                {"digest": beta_digest, "size": len(beta),
                 "media_type": "application/octet-stream", "refs": 1},
            ])
            self.assertEqual(listing["stats"], {"blobs": 2, "bytes": len(alpha) + len(beta),
                                                "puts": 3})
            # HEAD reports the restored media type and size.
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.port}/v1/blobs/{beta_digest}", method="HEAD")
            with urllib.request.urlopen(request, timeout=10) as response:
                self.assertEqual(response.headers["Content-Type"], "application/octet-stream")
                self.assertEqual(response.headers["Content-Length"], str(len(beta)))
        finally:
            server.stop()

    def test_zero_refs_record_survives_restart_until_next_gc(self) -> None:
        data = b"soon to be unreferenced"
        digest = digest_of(data)
        server = PersistedServer(self.path)
        try:
            self.assertEqual(put_blob(server, data)[0], 201)
            status, raw = call(server, "DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual((status, json.loads(raw)["refs"]), (200, 0))
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            # refs 0 but not yet reclaimed: still readable, still listed.
            self.assertEqual(call(server, "GET", f"/v1/blobs/{digest}")[0], 200)
            _, raw = call(server, "GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 0)
            # Releasing again is still a conflict after the restart.
            status, raw = call(server, "DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual((status, json.loads(raw)["error"]["code"]), (409, "conflict"))
            # The next gc reclaims it, and that deletion is itself persisted.
            status, raw = call(server, "POST", "/v1/gc", b"")
            self.assertEqual(json.loads(raw)["deleted"], [digest])
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            self.assertEqual(call(server, "GET", f"/v1/blobs/{digest}")[0], 404)
            _, raw = call(server, "GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["stats"], {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.stop()

    def test_release_count_survives_restart(self) -> None:
        data = b"referenced twice"
        digest = digest_of(data)
        server = PersistedServer(self.path)
        try:
            self.assertEqual(put_blob(server, data)[0], 201)
            self.assertEqual(put_blob(server, data)[0], 201)
            self.assertEqual(call(server, "DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            _, raw = call(server, "GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
        finally:
            server.stop()

    def test_completed_upload_blob_survives_restart_with_store_state(self) -> None:
        payload = b"uploaded and persisted"
        digest = digest_of(payload)
        server = PersistedServer(self.path)
        try:
            status, raw = call(server, "POST", "/v1/uploads",
                               json.dumps({"size": len(payload),
                                           "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            self.assertEqual(call(server, "PUT", f"/v1/uploads/{upload_id}", payload,
                                  {"X-Upload-Offset": "0"})[0], 200)
            self.assertEqual(call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")[0],
                             201)
        finally:
            server.stop()

        server = PersistedServer(self.path)
        try:
            status, raw = call(server, "GET", f"/v1/blobs/{digest}")
            self.assertEqual((status, raw), (200, payload))
        finally:
            server.stop()

    def test_store_state_and_upload_state_are_independently_restored(self) -> None:
        payload = b"both kinds of state"
        digest = digest_of(payload)
        uploads_path = os.path.join(self.tmp.name, "uploads.json")
        server = PersistedServer(self.path, upload_state=uploads_path)
        try:
            status, raw = call(server, "POST", "/v1/uploads",
                               json.dumps({"size": len(payload),
                                           "media_type": "text/plain"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            self.assertEqual(call(server, "PUT", f"/v1/uploads/{upload_id}", payload,
                                  {"X-Upload-Offset": "0"})[0], 200)
            self.assertEqual(call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")[0],
                             201)
        finally:
            server.stop()

        server = PersistedServer(self.path, upload_state=uploads_path)
        try:
            # The session is still committed and the blob is still there.
            _, raw = call(server, "GET", f"/v1/uploads/{upload_id}")
            view = json.loads(raw)
            self.assertEqual((view["status"], view["digest"]), ("committed", digest))
            self.assertEqual(call(server, "GET", f"/v1/blobs/{digest}")[0], 200)
        finally:
            server.stop()

    def test_mirror_pull_is_persisted(self) -> None:
        one, two = b"mirror one", b"mirror two"
        one_digest, two_digest = digest_of(one), digest_of(two)
        remote = PersistedServer(None)
        try:
            self.assertEqual(put_blob(remote, one, "text/plain")[0], 201)
            self.assertEqual(put_blob(remote, two)[0], 201)
            server = PersistedServer(self.path)
            try:
                body = json.dumps({"base_url": f"http://127.0.0.1:{remote.port}",
                                   "digests": sorted([one_digest, two_digest])}).encode()
                status, raw = call(server, "POST", "/v1/mirror/pull", body)
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(raw)["synced"], sorted([one_digest, two_digest]))
            finally:
                server.stop()
        finally:
            remote.stop()

        server = PersistedServer(self.path)
        try:
            self.assertEqual(call(server, "GET", f"/v1/blobs/{one_digest}")[1], one)
            self.assertEqual(call(server, "GET", f"/v1/blobs/{two_digest}")[1], two)
            _, raw = call(server, "GET", "/v1/blobs")
            media = {entry["digest"]: entry["media_type"]
                     for entry in json.loads(raw)["blobs"]}
            self.assertEqual(media[one_digest], "text/plain")
        finally:
            server.stop()

    def test_missing_state_file_starts_empty(self) -> None:
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
        path = os.path.join(self.tmp.name, "state.json")
        if content is not None:
            mode = "wb" if isinstance(content, bytes) else "w"
            with open(path, mode) as handle:
                handle.write(content)
        return path

    def assert_invalid(self, content: bytes | str | None) -> None:
        path = self.write_state(content)
        with self.assertRaises(StoreStateInvalid):
            serve(port=0, store_state=path).server_close()

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
        self.assert_invalid(json.dumps({"version": 1, "blobs": {}}))
        self.assert_invalid(json.dumps({"version": "1", "blobs": []}))
        self.assert_invalid(json.dumps({"version": True, "blobs": []}))

    def test_wrong_version_is_invalid(self) -> None:
        self.assert_invalid(json.dumps({"version": 2, "blobs": []}))
        self.assert_invalid(json.dumps({"version": 0, "blobs": []}))

    def good_entry(self) -> dict:
        data = b"good bytes"
        return {
            "digest": digest_of(data),
            "media_type": "text/plain",
            "refs": 1,
            "data": base64.b64encode(data).decode("ascii"),
        }

    def test_bad_blob_entries_are_invalid(self) -> None:
        good = self.good_entry()
        other_digest = digest_of(b"other")
        bad_entries: list[object] = [
            [],
            {**good, "digest": "z" * 64},
            {**good, "digest": other_digest},                    # hash mismatch
            {**good, "media_type": ""},
            {**good, "media_type": "x" * 201},
            {**good, "media_type": 5},
            {**good, "refs": -1},
            {**good, "refs": True},
            {**good, "refs": "1"},
            {**good, "data": "not base64!!"},
            {**good, "data": ""},
            {**good, "data": 5},
            {**good, "extra": 1},
            {k: v for k, v in good.items() if k != "refs"},
            {k: v for k, v in good.items() if k != "data"},
        ]
        empty = base64.b64encode(b"").decode("ascii")
        bad_entries.append({**good, "data": empty})
        oversized = base64.b64encode(b"x" * (MAX_BLOB + 1)).decode("ascii")
        bad_entries.append({**good, "data": oversized})
        for entry in bad_entries:
            with self.subTest(entry=str(entry)[:80]):
                self.assert_invalid(json.dumps({"version": 1, "blobs": [entry]}))

    def test_duplicate_digests_are_invalid(self) -> None:
        entry = self.good_entry()
        self.assert_invalid(json.dumps({"version": 1, "blobs": [entry, entry]}))

    def test_zero_refs_entry_is_valid_and_restored(self) -> None:
        entry = {**self.good_entry(), "refs": 0}
        path = self.write_state(json.dumps({"version": 1, "blobs": [entry]}))
        server = serve(port=0, store_state=path)
        try:
            self.assertEqual(server.store.stats()["blobs"], 1)
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
        self.state = StoreState(self.path)
        self.store = Store(state=self.state, restored=self.state.load())

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_server(self, store: Store) -> PersistedServer:
        from artifacts import UploadManager, make_handler
        from http.server import ThreadingHTTPServer
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store, UploadManager(store)))
        server = PersistedServer.__new__(PersistedServer)
        server.server = httpd
        server.port = httpd.server_address[1]
        server.thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server.thread.start()
        return server

    def fail_save(self) -> None:
        def fail(blobs: dict) -> None:
            raise StoreStateError()
        self.state.save = fail  # type: ignore[method-assign]

    def restore_save(self) -> None:
        del self.state.save  # type: ignore[attr-defined]

    def test_failed_put_returns_500_rolls_back_and_keeps_last_good_state(self) -> None:
        server = self.make_server(self.store)
        try:
            self.assertEqual(put_blob(server, b"kept")[0], 201)
            self.fail_save()
            try:
                status, raw = put_blob(server, b"dropped")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(raw),
                                 {"error": {"code": "internal_error",
                                            "message": "store state write failed"}})
            finally:
                self.restore_save()
            # The failed write left no trace in memory.
            self.assertEqual(call(server, "GET", f"/v1/blobs/{digest_of(b'dropped')}")[0], 404)
            self.assertEqual(self.store.stats(), {"blobs": 1, "bytes": 4, "puts": 1})
        finally:
            server.stop()
        # The file on disk never held the failed write.
        restored = StoreState(self.path).load()
        self.assertEqual(list(restored), [digest_of(b"kept")])

    def test_failed_repeat_put_rolls_refs_back(self) -> None:
        data = b"dedup me"
        digest = digest_of(data)
        server = self.make_server(self.store)
        try:
            self.assertEqual(put_blob(server, data)[0], 201)
            self.fail_save()
            try:
                self.assertEqual(put_blob(server, data)[0], 500)
            finally:
                self.restore_save()
            _, raw = call(server, "GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
        finally:
            server.stop()
        restored = StoreState(self.path).load()
        self.assertEqual(restored[digest][2], 1)

    def test_failed_release_rolls_refs_back(self) -> None:
        data = b"release me"
        digest = digest_of(data)
        server = self.make_server(self.store)
        try:
            self.assertEqual(put_blob(server, data)[0], 201)
            self.fail_save()
            try:
                self.assertEqual(call(server, "DELETE", f"/v1/blobs/{digest}/refs")[0], 500)
            finally:
                self.restore_save()
            _, raw = call(server, "GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 1)
        finally:
            server.stop()
        restored = StoreState(self.path).load()
        self.assertEqual(restored[digest][2], 1)

    def test_failed_gc_restores_reclaimed_blobs(self) -> None:
        data = b"collect me"
        digest = digest_of(data)
        server = self.make_server(self.store)
        try:
            self.assertEqual(put_blob(server, data)[0], 201)
            self.assertEqual(call(server, "DELETE", f"/v1/blobs/{digest}/refs")[0], 200)
            self.fail_save()
            try:
                self.assertEqual(call(server, "POST", "/v1/gc", b"")[0], 500)
            finally:
                self.restore_save()
            # Nothing was confirmed deleted: the blob is back at refs 0.
            self.assertEqual(call(server, "GET", f"/v1/blobs/{digest}")[0], 200)
            _, raw = call(server, "GET", "/v1/blobs")
            self.assertEqual(json.loads(raw)["blobs"][0]["refs"], 0)
            # A retry once writes succeed collects it for real.
            status, raw = call(server, "POST", "/v1/gc", b"")
            self.assertEqual(json.loads(raw)["deleted"], [digest])
        finally:
            server.stop()

    def test_failed_mirror_pull_stores_no_part_of_the_batch(self) -> None:
        one, two = b"batch one", b"batch two"
        one_digest, two_digest = digest_of(one), digest_of(two)
        remote_store = Store()
        remote = self.make_server(remote_store)
        server = self.make_server(self.store)
        try:
            self.assertEqual(put_blob(remote, one)[0], 201)
            self.assertEqual(put_blob(remote, two)[0], 201)
            self.fail_save()
            try:
                body = json.dumps({"base_url": f"http://127.0.0.1:{remote.port}",
                                   "digests": sorted([one_digest, two_digest])}).encode()
                status, raw = call(server, "POST", "/v1/mirror/pull", body)
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(raw),
                                 {"error": {"code": "internal_error",
                                            "message": "store state write failed"}})
            finally:
                self.restore_save()
            # All-or-nothing: neither digest entered the store.
            self.assertEqual(self.store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
            self.assertEqual(call(server, "GET", f"/v1/blobs/{one_digest}")[0], 404)
            # Retrying after the failure clears syncs the whole batch.
            status, raw = call(server, "POST", "/v1/mirror/pull", body)
            self.assertEqual(json.loads(raw)["synced"], sorted([one_digest, two_digest]))
        finally:
            server.stop()
            remote.stop()

    def test_failed_complete_leaves_session_resumable_and_creates_no_blob(self) -> None:
        payload = b"complete me"
        digest = digest_of(payload)
        server = self.make_server(self.store)
        try:
            _, raw = call(server, "POST", "/v1/uploads",
                          json.dumps({"size": len(payload), "media_type": "x"}).encode())
            upload_id = json.loads(raw)["upload_id"]
            self.assertEqual(call(server, "PUT", f"/v1/uploads/{upload_id}", payload,
                                  {"X-Upload-Offset": "0"})[0], 200)
            self.fail_save()
            try:
                status, raw = call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")
                self.assertEqual(status, 500)
                self.assertEqual(json.loads(raw),
                                 {"error": {"code": "internal_error",
                                            "message": "store state write failed"}})
            finally:
                self.restore_save()
            # Session is still resumable; nothing entered the store.
            _, raw = call(server, "GET", f"/v1/uploads/{upload_id}")
            self.assertEqual(json.loads(raw)["status"], "uncommitted")
            self.assertEqual(self.store.stats()["blobs"], 0)
            status, raw = call(server, "POST", f"/v1/uploads/{upload_id}/complete", b"")
            self.assertEqual((status, json.loads(raw)["digest"]), (201, digest))
        finally:
            server.stop()

    def test_unwritable_path_makes_put_return_500_with_exact_body(self) -> None:
        # A path inside a nonexistent directory: load treated it as absent, but
        # every write fails.
        bad_directory = tempfile.mkdtemp(dir=self.tmp.name)
        os.rmdir(bad_directory)
        state = StoreState(os.path.join(bad_directory, "nested", "state.json"))
        store = Store(state=state, restored=state.load())
        server = self.make_server(store)
        try:
            status, raw = put_blob(server, b"nowhere to go")
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(raw),
                             {"error": {"code": "internal_error",
                                        "message": "store state write failed"}})
            self.assertEqual(store.stats(), {"blobs": 0, "bytes": 0, "puts": 0})
        finally:
            server.stop()


class NoStoreStateTests(unittest.TestCase):
    """Without --store-state nothing is written and behavior is purely in-memory."""

    def test_serve_without_store_state_has_no_backing_file(self) -> None:
        server = serve(port=0)
        try:
            self.assertIsNone(server.store._state)
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main()
