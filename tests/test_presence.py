"""Tests for POST /v1/blobs/presence: validation, mixed hits/misses, ordering,
read-only snapshot semantics and compatibility with the rest of the surface."""
from __future__ import annotations

import json
import socket
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import MAX_BLOB, Store, digest_of, parse_presence

D = lambda text: digest_of(text.encode())  # noqa: E731


class ParsePresenceTests(unittest.TestCase):
    def valid(self, n: int) -> bytes:
        digests = [f"{i:064x}" for i in range(n)]
        return json.dumps({"digests": digests}).encode()

    def test_accepts_one_and_one_hundred_unique_lowercase_hex(self) -> None:
        self.assertEqual(len(parse_presence(self.valid(1))), 1)
        self.assertEqual(len(parse_presence(self.valid(100))), 100)

    def test_rejects_every_bad_shape_with_invalid_request(self) -> None:
        from artifacts import InvalidRequest

        bad_bodies = [
            b"not json at all",
            b"\xff\xfe{\"digests\":[]}",                    # not UTF-8
            b"[1, 2, 3]",                                   # JSON array, not object
            b"\"a string\"",
            b"42",
            b"null",
            b"{}",                                          # digests missing
            b'{"digests": []}',                             # empty array
            b'{"digests": ["00"]}',                         # element too short
            b'{"digests": ["' + b"a" * 63 + b'"]',          # 63 hex
            b'{"digests": ["' + b"a" * 65 + b'"]',          # 65 hex
            b'{"digests": ["' + b"A" * 64 + b'"]',          # uppercase
            b'{"digests": ["' + b"g" * 64 + b'"]',          # non-hex
            b'{"digests": [123]}',                          # number element
            b'{"digests": [true]}',                         # boolean element
            b'{"digests": [null]}',                         # null element
            b'{"digests": [{}]}',                           # object element
            b'{"digests": "not-an-array"}',                 # digests not an array
            b'{"digests": 5}',
        ]
        # 101 entries and duplicates need proper bodies.
        bad_bodies.append(self.valid(101))
        dup = json.dumps({"digests": ["a" * 64, "a" * 64]}).encode()
        bad_bodies.append(dup)
        # Unknown fields, including one alongside a otherwise-valid digests array.
        bad_bodies.append(b'{"digests": ["' + b"a" * 64 + b'"], "extra": 1}')
        bad_bodies.append(b'{"nope": 1}')
        # JSON must end cleanly.
        bad_bodies.append(b'{"digests": ["' + b"a" * 64 + b'"]} trailing junk')
        for body in bad_bodies:
            with self.subTest(body=body[:60]):
                with self.assertRaises(InvalidRequest):
                    parse_presence(body)


class StorePresenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()
        self.existing = self.store.put(b"existing", media_type="text/plain")
        self.repeated = self.store.put(b"repeated")
        self.store.put(b"repeated")  # refs == 2
        self.absent = "0" * 64
        self.other_absent = "f" * 64

    def test_partitions_into_sorted_present_and_missing(self) -> None:
        requested = [self.other_absent, self.existing.digest, self.absent,
                     self.repeated.digest]
        result = self.store.presence(requested)
        self.assertEqual(
            [entry["digest"] for entry in result["present"]],
            sorted([self.existing.digest, self.repeated.digest]))
        self.assertEqual(result["missing"], sorted([self.absent, self.other_absent]))
        self.assertEqual(set(result), {"present", "missing", "stats"})

    def test_present_records_match_listing_records(self) -> None:
        result = self.store.presence([self.repeated.digest, self.existing.digest])
        listing = {entry["digest"]: entry for entry in self.store.listing()}
        for entry in result["present"]:
            self.assertEqual(set(entry), {"digest", "size", "media_type", "refs"})
            self.assertEqual(entry, listing[entry["digest"]])
        self.assertEqual(result["present"][0 if self.existing.digest
                         < self.repeated.digest else 1]["refs"], 1)

    def test_absent_digests_are_missing_not_an_error(self) -> None:
        result = self.store.presence([self.absent])
        self.assertEqual(result["present"], [])
        self.assertEqual(result["missing"], [self.absent])

    def test_stats_describe_the_whole_store_not_the_filter(self) -> None:
        result = self.store.presence([self.absent, self.other_absent])
        self.assertEqual(result["present"], [])
        self.assertEqual(result["stats"], self.store.stats())
        self.assertEqual(result["stats"], {"blobs": 2, "bytes": 16, "puts": 3})

    def test_is_read_only_no_refs_or_state_change(self) -> None:
        before = self.store.stats()
        for _ in range(5):
            self.store.presence([self.existing.digest, self.absent])
        self.assertEqual(self.store.stats(), before)
        self.assertEqual(
            self.store.listing(),
            [{"digest": self.existing.digest, "size": 8,
              "media_type": "text/plain", "refs": 1},
             {"digest": self.repeated.digest, "size": 8,
              "media_type": "application/octet-stream", "refs": 2}])

    def test_zero_ref_blob_is_present_until_gc(self) -> None:
        self.store.release(self.existing.digest)
        result = self.store.presence([self.existing.digest])
        self.assertEqual([p["digest"] for p in result["present"]], [self.existing.digest])
        self.assertEqual(result["present"][0]["refs"], 0)
        self.store.gc()
        result = self.store.presence([self.existing.digest])
        self.assertEqual(result["present"], [])
        self.assertEqual(result["missing"], [self.existing.digest])


class _SnapshotGateStore(Store):
    """Store whose single metadata snapshot blocks on events mid-request.

    The gate fires only after the real snapshot is taken; the test mutates the
    store while the request is parked and then releases it, proving presence is
    built entirely from the request-start snapshot and never re-reads state.
    """

    def __init__(self) -> None:
        super().__init__()
        self.taken = threading.Event()
        self.proceed = threading.Event()

    def _snapshot(self):  # type: ignore[override]
        snapshot = super()._snapshot()
        self.taken.set()
        self.proceed.wait(timeout=5)
        return snapshot


class PresenceSnapshotTests(unittest.TestCase):
    def test_present_at_start_stays_present_after_gc(self) -> None:
        store = _SnapshotGateStore()
        blob = store.put(b"snap-payload", media_type="text/plain")
        absent = "0" * 64
        result: dict = {}
        thread = threading.Thread(
            target=lambda: result.update(store.presence([absent, blob.digest])))
        thread.start()
        self.assertTrue(store.taken.wait(timeout=5))
        store.release(blob.digest)  # refs 1 -> 0
        self.assertEqual(store.gc()["deleted"], [blob.digest])
        store.proceed.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual([p["digest"] for p in result["present"]], [blob.digest])
        self.assertEqual(result["present"][0]["refs"], 1)  # refs at request start
        self.assertEqual(result["missing"], [absent])
        self.assertEqual(result["stats"], {"blobs": 1, "bytes": 12, "puts": 1})

    def test_missing_at_start_stays_missing_after_upload(self) -> None:
        store = _SnapshotGateStore()
        digest = digest_of(b"lands-during-request")
        result: dict = {}
        thread = threading.Thread(target=lambda: result.update(store.presence([digest])))
        thread.start()
        self.assertTrue(store.taken.wait(timeout=5))
        store.put(b"lands-during-request")  # exists only after the snapshot
        store.proceed.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result["present"], [])
        self.assertEqual(result["missing"], [digest])
        self.assertEqual(result["stats"], {"blobs": 0, "bytes": 0, "puts": 0})


class PresenceHttpTests(unittest.TestCase):
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
        store = self.server.store
        store._blobs.clear()
        store._meta.clear()
        store._refs.clear()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def presence(self, digests):
        body = json.dumps({"digests": digests}).encode()
        return self.call("POST", "/v1/blobs/presence", body,
                         {"Content-Type": "application/json"})

    def raw(self, raw_headers: bytes, body: bytes | None = None) -> bytes:
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
            sock.sendall(raw_headers)
            if body is not None:
                sock.sendall(body)
            chunks = []
            while True:
                part = sock.recv(8192)
                if not part:
                    break
                chunks.append(part)
        return b"".join(chunks)

    def put(self, payload: bytes, media_type: str | None = None) -> str:
        headers = {"Content-Type": media_type} if media_type else {}
        status, _, headers_out = self.call("PUT", "/v1/blobs", payload, headers)
        self.assertEqual(status, 201)
        return headers_out["X-Blob-Digest"]

    def test_mixed_hits_and_misses_sorted_with_full_records(self) -> None:
        first = self.put(b"aaa", "text/plain")
        second = self.put(b"bbbbbbbb", "application/json")
        self.call("PUT", "/v1/blobs", b"aaa", {"Content-Type": "text/plain"})
        absent_one, absent_two = "1" * 64, "e" * 64
        # Deliberately unsorted request; response must be sorted either way.
        status, raw, _ = self.presence([absent_two, second, first, absent_one])
        self.assertEqual(status, 200)
        body = json.loads(raw)
        self.assertEqual([p["digest"] for p in body["present"]], sorted([first, second]))
        self.assertEqual(body["missing"], sorted([absent_one, absent_two]))
        listing = {b["digest"]: b for b in json.loads(self.call("GET", "/v1/blobs")[1])["blobs"]}
        for present in body["present"]:
            self.assertEqual(present, listing[present["digest"]])
        self.assertEqual(listing[first]["refs"], 2)

    def test_all_missing_is_200_not_404(self) -> None:
        status, raw, _ = self.presence(["0" * 64, "f" * 64])
        self.assertEqual(status, 200)
        body = json.loads(raw)
        self.assertEqual(body["present"], [])
        self.assertEqual(body["missing"], ["0" * 64, "f" * 64])

    def test_stats_match_full_listing_ignoring_the_filter(self) -> None:
        self.put(b"unrelated-one")
        self.put(b"unrelated-two")
        _, full, _ = self.call("GET", "/v1/blobs")
        expected_stats = json.loads(full)["stats"]
        status, raw, _ = self.presence(["0" * 64])  # none of the stored blobs
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)["stats"], expected_stats)

    def test_one_hundred_digests_round_trip(self) -> None:
        digests = [f"{i:064x}" for i in range(100)]
        status, raw, _ = self.presence(digests)
        self.assertEqual(status, 200)
        body = json.loads(raw)
        self.assertEqual(body["present"], [])
        self.assertEqual(body["missing"], digests)  # already ascending

    def test_repeated_calls_return_byte_identical_json(self) -> None:
        self.put(b"stable-one")
        digests = ["9" * 64, "2" * 64, D("stable-one")]
        _, first, _ = self.presence(list(reversed(digests)))
        _, second, _ = self.presence(list(reversed(digests)))
        self.assertEqual(first, second)
        parsed = json.loads(first)
        self.assertEqual([p["digest"] for p in parsed["present"]], [D("stable-one")])
        self.assertEqual(parsed["missing"], ["2" * 64, "9" * 64])

    def test_bad_bodies_are_400_invalid_request(self) -> None:
        bad = [
            b"not json",
            b"[1, 2]",
            b"{}",
            b'{"digests": []}',
            b'{"digests": ["ZZ"]}',
            b'{"digests": ["' + b"A" * 64 + b'"]',
            b'{"digests": [1]}',
            b'{"digests": "x"}',
            b'{"digests": ["' + b"a" * 64 + b'"], "x": 1}',
            json.dumps({"digests": [f"{i:064x}" for i in range(101)]}).encode(),
            json.dumps({"digests": ["a" * 64, "a" * 64]}).encode(),
        ]
        for body in bad:
            status, raw, _ = self.call("POST", "/v1/blobs/presence", body)
            self.assertEqual(status, 400, body)
            self.assertEqual(json.loads(raw)["error"]["code"], "invalid_request", body)

    def test_content_length_missing_negative_and_non_numeric_are_400(self) -> None:
        for headers in [
            b"POST /v1/blobs/presence HTTP/1.1\r\nConnection: close\r\n\r\n",  # no CL
            b"POST /v1/blobs/presence HTTP/1.1\r\nContent-Length: -1\r\n"
            b"Connection: close\r\n\r\n",
            b"POST /v1/blobs/presence HTTP/1.1\r\nContent-Length: abc\r\n"
            b"Connection: close\r\n\r\n",
        ]:
            # No body: the server rejects Content-Length before reading anything.
            response = self.raw(headers)
            self.assertTrue(response.startswith(b"HTTP/1.1 400"), response[:40])
            self.assertIn(b'"invalid_request"', response)

    def test_content_length_over_the_cap_is_400(self) -> None:
        headers = (b"POST /v1/blobs/presence HTTP/1.1\r\n"
                   + f"Content-Length: {MAX_BLOB + 1025}\r\n".encode()
                   + b"Connection: close\r\n\r\n")
        # Headers alone suffice: the server rejects before reading the body.
        response = self.raw(headers)
        self.assertTrue(response.startswith(b"HTTP/1.1 400"), response[:40])
        self.assertIn(b'"invalid_request"', response)

    def test_presence_does_not_add_refs_or_block_gc(self) -> None:
        digest = self.put(b"pinned-check", "text/plain")
        self.call("PUT", "/v1/blobs", b"pinned-check", {"Content-Type": "text/plain"})
        for _ in range(3):
            status, raw, _ = self.presence([digest])
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(raw)["present"][0]["refs"], 2)
        status, raw, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)["refs"], 1)
        # Drop to zero: presence still sees it, but GC is free to reclaim it.
        self.call("DELETE", f"/v1/blobs/{digest}/refs")
        _, before_gc, _ = self.presence([digest])
        self.assertEqual([p["digest"] for p in json.loads(before_gc)["present"]], [digest])
        status, raw, _ = self.call("POST", "/v1/gc", b"")
        self.assertEqual(json.loads(raw)["deleted"], [digest])
        _, after_gc, _ = self.presence([digest])
        body = json.loads(after_gc)
        self.assertEqual(body["present"], [])
        self.assertEqual(body["missing"], [digest])

    def test_concurrent_churn_yields_consistent_partitions(self) -> None:
        churn_one, churn_two = D("churn-one"), D("churn-two")
        self.put(b"churn-one")
        self.put(b"churn-two")
        missing = ["5" * 64, "a" * 64]
        requested = [churn_one, churn_two, *missing]
        stop = threading.Event()

        def churn(payload: bytes) -> None:
            digest = D(payload.decode())
            while not stop.is_set():
                self.call("PUT", "/v1/blobs", payload)
                while True:
                    status, _, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
                    if status != 200:
                        break
                self.call("POST", "/v1/gc", b"")

        threads = [threading.Thread(target=churn, args=(b"churn-one",)),
                   threading.Thread(target=churn, args=(b"churn-two",))]
        for thread in threads:
            thread.start()
        try:
            for _ in range(50):
                status, raw, _ = self.presence(requested)
                self.assertEqual(status, 200)
                body = json.loads(raw)
                present_d = [p["digest"] for p in body["present"]]
                self.assertEqual(present_d, sorted(present_d))
                self.assertEqual(body["missing"], sorted(body["missing"]))
                self.assertEqual(set(present_d) & set(body["missing"]), set())
                self.assertEqual(set(present_d) | set(body["missing"]), set(requested))
                self.assertGreaterEqual(body["stats"]["blobs"], len(present_d))
                self.assertGreaterEqual(body["stats"]["bytes"],
                                        sum(p["size"] for p in body["present"]))
                self.assertGreaterEqual(body["stats"]["puts"],
                                        sum(p["refs"] for p in body["present"]))
                for present in body["present"]:
                    self.assertEqual(set(present),
                                     {"digest", "size", "media_type", "refs"})
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
