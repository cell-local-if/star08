"""Batch presence: POST /v1/blobs/presence.

Covers request-body validation, mixed hits/misses, sort stability, the
request-start read-only snapshot under concurrent release/gc/upload, and
compatibility with the existing read/write surface (no refs, no GC effects).
"""
from __future__ import annotations

import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from artifacts import MAX_BLOB, InvalidRequest, Store, make_handler, parse_presence

D = "0123456789abcdef" * 4  # 64 lowercase hex characters


def digest_value(i: int) -> str:
    return f"{i:064x}"


class ParsePresenceTests(unittest.TestCase):
    def parse(self, body: bytes | str | dict) -> list[str]:
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        return parse_presence(body)

    def test_valid_bodies(self) -> None:
        self.assertEqual(self.parse({"digests": [D]}), [D])
        hundred = [digest_value(i) for i in range(100)]
        self.assertEqual(self.parse({"digests": hundred}), hundred)
        # order is preserved for the caller; sorting happens on the result
        shuffled = [hundred[3], hundred[0], hundred[99], hundred[50]]
        self.assertEqual(self.parse({"digests": shuffled}), shuffled)

    def test_boundaries(self) -> None:
        self.assertEqual(self.parse({"digests": [digest_value(99)]}), [digest_value(99)])
        with self.assertRaises(InvalidRequest):
            self.parse({"digests": [digest_value(i) for i in range(101)]})

    def test_malformed_bodies_are_invalid_request(self) -> None:
        bad: list[bytes] = [
            b"",
            b"not json",
            b"[1, 2, 3]",
            b'"a string"',
            b"null",
            b"42",
            # syntactically valid JSON but not valid UTF-8
            b'{"digests":["' + b"\xff" * 64 + b'"]}',
        ]
        bad.extend(json.dumps(payload).encode() for payload in [
            {},                                      # digests missing
            {"digests": [D], "extra": 1},            # unknown field
            {"digests": []},                         # empty array
            {"digests": "not-an-array"},
            {"digests": None},
            {"digests": [D, 1]},                     # element not a string
            {"digests": [D, None]},
            {"digests": [D, D]},                     # duplicate
            {"digests": [digest_value(1), digest_value(1)]},
            {"digests": ["Z" * 64]},                 # uppercase
            {"digests": ["a" * 63]},                 # too short
            {"digests": ["a" * 65]},                 # too long
            {"digests": [D + "g"]},                  # non-hex trailing
            {"digests": [""]},
            {"digests": [True]},
            {"digests": [12345]},
        ])
        for body in bad:
            with self.assertRaises(InvalidRequest, msg=body):
                parse_presence(body)


class PresenceStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def test_mixed_present_and_missing_sorted_with_global_stats(self) -> None:
        b1 = self.store.put(b"alpha", media_type="text/plain")
        b2 = self.store.put(b"beta-beta", media_type="application/json")
        self.store.put(b"alpha")  # refs(b1) -> 2
        absent_a = "f" * 64
        absent_b = "0" * 64
        result = self.store.presence([absent_a, b2.digest, absent_b, b1.digest])
        self.assertEqual([item["digest"] for item in result["present"]],
                         sorted([b1.digest, b2.digest]))
        self.assertEqual(result["present"][0],
                         {"digest": b1.digest, "size": 5,
                          "media_type": "text/plain", "refs": 2})
        self.assertEqual(result["present"][1],
                         {"digest": b2.digest, "size": 9,
                          "media_type": "application/json", "refs": 1})
        self.assertEqual(result["missing"], sorted([absent_a, absent_b]))
        self.assertEqual(result["stats"], {"blobs": 2, "bytes": 14, "puts": 3})

    def test_all_present_all_missing_and_disjoint_partition(self) -> None:
        b = self.store.put(b"x")
        only_present = self.store.presence([b.digest])
        self.assertEqual(len(only_present["missing"]), 0)
        absent = digest_value(7)
        only_missing = self.store.presence([absent])
        self.assertEqual(only_missing["present"], [])
        self.assertEqual(only_missing["missing"], [absent])
        self.assertEqual(only_missing["stats"], {"blobs": 1, "bytes": 1, "puts": 1})

    def test_is_read_only(self) -> None:
        b = self.store.put(b"alpha")
        self.store.put(b"alpha")
        before = self.store.stats()
        for _ in range(5):
            self.store.presence([b.digest, digest_value(12)])
        self.assertEqual(self.store.stats(), before)
        self.assertEqual(self.store.head(b.digest).media_type, "application/octet-stream")


def start_server(store: Store | None = None) -> tuple[ThreadingHTTPServer, int]:
    store = store or Store()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(store))
    httpd.store = store  # type: ignore[attr-defined]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, httpd.server_address[1]


class PresenceHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server, cls.port = start_server()

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
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def presence(self, payload: dict | bytes) -> tuple[int, dict]:
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        status, raw, _ = self.call("POST", "/v1/blobs/presence", body,
                                   {"Content-Type": "application/json"})
        return status, json.loads(raw)

    def put(self, payload: bytes, media_type: str = "application/octet-stream") -> str:
        _, _, headers = self.call("PUT", "/v1/blobs", payload, {"Content-Type": media_type})
        return headers["X-Blob-Digest"]

    def test_mixed_hits_and_misses_match_listing_records(self) -> None:
        d1 = self.put(b"alpha", "text/plain")
        d2 = self.put(b"beta-beta", "application/json")
        missing1 = "0" * 64
        missing2 = "f" * 64
        status, body = self.presence(
            {"digests": [missing2, d1, missing1, d2]})
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"present", "missing", "stats"})
        self.assertEqual([b["digest"] for b in body["present"]], sorted([d1, d2]))
        self.assertEqual(body["missing"], sorted([missing1, missing2]))
        for record in body["present"]:
            self.assertEqual(set(record), {"digest", "size", "media_type", "refs"})
        # present records are semantically identical to GET /v1/blobs rows
        listing = {b["digest"]: b for b in json.loads(self.call("GET", "/v1/blobs")[1])["blobs"]}
        for record in body["present"]:
            self.assertEqual(record, listing[record["digest"]])
        self.assertEqual(body["stats"], json.loads(self.call("GET", "/v1/blobs")[1])["stats"])

    def test_missing_is_not_an_error_and_everything_is_sorted(self) -> None:
        status, body = self.presence({"digests": ["9" * 64, "1" * 64, "a" * 64]})
        self.assertEqual(status, 200)
        self.assertEqual(body["present"], [])
        self.assertEqual(body["missing"], ["1" * 64, "9" * 64, "a" * 64])

    def test_hundred_digests_accepted(self) -> None:
        present = self.put(b"the-only-blob")
        digests = [present] + [digest_value(i) for i in range(1, 100)]
        status, body = self.presence({"digests": digests})
        self.assertEqual(status, 200)
        self.assertEqual([b["digest"] for b in body["present"]], [present])
        self.assertEqual(body["missing"], sorted(d for d in digests if d != present))
        self.assertEqual(len(body["present"]) + len(body["missing"]), 100)

    def test_invalid_bodies_are_400_and_change_nothing(self) -> None:
        present = self.put(b"keep-me")
        bad_payloads = [
            b"not json", b"[1,2]", b"null",
            b'{"digests":["' + b"\xff" * 64 + b'"]}',
        ]
        bad_payloads.extend(json.dumps(payload).encode() for payload in [
            {}, {"digests": [D], "nope": 1}, {"digests": []},
            {"digests": "x"}, {"digests": [D, D]}, {"digests": ["Z" * 64]},
            {"digests": [D, 1]}, {"digests": [D, D, D]},
            {"digests": [digest_value(i) for i in range(101)]},
        ])
        for body in bad_payloads:
            status, raw, _ = self.call("POST", "/v1/blobs/presence", body,
                                       {"Content-Type": "application/json"})
            self.assertEqual(status, 400, body)
            self.assertEqual(json.loads(raw)["error"]["code"], "invalid_request", body)
        # failures never add refs or hide blobs
        self.assertEqual(self.presence({"digests": [present]})[1]["present"][0]["refs"], 1)

    def test_content_length_problems_follow_the_400_convention(self) -> None:
        cases = [
            (None, b""),                                   # missing
            (b"-1", b'{"digests":["' + D.encode() + b'"]}'),
            (b"abc", b"{}"),
            (b"1.5", b"{}"),
            (str(MAX_BLOB + 1024 + 1).encode(), b""),      # past JSON endpoint cap
        ]
        for length, body in cases:
            with self.subTest(length=length):
                with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
                    headers = b"POST /v1/blobs/presence HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
                    if length is not None:
                        headers += b"Content-Length: " + length + b"\r\n"
                    sock.sendall(headers + b"\r\n" + body)
                    chunks = []
                    while True:
                        data = sock.recv(65536)
                        if not data:
                            break
                        chunks.append(data)
                response = b"".join(chunks)
                status = int(response.split(b"\r\n", 1)[0].split()[1])
                self.assertEqual(status, 400)
                self.assertIn(b"invalid_request", response)

    def test_does_not_add_refs_or_block_gc(self) -> None:
        d = self.put(b"soon-released")
        self.assertEqual(self.presence({"digests": [d]})[1]["present"][0]["refs"], 1)
        self.call("DELETE", f"/v1/blobs/{d}/refs")  # refs -> 0, content retained
        body = self.presence({"digests": [d]})[1]
        self.assertEqual(body["present"][0]["refs"], 0)
        # blobs/bytes still count the retained content; puts is the live-ref total
        self.assertEqual(body["stats"], {"blobs": 1, "bytes": 13, "puts": 0})
        gc = json.loads(self.call("POST", "/v1/gc", b"")[1])
        self.assertEqual(gc["deleted"], [d])
        body = self.presence({"digests": [d]})[1]
        self.assertEqual(body["missing"], [d])
        self.assertEqual(body["stats"], {"blobs": 0, "bytes": 0, "puts": 0})

    def test_repeated_calls_are_byte_identical_regardless_of_request_order(self) -> None:
        d1 = self.put(b"alpha", "text/plain")
        d2 = self.put(b"beta-beta", "application/json")
        digests = ["f" * 64, d2, "0" * 64, d1]
        encoded_a = json.dumps({"digests": digests}).encode()
        encoded_b = json.dumps({"digests": list(reversed(digests))}).encode()
        _, raw_a, _ = self.call("POST", "/v1/blobs/presence", encoded_a)
        _, raw_b, _ = self.call("POST", "/v1/blobs/presence", encoded_b)
        self.assertEqual(raw_a, raw_b)  # sorting removes request-order effects

        barrier = threading.Barrier(5)

        def fetch(out: list[bytes]) -> None:
            barrier.wait()
            _, raw, _ = self.call("POST", "/v1/blobs/presence", encoded_a)
            out.append(raw)

        outputs: list[list[bytes]] = [[] for _ in range(5)]
        threads = [threading.Thread(target=fetch, args=(out,)) for out in outputs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertTrue(all(out[0] == raw_a for out in outputs))

    def test_existing_endpoints_unchanged(self) -> None:
        d = self.put(b"compatibility", "text/plain")
        self.assertEqual(self.call("GET", f"/v1/blobs/{d}")[1], b"compatibility")
        self.assertEqual(self.call("HEAD", f"/v1/blobs/{d}")[0], 200)
        self.assertEqual(self.call("GET", f"/v1/blobs/{d}/graph")[0], 409)  # not a manifest
        self.assertEqual(self.call("POST", "/v1/blobs/presence", b"",
                                   {"Content-Length": "0"})[0], 400)
        # only POST is routed; other verbs on the path keep prior behaviour
        self.assertEqual(self.call("GET", "/v1/blobs/presence")[0], 400)


class PausingStore(Store):
    """A store whose presence response is computed first, then held back while a
    test mutates the store, isolating the request-start snapshot guarantee."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.proceed = threading.Event()

    def presence(self, digests: list[str]) -> dict:
        result = super().presence(digests)
        self.entered.set()
        self.proceed.wait(5)
        return result


class PresenceSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.store = PausingStore()
        cls.server, cls.port = start_server(cls.store)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def setUp(self) -> None:
        self.store.proceed.clear()
        self.store.entered.clear()
        self.store._blobs.clear()
        self.store._meta.clear()
        self.store._refs.clear()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def test_present_at_start_stays_present_after_release_and_gc(self) -> None:
        status, _, headers = self.call("PUT", "/v1/blobs", b"doomed")
        digest = headers["X-Blob-Digest"]
        outcome = {}

        def query() -> None:
            outcome["status"], raw, _ = self.call(
                "POST", "/v1/blobs/presence", json.dumps({"digests": [digest]}).encode())
            outcome["body"] = json.loads(raw)

        thread = threading.Thread(target=query)
        thread.start()
        self.assertTrue(self.store.entered.wait(5))
        # Snapshot captured; release the only ref and reclaim it before the answer ships.
        self.call("DELETE", f"/v1/blobs/{digest}/refs")
        gc = json.loads(self.call("POST", "/v1/gc", b"")[1])
        self.assertEqual(gc["deleted"], [digest])
        self.store.proceed.set()
        thread.join()
        self.assertEqual(outcome["status"], 200)
        self.assertEqual([b["digest"] for b in outcome["body"]["present"]], [digest])
        self.assertEqual(outcome["body"]["missing"], [])
        # A follow-up request observes the post-gc truth.
        status, body = (lambda r: (r[0], json.loads(r[1])))(
            self.call("POST", "/v1/blobs/presence", json.dumps({"digests": [digest]}).encode()))
        self.assertEqual(status, 200)
        self.assertEqual(body["present"], [])
        self.assertEqual(body["missing"], [digest])

    def test_missing_at_start_stays_missing_after_upload_lands(self) -> None:
        data = b"landed-during-request"
        import hashlib
        digest = hashlib.sha256(data).hexdigest()
        outcome = {}

        def query() -> None:
            outcome["status"], raw, _ = self.call(
                "POST", "/v1/blobs/presence", json.dumps({"digests": [digest]}).encode())
            outcome["body"] = json.loads(raw)

        thread = threading.Thread(target=query)
        thread.start()
        self.assertTrue(self.store.entered.wait(5))
        # The blob comes into existence while the presence answer is held.
        self.call("PUT", "/v1/blobs", data, {"Content-Type": "text/plain"})
        self.assertEqual(self.call("HEAD", f"/v1/blobs/{digest}")[0], 200)
        self.store.proceed.set()
        thread.join()
        self.assertEqual(outcome["status"], 200)
        self.assertEqual(outcome["body"]["present"], [])
        self.assertEqual(outcome["body"]["missing"], [digest])
        # The next request sees it, with stats from the later snapshot.
        status, raw, _ = self.call("POST", "/v1/blobs/presence",
                                   json.dumps({"digests": [digest]}).encode())
        body = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual([b["digest"] for b in body["present"]], [digest])
        self.assertEqual(body["stats"]["blobs"], 1)


if __name__ == "__main__":
    unittest.main()
