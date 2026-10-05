"""Tests for single-range conditional download on GET /v1/blobs/{digest}."""
from __future__ import annotations

import http.client
import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import InvalidRequest, RangeNotSatisfiable, parse_if_range, parse_range

SIZE = 10  # every parse_range case below runs against a 10-byte blob


class ParseRangeTests(unittest.TestCase):
    def test_valid_forms(self) -> None:
        self.assertEqual(parse_range("bytes=0-9", SIZE), (0, 9))
        self.assertEqual(parse_range("bytes=2-5", SIZE), (2, 5))
        self.assertEqual(parse_range("bytes=4-", SIZE), (4, SIZE - 1))
        self.assertEqual(parse_range("bytes=-3", SIZE), (SIZE - 3, SIZE - 1))
        self.assertEqual(parse_range("bytes=0-0", SIZE), (0, 0))

    def test_overlong_bounds_clamp_to_the_blob(self) -> None:
        self.assertEqual(parse_range("bytes=8-999", SIZE), (8, SIZE - 1))
        self.assertEqual(parse_range("bytes=-999", SIZE), (0, SIZE - 1))
        self.assertEqual(parse_range("bytes=0-99999999999999999999", SIZE), (0, SIZE - 1))

    def test_leading_zeros_are_decimal(self) -> None:
        self.assertEqual(parse_range("bytes=007-009", SIZE), (7, 9))

    def test_unsatisfiable_ranges_are_416(self) -> None:
        for value in ["bytes=10-11", "bytes=10-", "bytes=999-1000", "bytes=5-3"]:
            with self.assertRaises(RangeNotSatisfiable, msg=value):
                parse_range(value, SIZE)

    def test_malformed_ranges_are_400(self) -> None:
        bad = [
            "items=0-9",        # non-bytes unit
            "Bytes=0-9",        # unit is case-sensitive
            "bytes=",           # empty spec
            "bytes=-",          # both bounds omitted
            "bytes=-0",         # zero suffix
            "bytes=0-2,4-5",    # multiple ranges
            "bytes=0-2, 4-5",   # multiple ranges with space
            "bytes=1 -2",       # whitespace
            "bytes=1- 2",
            "bytes= 1-2",
            "bytes=1-2 ",
            "bytes=a-2",        # non-decimal bounds
            "bytes=1-b",
            "bytes=1.5-2",
            "bytes=+1-2",
            "bytes=1",          # no dash
            "bytes=1-2-3",      # too many dashes
            "bytes=--5",
            "",
        ]
        for value in bad:
            with self.assertRaises(InvalidRequest, msg=value):
                parse_range(value, SIZE)


class ParseIfRangeTests(unittest.TestCase):
    def test_quoted_digest_etag(self) -> None:
        digest = "a" * 64
        self.assertEqual(parse_if_range(f'"{digest}"'), digest)

    def test_malformed_if_range_is_400(self) -> None:
        for value in ["a" * 64,            # unquoted
                      "'" + "a" * 64 + "'",  # single quotes
                      '"' + "A" * 64 + '"',  # uppercase hex
                      '"' + "a" * 63 + '"',  # too short
                      '"' + "a" * 65 + '"',  # too long
                      '" ' + "a" * 64 + '"',  # whitespace inside
                      ' "' + "a" * 64 + '"',  # whitespace outside
                      "Wed, 21 Oct 2015 07:28:00 GMT",
                      ""]:
            with self.assertRaises(InvalidRequest, msg=value):
                parse_if_range(value)


class RangeHttpTests(unittest.TestCase):
    PAYLOAD = b"0123456789abcdef"  # 16 bytes, positionally distinct

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
        self.server.store._blobs.clear()
        self.server.store._meta.clear()
        self.server.store._refs.clear()
        status, _, headers = self.call("PUT", "/v1/blobs", self.PAYLOAD,
                                       {"Content-Type": "application/x-test"})
        assert status == 201
        self.digest = headers["X-Blob-Digest"]
        self.path = f"/v1/blobs/{self.digest}"

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def raw_get(self, path: str, headers: list[tuple[str, str]]):
        """GET with full control over repeated headers (urllib collapses duplicates)."""
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.putrequest("GET", path)
            for name, value in headers:
                connection.putheader(name, value)
            connection.endheaders()
            response = connection.getresponse()
            return response.status, response.read(), dict(response.headers.items())
        finally:
            connection.close()

    def test_plain_get_reports_full_body_and_new_headers(self) -> None:
        status, body, headers = self.call("GET", self.path)
        self.assertEqual(status, 200)
        self.assertEqual(body, self.PAYLOAD)
        self.assertEqual(headers["Content-Type"], "application/x-test")
        self.assertEqual(headers["Content-Length"], str(len(self.PAYLOAD)))
        self.assertEqual(headers["X-Blob-Digest"], self.digest)
        self.assertEqual(headers["ETag"], f'"{self.digest}"')
        self.assertEqual(headers["Accept-Ranges"], "bytes")

    def test_head_reports_the_same_metadata_without_a_body(self) -> None:
        status, body, headers = self.call("HEAD", self.path)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(headers["Content-Length"], str(len(self.PAYLOAD)))
        self.assertEqual(headers["Content-Type"], "application/x-test")
        self.assertEqual(headers["X-Blob-Digest"], self.digest)
        self.assertEqual(headers["ETag"], f'"{self.digest}"')
        self.assertEqual(headers["Accept-Ranges"], "bytes")

    def test_range_forms_return_206_with_content_range(self) -> None:
        cases = {
            "bytes=0-3": (0, 3),
            "bytes=5-": (5, 15),
            "bytes=-4": (12, 15),
            "bytes=14-999": (14, 15),   # last clamps to size - 1
            "bytes=-999": (0, 15),      # suffix longer than the blob returns it whole
            "bytes=0-15": (0, 15),
        }
        for range_header, (first, last) in cases.items():
            status, body, headers = self.call("GET", self.path, headers={"Range": range_header})
            self.assertEqual(status, 206, range_header)
            self.assertEqual(body, self.PAYLOAD[first:last + 1], range_header)
            self.assertEqual(headers["Content-Length"], str(last - first + 1), range_header)
            self.assertEqual(headers["Content-Range"], f"bytes {first}-{last}/16", range_header)
            self.assertEqual(headers["X-Blob-Digest"], self.digest, range_header)
            self.assertEqual(headers["ETag"], f'"{self.digest}"', range_header)
            self.assertEqual(headers["Accept-Ranges"], "bytes", range_header)
            self.assertEqual(headers["Content-Type"], "application/x-test", range_header)

    def test_unsatisfiable_range_is_416_with_conflict_body(self) -> None:
        for range_header in ["bytes=16-20", "bytes=16-", "bytes=5-3"]:
            status, body, headers = self.call("GET", self.path, headers={"Range": range_header})
            self.assertEqual(status, 416, range_header)
            self.assertEqual(headers["Content-Range"], "bytes */16", range_header)
            payload = json.loads(body)
            self.assertEqual(payload["error"]["code"], "conflict", range_header)
            self.assertTrue(payload["error"]["message"], range_header)

    def test_malformed_range_is_400_invalid_request(self) -> None:
        bad = ["items=0-3", "bytes=", "bytes=-", "bytes=-0", "bytes=0-2,4-5",
               "bytes=1 -2", "bytes=1- 2", "bytes=a-2", "bytes=1-b", "bytes=1.5-2",
               "bytes=1", "bytes=1-2-3"]
        for range_header in bad:
            status, body, _ = self.call("GET", self.path, headers={"Range": range_header})
            self.assertEqual(status, 400, range_header)
            self.assertEqual(json.loads(body)["error"]["code"], "invalid_request", range_header)

    def test_duplicate_range_headers_are_400(self) -> None:
        status, body, _ = self.raw_get(self.path, [("Range", "bytes=0-1"), ("Range", "bytes=2-3")])
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_request")

    def test_if_range_match_applies_the_range(self) -> None:
        status, body, headers = self.call(
            "GET", self.path,
            headers={"Range": "bytes=0-3", "If-Range": f'"{self.digest}"'})
        self.assertEqual(status, 206)
        self.assertEqual(body, self.PAYLOAD[:4])
        self.assertEqual(headers["Content-Range"], "bytes 0-3/16")

    def test_if_range_mismatch_ignores_the_range(self) -> None:
        other = "0" * 64
        assert other != self.digest
        status, body, headers = self.call(
            "GET", self.path,
            headers={"Range": "bytes=0-3", "If-Range": f'"{other}"'})
        self.assertEqual(status, 200)
        self.assertEqual(body, self.PAYLOAD)
        self.assertEqual(headers["Content-Length"], str(len(self.PAYLOAD)))

    def test_if_range_mismatch_ignores_even_an_unsatisfiable_range(self) -> None:
        status, body, _ = self.call(
            "GET", self.path,
            headers={"Range": "bytes=99-100", "If-Range": '"0' + "0" * 63 + '"'})
        self.assertEqual(status, 200)
        self.assertEqual(body, self.PAYLOAD)

    def test_malformed_if_range_is_400(self) -> None:
        for if_range in [self.digest, "not-an-etag", '"' + "A" * 64 + '"']:
            status, body, _ = self.call(
                "GET", self.path, headers={"Range": "bytes=0-3", "If-Range": if_range})
            self.assertEqual(status, 400, if_range)
            self.assertEqual(json.loads(body)["error"]["code"], "invalid_request", if_range)

    def test_digest_errors_are_unchanged(self) -> None:
        status, body, _ = self.call("GET", "/v1/blobs/nothex", headers={"Range": "bytes=0-1"})
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_request")
        status, body, _ = self.call("GET", f"/v1/blobs/{'0' * 64}", headers={"Range": "bytes=0-1"})
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["code"], "not_found")

    def test_range_read_survives_release_and_gc_mid_request(self) -> None:
        # A blob at refs 0 is still served; the bytes a request reads are the ones
        # it fetched at request start, so a gc landing afterwards cannot tear them.
        self.call("DELETE", f"/v1/blobs/{self.digest}/refs")
        status, body, _ = self.call("GET", self.path, headers={"Range": "bytes=2-5"})
        self.assertEqual(status, 206)
        self.assertEqual(body, self.PAYLOAD[2:6])
        self.call("POST", "/v1/gc")
        status, body, _ = self.call("GET", self.path, headers={"Range": "bytes=2-5"})
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["code"], "not_found")

    def test_other_surfaces_are_unchanged(self) -> None:
        status, body, _ = self.call("GET", "/v1/blobs")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["stats"]["blobs"], 1)
        status, body, headers = self.call("GET", f"/v1/uploads/{'0' * 32}")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
