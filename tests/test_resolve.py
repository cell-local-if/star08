"""Tests for version-constraint adjudication and GET /v1/blobs/{digest}/resolve."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import (
    BlobNotFound,
    DigestConflict,
    InvalidRequest,
    ResolveConflict,
    Store,
    constraint_interval,
    intersect_intervals,
    interval_contains,
    parse_version,
)


def manifest(name: str, version: str, deps: dict | None = ...) -> bytes:
    payload: dict = {"name": name, "version": version}
    if deps is not ...:
        payload["dependencies"] = deps
    return json.dumps(payload).encode()


def dep(digest: str, constraint: str) -> dict:
    return {"digest": digest, "constraint": constraint}


CONFLICT_PREFIXES = (
    "cycle", "version_syntax", "constraint_syntax", "name_mismatch",
    "ambiguous_name", "empty_intersection", "version_mismatch",
)


class VersionSyntaxTests(unittest.TestCase):
    def test_one_to_three_segments_are_zero_padded(self) -> None:
        self.assertEqual(parse_version("1"), (1, 0, 0))
        self.assertEqual(parse_version("1.2"), (1, 2, 0))
        self.assertEqual(parse_version("1.2.3"), (1, 2, 3))
        self.assertEqual(parse_version("0"), (0, 0, 0))
        self.assertEqual(parse_version("0.0.0"), (0, 0, 0))

    def test_leading_zeros_are_rejected_except_lone_zero(self) -> None:
        for bad in ("01", "00", "1.02", "1.2.03", "01.0", "0.01", "10.00"):
            with self.assertRaises(ResolveConflict, msg=bad) as caught:
                parse_version(bad)
            self.assertTrue(str(caught.exception).startswith("version_syntax"))

    def test_segment_count_and_characters(self) -> None:
        for bad in ("", "1.", ".1", "1..2", "1.2.3.4", "v1", "1.x",
                    "-1", "1.-2", "+", "1.2 ", " 1", "1.a", "①"):
            with self.assertRaises(ResolveConflict, msg=bad) as caught:
                parse_version(bad)
            self.assertTrue(str(caught.exception).startswith("version_syntax"))


class ConstraintIntervalTests(unittest.TestCase):
    def test_exact_and_ordering_operators(self) -> None:
        self.assertEqual(constraint_interval("=1.2.3"), ((1, 2, 3), (1, 2, 4)))
        self.assertEqual(constraint_interval(">1"), ((1, 0, 1), None))
        self.assertEqual(constraint_interval(">=1"), ((1, 0, 0), None))
        self.assertEqual(constraint_interval("<1.2"), ((0, 0, 0), (1, 2, 0)))
        self.assertEqual(constraint_interval("<=1.2"), ((0, 0, 0), (1, 2, 1)))

    def test_caret_major_positive(self) -> None:
        self.assertEqual(constraint_interval("^1.2.3"), ((1, 2, 3), (2, 0, 0)))
        self.assertEqual(constraint_interval("^1"), ((1, 0, 0), (2, 0, 0)))
        self.assertTrue(interval_contains(constraint_interval("^1.2.3"), (1, 9, 9)))
        self.assertFalse(interval_contains(constraint_interval("^1.2.3"), (2, 0, 0)))
        self.assertFalse(interval_contains(constraint_interval("^1.2.3"), (1, 2, 2)))

    def test_caret_zero_minor(self) -> None:
        self.assertEqual(constraint_interval("^0.2.3"), ((0, 2, 3), (0, 3, 0)))
        self.assertTrue(interval_contains(constraint_interval("^0.2"), (0, 2, 99)))
        self.assertFalse(interval_contains(constraint_interval("^0.2"), (0, 3, 0)))

    def test_caret_zero_zero_patch(self) -> None:
        self.assertEqual(constraint_interval("^0.0.3"), ((0, 0, 3), (0, 0, 4)))
        self.assertTrue(interval_contains(constraint_interval("^0.0.3"), (0, 0, 3)))
        self.assertFalse(interval_contains(constraint_interval("^0.0.3"), (0, 0, 4)))
        self.assertEqual(constraint_interval("^0.0.0"), ((0, 0, 0), (0, 0, 1)))

    def test_tilde_pins_by_segment_count(self) -> None:
        self.assertEqual(constraint_interval("~1"), ((1, 0, 0), (2, 0, 0)))
        self.assertEqual(constraint_interval("~1.2"), ((1, 2, 0), (1, 3, 0)))
        self.assertEqual(constraint_interval("~1.2.3"), ((1, 2, 3), (1, 3, 0)))
        self.assertTrue(interval_contains(constraint_interval("~1"), (1, 9, 9)))
        self.assertFalse(interval_contains(constraint_interval("~1"), (2, 0, 0)))
        self.assertTrue(interval_contains(constraint_interval("~1.2"), (1, 2, 9)))
        self.assertFalse(interval_contains(constraint_interval("~1.2"), (1, 3, 0)))

    def test_boundary_points(self) -> None:
        checks = [
            (">1", (1, 0, 0), False), (">1", (1, 0, 1), True),
            ("<2", (2, 0, 0), False), ("<2", (1, 9, 9), True),
            ("<=2.0.0", (2, 0, 0), True), ("<=2.0.0", (2, 0, 1), False),
            ("=1.0", (1, 0, 0), True), ("=1.0", (1, 0, 1), False),
        ]
        for token, version, expected in checks:
            self.assertEqual(interval_contains(constraint_interval(token), version),
                             expected, (token, version))

    def test_syntax_errors(self) -> None:
        for bad in ("1", "1.0", "==", "==1", ">>1", ">= 1", ">=1 ", " >=1",
                    "^", "~", ">=v1", ">=01", "^1.2.3.4", ">=", "!1",
                    ">=1.x", "<= 1.0", "=>1", "≥1", "", " "):
            with self.assertRaises(ResolveConflict, msg=bad) as caught:
                constraint_interval(bad)
            self.assertTrue(str(caught.exception).startswith("constraint_syntax"), bad)

    def test_intersection_of_intervals(self) -> None:
        interval = intersect_intervals([constraint_interval(">=1.0.0"),
                                        constraint_interval("<2.0.0")])
        self.assertEqual(interval, ((1, 0, 0), (2, 0, 0)))
        self.assertTrue(interval_contains(interval, (1, 5, 0)))
        self.assertFalse(interval_contains(interval, (2, 0, 0)))

    def test_empty_intersection_is_detectable(self) -> None:
        interval = intersect_intervals([constraint_interval(">=3.0.0"),
                                        constraint_interval("<3.0.0")])
        lower, upper = interval
        self.assertIsNotNone(upper)
        self.assertGreaterEqual(lower, upper)

    def test_open_upper_means_unbounded(self) -> None:
        interval = intersect_intervals([constraint_interval(">=1.0.0"),
                                        constraint_interval(">=1.5.0")])
        self.assertEqual(interval, ((1, 5, 0), None))
        self.assertTrue(interval_contains(interval, (10**9, 0, 0)))


class StoreResolveUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def put_manifest(self, name: str, version: str, deps: dict | None = ...) -> str:
        return self.store.put(manifest(name, version, deps)).digest

    def resolve_conflict(self, digest: str) -> str:
        with self.assertRaises(ResolveConflict) as caught:
            self.store.resolve(digest)
        return str(caught.exception)

    def test_root_without_dependencies_resolves_empty(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.resolve(root), {"root": root, "resolved": []})

    def test_dependencies_field_may_be_omitted(self) -> None:
        root = self.put_manifest("root", "1.0.0")
        self.assertEqual(self.store.resolve(root)["resolved"], [])

    def test_happy_diamond_pools_and_dedups_constraints(self) -> None:
        leaf = self.put_manifest("leaf", "2.4.1", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, ">=2.0")})
        right = self.put_manifest("right", "1.0.0",
                                  {"leaf": dep(leaf, "<3.0.0\t^2.1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "~1")})
        result = self.store.resolve(root)
        self.assertEqual(result["root"], root)
        self.assertEqual(result["resolved"], [
            {"name": "leaf", "digest": leaf, "version": "2.4.1",
             "constraints": ["<3.0.0\t^2.1", ">=2.0"]},
            {"name": "left", "digest": left, "version": "1.0.0",
             "constraints": ["^1"]},
            {"name": "right", "digest": right, "version": "1.0.0",
             "constraints": ["~1"]},
        ])

    def test_resolved_is_sorted_by_name_and_unique(self) -> None:
        zebra = self.put_manifest("zebra", "1.0.0", {})
        apple = self.put_manifest("apple", "1.0.0", {})
        middle = self.put_manifest("middle", "1.0.0", {})
        root = self.put_manifest(
            "root", "1.0.0", {
                "zebra": dep(zebra, "^1"),
                "apple": dep(apple, "^1"),
                "middle": dep(middle, "^1"),
            })
        names = [entry["name"] for entry in self.store.resolve(root)["resolved"]]
        self.assertEqual(names, ["apple", "middle", "zebra"])

    def test_constraint_strings_are_deduplicated(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, "^1")})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf, "^1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        entry = [e for e in self.store.resolve(root)["resolved"]
                 if e["name"] == "leaf"][0]
        self.assertEqual(entry["constraints"], ["^1"])

    def test_version_strings_are_reported_verbatim(self) -> None:
        leaf = self.put_manifest("leaf", "2", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "~2")})
        entry = self.store.resolve(root)["resolved"][0]
        self.assertEqual(entry["version"], "2")

    def test_deep_chain_does_not_hit_recursion_limits(self) -> None:
        previous = self.put_manifest("pkg-0", "1.0.0", {})
        for i in range(1, 1200):
            previous = self.put_manifest(
                f"pkg-{i}", "1.0.0",
                {f"pkg-{i - 1}": dep(previous, "^1")})
        result = self.store.resolve(previous)
        self.assertEqual(len(result["resolved"]), 1199)
        self.assertEqual([e["name"] for e in result["resolved"]][:3],
                         ["pkg-0", "pkg-1", "pkg-10"])

    def test_invalid_root_digest_is_invalid_request(self) -> None:
        for bad in ("zz", "A" * 64, "0" * 63, 42):
            with self.assertRaises(InvalidRequest):
                self.store.resolve(bad)

    def test_unknown_root_digest_is_not_found(self) -> None:
        with self.assertRaises(BlobNotFound):
            self.store.resolve("0" * 64)

    def test_malformed_manifest_uses_graph_conflict_text(self) -> None:
        for raw in (b"not json", b"[1, 2]",
                    json.dumps({"name": "a", "version": "1", "extra": 1}).encode(),
                    json.dumps({"version": "1"}).encode(),
                    json.dumps({"name": "a", "version": "1",
                                "dependencies": []}).encode()):
            digest = self.store.put(raw).digest
            with self.assertRaises(DigestConflict, msg=raw) as caught:
                self.store.resolve(digest)
            self.assertIs(type(caught.exception), DigestConflict)
            self.assertIn("is not a valid manifest", str(caught.exception))

    def test_missing_dependency_uses_graph_conflict_text(self) -> None:
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("f" * 64, "^1")})
        with self.assertRaises(DigestConflict) as caught:
            self.store.resolve(root)
        self.assertIs(type(caught.exception), DigestConflict)
        self.assertIn("dependency blob", str(caught.exception))
        self.assertFalse(str(caught.exception).startswith(CONFLICT_PREFIXES))

    def test_cycle_prefix(self) -> None:
        a, b = "a" * 64, "b" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"b": dep(b, "^1")})
        self.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a, "^1")})
        message = self.resolve_conflict(a)
        self.assertTrue(message.startswith("cycle"))

    def test_self_dependency_is_cycle(self) -> None:
        a = "a" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"a": dep(a, "^1")})
        self.assertTrue(self.resolve_conflict(a).startswith("cycle"))

    def test_version_syntax_prefix(self) -> None:
        for bad_version in ("01", "1.02", "1.2.3.4"):
            bad = self.put_manifest("bad", bad_version, {})
            root = self.put_manifest("root", "1.0.0", {"bad": dep(bad, "^1")})
            self.assertTrue(self.resolve_conflict(root).startswith("version_syntax"),
                            bad_version)

    def test_constraint_syntax_prefix(self) -> None:
        target = self.put_manifest("leaf", "1.0.0", {})
        for bad_constraint in ("nope", ">=1.2.3.4", "==1", ">= 1", "^",
                               ">=v1", "!1", "1.0", " "):
            root = self.put_manifest(
                "root", "1.0.0", {"leaf": dep(target, bad_constraint)})
            self.assertTrue(
                self.resolve_conflict(root).startswith("constraint_syntax"),
                bad_constraint)

    def test_name_mismatch_prefix(self) -> None:
        actual = self.put_manifest("actual", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"wanted": dep(actual, "^1")})
        self.assertTrue(self.resolve_conflict(root).startswith("name_mismatch"))

    def test_ambiguous_name_prefix(self) -> None:
        first = self.put_manifest("lib", "1.0.0", {})
        second = self.put_manifest("lib", "2.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"lib": dep(first, "^1")})
        right = self.put_manifest("right", "1.0.0", {"lib": dep(second, "^2")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        self.assertTrue(self.resolve_conflict(root).startswith("ambiguous_name"))

    def test_same_name_same_digest_is_not_ambiguous(self) -> None:
        leaf = self.put_manifest("lib", "1.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"lib": dep(leaf, "^1")})
        right = self.put_manifest("right", "1.0.0", {"lib": dep(leaf, "^1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        self.assertEqual(len(self.store.resolve(root)["resolved"]), 3)

    def test_empty_intersection_prefix(self) -> None:
        leaf = self.put_manifest("leaf", "3.0.0", {})
        root = self.put_manifest(
            "root", "1.0.0", {"leaf": dep(leaf, ">=3.0.0 <3.0.0")})
        self.assertTrue(self.resolve_conflict(root).startswith("empty_intersection"))

    def test_empty_intersection_across_diamond_edges(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, ">=2.0.0")})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf, "<2.0.0")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        self.assertTrue(self.resolve_conflict(root).startswith("empty_intersection"))

    def test_version_mismatch_prefix(self) -> None:
        leaf = self.put_manifest("leaf", "5.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.assertTrue(self.resolve_conflict(root).startswith("version_mismatch"))

    def test_non_empty_intersection_excluding_target_is_mismatch_not_empty(self) -> None:
        # [3, 4) is a perfectly non-empty interval; a 2.0.0 target is a mismatch.
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest(
            "root", "1.0.0", {"leaf": dep(leaf, ">=3.0.0 <4.0.0")})
        self.assertTrue(self.resolve_conflict(root).startswith("version_mismatch"))

    def test_no_partial_resolution_on_failure(self) -> None:
        good = self.put_manifest("good", "1.0.0", {})
        bad = self.put_manifest("bad", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {
            "good": dep(good, "^1"),
            "bad": dep(bad, "^1"),
        })
        message = self.resolve_conflict(root)
        self.assertTrue(message.startswith("version_mismatch"))

    def test_root_own_version_is_not_adjudicated(self) -> None:
        root = self.put_manifest("root", "01", {})
        self.assertEqual(self.store.resolve(root)["resolved"], [])

    def test_resolve_does_not_touch_refs(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        before = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.store.resolve(root)
        after = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.assertEqual(before, after)
        self.assertEqual(self.store.stats()["puts"], 2)

    def test_resolve_reads_zero_ref_blobs_still_retained(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.store.release(leaf)
        self.store.release(root)
        self.assertEqual(len(self.store.resolve(root)["resolved"]), 1)

    def test_unreachable_invalid_blobs_do_not_affect_resolution(self) -> None:
        self.store.put(b"garbage, not a manifest")
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.resolve(root)["resolved"], [])

    def test_constraint_tokens_separated_by_ascii_whitespace(self) -> None:
        leaf = self.put_manifest("leaf", "1.4.0", {})
        for whitespace in (" ", "\t", "\n", "\r", "\x0b", "\x0c",
                           "  ", " \t\n "):
            text = f">=1.0.0{whitespace}<2.0.0"
            root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, text)})
            result = self.store.resolve(root)
            self.assertEqual(result["resolved"][0]["constraints"], [text])

    def test_adjacency_cycle_is_detected_before_version_checks(self) -> None:
        # A manifest whose own version is bad AND sits in a cycle: cycle wins.
        a, b = "a" * 64, "b" * 64
        self.store._blobs[a] = manifest("a", "01", {"b": dep(b, "^1")})
        self.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a, "^1")})
        self.assertTrue(self.resolve_conflict(a).startswith("cycle"))


class ResolveHttpTests(unittest.TestCase):
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
        with self.server.store._lock:
            self.server.store._blobs.clear()
            self.server.store._meta.clear()
            self.server.store._refs.clear()

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def put(self, payload: bytes) -> str:
        status, _, headers = self.call("PUT", "/v1/blobs", payload)
        self.assertEqual(status, 201)
        return headers["X-Blob-Digest"]

    def put_manifest(self, name: str, version: str, deps: dict | None = ...) -> str:
        return self.put(manifest(name, version, deps))

    def resolve(self, digest: str):
        return self.call("GET", f"/v1/blobs/{digest}/resolve")

    def test_resolve_happy_path_shape(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        status, body, _ = self.resolve(root)
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(set(payload), {"root", "resolved"})
        self.assertEqual(payload, {
            "root": root,
            "resolved": [{
                "name": "leaf", "digest": leaf, "version": "2.0.0",
                "constraints": ["^2"],
            }],
        })

    def test_empty_resolution_shape(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        status, body, _ = self.resolve(root)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"root": root, "resolved": []})

    def test_resolve_error_mapping(self) -> None:
        status, body, _ = self.resolve("not-a-digest")
        self.assertEqual((status, json.loads(body)["error"]["code"]),
                         (400, "invalid_request"))
        status, body, _ = self.resolve("0" * 64)
        self.assertEqual((status, json.loads(body)["error"]["code"]),
                         (404, "not_found"))
        garbage = self.put(b"definitely not a manifest")
        status, body, _ = self.resolve(garbage)
        error = json.loads(body)["error"]
        self.assertEqual((status, error["code"]), (409, "conflict"))
        self.assertIn("is not a valid manifest", error["message"])
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("e" * 64, "^1")})
        status, body, _ = self.resolve(root)
        self.assertEqual((status, json.loads(body)["error"]["code"]),
                         (409, "conflict"))

    def test_conflict_message_prefixes_over_http(self) -> None:
        # cycle
        a, b = "a" * 64, "b" * 64
        with self.server.store._lock:
            self.server.store._blobs[a] = manifest("a", "1.0.0", {"b": dep(b, "^1")})
            self.server.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a, "^1")})
        status, body, _ = self.resolve(a)
        self.assertEqual(status, 409)
        self.assertTrue(json.loads(body)["error"]["message"].startswith("cycle"))

        cases = {
            "version_syntax": ("bad", "01.0", "^1"),
            "constraint_syntax": ("leaf", "1.0.0", "nope"),
            "name_mismatch": None,  # built below
            "empty_intersection": None,
            "version_mismatch": None,
            "ambiguous_name": None,
        }
        # version_syntax / constraint_syntax
        for prefix, spec in cases.items():
            if spec is None:
                continue
            dep_name, version, constraint = spec
            target = self.put_manifest(dep_name, version, {})
            root = self.put_manifest(
                "root", "1.0.0", {dep_name: dep(target, constraint)})
            status, body, _ = self.resolve(root)
            self.assertEqual(status, 409)
            self.assertTrue(
                json.loads(body)["error"]["message"].startswith(prefix), prefix)

        # name_mismatch
        actual = self.put_manifest("actual", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"wanted": dep(actual, "^1")})
        status, body, _ = self.resolve(root)
        self.assertTrue(
            json.loads(body)["error"]["message"].startswith("name_mismatch"))

        # empty_intersection
        target = self.put_manifest("leaf", "3.0.0", {})
        root = self.put_manifest(
            "root", "1.0.0", {"leaf": dep(target, ">=3 <3")})
        status, body, _ = self.resolve(root)
        self.assertTrue(
            json.loads(body)["error"]["message"].startswith("empty_intersection"))

        # version_mismatch
        target = self.put_manifest("leaf", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(target, "^1")})
        status, body, _ = self.resolve(root)
        self.assertTrue(
            json.loads(body)["error"]["message"].startswith("version_mismatch"))

        # ambiguous_name
        first = self.put_manifest("lib", "1.0.0", {})
        second = self.put_manifest("lib", "2.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"lib": dep(first, "^1")})
        right = self.put_manifest("right", "1.0.0", {"lib": dep(second, "^2")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        status, body, _ = self.resolve(root)
        self.assertTrue(
            json.loads(body)["error"]["message"].startswith("ambiguous_name"))

    def test_no_partial_resolved_in_error_body(self) -> None:
        good = self.put_manifest("good", "1.0.0", {})
        bad = self.put_manifest("bad", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {
            "good": dep(good, "^1"),
            "bad": dep(bad, "^1"),
        })
        status, body, _ = self.resolve(root)
        self.assertEqual(status, 409)
        self.assertNotIn("resolved", json.loads(body))

    def test_resolve_does_not_change_refs_or_gc_semantics(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.assertEqual(self.resolve(root)[0], 200)
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = {item["digest"]: item["refs"] for item in json.loads(body)["blobs"]}
        self.assertEqual(listing, {leaf: 1, root: 1})
        self.call("DELETE", f"/v1/blobs/{root}/refs")
        self.call("DELETE", f"/v1/blobs/{leaf}/refs")
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["deleted"], sorted([leaf, root]))
        self.assertEqual(self.resolve(root)[0], 404)

    def test_graph_endpoint_still_passes_constraints_through(self) -> None:
        # The graph entry must remain untouched: an unsatisfiable/bogus constraint
        # does not tighten graph's validation at all.
        leaf = self.put_manifest("leaf", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "nope")})
        status, _, _ = self.call("GET", f"/v1/blobs/{root}/graph")
        self.assertEqual(status, 200)
        self.assertEqual(self.resolve(root)[0], 409)

    def test_resolve_unknown_route_segment_is_not_found(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        status, _, _ = self.call("GET", f"/v1/blobs/{root}/resolve/extra")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
