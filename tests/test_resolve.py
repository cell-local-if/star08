"""Tests for version/constraint resolution and GET /v1/blobs/{digest}/resolve."""
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
    Store,
    parse_constraint,
    parse_version,
)


def manifest(name: str, version: str, deps: dict | None = ...) -> bytes:
    payload: dict = {"name": name, "version": version}
    if deps is not ...:
        payload["dependencies"] = deps
    return json.dumps(payload).encode()


def dep(digest: str, constraint: str = ">=1.0") -> dict:
    return {"digest": digest, "constraint": constraint}


class ParseVersionTests(unittest.TestCase):
    def test_valid_versions_pad_to_three_segments(self) -> None:
        self.assertEqual(parse_version("0"), ((0, 0, 0), 1))
        self.assertEqual(parse_version("1"), ((1, 0, 0), 1))
        self.assertEqual(parse_version("1.2"), ((1, 2, 0), 2))
        self.assertEqual(parse_version("1.2.3"), ((1, 2, 3), 3))
        self.assertEqual(parse_version("0.0.0"), ((0, 0, 0), 3))
        self.assertEqual(parse_version("10.20.30"), ((10, 20, 30), 3))

    def test_invalid_versions(self) -> None:
        for bad in ("", ".", "1.", ".1", "1..2", "1.2.3.4", "01", "1.02", "00",
                    "1.2.x", "-1", "+1", "1 2", "v1", "1.2 ", 1, None):
            self.assertIsNone(parse_version(bad), msg=bad)


class ParseConstraintTests(unittest.TestCase):
    def test_single_operators(self) -> None:
        self.assertEqual(parse_constraint("=1.2.3"), [((1, 2, 3), True, (1, 2, 3), True)])
        self.assertEqual(parse_constraint(">1"), [((1, 0, 0), False, None, False)])
        self.assertEqual(parse_constraint(">=1.2"), [((1, 2, 0), True, None, False)])
        self.assertEqual(parse_constraint("<2"), [((0, 0, 0), True, (2, 0, 0), False)])
        self.assertEqual(parse_constraint("<=2"), [((0, 0, 0), True, (2, 0, 0), True)])

    def test_caret_ranges(self) -> None:
        self.assertEqual(parse_constraint("^1.2.3"), [((1, 2, 3), True, (2, 0, 0), False)])
        self.assertEqual(parse_constraint("^1"), [((1, 0, 0), True, (2, 0, 0), False)])
        self.assertEqual(parse_constraint("^0.2.3"), [((0, 2, 3), True, (0, 3, 0), False)])
        self.assertEqual(parse_constraint("^0.0.3"), [((0, 0, 3), True, (0, 0, 4), False)])
        self.assertEqual(parse_constraint("^0"), [((0, 0, 0), True, (0, 0, 1), False)])

    def test_tilde_ranges(self) -> None:
        self.assertEqual(parse_constraint("~1"), [((1, 0, 0), True, (2, 0, 0), False)])
        self.assertEqual(parse_constraint("~1.2"), [((1, 2, 0), True, (1, 3, 0), False)])
        self.assertEqual(parse_constraint("~1.2.3"), [((1, 2, 3), True, (1, 3, 0), False)])
        self.assertEqual(parse_constraint("~0.2"), [((0, 2, 0), True, (0, 3, 0), False)])

    def test_multiple_tokens_split_on_ascii_whitespace(self) -> None:
        self.assertEqual(parse_constraint(">=1.0  <2.0"),
                         [((1, 0, 0), True, None, False), ((0, 0, 0), True, (2, 0, 0), False)])
        self.assertEqual(len(parse_constraint(">=1.0\t<2.0\n<=3.0")), 3)

    def test_invalid_constraints(self) -> None:
        for bad in ("", "   ", "1.0", "==1.0", "=>1.0", "~", "^", ">=1.0.0.1",
                    ">01", "<1..2", ">= 1.0", "!1.0", "1.0.0-", ">=1.0 <2.0"):
            self.assertIsNone(parse_constraint(bad), msg=bad)


class StoreResolveUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def put_manifest(self, name: str, version: str, deps: dict | None = ...) -> str:
        return self.store.put(manifest(name, version, deps)).digest

    def test_root_without_dependencies_resolves_to_empty_list(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.resolve(root), {"root": root, "resolved": []})

    def test_single_dependency_happy_path(self) -> None:
        leaf = self.put_manifest("leaf", "1.4.2", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1.2")})
        result = self.store.resolve(root)
        self.assertEqual(result["root"], root)
        self.assertEqual(result["resolved"], [
            {"name": "leaf", "digest": leaf, "version": "1.4.2", "constraints": ["^1.2"]},
        ])

    def test_transitive_constraints_merge_per_name(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        mid = self.put_manifest("mid", "2.0.0", {"leaf": dep(leaf, "<2.0 >=1.4")})
        other = self.put_manifest("other", "3.0.0", {"leaf": dep(leaf, "^1.2")})
        root = self.put_manifest("root", "1.0.0",
                                 {"mid": dep(mid, "~2.0"), "other": dep(other, "=3.0.0"),
                                  "leaf": dep(leaf, "^1.2")})
        result = self.store.resolve(root)
        self.assertEqual([entry["name"] for entry in result["resolved"]],
                         ["leaf", "mid", "other"])
        leaf_entry = result["resolved"][0]
        # identical constraint strings from different edges dedupe; merged across the graph
        self.assertEqual(leaf_entry["constraints"], ["<2.0 >=1.4", "^1.2"])
        self.assertEqual(leaf_entry["version"], "1.5.0")

    def test_resolved_sorted_by_name(self) -> None:
        b = self.put_manifest("b", "1.0.0", {})
        a = self.put_manifest("a", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0",
                                 {"b": dep(b, "=1.0.0"), "a": dep(a, "=1.0.0")})
        self.assertEqual([e["name"] for e in self.store.resolve(root)["resolved"]], ["a", "b"])

    def test_resolve_does_not_touch_refs(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf)})
        before = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.store.resolve(root)
        after = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.assertEqual(before, after)
        self.assertEqual(self.store.stats()["puts"], 2)

    def test_invalid_root_digest_is_invalid_request(self) -> None:
        for bad in ("zz", "A" * 64, "0" * 63, 42):
            with self.assertRaises(InvalidRequest):
                self.store.resolve(bad)

    def test_unknown_root_digest_is_not_found(self) -> None:
        with self.assertRaises(BlobNotFound):
            self.store.resolve("0" * 64)

    def test_non_manifest_root_is_a_conflict(self) -> None:
        digest = self.store.put(b"not json").digest
        with self.assertRaises(DigestConflict):
            self.store.resolve(digest)

    def test_missing_dependency_blob_is_a_conflict(self) -> None:
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("f" * 64)})
        with self.assertRaises(DigestConflict) as ctx:
            self.store.resolve(root)
        self.assertIn("does not exist", str(ctx.exception))

    def test_cycle_is_a_conflict_with_cycle_prefix(self) -> None:
        a, b = "a" * 64, "b" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"b": dep(b)})
        self.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a)})
        with self.assertRaises(DigestConflict) as ctx:
            self.store.resolve(a)
        self.assertTrue(str(ctx.exception).startswith("cycle"), str(ctx.exception))

    def assert_conflict_prefix(self, root: str, prefix: str) -> None:
        with self.assertRaises(DigestConflict) as ctx:
            self.store.resolve(root)
        self.assertTrue(str(ctx.exception).startswith(prefix),
                        f"{prefix!r} vs {ctx.exception}")

    def test_version_syntax_error(self) -> None:
        leaf = self.put_manifest("leaf", "01.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, ">=1")})
        self.assert_conflict_prefix(root, "version_syntax")

    def test_root_version_is_not_checked(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "not-a-version", {"leaf": dep(leaf, "=1.0.0")})
        self.assertEqual(len(self.store.resolve(root)["resolved"]), 1)

    def test_constraint_syntax_error(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        for bad in ("1.0", "=>1.0", ">= 1.0", ">>1"):
            root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, bad)})
            self.assert_conflict_prefix(root, "constraint_syntax")

    def test_name_mismatch(self) -> None:
        leaf = self.put_manifest("actual", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"declared": dep(leaf, ">=1")})
        self.assert_conflict_prefix(root, "name_mismatch")

    def test_ambiguous_name(self) -> None:
        one = self.put_manifest("leaf", "1.0.0", {})
        two = self.put_manifest("leaf", "1.1.0", {})
        root = self.put_manifest("root", "1.0.0",
                                 {"leaf": dep(one), "other": dep(two)})
        # same name via two paths: rename the second edge's key to collide
        mid = self.put_manifest("mid", "1.0.0", {"leaf": dep(two)})
        root = self.put_manifest("root", "1.0.0",
                                 {"leaf": dep(one), "mid": dep(mid, ">=1")})
        self.assert_conflict_prefix(root, "ambiguous_name")

    def test_same_digest_same_name_is_not_ambiguous(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        mid = self.put_manifest("mid", "1.0.0", {"leaf": dep(leaf, ">=1.0")})
        root = self.put_manifest("root", "1.0.0",
                                 {"leaf": dep(leaf, "<2.0"), "mid": dep(mid, "=1.0.0")})
        self.assertEqual(len(self.store.resolve(root)["resolved"]), 2)

    def test_empty_intersection(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, ">=2.0 <2.0")})
        self.assert_conflict_prefix(root, "empty_intersection")

    def test_empty_intersection_across_manifests(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        mid = self.put_manifest("mid", "1.0.0", {"leaf": dep(leaf, "<1.0")})
        root = self.put_manifest("root", "1.0.0",
                                 {"leaf": dep(leaf, ">=1.0"), "mid": dep(mid, "=1.0.0")})
        self.assert_conflict_prefix(root, "empty_intersection")

    def test_version_mismatch(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2.0")})
        self.assert_conflict_prefix(root, "version_mismatch")

    def test_exclusive_bound_mismatch(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "<2.0.0")})
        self.assert_conflict_prefix(root, "version_mismatch")

    def test_caret_zero_major_boundaries(self) -> None:
        ok = self.put_manifest("ok", "0.2.9", {})
        root = self.put_manifest("root", "1.0.0", {"ok": dep(ok, "^0.2.3")})
        self.assertEqual(len(self.store.resolve(root)["resolved"]), 1)
        bad = self.put_manifest("bad", "0.3.0", {})
        root = self.put_manifest("root", "1.0.0", {"bad": dep(bad, "^0.2.3")})
        self.assert_conflict_prefix(root, "version_mismatch")

    def test_tilde_segment_count_boundaries(self) -> None:
        leaf = self.put_manifest("leaf", "1.9.9", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "~1")})
        self.assertEqual(len(self.store.resolve(root)["resolved"]), 1)
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "~1.2")})
        self.assert_conflict_prefix(root, "version_mismatch")

    def test_unreachable_invalid_blobs_do_not_affect_resolution(self) -> None:
        self.store.put(b"garbage, not a manifest")
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.resolve(root)["resolved"], [])


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

    def call(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
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
        leaf = self.put_manifest("leaf", "2.1.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        status, body, _ = self.resolve(root)
        self.assertEqual(status, 200)
        result = json.loads(body)
        self.assertEqual(set(result), {"root", "resolved"})
        self.assertEqual(result["root"], root)
        self.assertEqual(result["resolved"], [
            {"name": "leaf", "digest": leaf, "version": "2.1.0", "constraints": ["^2"]},
        ])

    def test_resolve_error_mapping(self) -> None:
        status, body, _ = self.resolve("not-a-digest")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.resolve("0" * 64)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (404, "not_found"))
        garbage = self.put(b"definitely not a manifest")
        status, body, _ = self.resolve(garbage)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
        error = json.loads(body)["error"]
        self.assertEqual(set(error), {"code", "message"})

    def test_resolve_conflict_prefixes_over_http(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        cases = {
            "version_mismatch": {"leaf": dep(leaf, "^2")},
            "empty_intersection": {"leaf": dep(leaf, ">2 <2")},
            "constraint_syntax": {"leaf": dep(leaf, "=>1")},
            "name_mismatch": {"renamed": dep(leaf, ">=1")},
        }
        for prefix, deps in cases.items():
            root = self.put_manifest("root", "1.0.0", deps)
            status, body, _ = self.resolve(root)
            self.assertEqual(status, 409, prefix)
            message = json.loads(body)["error"]["message"]
            self.assertTrue(message.startswith(prefix), f"{prefix} vs {message}")

    def test_resolve_never_returns_partial_results(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        broken = self.put(b"not json at all")
        root = self.put_manifest("root", "1.0.0",
                                 {"leaf": dep(leaf), "broken": dep(broken)})
        status, body, _ = self.resolve(root)
        self.assertEqual(status, 409)
        self.assertNotIn("resolved", json.loads(body))

    def test_resolve_does_not_change_refs_or_gc_semantics(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "=1.0.0")})
        self.resolve(root)
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = {item["digest"]: item["refs"] for item in json.loads(body)["blobs"]}
        self.assertEqual(listing, {leaf: 1, root: 1})
        self.call("DELETE", f"/v1/blobs/{root}/refs")
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(json.loads(body)["deleted"], [root])
        self.assertEqual(self.resolve(root)[0], 404)
        self.assertEqual(self.resolve(leaf)[0], 200)


if __name__ == "__main__":
    unittest.main()
