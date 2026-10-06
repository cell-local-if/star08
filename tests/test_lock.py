"""Tests for GET /v1/blobs/{digest}/lock: reproducible lock documents."""
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
)


def manifest(name: str, version: str, deps: dict | None = ...) -> bytes:
    payload: dict = {"name": name, "version": version}
    if deps is not ...:
        payload["dependencies"] = deps
    return json.dumps(payload).encode()


def dep(digest: str, constraint: str) -> dict:
    return {"digest": digest, "constraint": constraint}


class StoreLockUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def put_manifest(self, name: str, version: str, deps: dict | None = ...) -> str:
        return self.store.put(manifest(name, version, deps)).digest

    def lock_conflict(self, digest: str) -> str:
        with self.assertRaises(ResolveConflict) as caught:
            self.store.lock(digest)
        return str(caught.exception)

    def test_root_without_dependencies_locks_empty(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.lock(root), {
            "lock_version": 1,
            "root": {"name": "root", "version": "1.0.0", "digest": root},
            "packages": [],
        })

    def test_top_level_and_entry_key_order(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        raw = json.dumps(self.store.lock(root))
        self.assertLess(raw.index('"lock_version"'), raw.index('"root"'))
        self.assertLess(raw.index('"root"'), raw.index('"packages"'))
        entry = json.dumps(self.store.lock(root)["packages"][0])
        keys = ['"name"', '"version"', '"digest"', '"constraints"', '"dependencies"']
        positions = [entry.index(k) for k in keys]
        self.assertEqual(positions, sorted(positions))
        root_obj = json.dumps(self.store.lock(root)["root"])
        positions = [root_obj.index(k) for k in ('"name"', '"version"', '"digest"')]
        self.assertEqual(positions, sorted(positions))

    def test_happy_diamond_pools_inbound_constraints(self) -> None:
        leaf = self.put_manifest("leaf", "2.4.1", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, ">=2.0")})
        right = self.put_manifest("right", "1.0.0",
                                  {"leaf": dep(leaf, "<3.0.0\t^2.1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "~1")})
        result = self.store.lock(root)
        self.assertEqual(result["lock_version"], 1)
        self.assertEqual(result["root"],
                         {"name": "root", "version": "1.0.0", "digest": root})
        self.assertEqual(result["packages"], [
            {"name": "leaf", "version": "2.4.1", "digest": leaf,
             "constraints": ["<3.0.0\t^2.1", ">=2.0"],
             "dependencies": []},
            {"name": "left", "version": "1.0.0", "digest": left,
             "constraints": ["^1"],
             "dependencies": [
                 {"name": "leaf", "digest": leaf, "constraint": ">=2.0"}]},
            {"name": "right", "version": "1.0.0", "digest": right,
             "constraints": ["~1"],
             "dependencies": [
                 {"name": "leaf", "digest": leaf, "constraint": "<3.0.0\t^2.1"}]},
        ])

    def test_packages_sorted_by_name_then_digest(self) -> None:
        zebra = self.put_manifest("zebra", "1.0.0", {})
        apple = self.put_manifest("apple", "1.0.0", {})
        middle = self.put_manifest("middle", "1.0.0", {})
        root = self.put_manifest(
            "root", "1.0.0", {
                "zebra": dep(zebra, "^1"),
                "apple": dep(apple, "^1"),
                "middle": dep(middle, "^1"),
            })
        names = [p["name"] for p in self.store.lock(root)["packages"]]
        self.assertEqual(names, ["apple", "middle", "zebra"])

    def test_dependencies_sorted_by_name_with_verbatim_constraint(self) -> None:
        zebra = self.put_manifest("zebra", "1.0.0", {})
        apple = self.put_manifest("apple", "1.0.0", {})
        child = self.put_manifest(
            "child", "1.0.0", {
                "zebra": dep(zebra, "^1"),
                "apple": dep(apple, ">=1.0.0  <2.0.0"),
            })
        root = self.put_manifest("root", "1.0.0", {"child": dep(child, "^1")})
        packages = {p["name"]: p for p in self.store.lock(root)["packages"]}
        self.assertEqual(packages["child"]["dependencies"], [
            {"name": "apple", "digest": apple, "constraint": ">=1.0.0  <2.0.0"},
            {"name": "zebra", "digest": zebra, "constraint": "^1"},
        ])

    def test_constraints_deduplicated_as_whole_strings(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, "^1")})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf, "^1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        packages = {p["name"]: p for p in self.store.lock(root)["packages"]}
        self.assertEqual(packages["leaf"]["constraints"], ["^1"])

    def test_version_strings_are_verbatim(self) -> None:
        leaf = self.put_manifest("leaf", "2", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "~2")})
        packages = self.store.lock(root)["packages"]
        self.assertEqual(packages[0]["version"], "2")

    def test_root_excluded_from_packages(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        digests = [p["digest"] for p in self.store.lock(root)["packages"]]
        self.assertNotIn(root, digests)

    def test_repeated_lock_is_byte_identical(self) -> None:
        leaf = self.put_manifest("leaf", "2.4.1", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, ">=2.0")})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf, "<3.0.0")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        first = json.dumps(self.store.lock(root))
        for _ in range(5):
            self.assertEqual(json.dumps(self.store.lock(root)), first)

    def test_deep_chain_does_not_hit_recursion_limits(self) -> None:
        previous = self.put_manifest("pkg-0", "1.0.0", {})
        for i in range(1, 1200):
            previous = self.put_manifest(
                f"pkg-{i}", "1.0.0",
                {f"pkg-{i - 1}": dep(previous, "^1")})
        self.assertEqual(len(self.store.lock(previous)["packages"]), 1199)

    def test_invalid_root_digest_is_invalid_request(self) -> None:
        for bad in ("zz", "A" * 64, "0" * 63, 42):
            with self.assertRaises(InvalidRequest):
                self.store.lock(bad)

    def test_unknown_root_digest_is_not_found(self) -> None:
        with self.assertRaises(BlobNotFound):
            self.store.lock("0" * 64)

    def test_malformed_manifest_uses_graph_conflict_text(self) -> None:
        digest = self.store.put(b"not json").digest
        with self.assertRaises(DigestConflict) as caught:
            self.store.lock(digest)
        self.assertIs(type(caught.exception), DigestConflict)
        self.assertIn("is not a valid manifest", str(caught.exception))

    def test_missing_dependency_uses_graph_conflict_text(self) -> None:
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("f" * 64, "^1")})
        with self.assertRaises(DigestConflict) as caught:
            self.store.lock(root)
        self.assertIs(type(caught.exception), DigestConflict)
        self.assertIn("dependency blob", str(caught.exception))

    def test_conflict_prefixes_match_resolve(self) -> None:
        # cycle
        a, b = "a" * 64, "b" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"b": dep(b, "^1")})
        self.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a, "^1")})
        self.assertTrue(self.lock_conflict(a).startswith("cycle"))

        # version_syntax
        bad = self.put_manifest("bad", "01", {})
        root = self.put_manifest("root", "1.0.0", {"bad": dep(bad, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("version_syntax"))

        # constraint_syntax
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "nope")})
        self.assertTrue(self.lock_conflict(root).startswith("constraint_syntax"))

        # name_mismatch
        actual = self.put_manifest("actual", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"wanted": dep(actual, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("name_mismatch"))

        # ambiguous_name
        first = self.put_manifest("lib", "1.0.0", {})
        second = self.put_manifest("lib", "2.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"lib": dep(first, "^1")})
        right = self.put_manifest("right", "1.0.0", {"lib": dep(second, "^2")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("ambiguous_name"))

        # empty_intersection
        target = self.put_manifest("leaf", "3.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(target, ">=3 <3")})
        self.assertTrue(self.lock_conflict(root).startswith("empty_intersection"))

        # version_mismatch
        target = self.put_manifest("leaf", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(target, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("version_mismatch"))

    def test_root_own_version_is_not_adjudicated(self) -> None:
        root = self.put_manifest("root", "01", {})
        result = self.store.lock(root)
        self.assertEqual(result["root"]["version"], "01")
        self.assertEqual(result["packages"], [])

    def test_lock_does_not_touch_refs(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        before = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.store.lock(root)
        after = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.assertEqual(before, after)
        self.assertEqual(self.store.stats()["puts"], 2)

    def test_lock_reads_zero_ref_blobs_still_retained(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.store.release(leaf)
        self.store.release(root)
        self.assertEqual(len(self.store.lock(root)["packages"]), 1)


class LockHttpTests(unittest.TestCase):
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

    def lock(self, digest: str):
        return self.call("GET", f"/v1/blobs/{digest}/lock")

    def test_lock_happy_path_shape(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        status, body, headers = self.lock(root)
        self.assertEqual(status, 200)
        self.assertTrue(headers["Content-Type"].startswith("application/json"))
        payload = json.loads(body)
        self.assertEqual(set(payload), {"lock_version", "root", "packages"})
        self.assertEqual(payload, {
            "lock_version": 1,
            "root": {"name": "root", "version": "1.0.0", "digest": root},
            "packages": [{
                "name": "leaf", "version": "2.0.0", "digest": leaf,
                "constraints": ["^2"], "dependencies": [],
            }],
        })

    def test_repeated_requests_are_byte_identical(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        status, first, _ = self.lock(root)
        self.assertEqual(status, 200)
        for _ in range(3):
            status, body, _ = self.lock(root)
            self.assertEqual(status, 200)
            self.assertEqual(body, first)

    def test_lock_error_mapping(self) -> None:
        status, body, _ = self.lock("not-a-digest")
        self.assertEqual((status, json.loads(body)["error"]["code"]),
                         (400, "invalid_request"))
        status, body, _ = self.lock("0" * 64)
        self.assertEqual((status, json.loads(body)["error"]["code"]),
                         (404, "not_found"))
        garbage = self.put(b"definitely not a manifest")
        status, body, _ = self.lock(garbage)
        error = json.loads(body)["error"]
        self.assertEqual((status, error["code"]), (409, "conflict"))
        self.assertIn("is not a valid manifest", error["message"])
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("e" * 64, "^1")})
        status, body, _ = self.lock(root)
        self.assertEqual((status, json.loads(body)["error"]["code"]),
                         (409, "conflict"))

    def test_conflict_prefix_over_http(self) -> None:
        target = self.put_manifest("leaf", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(target, "^1")})
        status, body, _ = self.lock(root)
        self.assertEqual(status, 409)
        self.assertTrue(
            json.loads(body)["error"]["message"].startswith("version_mismatch"))

    def test_no_partial_lock_in_error_body(self) -> None:
        good = self.put_manifest("good", "1.0.0", {})
        bad = self.put_manifest("bad", "9.0.0", {})
        root = self.put_manifest("root", "1.0.0", {
            "good": dep(good, "^1"),
            "bad": dep(bad, "^1"),
        })
        status, body, _ = self.lock(root)
        self.assertEqual(status, 409)
        self.assertNotIn("packages", json.loads(body))

    def test_lock_does_not_change_refs_or_gc_semantics(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.assertEqual(self.lock(root)[0], 200)
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = {item["digest"]: item["refs"] for item in json.loads(body)["blobs"]}
        self.assertEqual(listing, {leaf: 1, root: 1})
        self.call("DELETE", f"/v1/blobs/{root}/refs")
        self.call("DELETE", f"/v1/blobs/{leaf}/refs")
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["deleted"], sorted([leaf, root]))
        self.assertEqual(self.lock(root)[0], 404)

    def test_lock_unknown_route_segment_is_not_found(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        status, _, _ = self.call("GET", f"/v1/blobs/{root}/lock/extra")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
