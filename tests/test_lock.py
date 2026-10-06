"""Tests for the reproducible lock view at GET /v1/blobs/{digest}/lock."""
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


CONFLICT_PREFIXES = (
    "cycle", "version_syntax", "constraint_syntax", "name_mismatch",
    "ambiguous_name", "empty_intersection", "version_mismatch",
)


class StoreLockUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def put_manifest(self, name: str, version: str, deps: dict | None = ...) -> str:
        return self.store.put(manifest(name, version, deps)).digest

    def lock_conflict(self, digest: str) -> str:
        with self.assertRaises(DigestConflict) as caught:
            self.store.lock(digest)
        return str(caught.exception)

    def test_root_without_dependencies_locks_empty(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.lock(root), {
            "lock_version": 1,
            "root": {"name": "root", "version": "1.0.0", "digest": root},
            "packages": [],
        })

    def test_dependencies_field_may_be_omitted(self) -> None:
        root = self.put_manifest("root", "1.0.0")
        self.assertEqual(self.store.lock(root)["packages"], [])

    def test_root_reports_verbatim_strings_and_is_not_a_package(self) -> None:
        leaf = self.put_manifest("leaf", "2", {})
        root = self.put_manifest("root", "01", {"leaf": dep(leaf, "~2")})
        result = self.store.lock(root)
        self.assertEqual(result["root"],
                         {"name": "root", "version": "01", "digest": root})
        self.assertEqual([p["digest"] for p in result["packages"]], [leaf])

    def test_happy_diamond_shape(self) -> None:
        leaf = self.put_manifest("leaf", "2.4.1", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, ">=2.0")})
        right = self.put_manifest("right", "1.0.0",
                                  {"leaf": dep(leaf, "<3.0.0\t^2.1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "~1")})
        result = self.store.lock(root)
        self.assertEqual(result["lock_version"], 1)
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

    def test_constraints_deduplicated_whole_and_sorted(self) -> None:
        leaf = self.put_manifest("leaf", "1.5.0", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, "^1")})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf, "^1")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        package = [p for p in self.store.lock(root)["packages"]
                   if p["name"] == "leaf"][0]
        self.assertEqual(package["constraints"], ["^1"])

    def test_constraint_strings_keep_whitespace_and_spelling(self) -> None:
        leaf = self.put_manifest("leaf", "1.4.0", {})
        text = ">=1.0.0 \t\n<2.0.0"
        middle = self.put_manifest("middle", "1.0.0", {"leaf": dep(leaf, text)})
        root = self.put_manifest("root", "1.0.0", {"middle": dep(middle, "^1")})
        packages = {p["name"]: p for p in self.store.lock(root)["packages"]}
        self.assertEqual(packages["leaf"]["constraints"], [text])
        self.assertEqual(packages["middle"]["dependencies"][0]["constraint"], text)

    def test_dependencies_sorted_by_name_with_verbatim_constraint(self) -> None:
        zebra = self.put_manifest("zebra", "1.0.0", {})
        apple = self.put_manifest("apple", "2.0.0", {})
        middle = self.put_manifest(
            "middle", "1.0.0", {
                "zebra": dep(zebra, "^1"),
                "apple": dep(apple, "~2"),
            })
        root = self.put_manifest("root", "1.0.0", {"middle": dep(middle, "^1")})
        package = [p for p in self.store.lock(root)["packages"]
                   if p["name"] == "middle"][0]
        self.assertEqual(package["dependencies"], [
            {"name": "apple", "digest": apple, "constraint": "~2"},
            {"name": "zebra", "digest": zebra, "constraint": "^1"},
        ])

    def test_version_strings_are_reported_verbatim(self) -> None:
        leaf = self.put_manifest("leaf", "2", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "~2")})
        self.assertEqual(self.store.lock(root)["packages"][0]["version"], "2")

    def test_deep_chain_does_not_hit_recursion_limits(self) -> None:
        previous = self.put_manifest("pkg-0", "1.0.0", {})
        for i in range(1, 1200):
            previous = self.put_manifest(
                f"pkg-{i}", "1.0.0",
                {f"pkg-{i - 1}": dep(previous, "^1")})
        result = self.store.lock(previous)
        self.assertEqual(len(result["packages"]), 1199)

    def test_invalid_root_digest_is_invalid_request(self) -> None:
        for bad in ("zz", "A" * 64, "0" * 63, 42):
            with self.assertRaises(InvalidRequest):
                self.store.lock(bad)

    def test_unknown_root_digest_is_not_found(self) -> None:
        with self.assertRaises(BlobNotFound):
            self.store.lock("0" * 64)

    def test_malformed_manifest_uses_graph_conflict_text(self) -> None:
        for raw in (b"not json", b"[1, 2]",
                    json.dumps({"name": "a", "version": "1", "extra": 1}).encode()):
            digest = self.store.put(raw).digest
            with self.assertRaises(DigestConflict, msg=raw) as caught:
                self.store.lock(digest)
            self.assertIs(type(caught.exception), DigestConflict)
            self.assertIn("is not a valid manifest", str(caught.exception))

    def test_missing_dependency_uses_graph_conflict_text(self) -> None:
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("f" * 64, "^1")})
        with self.assertRaises(DigestConflict) as caught:
            self.store.lock(root)
        self.assertIs(type(caught.exception), DigestConflict)
        self.assertIn("dependency blob", str(caught.exception))
        self.assertFalse(str(caught.exception).startswith(CONFLICT_PREFIXES))

    def test_cycle_prefix(self) -> None:
        a, b = "a" * 64, "b" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"b": dep(b, "^1")})
        self.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a, "^1")})
        message = self.lock_conflict(a)
        self.assertIsInstance(message, str)
        self.assertTrue(message.startswith("cycle"))

    def test_self_dependency_is_cycle(self) -> None:
        a = "a" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"a": dep(a, "^1")})
        self.assertTrue(self.lock_conflict(a).startswith("cycle"))

    def test_version_syntax_prefix(self) -> None:
        bad = self.put_manifest("bad", "1.02", {})
        root = self.put_manifest("root", "1.0.0", {"bad": dep(bad, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("version_syntax"))

    def test_constraint_syntax_prefix(self) -> None:
        target = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(target, "nope")})
        self.assertTrue(self.lock_conflict(root).startswith("constraint_syntax"))

    def test_name_mismatch_prefix(self) -> None:
        actual = self.put_manifest("actual", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"wanted": dep(actual, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("name_mismatch"))

    def test_ambiguous_name_prefix(self) -> None:
        first = self.put_manifest("lib", "1.0.0", {})
        second = self.put_manifest("lib", "2.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"lib": dep(first, "^1")})
        right = self.put_manifest("right", "1.0.0", {"lib": dep(second, "^2")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("ambiguous_name"))

    def test_empty_intersection_prefix(self) -> None:
        leaf = self.put_manifest("leaf", "3.0.0", {})
        root = self.put_manifest(
            "root", "1.0.0", {"leaf": dep(leaf, ">=3.0.0 <3.0.0")})
        self.assertTrue(self.lock_conflict(root).startswith("empty_intersection"))

    def test_version_mismatch_prefix(self) -> None:
        leaf = self.put_manifest("leaf", "5.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.assertTrue(self.lock_conflict(root).startswith("version_mismatch"))

    def test_conflicts_are_resolve_conflicts(self) -> None:
        leaf = self.put_manifest("leaf", "5.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        with self.assertRaises(ResolveConflict):
            self.store.lock(root)

    def test_root_own_version_is_not_adjudicated(self) -> None:
        root = self.put_manifest("root", "01", {})
        self.assertEqual(self.store.lock(root)["packages"], [])

    def test_lock_does_not_touch_refs_or_stats(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        before = {item["digest"]: item["refs"] for item in self.store.listing()}
        stats_before = self.store.stats()
        self.store.lock(root)
        after = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.assertEqual(before, after)
        self.assertEqual(self.store.stats(), stats_before)

    def test_lock_reads_zero_ref_blobs_still_retained(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        self.store.release(leaf)
        self.store.release(root)
        self.assertEqual(len(self.store.lock(root)["packages"]), 1)

    def test_unreachable_invalid_blobs_do_not_affect_lock(self) -> None:
        self.store.put(b"garbage, not a manifest")
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(self.store.lock(root)["packages"], [])


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
        self.assertEqual(headers["Content-Type"], "application/json")
        payload = json.loads(body.decode("utf-8"))
        self.assertEqual(payload, {
            "lock_version": 1,
            "root": {"name": "root", "version": "1.0.0", "digest": root},
            "packages": [
                {"name": "leaf", "version": "2.0.0", "digest": leaf,
                 "constraints": ["^2"],
                 "dependencies": []},
            ],
        })

    def test_top_level_and_member_key_order(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        status, body, _ = self.lock(root)
        self.assertEqual(status, 200)
        text = body.decode("utf-8")
        self.assertLess(text.index('"lock_version"'), text.index('"root"'))
        self.assertLess(text.index('"root"'), text.index('"packages"'))
        self.assertLess(text.index('"name"'), text.index('"version"'))
        self.assertLess(text.index('"version"'), text.index('"digest"'))
        package = text[text.index('"packages"'):]
        self.assertLess(package.index('"digest"'), package.index('"constraints"'))
        self.assertLess(package.index('"constraints"'),
                        package.index('"dependencies"'))

    def test_dependency_member_key_order(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        middle = self.put_manifest("middle", "1.0.0", {"leaf": dep(leaf, "^2")})
        root = self.put_manifest("root", "1.0.0", {"middle": dep(middle, "^1")})
        status, body, _ = self.lock(root)
        self.assertEqual(status, 200)
        text = body.decode("utf-8")
        entry = text[text.index('"dependencies"', text.index('"packages"')):]
        self.assertLess(entry.index('"name"'), entry.index('"digest"'))
        self.assertLess(entry.index('"digest"'), entry.index('"constraint"'))

    def test_repeated_requests_are_byte_identical(self) -> None:
        leaf = self.put_manifest("leaf", "2.4.1", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf, ">=2.0")})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf, "<3.0.0")})
        root = self.put_manifest(
            "root", "1.0.0",
            {"left": dep(left, "^1"), "right": dep(right, "~1")})
        bodies = set()
        for _ in range(5):
            status, body, _ = self.lock(root)
            self.assertEqual(status, 200)
            bodies.add(body)
        self.assertEqual(len(bodies), 1)

    def test_invalid_digest_is_400(self) -> None:
        status, body, _ = self.lock("zz")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_request")

    def test_unknown_digest_is_404(self) -> None:
        status, body, _ = self.lock("0" * 64)
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["code"], "not_found")

    def test_conflict_is_409_with_prefix_and_no_partial_lock(self) -> None:
        leaf = self.put_manifest("leaf", "5.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        status, body, _ = self.lock(root)
        self.assertEqual(status, 409)
        payload = json.loads(body)
        self.assertEqual(payload["error"]["code"], "conflict")
        self.assertTrue(payload["error"]["message"].startswith("version_mismatch"))
        self.assertNotIn("packages", payload)

    def test_missing_dependency_blob_is_409(self) -> None:
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("f" * 64, "^1")})
        status, body, _ = self.lock(root)
        self.assertEqual(status, 409)
        payload = json.loads(body)
        self.assertEqual(payload["error"]["code"], "conflict")
        self.assertIn("dependency blob", payload["error"]["message"])

    def test_lock_does_not_change_refs_stats_or_gc(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        _, before, _ = self.call("GET", "/v1/blobs")
        status, _, _ = self.lock(root)
        self.assertEqual(status, 200)
        _, after, _ = self.call("GET", "/v1/blobs")
        self.assertEqual(json.loads(before), json.loads(after))
        # Nothing became collectable: gc deletes only refs == 0 blobs.
        _, gc_body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(json.loads(gc_body)["deleted"], [])

    def test_lock_survives_release_of_zero_ref_content(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^1")})
        for digest in (leaf, root):
            status, _, _ = self.call("DELETE", f"/v1/blobs/{digest}/refs")
            self.assertEqual(status, 200)
        status, body, _ = self.lock(root)
        self.assertEqual(status, 200)
        self.assertEqual(len(json.loads(body)["packages"]), 1)


if __name__ == "__main__":
    unittest.main()
