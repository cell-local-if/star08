"""Tests for dependency-manifest resolution and GET /v1/blobs/{digest}/graph."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import BlobNotFound, DigestConflict, InvalidRequest, Store


def manifest(name: str, version: str, deps: dict | None = ...) -> bytes:
    payload: dict = {"name": name, "version": version}
    if deps is not ...:
        payload["dependencies"] = deps
    return json.dumps(payload).encode()


def dep(digest: str, constraint: str = ">=1.0") -> dict:
    return {"digest": digest, "constraint": constraint}


class StoreGraphUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def put_manifest(self, name: str, version: str, deps: dict | None = ...) -> str:
        return self.store.put(manifest(name, version, deps)).digest

    def test_root_without_dependencies_has_single_node_and_no_edges(self) -> None:
        root = self.put_manifest("root", "1.0.0", {})
        graph = self.store.graph(root)
        self.assertEqual(graph, {
            "root": root,
            "nodes": [{"digest": root, "name": "root", "version": "1.0.0"}],
            "edges": [],
        })

    def test_dependencies_field_may_be_omitted(self) -> None:
        root = self.put_manifest("root", "1.0.0")
        graph = self.store.graph(root)
        self.assertEqual(len(graph["nodes"]), 1)
        self.assertEqual(graph["edges"], [])

    def test_diamond_graph_dedups_nodes_and_edges(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        left = self.put_manifest("left", "1.0.0", {"leaf": dep(leaf)})
        right = self.put_manifest("right", "1.0.0", {"leaf": dep(leaf)})
        root = self.put_manifest("root", "1.0.0",
                                 {"left": dep(left, "^1"), "right": dep(right, "~1")})
        graph = self.store.graph(root)
        self.assertEqual(graph["root"], root)
        self.assertEqual([n["digest"] for n in graph["nodes"]],
                         sorted([root, left, right, leaf]))
        by_digest = {n["digest"]: n for n in graph["nodes"]}
        self.assertEqual(by_digest[leaf], {"digest": leaf, "name": "leaf", "version": "1.0.0"})
        expected_edges = sorted([
            (root, left, "left", "^1"),
            (root, right, "right", "~1"),
            (left, leaf, "leaf", ">=1.0"),
            (right, leaf, "leaf", ">=1.0"),
        ])
        self.assertEqual([[e["from"], e["to"], e["name"], e["constraint"]]
                          for e in graph["edges"]],
                         [[f, t, n, c] for f, t, n, c in expected_edges])

    def test_deep_chain_does_not_hit_recursion_limits(self) -> None:
        previous = self.put_manifest("pkg-0", "1.0.0", {})
        for i in range(1, 1200):
            previous = self.put_manifest(f"pkg-{i}", "1.0.0", {"next": dep(previous)})
        graph = self.store.graph(previous)
        self.assertEqual(len(graph["nodes"]), 1200)
        self.assertEqual(len(graph["edges"]), 1199)

    def test_graph_does_not_touch_refs(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf)})
        before = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.store.graph(root)
        after = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.assertEqual(before, after)
        self.assertEqual(self.store.stats()["puts"], 2)

    def test_graph_reads_zero_ref_blobs_still_retained(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf)})
        self.store.release(leaf)
        self.store.release(root)
        graph = self.store.graph(root)
        self.assertEqual(len(graph["nodes"]), 2)

    def test_invalid_root_digest_is_invalid_request(self) -> None:
        for bad in ("zz", "A" * 64, "0" * 63, 42):
            with self.assertRaises(InvalidRequest):
                self.store.graph(bad)

    def test_unknown_root_digest_is_not_found(self) -> None:
        with self.assertRaises(BlobNotFound):
            self.store.graph("0" * 64)

    def test_non_manifest_root_is_a_conflict(self) -> None:
        for raw in (b"not json", b"[1, 2]", b"\"text\"", b"\xff\xfe{}",
                    json.dumps({"name": "a", "version": "1", "extra": 1}).encode(),
                    json.dumps({"name": "", "version": "1"}).encode(),
                    json.dumps({"name": "a" * 101, "version": "1"}).encode(),
                    json.dumps({"version": "1"}).encode(),
                    json.dumps({"name": "a", "version": "1", "dependencies": []}).encode()):
            digest = self.store.put(raw).digest
            with self.assertRaises(DigestConflict, msg=raw):
                self.store.graph(digest)

    def test_bad_dependency_shapes_are_conflicts(self) -> None:
        target = self.put_manifest("leaf", "1.0.0", {})
        cases = [
            {"x": "not-an-object"},
            {"x": {"digest": target}},
            {"x": {"constraint": "1"}},
            {"x": {"digest": "zz", "constraint": "1"}},
            {"x": {"digest": target, "constraint": ""}},
            {"x": {"digest": target, "constraint": "c" * 201}},
            {"x": {"digest": target, "constraint": "1", "extra": 1}},
        ]
        for deps in cases:
            digest = self.put_manifest("root", "1.0.0", deps)
            with self.assertRaises(DigestConflict, msg=deps):
                self.store.graph(digest)

    def test_missing_dependency_blob_is_a_conflict(self) -> None:
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("f" * 64)})
        with self.assertRaises(DigestConflict):
            self.store.graph(root)

    def test_dependency_cycle_is_a_conflict(self) -> None:
        # A true cycle cannot be PUT (digests commit to content), so inject directly.
        a, b = "a" * 64, "b" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"b": dep(b)})
        self.store._blobs[b] = manifest("b", "1.0.0", {"a": dep(a)})
        with self.assertRaises(DigestConflict):
            self.store.graph(a)
        with self.assertRaises(DigestConflict):
            self.store.graph(b)

    def test_self_dependency_is_a_conflict(self) -> None:
        a = "a" * 64
        self.store._blobs[a] = manifest("a", "1.0.0", {"me": dep(a)})
        with self.assertRaises(DigestConflict):
            self.store.graph(a)

    def test_unreachable_invalid_blobs_do_not_affect_the_graph(self) -> None:
        self.store.put(b"garbage, not a manifest")
        root = self.put_manifest("root", "1.0.0", {})
        self.assertEqual(len(self.store.graph(root)["nodes"]), 1)


class GraphHttpTests(unittest.TestCase):
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

    def graph(self, digest: str):
        return self.call("GET", f"/v1/blobs/{digest}/graph")

    def test_graph_happy_path_shape(self) -> None:
        leaf = self.put_manifest("leaf", "2.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf, "^2")})
        status, body, _ = self.graph(root)
        self.assertEqual(status, 200)
        graph = json.loads(body)
        self.assertEqual(set(graph), {"root", "nodes", "edges"})
        self.assertEqual(graph["root"], root)
        self.assertEqual(graph["nodes"], [
            {"digest": leaf, "name": "leaf", "version": "2.0.0"},
            {"digest": root, "name": "root", "version": "1.0.0"},
        ] if leaf < root else [
            {"digest": root, "name": "root", "version": "1.0.0"},
            {"digest": leaf, "name": "leaf", "version": "2.0.0"},
        ])
        self.assertEqual(graph["edges"],
                         [{"from": root, "to": leaf, "name": "leaf", "constraint": "^2"}])

    def test_graph_error_mapping(self) -> None:
        status, body, _ = self.graph("not-a-digest")
        self.assertEqual((status, json.loads(body)["error"]["code"]), (400, "invalid_request"))
        status, body, _ = self.graph("0" * 64)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (404, "not_found"))
        garbage = self.put(b"definitely not a manifest")
        status, body, _ = self.graph(garbage)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
        root = self.put_manifest("root", "1.0.0", {"ghost": dep("e" * 64)})
        status, body, _ = self.graph(root)
        self.assertEqual((status, json.loads(body)["error"]["code"]), (409, "conflict"))
        error = json.loads(body)["error"]
        self.assertEqual(set(error), {"code", "message"})

    def test_graph_never_returns_a_partial_graph(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        broken = self.put(b"not json at all")
        root = self.put_manifest("root", "1.0.0",
                                 {"leaf": dep(leaf), "broken": dep(broken)})
        status, body, _ = self.graph(root)
        self.assertEqual(status, 409)
        self.assertNotIn("nodes", json.loads(body))

    def test_manifest_ingested_via_upload_session_is_graphable(self) -> None:
        raw = manifest("uploaded", "3.1.4", {})
        status, body, _ = self.call(
            "POST", "/v1/uploads",
            json.dumps({"size": len(raw), "media_type": "application/json"}).encode())
        self.assertEqual(status, 201)
        upload_id = json.loads(body)["upload_id"]
        status, _, _ = self.call("PUT", f"/v1/uploads/{upload_id}", raw,
                                 {"X-Upload-Offset": "0"})
        self.assertEqual(status, 200)
        status, body, _ = self.call("POST", f"/v1/uploads/{upload_id}/complete")
        self.assertEqual(status, 201)
        digest = json.loads(body)["digest"]
        status, body, _ = self.graph(digest)
        self.assertEqual(status, 200)
        graph = json.loads(body)
        self.assertEqual(graph["nodes"],
                         [{"digest": digest, "name": "uploaded", "version": "3.1.4"}])
        self.assertEqual(graph["edges"], [])

    def test_graph_does_not_change_refs_or_gc_semantics(self) -> None:
        leaf = self.put_manifest("leaf", "1.0.0", {})
        root = self.put_manifest("root", "1.0.0", {"leaf": dep(leaf)})
        self.graph(root)
        _, body, _ = self.call("GET", "/v1/blobs")
        listing = {item["digest"]: item["refs"] for item in json.loads(body)["blobs"]}
        self.assertEqual(listing, {leaf: 1, root: 1})
        # releasing the root to zero and collecting leaves the graph unreadable at the root
        self.call("DELETE", f"/v1/blobs/{root}/refs")
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertEqual(json.loads(body)["deleted"], [root])
        self.assertEqual(self.graph(root)[0], 404)
        self.assertEqual(self.graph(leaf)[0], 200)


if __name__ == "__main__":
    unittest.main()
