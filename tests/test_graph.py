"""Tests for dependency manifest parsing and the reachable-graph endpoint."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from artifacts import DigestConflict, InvalidRequest, Store, parse_manifest


def manifest_bytes(name: str, version: str = "1.0.0", dependencies: dict | None = None,
                   **extra: object) -> bytes:
    payload: dict[str, object] = {"name": name, "version": version}
    if dependencies is not None:
        payload["dependencies"] = dependencies
    payload.update(extra)
    return json.dumps(payload).encode("utf-8")


class ParseManifestTests(unittest.TestCase):
    def test_minimal_manifest_without_dependencies(self) -> None:
        parsed = parse_manifest(manifest_bytes("alpha"))
        self.assertEqual(parsed, {"name": "alpha", "version": "1.0.0", "dependencies": {}})

    def test_full_manifest_normalizes_missing_constraint_to_none(self) -> None:
        dep = "a" * 64
        parsed = parse_manifest(manifest_bytes(
            "alpha", "2.0", {"beta": {"digest": dep, "constraint": "^1.0"},
                             "gamma": {"digest": dep}}))
        self.assertEqual(parsed["dependencies"]["beta"], {"digest": dep, "constraint": "^1.0"})
        self.assertEqual(parsed["dependencies"]["gamma"], {"digest": dep, "constraint": None})

    def test_rejects_non_object_and_non_utf8_and_bad_json(self) -> None:
        for raw in [b"[1, 2]", b"\"text\"", b"42", b"\xff\xfe{}", b"{not json"]:
            with self.assertRaises(DigestConflict, msg=raw):
                parse_manifest(raw)

    def test_rejects_unknown_or_missing_or_invalid_fields(self) -> None:
        dep = "a" * 64
        bad = [
            manifest_bytes("a", extra_field=1),
            manifest_bytes("", "1.0"),                       # empty name
            manifest_bytes("x" * 101),                       # name too long
            manifest_bytes("a", ""),                         # empty version
            manifest_bytes("a", "v" * 101),                  # version too long
            manifest_bytes("a", "1.0", []),                  # dependencies not an object
            manifest_bytes("a", "1.0", {"b": "not-object"}),
            manifest_bytes("a", "1.0", {"b": {"digest": "zz"}}),
            manifest_bytes("a", "1.0", {"b": {"digest": "A" * 64}}),
            manifest_bytes("a", "1.0", {"b": {"digest": dep, "constraint": ""}}),
            manifest_bytes("a", "1.0", {"b": {"digest": dep, "constraint": "c" * 201}}),
            manifest_bytes("a", "1.0", {"b": {"digest": dep, "constraint": 5}}),
            manifest_bytes("a", "1.0", {"b": {"digest": dep, "extra": 1}}),
            manifest_bytes("a", "1.0", {"b": {"constraint": "^1"}}),  # digest required
        ]
        for raw in bad:
            with self.assertRaises(DigestConflict, msg=raw):
                parse_manifest(raw)

    def test_boundary_lengths_are_accepted(self) -> None:
        dep = "a" * 64
        parsed = parse_manifest(manifest_bytes(
            "n" * 100, "v" * 100, {"b": {"digest": dep, "constraint": "c" * 200}}))
        self.assertEqual(len(parsed["name"]), 100)
        self.assertEqual(len(parsed["dependencies"]["b"]["constraint"]), 200)


class StoreGraphTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()

    def put_manifest(self, name: str, dependencies: dict | None = None,
                     version: str = "1.0.0") -> str:
        return self.store.put(manifest_bytes(name, version, dependencies),
                              media_type="application/json").digest

    def test_root_without_dependencies_has_single_node_and_no_edges(self) -> None:
        root = self.put_manifest("alpha")
        graph = self.store.graph(root)
        self.assertEqual(graph, {"root": root, "nodes": [
            {"digest": root, "name": "alpha", "version": "1.0.0"}], "edges": []})

    def test_chain_resolves_nodes_sorted_by_digest_and_edges_sorted(self) -> None:
        leaf = self.put_manifest("leaf")
        mid = self.put_manifest("mid", {"leaf": {"digest": leaf, "constraint": "^1"}})
        root = self.put_manifest("root", {"mid": {"digest": mid, "constraint": "~2"}})
        graph = self.store.graph(root)
        self.assertEqual([n["digest"] for n in graph["nodes"]], sorted([root, mid, leaf]))
        edge_keys = [(e["from"], e["to"], e["name"]) for e in graph["edges"]]
        self.assertEqual(edge_keys, sorted(edge_keys))
        self.assertEqual({(e["from"], e["to"]): e["constraint"] for e in graph["edges"]},
                         {(root, mid): "~2", (mid, leaf): "^1"})

    def test_diamond_reaches_shared_node_once_and_dedupes_edges(self) -> None:
        shared = self.put_manifest("shared")
        left = self.put_manifest("left", {"s": {"digest": shared}})
        right = self.put_manifest("right", {"s": {"digest": shared}})
        root = self.put_manifest("root", {"l": {"digest": left}, "r": {"digest": right}})
        graph = self.store.graph(root)
        self.assertEqual(len(graph["nodes"]), 4)
        self.assertEqual([n["digest"] for n in graph["nodes"]],
                         sorted([root, left, right, shared]))
        edge_keys = [(e["from"], e["to"], e["name"]) for e in graph["edges"]]
        self.assertEqual(edge_keys, sorted(edge_keys))
        self.assertEqual(len(edge_keys), len(set(edge_keys)))

    def test_missing_dependency_blob_is_conflict(self) -> None:
        root = self.put_manifest("root", {"ghost": {"digest": "f" * 64}})
        with self.assertRaises(DigestConflict):
            self.store.graph(root)

    def test_invalid_manifest_anywhere_in_graph_is_conflict(self) -> None:
        bad = self.store.put(b"not json at all").digest
        root = self.put_manifest("root", {"bad": {"digest": bad}})
        with self.assertRaises(DigestConflict):
            self.store.graph(root)
        with self.assertRaises(DigestConflict):
            self.store.graph(bad)

    def test_cycle_is_conflict(self) -> None:
        # A true cycle cannot be constructed through content-addressed PUTs (a manifest's
        # digest commits to its bytes), so exercise detection on a synthetic snapshot.
        from artifacts import build_graph

        a, b = "a" * 64, "b" * 64
        blobs = {
            a: json.dumps({"name": "a", "version": "1",
                           "dependencies": {"b": {"digest": b}}}).encode(),
            b: json.dumps({"name": "b", "version": "1",
                           "dependencies": {"a": {"digest": a}}}).encode(),
        }
        with self.assertRaises(DigestConflict):
            build_graph(a, blobs)
        with self.assertRaises(DigestConflict):
            build_graph(b, blobs)

    def test_self_dependency_is_a_cycle(self) -> None:
        from artifacts import build_graph

        digest = "x" * 64
        raw = json.dumps({"name": "s", "version": "1",
                          "dependencies": {"me": {"digest": digest}}}).encode()
        with self.assertRaises(DigestConflict):
            build_graph(digest, {digest: raw})

    def test_graph_adds_no_refs_and_changes_no_stats(self) -> None:
        leaf = self.put_manifest("leaf")
        root = self.put_manifest("root", {"leaf": {"digest": leaf}})
        before = self.store.stats()
        self.store.graph(root)
        self.assertEqual(self.store.stats(), before)
        listing = {item["digest"]: item["refs"] for item in self.store.listing()}
        self.assertEqual(listing[root], 1)
        self.assertEqual(listing[leaf], 1)

    def test_invalid_digest_and_unknown_root(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.store.graph("not-a-digest")
        from artifacts import BlobNotFound
        with self.assertRaises(BlobNotFound):
            self.store.graph("0" * 64)


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

    def call(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, error.read(), dict(error.headers)

    def put(self, raw: bytes) -> str:
        status, body, headers = self.call("PUT", "/v1/blobs", raw,
                                          {"Content-Type": "application/json"})
        self.assertEqual(status, 201)
        return json.loads(body)["digest"]

    def test_graph_endpoint_roundtrip(self) -> None:
        leaf = self.put(manifest_bytes("leaf"))
        root = self.put(manifest_bytes(
            "root", "3.1", {"leaf": {"digest": leaf, "constraint": ">=1"}}))
        status, body, _ = self.call("GET", f"/v1/blobs/{root}/graph")
        self.assertEqual(status, 200)
        graph = json.loads(body)
        self.assertEqual(set(graph), {"root", "nodes", "edges"})
        self.assertEqual(graph["root"], root)
        self.assertEqual([n["digest"] for n in graph["nodes"]], sorted([root, leaf]))
        self.assertEqual(graph["edges"], [
            {"from": root, "to": leaf, "name": "leaf", "constraint": ">=1"}])

    def test_error_mapping(self) -> None:
        status, body, _ = self.call("GET", "/v1/blobs/xyz/graph")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_request")

        status, body, _ = self.call("GET", f"/v1/blobs/{'0' * 64}/graph")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["code"], "not_found")

        bad = self.put(b"\xff\xff")
        status, body, _ = self.call("GET", f"/v1/blobs/{bad}/graph")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "conflict")

        missing_dep = self.put(manifest_bytes("m", "1", {"g": {"digest": "e" * 64}}))
        status, body, _ = self.call("GET", f"/v1/blobs/{missing_dep}/graph")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "conflict")

    def test_graph_does_not_change_refs_or_gc(self) -> None:
        leaf = self.put(manifest_bytes("leaf2"))
        root = self.put(manifest_bytes("root2", "1", {"l": {"digest": leaf}}))
        self.call("GET", f"/v1/blobs/{root}/graph")
        status, body, _ = self.call("GET", "/v1/blobs")
        refs = {item["digest"]: item["refs"] for item in json.loads(body)["blobs"]}
        self.assertEqual(refs[root], 1)
        self.assertEqual(refs[leaf], 1)
        # release both to zero and gc: graph reads must not have pinned anything
        self.call("DELETE", f"/v1/blobs/{root}/refs")
        self.call("DELETE", f"/v1/blobs/{leaf}/refs")
        status, body, _ = self.call("POST", "/v1/gc")
        self.assertIn(root, json.loads(body)["deleted"])
        self.assertIn(leaf, json.loads(body)["deleted"])


if __name__ == "__main__":
    unittest.main()
