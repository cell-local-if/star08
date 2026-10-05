"""Content-addressed artifact store: the baseline service.

Public contract is README.md. Blobs are addressed by the SHA-256 of their bytes; identical bytes are stored once.
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

MAX_BLOB = 1_048_576
DIGEST_LENGTH = 64
MAX_LIMIT = 100
_DECIMAL = re.compile(r"[0-9]+")
_HEX = frozenset("0123456789abcdef")


class StoreError(Exception):
    code = "internal_error"
    status = 500


class InvalidRequest(StoreError):
    code, status = "invalid_request", 400


class BlobNotFound(StoreError):
    code, status = "not_found", 404


class DigestConflict(StoreError):
    code, status = "conflict", 409


def is_digest(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != DIGEST_LENGTH:
        return False
    return all(c in _HEX for c in value)


def is_hex_prefix(value: Any) -> bool:
    if not isinstance(value, str) or not 1 <= len(value) <= DIGEST_LENGTH:
        return False
    return all(c in _HEX for c in value)


def digest_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _decimal_param(name: str, value: str) -> int:
    if not _DECIMAL.fullmatch(value):
        raise InvalidRequest(f"{name} must be a non-negative decimal integer")
    return int(value)


def parse_blob_query(query_string: str) -> dict[str, Any]:
    """Validate the GET /v1/blobs query string; any violation is an InvalidRequest."""
    pairs = urllib.parse.parse_qsl(query_string, keep_blank_values=True)
    names = [name for name, _ in pairs]
    known = {"digest_prefix", "min_size", "max_size", "min_refs", "limit", "after"}
    for name in names:
        if name not in known:
            raise InvalidRequest(f"unknown query parameter {name!r}")
    if len(names) != len(set(names)):
        raise InvalidRequest("query parameters must not repeat")
    raw = dict(pairs)
    filters: dict[str, Any] = {}
    if "digest_prefix" in raw:
        if not is_hex_prefix(raw["digest_prefix"]):
            raise InvalidRequest("digest_prefix must be 1 to 64 lowercase hex characters")
        filters["digest_prefix"] = raw["digest_prefix"]
    for name in ("min_size", "max_size", "min_refs"):
        if name in raw:
            filters[name] = _decimal_param(name, raw[name])
    for name in ("min_size", "max_size"):
        if filters.get(name, 0) > MAX_BLOB:
            raise InvalidRequest(f"{name} must be at most {MAX_BLOB}")
    if "min_size" in filters and "max_size" in filters and filters["min_size"] > filters["max_size"]:
        raise InvalidRequest("min_size must not exceed max_size")
    if "limit" in raw:
        limit = _decimal_param("limit", raw["limit"])
        if not 1 <= limit <= MAX_LIMIT:
            raise InvalidRequest(f"limit must be between 1 and {MAX_LIMIT}")
        filters["limit"] = limit
    if "after" in raw:
        if not is_digest(raw["after"]):
            raise InvalidRequest("after must be 64 lowercase hex characters")
        if "limit" not in filters:
            raise InvalidRequest("after requires limit")
        filters["after"] = raw["after"]
    return filters


@dataclass(frozen=True)
class Blob:
    digest: str
    size: int
    media_type: str


class Store:
    """In-memory, deduplicated by digest. `put` returns the blob metadata; repeats are no-ops."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._blobs: dict[str, bytes] = {}
        self._meta: dict[str, Blob] = {}
        self._refs: dict[str, int] = {}

    def put(self, data: Any, declared_digest: Any = None, media_type: Any = None) -> Blob:
        if not isinstance(data, (bytes, bytearray)):
            raise InvalidRequest("body must be raw bytes")
        if len(data) == 0:
            raise InvalidRequest("blob must not be empty")
        if len(data) > MAX_BLOB:
            raise InvalidRequest(f"blob must be at most {MAX_BLOB} bytes")
        if declared_digest is not None and not is_digest(declared_digest):
            raise InvalidRequest("X-Blob-Digest must be 64 lowercase hex characters")
        if media_type is not None and (not isinstance(media_type, str) or len(media_type) > 200):
            raise InvalidRequest("Content-Type must be a string of at most 200 characters")
        raw = bytes(data)
        digest = digest_of(raw)
        if declared_digest is not None and declared_digest != digest:
            raise DigestConflict(f"declared {declared_digest} does not match computed {digest}")
        with self._lock:
            if digest in self._blobs:
                self._refs[digest] += 1
            else:
                self._blobs[digest] = raw
                self._meta[digest] = Blob(digest, len(raw), media_type or "application/octet-stream")
                self._refs[digest] = 1
            return self._meta[digest]

    def get(self, digest: Any) -> bytes:
        if not is_digest(digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if digest not in self._blobs:
                raise BlobNotFound(f"no blob {digest}")
            return self._blobs[digest]

    def head(self, digest: Any) -> Blob:
        if not is_digest(digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if digest not in self._meta:
                raise BlobNotFound(f"no blob {digest}")
            return self._meta[digest]

    def listing(self) -> list[dict[str, Any]]:
        with self._lock:
            return [{"digest": b.digest, "size": b.size, "media_type": b.media_type, "refs": self._refs[b.digest]}
                    for b in sorted(self._meta.values(), key=lambda item: item.digest)]

    def query(self, filters: dict[str, Any]) -> dict[str, Any]:
        """Filtered, paginated listing from one consistent snapshot, ordered by digest."""
        items, stats = self._snapshot()
        prefix = filters.get("digest_prefix")
        if prefix is not None:
            items = [pair for pair in items if pair[0].digest.startswith(prefix)]
        if "min_size" in filters:
            items = [pair for pair in items if pair[0].size >= filters["min_size"]]
        if "max_size" in filters:
            items = [pair for pair in items if pair[0].size <= filters["max_size"]]
        if "min_refs" in filters:
            items = [pair for pair in items if pair[1] >= filters["min_refs"]]
        if "after" in filters:
            items = [pair for pair in items if pair[0].digest > filters["after"]]
        limit = filters.get("limit")
        page = items[:limit] if limit is not None else items
        has_more = limit is not None and len(items) > limit
        return {
            "blobs": [{"digest": b.digest, "size": b.size, "media_type": b.media_type, "refs": refs}
                      for b, refs in page],
            "next_cursor": page[-1][0].digest if has_more else None,
            "stats": stats,
        }

    def _snapshot(self) -> tuple[list[tuple[Blob, int]], dict[str, Any]]:
        """One lock acquisition: sorted (blob, refs) pairs plus global stats from the same instant."""
        with self._lock:
            items = [(b, self._refs[b.digest])
                     for b in sorted(self._meta.values(), key=lambda item: item.digest)]
            stats = {"blobs": len(self._blobs), "bytes": sum(len(v) for v in self._blobs.values()),
                     "puts": sum(self._refs.values())}
        return items, stats

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {"blobs": len(self._blobs), "bytes": sum(len(v) for v in self._blobs.values()),
                    "puts": sum(self._refs.values())}


def make_handler(store: Store) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "artifact-store/0.1"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:
            return

        def _send(self, status: int, body: dict[str, Any] | bytes, content_type: str = "application/json") -> None:
            raw = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_error(self, error: StoreError) -> None:
            self._send(error.status, {"error": {"code": error.code, "message": str(error)}})

        def _parts(self) -> list[str]:
            return [p for p in self.path.split("?")[0].split("/") if p]

        def _read_body(self) -> bytes:
            length = self.headers.get("Content-Length")
            if length is None:
                raise InvalidRequest("Content-Length is required")
            try:
                size = int(length)
            except ValueError as error:
                raise InvalidRequest("Content-Length must be an integer") from error
            if size < 0 or size > MAX_BLOB + 1024:
                raise InvalidRequest(f"Content-Length must be between 0 and {MAX_BLOB}")
            return self.rfile.read(size) if size else b""

        def do_GET(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if parts == ["v1", "blobs"]:
                    query_string = urllib.parse.urlsplit(self.path).query
                    if query_string:
                        return self._send(200, store.query(parse_blob_query(query_string)))
                    items, stats = store._snapshot()
                    blobs = [{"digest": b.digest, "size": b.size, "media_type": b.media_type, "refs": refs}
                             for b, refs in items]
                    return self._send(200, {"blobs": blobs, "stats": stats})
                if len(parts) == 3 and parts[:2] == ["v1", "blobs"]:
                    blob = store.head(parts[2])
                    return self._send(200, store.get(parts[2]), blob.media_type)
                return self._send(404, {"error": {"code": "not_found"}})
            except StoreError as error:
                return self._send_error(error)
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_HEAD(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if len(parts) == 3 and parts[:2] == ["v1", "blobs"]:
                    blob = store.head(parts[2])
                    self.send_response(200)
                    self.send_header("Content-Length", str(blob.size))
                    self.send_header("Content-Type", blob.media_type)
                    self.send_header("X-Blob-Digest", blob.digest)
                    self.end_headers()
                    return
                self.send_response(404)
                self.end_headers()
            except StoreError as error:
                self.send_response(error.status)
                self.end_headers()

        def do_PUT(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if parts != ["v1", "blobs"]:
                    return self._send(404, {"error": {"code": "not_found"}})
                blob = store.put(self._read_body(), self.headers.get("X-Blob-Digest"), self.headers.get("Content-Type"))
                self.send_response(201)
                self.send_header("X-Blob-Digest", blob.digest)
                raw = json.dumps({"digest": blob.digest, "size": blob.size, "media_type": blob.media_type}).encode()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except StoreError as error:
                return self._send_error(error)
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 18895) -> ThreadingHTTPServer:
    store = Store()
    httpd = ThreadingHTTPServer((host, port), make_handler(store))
    httpd.store = store  # type: ignore[attr-defined]
    return httpd


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="content-addressed artifact store")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18895)
    args = parser.parse_args()
    server = serve(args.host, args.port)
    print(f"artifact store listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
