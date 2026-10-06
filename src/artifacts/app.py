"""Content-addressed artifact store: the baseline service.

Public contract is README.md. Blobs are addressed by the SHA-256 of their bytes; identical bytes are stored once.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import threading
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

MAX_BLOB = 1_048_576
CHUNK_MAX = 262_144
UPLOAD_ID_BYTES = 16  # token_hex(16) -> 32 lowercase hex characters
DIGEST_LENGTH = 64
MAX_LIMIT = 100
_DECIMAL = re.compile(r"[0-9]+")
_HEX = frozenset("0123456789abcdef")
_UPLOAD_ID = re.compile(r"[0-9a-f]{32}")


class StoreError(Exception):
    code = "internal_error"
    status = 500


class InvalidRequest(StoreError):
    code, status = "invalid_request", 400


class BlobNotFound(StoreError):
    code, status = "not_found", 404


class DigestConflict(StoreError):
    code, status = "conflict", 409


class RangeNotSatisfiable(StoreError):
    """A syntactically valid Range that cannot intersect the blob."""

    code, status = "conflict", 416

    def __init__(self, message: str, size: int) -> None:
        super().__init__(message)
        self.size = size


class UploadConflict(DigestConflict):
    """A resumable upload session is in the wrong state for the requested action."""


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


MANIFEST_NAME_MAX = 100
MANIFEST_CONSTRAINT_MAX = 200
_MANIFEST_KEYS = {"name", "version", "dependencies"}
_DEPENDENCY_KEYS = {"digest", "constraint"}


def parse_manifest(raw: bytes, digest: str) -> dict[str, Any]:
    """Validate a manifest blob. Any violation is a DigestConflict (409): the blob is
    stored fine, it just cannot serve as a dependency manifest."""
    def bad(reason: str) -> DigestConflict:
        return DigestConflict(f"blob {digest} is not a valid manifest: {reason}")

    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise bad("not a UTF-8 JSON object") from error
    if not isinstance(payload, dict):
        raise bad("not a UTF-8 JSON object")
    extra = set(payload) - _MANIFEST_KEYS
    if extra:
        raise bad(f"unknown field {sorted(extra)[0]!r}")
    for key in ("name", "version"):
        value = payload.get(key)
        if not isinstance(value, str) or not value or len(value) > MANIFEST_NAME_MAX:
            raise bad(f"{key} must be a non-empty string of at most {MANIFEST_NAME_MAX} characters")
    dependencies = payload.get("dependencies", {})
    if not isinstance(dependencies, dict):
        raise bad("dependencies must be an object")
    parsed: dict[str, tuple[str, str]] = {}
    for dep_name, dep in dependencies.items():
        if not isinstance(dep, dict):
            raise bad(f"dependency {dep_name!r} must be an object")
        extra = set(dep) - _DEPENDENCY_KEYS
        if extra:
            raise bad(f"dependency {dep_name!r} has unknown field {sorted(extra)[0]!r}")
        dep_digest = dep.get("digest")
        if not is_digest(dep_digest):
            raise bad(f"dependency {dep_name!r} digest must be 64 lowercase hex characters")
        constraint = dep.get("constraint")
        if not isinstance(constraint, str) or not constraint or len(constraint) > MANIFEST_CONSTRAINT_MAX:
            raise bad(f"dependency {dep_name!r} constraint must be a non-empty string "
                      f"of at most {MANIFEST_CONSTRAINT_MAX} characters")
        parsed[dep_name] = (dep_digest, constraint)
    return {"name": payload["name"], "version": payload["version"], "dependencies": parsed}


_VERSION_SEGMENT = re.compile(r"0|[1-9][0-9]*")
_ASCII_WHITESPACE = re.compile(r"[ \t\n\r\f\v]+")
_CONSTRAINT_OPERATORS = (">=", "<=", "=", ">", "<", "^", "~")

# A version interval: (lower, lower_inclusive, upper, upper_inclusive); upper None means unbounded.
Interval = tuple[tuple[int, int, int], bool, "tuple[int, int, int] | None", bool]


def parse_version(text: Any) -> tuple[tuple[int, int, int], int] | None:
    """Parse a version: 1 to 3 dot-separated decimal segments, no leading zeros except a
    lone 0. Returns the zero-padded (major, minor, patch) tuple plus the segment count
    (the count matters for ~V), or None when the syntax is invalid."""
    if not isinstance(text, str):
        return None
    segments = text.split(".")
    if not 1 <= len(segments) <= 3:
        return None
    values = []
    for segment in segments:
        if not _VERSION_SEGMENT.fullmatch(segment):
            return None
        values.append(int(segment))
    count = len(values)
    values.extend([0] * (3 - count))
    return (values[0], values[1], values[2]), count


def _constraint_interval(op: str, version: tuple[int, int, int], segments: int) -> Interval:
    major, minor, patch = version
    if op == "=":
        return (version, True, version, True)
    if op == ">":
        return (version, False, None, False)
    if op == ">=":
        return (version, True, None, False)
    if op == "<":
        return ((0, 0, 0), True, version, False)
    if op == "<=":
        return ((0, 0, 0), True, version, True)
    if op == "^":
        if major > 0:
            upper = (major + 1, 0, 0)
        elif minor > 0:
            upper = (0, minor + 1, 0)
        else:
            upper = (0, 0, patch + 1)
        return (version, True, upper, False)
    # op == "~": one segment allows the major to move, two or three pin it.
    upper = (major + 1, 0, 0) if segments == 1 else (major, minor + 1, 0)
    return (version, True, upper, False)


def parse_constraint(text: str) -> list[Interval] | None:
    """Parse a constraint string: comparison tokens separated by ASCII whitespace, each
    one of =V, >V, >=V, <V, <=V, ^V, ~V with V a valid version. Returns one interval per
    token, or None when any token (or the whole string) is syntactically invalid."""
    tokens = [token for token in _ASCII_WHITESPACE.split(text) if token]
    if not tokens:
        return None
    intervals = []
    for token in tokens:
        for op in _CONSTRAINT_OPERATORS:
            if token.startswith(op):
                break
        else:
            return None
        parsed = parse_version(token[len(op):])
        if parsed is None:
            return None
        intervals.append(_constraint_interval(op, *parsed))
    return intervals


def _intervals_intersect(intervals: list[Interval]) -> bool:
    """True when the intersection of these convex version intervals is non-empty."""
    lo_v: tuple[int, int, int] | None = None
    lo_inc = True
    hi_v: tuple[int, int, int] | None = None
    hi_inc = True
    for lv, li, hv, hi in intervals:
        if lo_v is None or lv > lo_v:
            lo_v, lo_inc = lv, li
        elif lv == lo_v:
            lo_inc = lo_inc and li
        if hv is not None:
            if hi_v is None or hv < hi_v:
                hi_v, hi_inc = hv, hi
            elif hv == hi_v:
                hi_inc = hi_inc and hi
    if lo_v is not None and hi_v is not None:
        if lo_v > hi_v or (lo_v == hi_v and not (lo_inc and hi_inc)):
            return False
    return True


def _interval_contains(interval: Interval, version: tuple[int, int, int]) -> bool:
    lo_v, lo_inc, hi_v, hi_inc = interval
    if version < lo_v or (version == lo_v and not lo_inc):
        return False
    if hi_v is not None and (version > hi_v or (version == hi_v and not hi_inc)):
        return False
    return True


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


def parse_range(value: str, size: int) -> tuple[int, int]:
    """Parse a single bytes= range against a blob of `size` bytes into a closed [first, last].

    Returns the inclusive byte positions to send. Only bytes=first-last,
    bytes=first- and bytes=-suffix are legal. Malformed syntax (whitespace,
    multiple ranges, a non-bytes unit, non-decimal bounds, both bounds omitted,
    a zero suffix) is an InvalidRequest (400). A well-formed range that cannot
    intersect the blob (start at/beyond the size, or last before first) is a
    RangeNotSatisfiable (416); overlong last clamps to the final byte.
    """
    if not value.startswith("bytes="):
        raise InvalidRequest("Range must use the bytes unit")
    spec = value[len("bytes="):]
    if spec == "" or any(c.isspace() for c in spec):
        raise InvalidRequest("Range must not be empty or contain whitespace")
    if "," in spec:
        raise InvalidRequest("Range must contain exactly one range")
    bounds = spec.split("-")
    if len(bounds) != 2:
        raise InvalidRequest("Range must be bytes=first-last, bytes=first- or bytes=-suffix")
    first_text, last_text = bounds
    if first_text == "" and last_text == "":
        raise InvalidRequest("Range must give a first position or a suffix length")
    if first_text and not _DECIMAL.fullmatch(first_text):
        raise InvalidRequest("Range bounds must be non-negative decimal integers")
    if last_text and not _DECIMAL.fullmatch(last_text):
        raise InvalidRequest("Range bounds must be non-negative decimal integers")
    if first_text == "":
        suffix = int(last_text)
        if suffix == 0:
            raise InvalidRequest("Range suffix length must be at least 1")
        if suffix >= size:
            return 0, size - 1
        return size - suffix, size - 1
    first = int(first_text)
    if first >= size:
        raise RangeNotSatisfiable(
            f"range start {first} is at or beyond the blob size of {size}", size)
    if last_text == "":
        return first, size - 1
    last = int(last_text)
    if last < first:
        raise RangeNotSatisfiable(
            f"range last position {last} is before first position {first}", size)
    return first, min(last, size - 1)


_ETAG = re.compile(r'"[0-9a-f]{64}"')


def parse_if_range(value: str) -> str:
    """Validate an If-Range header: only a quoted ETag wrapping a blob digest.

    Returns the digest inside the quotes; matching it against the current blob
    is the caller's job. Anything that is not exactly "<64 lowercase hex>" is
    an InvalidRequest — dates, weak tags and other validators are unsupported.
    """
    if not _ETAG.fullmatch(value):
        raise InvalidRequest('If-Range must be a quoted ETag of the form "<64 lowercase hex>"')
    return value[1:-1]


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

    def read(self, digest: Any) -> tuple[Blob, bytes]:
        """Return metadata and bytes in one lock acquisition.

        The bytes object is immutable; a release followed by gc() during the
        response removes the dict entry but cannot alter or reclaim the bytes
        this request already holds, so the client receives one consistent copy.
        """
        if not is_digest(digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if digest not in self._blobs:
                raise BlobNotFound(f"no blob {digest}")
            return self._meta[digest], self._blobs[digest]

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

    def release(self, digest: Any) -> int:
        """Drop one reference to a blob. Content is kept even at refs 0; only gc() reclaims it.

        Returns the remaining reference count.
        """
        if not is_digest(digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if digest not in self._refs:
                raise BlobNotFound(f"no blob {digest}")
            refs = self._refs[digest]
            if refs == 0:
                raise DigestConflict(f"blob {digest} has no references left to release")
            refs -= 1
            self._refs[digest] = refs
            return refs

    def gc(self) -> dict[str, Any]:
        """Atomically delete every blob with refs == 0; blobs still referenced are untouched."""
        with self._lock:
            deleted = sorted(d for d, refs in self._refs.items() if refs == 0)
            for digest in deleted:
                del self._blobs[digest]
                del self._meta[digest]
                del self._refs[digest]
            stats = {"blobs": len(self._blobs), "bytes": sum(len(v) for v in self._blobs.values()),
                     "puts": sum(self._refs.values())}
        return {"deleted": deleted, "stats": stats}

    def graph(self, root_digest: Any) -> dict[str, Any]:
        """Resolve the dependency graph reachable from a root manifest.

        One consistent read-only snapshot taken at request start: no refs change, no
        reclaim timing changes, and concurrent gc cannot tear the result. The whole
        graph is validated before anything is returned — errors never yield a partial
        graph.
        """
        if not is_digest(root_digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if root_digest not in self._blobs:
                raise BlobNotFound(f"no blob {root_digest}")
            snapshot = dict(self._blobs)
        nodes: dict[str, dict[str, str]] = {}
        edges: set[tuple[str, str, str, str]] = set()
        state: dict[str, str] = {}  # digest -> "visiting" | "done"
        # Iterative DFS (no recursion-depth limit); each stack entry is (digest, expanded).
        stack: list[tuple[str, bool]] = [(root_digest, False)]
        while stack:
            digest, expanded = stack.pop()
            if expanded:
                state[digest] = "done"
                continue
            current = state.get(digest)
            if current == "done":
                continue
            if current == "visiting":
                raise DigestConflict(f"dependency cycle detected at blob {digest}")
            raw = snapshot.get(digest)
            if raw is None:
                raise DigestConflict(f"dependency blob {digest} does not exist")
            manifest = parse_manifest(raw, digest)
            state[digest] = "visiting"
            stack.append((digest, True))
            nodes[digest] = {"digest": digest, "name": manifest["name"],
                             "version": manifest["version"]}
            for dep_name, (dep_digest, constraint) in manifest["dependencies"].items():
                edges.add((digest, dep_digest, dep_name, constraint))
                if state.get(dep_digest) != "done":
                    stack.append((dep_digest, False))
        return {
            "root": root_digest,
            "nodes": [nodes[d] for d in sorted(nodes)],
            "edges": [{"from": f, "to": t, "name": n, "constraint": c}
                      for f, t, n, c in sorted(edges)],
        }

    def resolve(self, root_digest: Any) -> dict[str, Any]:
        """Resolve dependency constraints reachable from a root manifest.

        Same read-only snapshot discipline as graph(): one consistent snapshot taken at
        request start, no refs change, no reclaim timing change, and errors never yield
        a partial result. Constraints on the same dependency name are merged and
        intersected; the target manifest's version must satisfy the intersection.
        """
        if not is_digest(root_digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if root_digest not in self._blobs:
                raise BlobNotFound(f"no blob {root_digest}")
            snapshot = dict(self._blobs)
        manifests: dict[str, dict[str, Any]] = {}
        state: dict[str, str] = {}  # digest -> "visiting" | "done"
        stack: list[tuple[str, bool]] = [(root_digest, False)]
        while stack:
            digest, expanded = stack.pop()
            if expanded:
                state[digest] = "done"
                continue
            current = state.get(digest)
            if current == "done":
                continue
            if current == "visiting":
                raise DigestConflict(f"cycle detected at blob {digest}")
            raw = snapshot.get(digest)
            if raw is None:
                raise DigestConflict(f"dependency blob {digest} does not exist")
            manifest = parse_manifest(raw, digest)
            state[digest] = "visiting"
            stack.append((digest, True))
            manifests[digest] = manifest
            for dep_digest, _constraint in manifest["dependencies"].values():
                if state.get(dep_digest) != "done":
                    stack.append((dep_digest, False))
        by_name: dict[str, list[tuple[str, str]]] = {}  # name -> [(digest, constraint)]
        for manifest in manifests.values():
            for dep_name, (dep_digest, constraint) in manifest["dependencies"].items():
                by_name.setdefault(dep_name, []).append((dep_digest, constraint))
        resolved = []
        for name in sorted(by_name):
            entries = by_name[name]
            digests = sorted({dep_digest for dep_digest, _ in entries})
            if len(digests) > 1:
                raise DigestConflict(
                    f"ambiguous_name: dependency {name!r} maps to {len(digests)} different digests")
            dep_digest = digests[0]
            target = manifests[dep_digest]
            if target["name"] != name:
                raise DigestConflict(
                    f"name_mismatch: dependency {name!r} resolves to manifest "
                    f"named {target['name']!r}")
            parsed_version = parse_version(target["version"])
            if parsed_version is None:
                raise DigestConflict(
                    f"version_syntax: manifest {dep_digest} version "
                    f"{target['version']!r} is invalid")
            version = parsed_version[0]
            constraints = sorted({constraint for _, constraint in entries})
            intervals: list[Interval] = []
            for constraint in constraints:
                parsed_constraint = parse_constraint(constraint)
                if parsed_constraint is None:
                    raise DigestConflict(
                        f"constraint_syntax: constraint {constraint!r} for "
                        f"dependency {name!r} is invalid")
                intervals.extend(parsed_constraint)
            if not _intervals_intersect(intervals):
                raise DigestConflict(
                    f"empty_intersection: constraints for dependency {name!r} admit no version")
            if not all(_interval_contains(interval, version) for interval in intervals):
                raise DigestConflict(
                    f"version_mismatch: version {target['version']!r} of dependency {name!r} "
                    f"is outside the constraint intersection")
            resolved.append({"name": name, "digest": dep_digest,
                             "version": target["version"], "constraints": constraints})
        return {"root": root_digest, "resolved": resolved}

    def put_completed(self, data: bytes, declared_digest: str | None, media_type: str) -> Blob:
        """Commit a finished upload session: verify digest, then dedupe/ref-count like a plain PUT."""
        digest = digest_of(data)
        if declared_digest is not None and declared_digest != digest:
            raise DigestConflict(f"declared {declared_digest} does not match computed {digest}")
        with self._lock:
            if digest in self._blobs:
                self._refs[digest] += 1
            else:
                self._blobs[digest] = data
                self._meta[digest] = Blob(digest, len(data), media_type)
                self._refs[digest] = 1
            return self._meta[digest]


@dataclass
class UploadSession:
    upload_id: str
    size: int
    media_type: str
    declared_digest: str | None = None
    chunks: list[bytes] = field(default_factory=list)
    received: int = 0
    committed: bool = False
    deleted: bool = False
    final_digest: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> dict[str, Any]:
        """GET shape; caller must hold self.lock. Digest appears only when known."""
        body: dict[str, Any] = {
            "size": self.size,
            "received": self.received,
            "media_type": self.media_type,
            "status": "committed" if self.committed else "uncommitted",
        }
        digest = self.final_digest if self.committed else self.declared_digest
        if digest is not None:
            body["digest"] = digest
        return body


class UploadNotFound(StoreError):
    code, status = "not_found", 404


class UploadManager:
    """Process-memory resumable uploads; each append/delete/complete is atomic per session.

    Deleted sessions stay in the table as tombstones, so an id is never reissued and a
    delete is strictly serialized against concurrent writes on the same session lock.
    """

    def __init__(self, store: Store) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._sessions: dict[str, UploadSession] = {}

    @staticmethod
    def parse_create(raw: bytes) -> tuple[int, str, str | None]:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise InvalidRequest("request body must be a JSON object") from error
        if not isinstance(payload, dict):
            raise InvalidRequest("request body must be a JSON object")
        if "size" not in payload:
            raise InvalidRequest("size is required")
        size = payload["size"]
        # bool is an int subclass; reject it explicitly along with floats and non-ints
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= MAX_BLOB:
            raise InvalidRequest(f"size must be an integer between 1 and {MAX_BLOB}")
        if "media_type" not in payload:
            raise InvalidRequest("media_type is required")
        media_type = payload["media_type"]
        if not isinstance(media_type, str) or not media_type or len(media_type) > 200:
            raise InvalidRequest("media_type must be a non-empty string of at most 200 characters")
        declared: str | None = None
        if "digest" in payload:
            declared = payload["digest"]
            if not is_digest(declared):
                raise InvalidRequest("digest must be 64 lowercase hex characters")
        extra = set(payload) - {"size", "media_type", "digest"}
        if extra:
            raise InvalidRequest(f"unknown field {sorted(extra)[0]!r}")
        return size, media_type, declared

    def create(self, size: int, media_type: str, declared_digest: str | None) -> UploadSession:
        with self._lock:
            while True:
                upload_id = secrets.token_hex(UPLOAD_ID_BYTES)
                if upload_id not in self._sessions:
                    break
            session = UploadSession(upload_id, size, media_type, declared_digest)
            self._sessions[upload_id] = session
            return session

    def _live(self, upload_id: str) -> UploadSession:
        with self._lock:
            session = self._sessions.get(upload_id)
        if session is None or session.deleted:
            raise UploadNotFound(f"no upload session {upload_id}")
        return session

    def get(self, upload_id: str) -> UploadSession:
        return self._live(upload_id)

    def view(self, upload_id: str) -> dict[str, Any]:
        """Consistent GET snapshot; serialized against delete on the same session lock."""
        session = self._live(upload_id)
        with session.lock:
            if session.deleted:
                raise UploadNotFound(f"no upload session {upload_id}")
            return session.snapshot()

    def append(self, upload_id: str, offset: int, chunk: bytes) -> UploadSession:
        # Request-shape validation (400) precedes routing/state errors.
        if not chunk:
            raise InvalidRequest("chunk must not be empty")
        if len(chunk) > CHUNK_MAX:
            raise InvalidRequest(f"chunk must be at most {CHUNK_MAX} bytes")
        session = self._live(upload_id)
        with session.lock:
            # A concurrent delete (serialized on this same lock) may have tombstoned it.
            if session.deleted:
                raise UploadNotFound(f"no upload session {upload_id}")
            if session.committed:
                raise UploadConflict("upload session is already committed")
            if offset != session.received:
                raise UploadConflict(f"offset {offset} does not match received {session.received}")
            if session.received + len(chunk) > session.size:
                raise UploadConflict("chunk would exceed the declared size")
            session.chunks.append(chunk)
            session.received += len(chunk)
            return session

    def delete(self, upload_id: str) -> None:
        session = self._live(upload_id)
        with session.lock:
            if session.committed:
                raise UploadConflict("upload session is already committed")
            # 204 wins exactly once: the id stays in the table as an invisible tombstone.
            session.deleted = True
            session.chunks.clear()

    def complete(self, upload_id: str) -> tuple[UploadSession, Blob]:
        session = self._live(upload_id)
        with session.lock:
            if session.deleted:
                raise UploadNotFound(f"no upload session {upload_id}")
            if session.committed:
                raise UploadConflict("upload session is already committed")
            if session.received != session.size:
                raise UploadConflict(
                    f"received {session.received} of {session.size} bytes; cannot complete")
            data = b"".join(session.chunks)
            # DigestConflict propagates before any state change: no blob, session stays resumable.
            blob = self._store.put_completed(data, session.declared_digest, session.media_type)
            session.committed = True
            session.final_digest = blob.digest
            session.chunks.clear()  # bytes now live in the store
        return session, blob


def make_handler(store: Store, uploads: UploadManager | None = None) -> type[BaseHTTPRequestHandler]:
    if uploads is None:
        uploads = UploadManager(store)

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
            if isinstance(error, RangeNotSatisfiable):
                # 416 states the current length so the client can rebuild its range.
                body = json.dumps({"error": {"code": error.code, "message": str(error)}}).encode("utf-8")
                self.send_response(error.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Range", f"bytes */{error.size}")
                self.end_headers()
                self.wfile.write(body)
                return
            self._send(error.status, {"error": {"code": error.code, "message": str(error)}})

        def _single_header(self, name: str) -> str | None:
            values = self.headers.get_all(name)
            if not values:
                return None
            if len(values) > 1:
                raise InvalidRequest(f"{name} must appear at most once")
            return values[0]

        def _send_blob(self, status: int, data: bytes, blob: Blob,
                       extra_headers: dict[str, str] | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", blob.media_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("X-Blob-Digest", blob.digest)
            self.send_header("ETag", f'"{blob.digest}"')
            self.send_header("Accept-Ranges", "bytes")
            for name, value in (extra_headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(data)

        def _get_blob(self, digest: str) -> None:
            # If-Range shape (400) is checked first, then the digest lookup
            # (400/404); one snapshot of (metadata, bytes) then serves the whole
            # response, and the Range itself is resolved against it. A release/gc
            # landing afterwards cannot change the bytes captured at request start.
            range_value = self._single_header("Range")
            if_range_value = self._single_header("If-Range")
            if_range = parse_if_range(if_range_value) if if_range_value is not None else None
            blob, data = store.read(digest)
            if range_value is not None and (if_range is None or if_range == blob.digest):
                first, last = parse_range(range_value, blob.size)  # 400 or 416 on failure
                return self._send_blob(206, data[first:last + 1], blob,
                                       {"Content-Range": f"bytes {first}-{last}/{blob.size}"})
            # No Range, or an If-Range that does not match the current digest: full body.
            return self._send_blob(200, data, blob)

        def _parts(self) -> list[str]:
            return [p for p in self.path.split("?")[0].split("/") if p]

        def _read_body(self, max_size: int = MAX_BLOB + 1024) -> bytes:
            length = self.headers.get("Content-Length")
            if length is None:
                raise InvalidRequest("Content-Length is required")
            try:
                size = int(length)
            except ValueError as error:
                raise InvalidRequest("Content-Length must be an integer") from error
            if size < 0 or size > max_size:
                raise InvalidRequest(f"Content-Length must be between 0 and {max_size}")
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
                    return self._get_blob(parts[2])
                if len(parts) == 4 and parts[:2] == ["v1", "blobs"] and parts[3] == "graph":
                    return self._send(200, store.graph(parts[2]))
                if len(parts) == 4 and parts[:2] == ["v1", "blobs"] and parts[3] == "resolve":
                    return self._send(200, store.resolve(parts[2]))
                if len(parts) == 3 and parts[:2] == ["v1", "uploads"]:
                    if not _UPLOAD_ID.fullmatch(parts[2]):
                        raise InvalidRequest("upload_id must be 32 lowercase hex characters")
                    return self._send(200, uploads.view(parts[2]))
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
                    self.send_header("ETag", f'"{blob.digest}"')
                    self.send_header("Accept-Ranges", "bytes")
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
                if parts == ["v1", "blobs"]:
                    blob = store.put(self._read_body(), self.headers.get("X-Blob-Digest"),
                                     self.headers.get("Content-Type"))
                    self.send_response(201)
                    self.send_header("X-Blob-Digest", blob.digest)
                    raw = json.dumps({"digest": blob.digest, "size": blob.size,
                                      "media_type": blob.media_type}).encode()
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                if len(parts) == 3 and parts[:2] == ["v1", "uploads"]:
                    if not _UPLOAD_ID.fullmatch(parts[2]):
                        raise InvalidRequest("upload_id must be 32 lowercase hex characters")
                    body = self._read_body(CHUNK_MAX)
                    offset_header = self.headers.get("X-Upload-Offset")
                    if offset_header is None:
                        raise InvalidRequest("X-Upload-Offset is required")
                    if not _DECIMAL.fullmatch(offset_header):
                        raise InvalidRequest("X-Upload-Offset must be a non-negative decimal integer")
                    offset = int(offset_header)
                    session = uploads.append(parts[2], offset, body)
                    return self._send(200, {"received": session.received,
                                            "status": "committed" if session.committed else "uncommitted"})
                return self._send(404, {"error": {"code": "not_found"}})
            except StoreError as error:
                return self._send_error(error)
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if parts == ["v1", "gc"]:
                    # The body carries no semantics for gc; drain any bytes to keep
                    # the keep-alive connection usable rather than treating them as an error.
                    if self.headers.get("Content-Length") is not None:
                        self._read_body(MAX_BLOB + 1024)
                    return self._send(200, store.gc())
                if parts == ["v1", "uploads"]:
                    raw = self._read_body(MAX_BLOB + 1024)
                    size, media_type, declared = UploadManager.parse_create(raw)
                    session = uploads.create(size, media_type, declared)
                    return self._send(201, {"upload_id": session.upload_id, "size": session.size,
                                            "received": 0, "status": "uncommitted"})
                if len(parts) == 4 and parts[:2] == ["v1", "uploads"] and parts[3] == "complete":
                    if not _UPLOAD_ID.fullmatch(parts[2]):
                        raise InvalidRequest("upload_id must be 32 lowercase hex characters")
                    # complete carries no semantics in its body; drain any bytes to keep
                    # the keep-alive connection usable rather than treating them as an error.
                    if self.headers.get("Content-Length") is not None:
                        self._read_body(MAX_BLOB + 1024)
                    session, blob = uploads.complete(parts[2])
                    return self._send(201, {"digest": blob.digest, "size": blob.size,
                                            "media_type": blob.media_type})
                return self._send(404, {"error": {"code": "not_found"}})
            except StoreError as error:
                return self._send_error(error)
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_DELETE(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if len(parts) == 4 and parts[:2] == ["v1", "blobs"] and parts[3] == "refs":
                    refs = store.release(parts[2])
                    return self._send(200, {"digest": parts[2], "refs": refs})
                if len(parts) == 3 and parts[:2] == ["v1", "uploads"]:
                    if not _UPLOAD_ID.fullmatch(parts[2]):
                        raise InvalidRequest("upload_id must be 32 lowercase hex characters")
                    uploads.delete(parts[2])
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                return self._send(404, {"error": {"code": "not_found"}})
            except StoreError as error:
                return self._send_error(error)
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 18895) -> ThreadingHTTPServer:
    store = Store()
    uploads = UploadManager(store)
    httpd = ThreadingHTTPServer((host, port), make_handler(store, uploads))
    httpd.store = store  # type: ignore[attr-defined]
    httpd.uploads = uploads  # type: ignore[attr-defined]
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
