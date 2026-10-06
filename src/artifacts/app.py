"""Content-addressed artifact store: the baseline service.

Public contract is README.md. Blobs are addressed by the SHA-256 of their bytes; identical bytes are stored once.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import sys
import tempfile
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
PRESENCE_MAX_DIGESTS = 100
UPLOAD_STATE_VERSION = 1
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


class UploadStateError(StoreError):
    """The upload state file cannot be written; the request must not be confirmed."""

    code, status = "internal_error", 500

    def __init__(self) -> None:
        super().__init__("upload state write failed")


class UploadStateInvalid(Exception):
    """The upload state file is empty, corrupt, structurally wrong or version-incompatible."""


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

# Version/constraint syntax for resolution (GET .../resolve). A version is one to
# three dot-separated non-negative decimal segments; missing segments count as 0,
# and leading zeros are banned except for the single value 0.
_VERSION_SEGMENT = re.compile(r"(0|[1-9][0-9]*)")
_CONSTRAINT_TOKEN = re.compile(
    r"(=|>=|<=|>|<|\^|~)(0|[1-9][0-9]*)"
    r"(?:\.(0|[1-9][0-9]*))?(?:\.(0|[1-9][0-9]*))?")
_CONSTRAINT_SEPARATOR = re.compile(r"[\t\n\x0b\x0c\r ]+")
_ASCII_WHITESPACE = " \t\n\r\x0b\x0c"
_VERSION_FLOOR: tuple[int, int, int] = (0, 0, 0)

Triple = tuple[int, int, int]
# An interval over integer triples: every v with lower <= v < upper; upper None is +inf.
Interval = tuple[Triple, Triple | None]


class ResolveConflict(DigestConflict):
    """A 409 surfaced only by resolve: cycle, syntax or version adjudication failure."""


def _conflict(prefix: str, detail: str = "") -> ResolveConflict:
    return ResolveConflict(prefix if not detail else f"{prefix} {detail}")


def parse_version(value: str) -> Triple:
    """Parse a one-to-three segment dotted version into a zero-padded triple.

    Raises ResolveConflict('version_syntax ...') on any syntax violation.
    """
    segments = value.split(".")
    if not 1 <= len(segments) <= 3:
        raise _conflict("version_syntax", repr(value))
    numbers: list[int] = []
    for text in segments:
        if not _VERSION_SEGMENT.fullmatch(text):
            raise _conflict("version_syntax", repr(value))
        numbers.append(int(text))
    numbers += [0] * (3 - len(numbers))
    return numbers[0], numbers[1], numbers[2]


def parse_constraint_token(token: str) -> tuple[str, Triple, int]:
    """Parse one operator+version token into (operator, version triple, segment count).

    Raises ResolveConflict('constraint_syntax ...') on any syntax violation.
    """
    match = _CONSTRAINT_TOKEN.fullmatch(token)
    if match is None:
        raise _conflict("constraint_syntax", repr(token))
    operator = match.group(1)
    version = (int(match.group(2)), int(match.group(3) or 0), int(match.group(4) or 0))
    segment_count = 3 if match.group(4) is not None else (2 if match.group(3) is not None else 1)
    return operator, version, segment_count


def _bump_patch(version: Triple) -> Triple:
    return version[0], version[1], version[2] + 1


def constraint_interval(token: str) -> Interval:
    """Map one constraint token to an integer-triple interval [lower, upper).

    All comparisons use three-segment numeric triples after zero-padding.
    ^V allows changes that do not modify the left-most non-zero element of V;
    ~V pins major (one-segment V) or major.minor (two/three-segment V).
    """
    operator, version, segment_count = parse_constraint_token(token)
    major, minor, patch = version
    if operator == "=":
        return version, _bump_patch(version)
    if operator == ">":
        return _bump_patch(version), None
    if operator == ">=":
        return version, None
    if operator == "<":
        return _VERSION_FLOOR, version
    if operator == "<=":
        return _VERSION_FLOOR, _bump_patch(version)
    if operator == "^":
        if major > 0:
            return version, (major + 1, 0, 0)
        if minor > 0:
            return version, (0, minor + 1, 0)
        return version, (0, 0, patch + 1)
    # ~
    if segment_count == 1:
        return (major, 0, 0), (major + 1, 0, 0)
    return version, (major, minor + 1, 0)


def intersect_intervals(intervals: list[Interval]) -> Interval:
    lower = _VERSION_FLOOR
    upper: Triple | None = None
    for candidate_lower, candidate_upper in intervals:
        if candidate_lower > lower:
            lower = candidate_lower
        if candidate_upper is not None and (upper is None or candidate_upper < upper):
            upper = candidate_upper
    return lower, upper


def interval_contains(interval: Interval, version: Triple) -> bool:
    lower, upper = interval
    if version < lower:
        return False
    return upper is None or version < upper


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


def parse_presence(raw: bytes) -> list[str]:
    """Validate a POST /v1/blobs/presence body into its list of unique digests.

    Must be a UTF-8 JSON object with exactly one field, ``digests``: an array of
    1..PRESENCE_MAX_DIGESTS distinct 64-character lowercase hex SHA-256 strings.
    Every shape violation is an InvalidRequest — nothing is filtered or skipped.
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidRequest("request body must be a UTF-8 JSON object") from error
    if not isinstance(payload, dict):
        raise InvalidRequest("request body must be a JSON object")
    if "digests" not in payload:
        raise InvalidRequest("digests is required")
    extra = set(payload) - {"digests"}
    if extra:
        raise InvalidRequest(f"unknown field {sorted(extra)[0]!r}")
    digests = payload["digests"]
    if not isinstance(digests, list):
        raise InvalidRequest("digests must be an array")
    if not 1 <= len(digests) <= PRESENCE_MAX_DIGESTS:
        raise InvalidRequest(
            f"digests must contain between 1 and {PRESENCE_MAX_DIGESTS} entries")
    for digest in digests:
        if not is_digest(digest):
            raise InvalidRequest("each digest must be 64 lowercase hex characters")
    if len(set(digests)) != len(digests):
        raise InvalidRequest("digests must not repeat")
    return digests


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

    def presence(self, digests: list[str]) -> dict[str, Any]:
        """Read-only batch existence check on one request-start snapshot.

        One lock acquisition yields the full sorted metadata snapshot and the
        global stats, exactly like query(), so presence/missing membership and
        stats always describe the same instant. Nothing is written: no refs are
        added and GC timing is unaffected. `digests` is assumed pre-validated by
        parse_presence(); requested-but-absent digests are `missing`, never 404.
        """
        items, stats = self._snapshot()
        requested = set(digests)
        present = [{"digest": blob.digest, "size": blob.size,
                    "media_type": blob.media_type, "refs": refs}
                   for blob, refs in items if blob.digest in requested]
        present_digests = {entry["digest"] for entry in present}
        missing = sorted(requested - present_digests)
        return {"present": present, "missing": missing, "stats": stats}

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
        """Adjudicate version constraints over the manifests reachable from a root.

        Like graph(), this runs on one read-only snapshot taken at request start
        and writes nothing: no refs, GC or consistency effects. Every reachable
        manifest is first validated exactly as graph() does; the constraints borne
        by all edges sharing a dependency name are then merged into one interval,
        and the version of the single digest carrying that name must lie in it.
        Nothing is returned on failure — errors never yield a partial resolution.
        """
        if not is_digest(root_digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if root_digest not in self._blobs:
                raise BlobNotFound(f"no blob {root_digest}")
            snapshot = dict(self._blobs)
        manifests: dict[str, dict[str, Any]] = {}
        state: dict[str, str] = {}  # digest -> "visiting" | "done"
        # Same iterative DFS as graph(): identical cycle/missing/structure semantics.
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
                raise _conflict("cycle", f"detected at blob {digest}")
            raw = snapshot.get(digest)
            if raw is None:
                raise DigestConflict(f"dependency blob {digest} does not exist")
            manifest = parse_manifest(raw, digest)
            state[digest] = "visiting"
            stack.append((digest, True))
            manifests[digest] = manifest
            for dep_name, (dep_digest, _constraint) in manifest["dependencies"].items():
                if state.get(dep_digest) != "done":
                    stack.append((dep_digest, False))

        # Merge by dependency name: one digest per name, constraints pooled across edges.
        targets: dict[str, str] = {}
        pooled: dict[str, list[str]] = {}
        for manifest in manifests.values():
            for dep_name, (dep_digest, constraint_text) in manifest["dependencies"].items():
                known = targets.get(dep_name)
                if known is not None and known != dep_digest:
                    raise _conflict(
                        "ambiguous_name",
                        f"dependency {dep_name!r} references more than one digest")
                targets[dep_name] = dep_digest
                pooled.setdefault(dep_name, []).append(constraint_text)

        resolved: list[dict[str, Any]] = []
        for name in sorted(targets):
            digest = targets[name]
            target = manifests[digest]
            if target["name"] != name:
                raise _conflict(
                    "name_mismatch",
                    f"dependency key {name!r} does not match manifest name "
                    f"{target['name']!r} at blob {digest}")
            try:
                version_triple = parse_version(target["version"])
            except ResolveConflict as error:
                raise ResolveConflict(
                    f"{error} for dependency {name!r} at blob {digest}") from error
            intervals: list[Interval] = []
            for constraint_text in pooled[name]:
                text = constraint_text.strip(_ASCII_WHITESPACE)
                if not text:
                    raise _conflict(
                        "constraint_syntax",
                        f"in dependency {name!r}: {constraint_text!r}")
                for token in _CONSTRAINT_SEPARATOR.split(text):
                    try:
                        intervals.append(constraint_interval(token))
                    except ResolveConflict as error:
                        raise ResolveConflict(
                            f"{error} in dependency {name!r}") from error
            intersection = intersect_intervals(intervals)
            lower, upper = intersection
            if upper is not None and lower >= upper:
                raise _conflict("empty_intersection", f"for dependency {name!r}")
            if not interval_contains(intersection, version_triple):
                raise _conflict(
                    "version_mismatch",
                    f"dependency {name!r} version {target['version']} does not satisfy "
                    f"the merged constraints {sorted(set(pooled[name]))}")
            resolved.append({
                "name": name,
                "digest": digest,
                "version": target["version"],
                "constraints": sorted(set(pooled[name])),
            })
        return {"root": root_digest, "resolved": resolved}

    def lock(self, root_digest: Any) -> dict[str, Any]:
        """Pin the resolution reachable from a root manifest into a reproducible lock.

        Same request-start read-only snapshot, traversal and adjudication as
        resolve(): identical cycle/missing/structure semantics and identical
        constraint-intersection and target-version checks (the root's own
        version is never adjudicated). Nothing is written — no refs, GC or
        consistency effects — and errors never yield a partial lock.

        The result is fully sorted (packages by name then digest, constraints
        and dependencies lexicographically), so repeated requests over the same
        store state produce byte-identical JSON regardless of dict order.
        """
        if not is_digest(root_digest):
            raise InvalidRequest("digest must be 64 lowercase hex characters")
        with self._lock:
            if root_digest not in self._blobs:
                raise BlobNotFound(f"no blob {root_digest}")
            snapshot = dict(self._blobs)
        manifests: dict[str, dict[str, Any]] = {}
        state: dict[str, str] = {}  # digest -> "visiting" | "done"
        # Same iterative DFS as graph()/resolve(): identical error semantics.
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
                raise _conflict("cycle", f"detected at blob {digest}")
            raw = snapshot.get(digest)
            if raw is None:
                raise DigestConflict(f"dependency blob {digest} does not exist")
            manifest = parse_manifest(raw, digest)
            state[digest] = "visiting"
            stack.append((digest, True))
            manifests[digest] = manifest
            for dep_name, (dep_digest, _constraint) in manifest["dependencies"].items():
                if state.get(dep_digest) != "done":
                    stack.append((dep_digest, False))

        # Merge by dependency name, exactly as resolve(): one digest per name,
        # constraints pooled across edges, same checks in the same order.
        targets: dict[str, str] = {}
        pooled: dict[str, list[str]] = {}
        for manifest in manifests.values():
            for dep_name, (dep_digest, constraint_text) in manifest["dependencies"].items():
                known = targets.get(dep_name)
                if known is not None and known != dep_digest:
                    raise _conflict(
                        "ambiguous_name",
                        f"dependency {dep_name!r} references more than one digest")
                targets[dep_name] = dep_digest
                pooled.setdefault(dep_name, []).append(constraint_text)

        for name in sorted(targets):
            digest = targets[name]
            target = manifests[digest]
            if target["name"] != name:
                raise _conflict(
                    "name_mismatch",
                    f"dependency key {name!r} does not match manifest name "
                    f"{target['name']!r} at blob {digest}")
            try:
                version_triple = parse_version(target["version"])
            except ResolveConflict as error:
                raise ResolveConflict(
                    f"{error} for dependency {name!r} at blob {digest}") from error
            intervals: list[Interval] = []
            for constraint_text in pooled[name]:
                text = constraint_text.strip(_ASCII_WHITESPACE)
                if not text:
                    raise _conflict(
                        "constraint_syntax",
                        f"in dependency {name!r}: {constraint_text!r}")
                for token in _CONSTRAINT_SEPARATOR.split(text):
                    try:
                        intervals.append(constraint_interval(token))
                    except ResolveConflict as error:
                        raise ResolveConflict(
                            f"{error} in dependency {name!r}") from error
            intersection = intersect_intervals(intervals)
            lower, upper = intersection
            if upper is not None and lower >= upper:
                raise _conflict("empty_intersection", f"for dependency {name!r}")
            if not interval_contains(intersection, version_triple):
                raise _conflict(
                    "version_mismatch",
                    f"dependency {name!r} version {target['version']} does not satisfy "
                    f"the merged constraints {sorted(set(pooled[name]))}")

        # Adjudication passed: name <-> digest is 1:1 among reachable non-root
        # manifests, so inbound constraints pool per digest exactly as per name.
        inbound: dict[str, set[str]] = {digest: set() for digest in manifests}
        for manifest in manifests.values():
            for _dep_name, (dep_digest, constraint_text) in manifest["dependencies"].items():
                inbound[dep_digest].add(constraint_text)

        root_manifest = manifests[root_digest]
        packages: list[dict[str, Any]] = []
        for digest, manifest in manifests.items():
            if digest == root_digest:
                continue
            dependencies = [
                {"name": dep_name, "digest": dep_digest, "constraint": constraint_text}
                for dep_name, (dep_digest, constraint_text)
                in sorted(manifest["dependencies"].items())
            ]
            packages.append({
                "name": manifest["name"],
                "version": manifest["version"],
                "digest": digest,
                "constraints": sorted(inbound[digest]),
                "dependencies": dependencies,
            })
        packages.sort(key=lambda entry: (entry["name"], entry["digest"]))
        return {
            "lock_version": 1,
            "root": {"name": root_manifest["name"],
                     "version": root_manifest["version"],
                     "digest": root_digest},
            "packages": packages,
        }

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


class UploadState:
    """JSON file backing upload sessions so they survive a process restart.

    The file is rewritten atomically (temp file + fsync + rename) on every
    confirmed create/append/delete/complete. A failed write never replaces the
    previously persisted file, so a restart restores only the last successful
    state. Loading is strictly validated: anything empty, corrupt, structurally
    wrong or version-incompatible raises UploadStateInvalid so the caller can
    refuse to start.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> dict[str, UploadSession]:
        try:
            with open(self.path, "rb") as handle:
                raw = handle.read()
        except OSError:
            # A missing file means "no sessions yet"; any other access problem
            # surfaces on the first write as a 500.
            if not os.path.exists(self.path):
                return {}
            raise UploadStateInvalid(self.path)
        if not raw:
            raise UploadStateInvalid(self.path)
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise UploadStateInvalid(self.path) from None
        try:
            return self._parse(doc)
        except UploadStateInvalid:
            raise UploadStateInvalid(self.path) from None

    @staticmethod
    def _invalid() -> UploadStateInvalid:
        return UploadStateInvalid("")

    @classmethod
    def _parse(cls, doc: Any) -> dict[str, UploadSession]:
        error = cls._invalid()
        if not isinstance(doc, dict):
            raise error
        if set(doc) != {"version", "sessions"}:
            raise error
        version = doc["version"]
        if not isinstance(version, int) or isinstance(version, bool) \
                or version != UPLOAD_STATE_VERSION:
            raise error
        entries = doc["sessions"]
        if not isinstance(entries, list):
            raise error
        sessions: dict[str, UploadSession] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                raise error
            keys = {"upload_id", "size", "media_type", "declared_digest", "chunks",
                    "received", "committed", "deleted", "final_digest"}
            if set(entry) != keys:
                raise error
            upload_id = entry["upload_id"]
            if not isinstance(upload_id, str) or not _UPLOAD_ID.fullmatch(upload_id) \
                    or upload_id in sessions:
                raise error
            size = entry["size"]
            received = entry["received"]
            if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= MAX_BLOB:
                raise error
            if not isinstance(received, int) or isinstance(received, bool) \
                    or not 0 <= received <= size:
                raise error
            media_type = entry["media_type"]
            if not isinstance(media_type, str) or not media_type or len(media_type) > 200:
                raise error
            declared = entry["declared_digest"]
            if declared is not None and not is_digest(declared):
                raise error
            final = entry["final_digest"]
            if final is not None and not is_digest(final):
                raise error
            committed = entry["committed"]
            deleted = entry["deleted"]
            if not isinstance(committed, bool) or not isinstance(deleted, bool) \
                    or (committed and deleted):
                raise error
            encoded = entry["chunks"]
            if not isinstance(encoded, list):
                raise error
            chunks: list[bytes] = []
            if committed:
                # Bytes now live in the (process-memory) blob store; the state
                # file records only the outcome.
                if encoded or received != size or final is None:
                    raise error
            else:
                total = 0
                for piece in encoded:
                    if not isinstance(piece, str):
                        raise error
                    try:
                        chunk = base64.b64decode(piece, validate=True)
                    except (ValueError, TypeError) as cause:
                        raise error from cause
                    if not chunk or len(chunk) > CHUNK_MAX:
                        raise error
                    total += len(chunk)
                    chunks.append(chunk)
                if final is not None:
                    raise error
                if deleted:
                    # A tombstone keeps received/size metadata but no bytes.
                    if chunks:
                        raise error
                elif total != received:
                    raise error
            session = UploadSession(upload_id, size, media_type, declared)
            session.chunks = chunks
            session.received = received
            session.committed = committed
            session.deleted = deleted
            session.final_digest = final
            sessions[upload_id] = session
        return sessions

    def save(self, sessions: dict[str, UploadSession]) -> None:
        """Atomically persist a snapshot of every live/tombstoned session.

        Write failure (unwritable path, I/O error at any stage) is surfaced as
        UploadStateError; the pre-existing state file is left untouched.
        """
        entries: list[dict[str, Any]] = []
        for upload_id in sorted(sessions):
            session = sessions[upload_id]
            entries.append({
                "upload_id": session.upload_id,
                "size": session.size,
                "media_type": session.media_type,
                "declared_digest": session.declared_digest,
                "chunks": [base64.b64encode(chunk).decode("ascii") for chunk in session.chunks],
                "received": session.received,
                "committed": session.committed,
                "deleted": session.deleted,
                "final_digest": session.final_digest,
            })
        payload = json.dumps({"version": UPLOAD_STATE_VERSION, "sessions": entries},
                             ensure_ascii=False).encode("utf-8")
        directory = os.path.dirname(os.path.abspath(self.path))
        try:
            handle, tmp_name = tempfile.mkstemp(dir=directory, prefix=".upload-state-")
            try:
                with os.fdopen(handle, "wb") as tmp:
                    tmp.write(payload)
                    tmp.flush()
                    os.fsync(tmp.fileno())
                os.replace(tmp_name, self.path)
                # Best-effort directory fsync so the rename itself is durable; on
                # filesystems that reject fsync on a directory the rename above
                # has already landed, so this must not turn into a failed write.
                try:
                    directory_fd = os.open(directory, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                except OSError:
                    pass
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
                raise
        except OSError as error:
            raise UploadStateError() from error


class UploadManager:
    """Resumable uploads; each append/delete/complete is atomic per session.

    Deleted sessions stay in the table as tombstones, so an id is never reissued
    and a delete is strictly serialized against concurrent writes on the same
    session lock. When an UploadState is configured, every confirmed
    create/append/delete/complete is persisted (under a global persist lock so
    full-snapshot writes never interleave) before success is returned; a failed
    write rolls the in-process change back and surfaces as a 500.
    """

    def __init__(self, store: Store, state: UploadState | None = None,
                 sessions: dict[str, UploadSession] | None = None) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._persist_lock = threading.Lock()
        self._state = state
        self._sessions: dict[str, UploadSession] = sessions if sessions is not None else {}

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
        if self._state is not None:
            # persist_lock is taken before the table mutation so the persisted
            # snapshot always includes this session; id generation happens under
            # the table lock as before.
            with self._persist_lock:
                with self._lock:
                    while True:
                        upload_id = secrets.token_hex(UPLOAD_ID_BYTES)
                        if upload_id not in self._sessions:
                            break
                    session = UploadSession(upload_id, size, media_type, declared_digest)
                    self._sessions[upload_id] = session
                try:
                    self._state.save(self._sessions)
                except UploadStateError:
                    with self._lock:
                        del self._sessions[upload_id]
                    raise
                return session
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
        if self._state is not None:
            with self._persist_lock:
                with session.lock:
                    self._guard_live(session, upload_id)
                    self._check_append(session, upload_id, offset, chunk)
                    session.chunks.append(chunk)
                    session.received += len(chunk)
                    try:
                        self._state.save(self._sessions)
                    except UploadStateError:
                        # The change is not confirmed: roll back so the session
                        # and the last successful state file agree.
                        session.chunks.pop()
                        session.received -= len(chunk)
                        raise
                    return session
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

    @staticmethod
    def _guard_live(session: UploadSession, upload_id: str) -> None:
        if session.deleted:
            raise UploadNotFound(f"no upload session {upload_id}")

    @staticmethod
    def _check_append(session: UploadSession, upload_id: str, offset: int, chunk: bytes) -> None:
        if session.committed:
            raise UploadConflict("upload session is already committed")
        if offset != session.received:
            raise UploadConflict(f"offset {offset} does not match received {session.received}")
        if session.received + len(chunk) > session.size:
            raise UploadConflict("chunk would exceed the declared size")

    def delete(self, upload_id: str) -> None:
        session = self._live(upload_id)
        if self._state is not None:
            with self._persist_lock:
                with session.lock:
                    if session.committed:
                        raise UploadConflict("upload session is already committed")
                    previous_chunks = session.chunks
                    # 204 wins exactly once: the id stays in the table as an invisible tombstone.
                    session.deleted = True
                    session.chunks = []
                    try:
                        self._state.save(self._sessions)
                    except UploadStateError:
                        session.deleted = False
                        session.chunks = previous_chunks
                        raise
                return
        with session.lock:
            if session.committed:
                raise UploadConflict("upload session is already committed")
            # 204 wins exactly once: the id stays in the table as an invisible tombstone.
            session.deleted = True
            session.chunks.clear()

    def complete(self, upload_id: str) -> tuple[UploadSession, Blob]:
        session = self._live(upload_id)
        if self._state is not None:
            with self._persist_lock:
                with session.lock:
                    self._guard_live(session, upload_id)
                    if session.committed:
                        raise UploadConflict("upload session is already committed")
                    if session.received != session.size:
                        raise UploadConflict(
                            f"received {session.received} of {session.size} bytes; cannot complete")
                    data = b"".join(session.chunks)
                    digest = digest_of(data)
                    # Verify the declared digest (409) before any state change: no
                    # persisted commit, no blob, the session stays resumable.
                    if session.declared_digest is not None and session.declared_digest != digest:
                        raise DigestConflict(
                            f"declared {session.declared_digest} does not match computed {digest}")
                    previous_chunks = session.chunks
                    session.committed = True
                    session.final_digest = digest
                    session.chunks = []
                    try:
                        self._state.save(self._sessions)
                    except UploadStateError:
                        # Commit not confirmed: roll the session back; nothing was stored.
                        session.committed = False
                        session.final_digest = None
                        session.chunks = previous_chunks
                        raise
                    # Persistence is confirmed; only now does the process-memory
                    # blob store gain the blob/reference.
                    blob = self._store.put_completed(data, session.declared_digest,
                                                     session.media_type)
                return session, blob
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
                if len(parts) == 4 and parts[:2] == ["v1", "blobs"] and parts[3] == "lock":
                    return self._send(200, store.lock(parts[2]))
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
                if parts == ["v1", "blobs", "presence"]:
                    raw = self._read_body(MAX_BLOB + 1024)
                    digests = parse_presence(raw)
                    return self._send(200, store.presence(digests))
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


def serve(host: str = "127.0.0.1", port: int = 18895,
          upload_state: str | None = None) -> ThreadingHTTPServer:
    store = Store()
    state: UploadState | None = None
    sessions: dict[str, UploadSession] | None = None
    if upload_state is not None:
        state = UploadState(upload_state)
        # Refuse to listen on an empty/corrupt/structurally wrong/incompatible file.
        sessions = state.load()
    uploads = UploadManager(store, state, sessions)
    httpd = ThreadingHTTPServer((host, port), make_handler(store, uploads))
    httpd.store = store  # type: ignore[attr-defined]
    httpd.uploads = uploads  # type: ignore[attr-defined]
    return httpd


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="content-addressed artifact store")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18895)
    parser.add_argument("--upload-state", default=None,
                        help="path to the upload session state file (resumable across restarts)")
    args = parser.parse_args()
    try:
        server = serve(args.host, args.port, args.upload_state)
    except UploadStateInvalid as error:
        print(f"upload state invalid: {error}", file=sys.stderr)
        sys.exit(1)
    print(f"artifact store listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
