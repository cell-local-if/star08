"""Content-addressed artifact store (baseline service)."""

import warnings

# ``python -m artifacts.app`` imports this package first, and the package imports
# app, which makes runpy emit a harmless "found in sys.modules ... prior to
# execution" RuntimeWarning. Silence just that one quirk so CLI diagnostics on
# stderr are not mixed with unrelated noise.
warnings.filterwarnings(
    "ignore",
    message=r"'artifacts\.app' found in sys\.modules .*",
    category=RuntimeWarning,
)

from .app import (  # noqa: E402,F401
    CHUNK_MAX,
    MAX_BLOB,
    Blob,
    BlobNotFound,
    DigestConflict,
    InvalidRequest,
    RangeNotSatisfiable,
    ResolveConflict,
    Store,
    StoreError,
    UploadConflict,
    UploadManager,
    UploadNotFound,
    UploadSession,
    UploadState,
    UploadStateError,
    UploadStateInvalid,
    UPLOAD_STATE_VERSION,
    constraint_interval,
    digest_of,
    intersect_intervals,
    interval_contains,
    is_digest,
    make_handler,
    parse_constraint_token,
    parse_if_range,
    parse_manifest,
    parse_presence,
    parse_range,
    parse_version,
    serve,
)

__all__ = [
    "CHUNK_MAX",
    "MAX_BLOB",
    "Blob",
    "BlobNotFound",
    "DigestConflict",
    "InvalidRequest",
    "RangeNotSatisfiable",
    "ResolveConflict",
    "Store",
    "StoreError",
    "UPLOAD_STATE_VERSION",
    "UploadConflict",
    "UploadManager",
    "UploadNotFound",
    "UploadSession",
    "UploadState",
    "UploadStateError",
    "UploadStateInvalid",
    "constraint_interval",
    "digest_of",
    "intersect_intervals",
    "interval_contains",
    "is_digest",
    "make_handler",
    "parse_constraint_token",
    "parse_if_range",
    "parse_manifest",
    "parse_presence",
    "parse_range",
    "parse_version",
    "serve",
]
