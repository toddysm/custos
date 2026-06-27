"""Classify ``skopeo`` failures into the manifest's declared errors
(COPY-IMPL-005).

``skopeo`` has no machine-readable error taxonomy, so this maps its stderr
text onto the activity manifest's declared error codes:

* ``source.unauthorized`` / ``dest.unauthorized`` (permanent)
* ``source.not_found`` (permanent)
* ``copy.manifest_mismatch`` (permanent)
* ``dest.push_failed`` (retryable) — also the catch-all for otherwise
  unclassified copy failures (network, 5xx, …), since it is the manifest's
  only retryable error.

The classification reads the raw stderr (keywords are not secret); the
``detail`` written to the failure envelope is always redacted.
"""

from __future__ import annotations

from collections.abc import Iterable

from copy_image.contract import ActivityError
from copy_image.credentials import redact

_MISMATCH = (
    "digest did not match",
    "digest mismatch",
    "manifest mismatch",
    "error verifying",
)
_NOT_FOUND = (
    "manifest unknown",
    "name unknown",
    "not known to registry",
    "repository name not known",
    "manifest not found",
)
_UNAUTHORIZED = (
    "unauthorized",
    "authentication required",
    "access to the resource is denied",
    "requested access to the resource is denied",
    "denied",
    "forbidden",
)
#: stderr fragments that indicate the failure happened on the push (dest) side.
_DEST_SIDE = (
    "writing",
    "uploading",
    "pushing",
    "to the destination",
    "trying to reuse blob",
    "error writing",
    "storing",
)


def classify_skopeo_error(stderr: str, *, redactions: Iterable[str] = ()) -> ActivityError:
    """Map a ``skopeo copy`` stderr blob onto a declared :class:`ActivityError`."""
    detail = redact(stderr.strip(), redactions) or "registry copy failed"
    low = stderr.lower()

    if any(token in low for token in _MISMATCH):
        return ActivityError("copy.manifest_mismatch", "permanent", detail)
    if any(token in low for token in _NOT_FOUND):
        return ActivityError("source.not_found", "permanent", detail)
    if any(token in low for token in _UNAUTHORIZED):
        side_is_dest = any(token in low for token in _DEST_SIDE)
        code = "dest.unauthorized" if side_is_dest else "source.unauthorized"
        return ActivityError(code, "permanent", detail)
    # Unclassified copy failure: retryable, mapped to the manifest's only
    # retryable error.
    return ActivityError("dest.push_failed", "retryable", detail)
