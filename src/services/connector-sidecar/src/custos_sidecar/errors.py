"""Sidecar error taxonomy and RFC 7807 helpers (CONN-IMPL-019).

The sidecar surfaces failures to the activity over the UDS as
`application/problem+json` (RFC 7807) responses. The taxonomy is
small and mirrors the design \u00a7 Secret and Token Flow to Activities
failure-mode table:

| Code              | HTTP | Retryable | Trigger                                                  |
|-------------------|------|-----------|----------------------------------------------------------|
| invalid-request   | 400  | (perm)    | Bad query/body shape, unknown purpose verb.              |
| bootstrap-invalid | 401  | (perm)    | Missing or non-verifiable ``Custos-Sidecar-Token``.      |
| capability-forbidden | 403 | (perm)    | ``purpose`` not in the slot's declared capabilities.    |
| slot-not-found    | 404  | (perm)    | The named slot was not bound at sidecar start.           |
| lease-not-found   | 404  | (perm)    | Refresh against an unknown / expired ``leaseId``.        |
| lease-revoked     | 410  | (perm)    | Refresh against a released / revoked lease.              |
| capacity-exceeded | 429  | (transient) | Concurrent-lease cap reached for the triple.        |
| upstream-failed   | 502  | (transient) | Identity / KMS provider returned an error.              |
| connector-unavailable | 503 | (transient) | Connector instance disabled or unhealthy.            |

Each :class:`SidecarError` carries the wire ``code`` plus a free-form
``detail``. :func:`problem_response` renders it as the canonical
``application/problem+json`` body the activity will decode.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Final

from starlette.responses import JSONResponse


class SidecarErrorCode(StrEnum):
    """Stable wire codes the sidecar emits in ``problem+json`` bodies."""

    INVALID_REQUEST = "invalid-request"
    BOOTSTRAP_INVALID = "bootstrap-invalid"
    CAPABILITY_FORBIDDEN = "capability-forbidden"
    SLOT_NOT_FOUND = "slot-not-found"
    LEASE_NOT_FOUND = "lease-not-found"
    LEASE_REVOKED = "lease-revoked"
    CAPACITY_EXCEEDED = "capacity-exceeded"
    UPSTREAM_FAILED = "upstream-failed"
    CONNECTOR_UNAVAILABLE = "connector-unavailable"


_HTTP_STATUS: Final[dict[SidecarErrorCode, int]] = {
    SidecarErrorCode.INVALID_REQUEST: 400,
    SidecarErrorCode.BOOTSTRAP_INVALID: 401,
    SidecarErrorCode.CAPABILITY_FORBIDDEN: 403,
    SidecarErrorCode.SLOT_NOT_FOUND: 404,
    SidecarErrorCode.LEASE_NOT_FOUND: 404,
    SidecarErrorCode.LEASE_REVOKED: 410,
    SidecarErrorCode.CAPACITY_EXCEEDED: 429,
    SidecarErrorCode.UPSTREAM_FAILED: 502,
    SidecarErrorCode.CONNECTOR_UNAVAILABLE: 503,
}


def http_status_for(code: SidecarErrorCode) -> int:
    """Return the HTTP status the sidecar serves for ``code``."""
    return _HTTP_STATUS[code]


class SidecarError(Exception):
    """A typed sidecar failure carrying its wire code and human detail.

    Constructors raise :class:`SidecarError`; the router catches it and
    converts to an RFC 7807 ``application/problem+json`` response.
    """

    def __init__(
        self,
        code: SidecarErrorCode,
        detail: str,
        *,
        retry_after_sec: int | None = None,
    ) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail
        self.retry_after_sec = retry_after_sec


def problem_response(
    exc: SidecarError,
    *,
    instance: str | None = None,
) -> JSONResponse:
    """Render a :class:`SidecarError` as an RFC 7807 problem document.

    ``Content-Type`` is set to ``application/problem+json`` (the
    canonical media type for RFC 7807). When the error carries a
    ``retry_after_sec`` hint (currently only :attr:`CAPACITY_EXCEEDED`),
    a ``Retry-After`` response header is added with the integer seconds
    value.

    The body contains the four standard members the spec recommends:

    * ``type`` \u2014 a stable URN tag scoped to this sidecar so log
      consumers can build a dispatch table without parsing prose.
    * ``title`` \u2014 the same value as ``code`` for caller convenience.
    * ``status`` \u2014 the HTTP status code (mirrors the response line).
    * ``detail`` \u2014 the human-readable explanation.
    """
    status = http_status_for(exc.code)
    body: dict[str, Any] = {
        "type": f"urn:custos:sidecar:error:{exc.code.value}",
        "title": exc.code.value,
        "status": status,
        "detail": exc.detail,
    }
    if instance is not None:
        body["instance"] = instance
    headers: dict[str, str] = {}
    if exc.retry_after_sec is not None:
        headers["Retry-After"] = str(exc.retry_after_sec)
    return JSONResponse(
        status_code=status,
        content=body,
        media_type="application/problem+json",
        headers=headers,
    )


__all__ = [
    "SidecarError",
    "SidecarErrorCode",
    "http_status_for",
    "problem_response",
]
