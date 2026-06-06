"""RFC 7807 ``application/problem+json`` envelope + the locked gateway error taxonomy.

Every *gateway-originated* failure the API Gateway raises (a
:class:`GatewayError`) flows through a single uniform Problem Details body so SDK
clients never see FastAPI's default error shape. The envelope mirrors RFC 7807
§ 3.1 with three extensions documented in
``design/components/api-gateway/design.md`` § Error Envelope:

* ``code`` — the structured taxonomy string clients branch on (the ``type`` URI
  MAY change without bumping ``code``).
* ``correlationId`` — the request correlation id (also returned in the
  ``x-correlation-id`` response header by the correlation middleware).
* ``auditEventId`` — populated for outcomes the Auth Service audited
  (``permission-denied``), absent otherwise.

The taxonomy is *locked*: a code absent from :data:`LOCKED_CODE_TO_STATUS` can
never reach the wire (:meth:`ProblemDetail.from_code` raises ``KeyError``), so an
undocumented failure is a loud bug rather than a 500 with a leaked stack trace.
Every gateway-originated failure is raised as a :class:`GatewayError`; the
request-shape codes (``body-too-large`` / ``unsupported-media-type``) are raised
by the request validator (AGW-IMPL-011), the device-code surface raises
``device-code-expired`` (AGW-IMPL-014), and so on.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Final

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "CORRELATION_ID_HEADER",
    "LOCKED_CODES",
    "LOCKED_CODE_TO_STATUS",
    "PROBLEM_MEDIA_TYPE",
    "PROBLEM_TYPE_PREFIX",
    "GatewayError",
    "GatewayErrorCode",
    "ProblemDetail",
    "problem_response",
    "register_exception_handlers",
]

#: Wire content type for the envelope (RFC 7807 § 3).
PROBLEM_MEDIA_TYPE: Final[str] = "application/problem+json"

#: Absolute-URI prefix the ``type`` field is derived from — the ``code`` appended
#: verbatim, e.g. ``permission-denied`` becomes
#: ``https://custos.dev/errors/permission-denied`` (design.md § Error Envelope).
#: Clients SHOULD treat the URI as opaque and branch on ``code`` instead.
PROBLEM_TYPE_PREFIX: Final[str] = "https://custos.dev/errors/"

#: Response header carrying the request correlation id on every response.
CORRELATION_ID_HEADER: Final[str] = "x-correlation-id"


class GatewayErrorCode(StrEnum):
    """The locked API Gateway error taxonomy (design.md § Failure Modes)."""

    INVALID_TOKEN = "invalid-token"
    PERMISSION_DENIED = "permission-denied"
    WORKSPACE_MISMATCH = "workspace-mismatch"
    IDEMPOTENCY_IN_FLIGHT = "idempotency-in-flight"
    IDEMPOTENCY_KEY_REUSE = "idempotency-key-reuse"
    RATE_LIMITED = "rate-limited"
    BODY_TOO_LARGE = "body-too-large"
    UNSUPPORTED_MEDIA_TYPE = "unsupported-media-type"
    DOWNSTREAM_UNAVAILABLE = "downstream-unavailable"
    WEBHOOK_ROUTE_NOT_FOUND = "webhook-route-not-found"
    DEVICE_CODE_EXPIRED = "device-code-expired"
    GATEWAY_STARTUP_PERMISSION_MISSING = "gateway-startup-permission-missing"


#: Code → HTTP status. The keys form the closed set the envelope may emit.
#: ``gateway-startup-permission-missing`` is a startup-time panic that refuses to
#: boot rather than a served response; it is mapped to 500 so the grid test can
#: assert a stable ``type`` + status for every taxonomy member, but the gateway
#: never reaches the wire with it.
LOCKED_CODE_TO_STATUS: Final[dict[GatewayErrorCode, int]] = {
    GatewayErrorCode.INVALID_TOKEN: 401,
    GatewayErrorCode.PERMISSION_DENIED: 403,
    GatewayErrorCode.WORKSPACE_MISMATCH: 400,
    GatewayErrorCode.IDEMPOTENCY_IN_FLIGHT: 409,
    GatewayErrorCode.IDEMPOTENCY_KEY_REUSE: 409,
    GatewayErrorCode.RATE_LIMITED: 429,
    GatewayErrorCode.BODY_TOO_LARGE: 413,
    GatewayErrorCode.UNSUPPORTED_MEDIA_TYPE: 415,
    GatewayErrorCode.DOWNSTREAM_UNAVAILABLE: 503,
    GatewayErrorCode.WEBHOOK_ROUTE_NOT_FOUND: 404,
    GatewayErrorCode.DEVICE_CODE_EXPIRED: 400,
    GatewayErrorCode.GATEWAY_STARTUP_PERMISSION_MISSING: 500,
}

#: Frozen view for fast membership tests.
LOCKED_CODES: Final[frozenset[GatewayErrorCode]] = frozenset(LOCKED_CODE_TO_STATUS)

#: Short, English-only human-readable title per code.
_TITLE_FOR_CODE: Final[dict[GatewayErrorCode, str]] = {
    GatewayErrorCode.INVALID_TOKEN: "Invalid token",
    GatewayErrorCode.PERMISSION_DENIED: "Permission denied",
    GatewayErrorCode.WORKSPACE_MISMATCH: "Workspace mismatch",
    GatewayErrorCode.IDEMPOTENCY_IN_FLIGHT: "Idempotent request in flight",
    GatewayErrorCode.IDEMPOTENCY_KEY_REUSE: "Idempotency key reuse",
    GatewayErrorCode.RATE_LIMITED: "Rate limited",
    GatewayErrorCode.BODY_TOO_LARGE: "Request body too large",
    GatewayErrorCode.UNSUPPORTED_MEDIA_TYPE: "Unsupported media type",
    GatewayErrorCode.DOWNSTREAM_UNAVAILABLE: "Downstream service unavailable",
    GatewayErrorCode.WEBHOOK_ROUTE_NOT_FOUND: "Webhook route not found",
    GatewayErrorCode.DEVICE_CODE_EXPIRED: "Device code expired",
    GatewayErrorCode.GATEWAY_STARTUP_PERMISSION_MISSING: "Gateway startup permission missing",
}


def _type_uri_for_code(code: GatewayErrorCode) -> str:
    """Render the RFC 7807 ``type`` URI for ``code``."""
    return f"{PROBLEM_TYPE_PREFIX}{code.value}"


def correlation_id_of(request: Request) -> str | None:
    """Return the correlation id the middleware bound to ``request.state``."""
    value = getattr(request.state, "correlation_id", None)
    return value if isinstance(value, str) else None


class GatewayError(Exception):
    """A gateway failure carrying a locked taxonomy ``code`` and optional extras.

    Raised by the cross-cutting middleware and the router; the registered
    handler renders it through :func:`problem_response`. ``extras`` populates
    extension fields (e.g. ``auditEventId``); ``headers`` carries response
    headers like ``Retry-After`` on ``rate-limited`` / ``idempotency-in-flight``.
    """

    def __init__(
        self,
        code: GatewayErrorCode,
        *,
        detail: str,
        extras: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.extras = extras
        self.headers = headers

    @property
    def status(self) -> int:
        return LOCKED_CODE_TO_STATUS[self.code]


class ProblemDetail(BaseModel):
    """Wire shape of the ``application/problem+json`` envelope (RFC 7807 § 3.1).

    The ``code`` extension carries the locked taxonomy string. ``correlationId``
    / ``auditEventId`` are populated when known; other per-code extension fields
    are allowed via ``extra="allow"``.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: str = Field(..., description="Absolute URI identifying the problem type.")
    title: str = Field(..., description="Short human-readable summary.")
    status: int = Field(..., ge=100, le=599, description="HTTP status echoed into the body.")
    detail: str = Field(..., description="Long human-readable explanation.")
    instance: str | None = Field(default=None, description="Request path for correlation.")
    correlation_id: str | None = Field(
        default=None,
        alias="correlationId",
        description="Request correlation id (also in the x-correlation-id header).",
    )
    code: str = Field(..., description="Structured taxonomy selector.")

    @classmethod
    def from_code(
        cls,
        code: GatewayErrorCode,
        *,
        detail: str,
        instance: str | None,
        correlation_id: str | None,
        extras: dict[str, Any] | None = None,
    ) -> ProblemDetail:
        """Build a :class:`ProblemDetail` from a code in :data:`LOCKED_CODES`.

        Raises:
            KeyError: When ``code`` is not in the locked table, so an
                undocumented code can never escape onto the wire.
        """
        status = LOCKED_CODE_TO_STATUS[code]
        payload: dict[str, Any] = {
            "type": _type_uri_for_code(code),
            "title": _TITLE_FOR_CODE[code],
            "status": status,
            "detail": detail,
            "instance": instance,
            "correlationId": correlation_id,
            "code": code.value,
        }
        if extras:
            for name, value in extras.items():
                if value is None:
                    continue
                payload[name] = value
        return cls.model_validate(payload)


def problem_response(
    request: Request,
    *,
    code: GatewayErrorCode,
    detail: str,
    extras: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Materialise a :class:`ProblemDetail` into a Problem+JSON response.

    ``model_dump(exclude_none=True, by_alias=True)`` keeps the envelope minimal
    and emits the ``correlationId`` alias rather than the snake-case field name.
    """
    problem = ProblemDetail.from_code(
        code,
        detail=detail,
        instance=request.url.path,
        correlation_id=correlation_id_of(request),
        extras=extras,
    )
    return JSONResponse(
        status_code=problem.status,
        media_type=PROBLEM_MEDIA_TYPE,
        content=problem.model_dump(exclude_none=True, by_alias=True),
        headers=headers,
    )


async def _gateway_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render a :class:`GatewayError` through the Problem+JSON envelope.

    The handler is registered only for :class:`GatewayError`, but the type guard
    is an explicit runtime check (not an ``assert``, which ``python -O`` strips)
    so a misrouted exception re-raises rather than rendering a malformed body.
    """
    if not isinstance(exc, GatewayError):  # pragma: no cover - defensive guard
        raise exc
    return problem_response(
        request,
        code=exc.code,
        detail=exc.detail,
        extras=exc.extras,
        headers=exc.headers,
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Register the Problem+JSON handler for :class:`GatewayError` on ``app``.

    Every *gateway-originated* failure is raised as a :class:`GatewayError`, so
    this single handler renders the locked taxonomy onto the uniform envelope.
    Two response classes are deliberately outside this contract:

    * the ``/healthz`` + ``/readyz`` probes, which return their fixed k8s probe
      shape (``include_in_schema=False``) rather than a domain Problem; and
    * generic framework errors (Starlette ``HTTPException``, FastAPI
      ``RequestValidationError``) for routes that do not yet exist at this
      phase — their normalization into the envelope is wired with the full
      ``create_app`` route surface (AGW-IMPL-016), since the locked taxonomy has
      no generic code to map them onto here.

    Success responses are returned raw.
    """
    app.add_exception_handler(GatewayError, _gateway_error_handler)
