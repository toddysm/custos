"""Call-context middleware + dev shim (ARM-IMPL-002).

The API Gateway (COMP-001) carries a signed call-context JWT in the
``x-custos-callctx`` header on every internal request. The Activity
Runtime Manager parses it, validates it, and exposes the result on
``request.state.call_context`` so route handlers and RPC adapters can
authorize via :func:`get_call_context`.

The full validation path lives behind ``ARM_AUTHZ_ENDPOINT``; until the
Auth Service real-verifier wiring lands the middleware raises
:class:`NotImplementedError` whenever ``ARM_AUTHZ_ENDPOINT`` is set. With
the env var left empty the middleware activates a **dev shim** that parses
an unsigned JSON header so the rest of the ARM-IMPL phases can proceed; the
shim logs a WARNING per request and refuses to start when ``ENVIRONMENT``
is ``production`` (case-insensitive), per design § Configuration.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from custos_callctx import CallContextVerifier, InvalidCallContextError, InvalidReason
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

if TYPE_CHECKING:
    from starlette.types import ASGIApp

#: Wire header carrying the call-context document. The API Gateway design
#: (COMP-001) pins the canonical lowercase form.
CALLCTX_HEADER: str = "x-custos-callctx"

#: Paths the middleware deliberately bypasses — probes and the OpenAPI
#: documentation surface require no call-context header.
_BYPASS_PATHS: frozenset[str] = frozenset(
    {"/healthz", "/readyz", "/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}
)

#: Path prefixes the middleware bypasses (e.g. the Prometheus ``/metrics``
#: mount added in ARM-IMPL-020, which must be reachable by the scraper
#: without minting an internal token).
_BYPASS_PREFIXES: tuple[str, ...] = ("/metrics",)

logger = logging.getLogger("custos_arm")


class DevShimDisabledInProductionError(RuntimeError):
    """Raised when the call-context dev shim is activated in production.

    Production deployments must set ``ARM_AUTHZ_ENDPOINT`` to a non-empty
    URL. Refusing to start (rather than silently accepting unsigned
    headers) is the design's guard against shipping a debug mode by
    accident.
    """


class CallContextError(Exception):
    """Authorization failure rendered through the shared error envelope.

    Both the middleware (which returns a :class:`JSONResponse` directly
    because Starlette would otherwise convert middleware-raised exceptions
    to a generic 500) and the FastAPI dependency :func:`get_call_context`
    channel every 4xx auth failure through this type so HTTP clients see
    one shape for a given logical failure mode regardless of where it was
    detected.
    """

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


class CallContext(BaseModel):
    """Per-request authorization context attached to ``request.state``.

    Mirrors the sibling-service dev-shim shape so the cross-service test
    fixtures and operator tools stay aligned. The production
    :class:`custos_callctx.CallContext` (returned by the Auth Service
    verifier once it lands) carries additional claims like ``jti``,
    ``iss``, ``aud``, and ``kid``; this dev-shim accepts a subset to keep
    the wire format compact for local development.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    workspace_id: str = Field(..., min_length=1)
    principal_id: str = Field(..., min_length=1)
    tenant_id: str | None = None
    permissions: frozenset[str] = Field(default_factory=frozenset)
    issued_at: int | None = Field(default=None, alias="iat")
    expires_at: int | None = Field(default=None, alias="exp")
    execution_operation: str | None = None

    def has_permission(self, name: str) -> bool:
        return name in self.permissions


def _render_envelope(status_code: int, code: str, detail: str) -> JSONResponse:
    """Single source of truth for the call-context error envelope."""
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


async def call_context_error_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """FastAPI exception handler that renders :class:`CallContextError`.

    Registered by :func:`custos_arm.create_app`; consumers mounting
    :class:`CallContextMiddleware` directly (e.g. tests) must also register
    this handler against :class:`CallContextError` so the dependency-side
    4xx responses match the middleware envelope.
    """
    assert isinstance(exc, CallContextError)
    return _render_envelope(exc.status_code, exc.code, exc.detail)


class CallContextMiddleware(BaseHTTPMiddleware):
    """Extracts and validates the call-context header on every request.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        authz_endpoint: Value of ``ARM_AUTHZ_ENDPOINT``. Empty enables the
            development shim; otherwise signed contexts are verified against
            the issuer's public keys, with no unsigned fallback.
        environment: Value of ``ENVIRONMENT``. ``"production"`` with an
            empty ``authz_endpoint`` raises
            :class:`DevShimDisabledInProductionError` at construction.
        verifier: Optional verifier injection for controlled key fetching in tests.

    Raises:
        DevShimDisabledInProductionError: When constructed with the dev
            shim active in a production environment.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        authz_endpoint: str,
        environment: str,
        verifier: CallContextVerifier | None = None,
    ) -> None:
        super().__init__(app)
        self._authz_endpoint = authz_endpoint
        self._dev_shim_active = authz_endpoint == ""
        self._verifier = verifier or (
            CallContextVerifier(jwks_url=authz_endpoint.rstrip("/") + "/.well-known/jwks.json")
            if authz_endpoint
            else None
        )
        if self._dev_shim_active and environment.lower() == "production":
            raise DevShimDisabledInProductionError(
                "ARM_AUTHZ_ENDPOINT is empty but ENVIRONMENT=production; "
                "the call-context dev shim is forbidden in production. "
                "Set ARM_AUTHZ_ENDPOINT to the Auth Service URL or run a "
                "non-production environment."
            )

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.url.path in _BYPASS_PATHS:
            return await call_next(request)
        if any(request.url.path.startswith(prefix) for prefix in _BYPASS_PREFIXES):
            return await call_next(request)

        raw_header = request.headers.get(CALLCTX_HEADER)
        if not raw_header:
            return _render_envelope(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")

        if self._verifier is not None:
            if len(request.headers.getlist(CALLCTX_HEADER)) != 1:
                return _render_envelope(401, "callctx_invalid", "one signed context is required")
            try:
                verified = await self._verifier.verify(metadata=request.headers)
            except InvalidCallContextError as exc:
                logger.warning("Rejected signed activity context: %s", exc.reason.value)
                return _render_envelope(
                    503 if exc.reason is InvalidReason.JWKS_UNAVAILABLE else 401,
                    "callctx_invalid",
                    "context verification failed",
                )
            if not verified.workspace_id:
                return _render_envelope(403, "workspace_required", "workspace scope is required")
            request.state.call_context = CallContext(
                workspace_id=verified.workspace_id,
                principal_id=verified.acting_principal_id,
                permissions=verified.permissions,
                iat=verified.issued_at,
                exp=verified.expires_at,
                execution_operation=verified.execution_operation,
            )
            request.state.signed_call_context = True
            return await call_next(request)

        try:
            ctx = self._parse_dev_shim_header(raw_header)
        except ValidationError as exc:
            return _render_envelope(400, "callctx_invalid", _summarize_validation_error(exc))
        except (ValueError, json.JSONDecodeError):
            return _render_envelope(400, "callctx_malformed", "header is not valid JSON")

        request.state.call_context = ctx
        logger.warning(
            "call-context dev shim active for %s %s "
            "(workspace=%s principal=%s) — set ARM_AUTHZ_ENDPOINT to disable",
            request.method,
            request.url.path,
            ctx.workspace_id,
            ctx.principal_id,
        )
        return await call_next(request)

    @staticmethod
    def _parse_dev_shim_header(raw: str) -> CallContext:
        payload: Any = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("dev-shim header must decode to a JSON object")
        # `permissions` is naturally a list on the wire; coerce to frozenset.
        perms = payload.get("permissions")
        if isinstance(perms, list):
            payload["permissions"] = frozenset(str(p) for p in perms)
        return CallContext.model_validate(payload)


def _summarize_validation_error(exc: ValidationError) -> str:
    """Render a one-line summary that does not leak internal field paths."""
    errors = exc.errors()
    if not errors:
        return "invalid call context"
    first: dict[str, Any] = dict(errors[0])
    msg = first.get("msg", "invalid call context")
    return str(msg)


async def get_call_context(request: Request) -> CallContext:
    """FastAPI dependency that returns the parsed call context.

    Raises :class:`CallContextError` (rendered through
    :func:`call_context_error_handler`) when the middleware was not run, so
    handlers always observe a populated context on a 2xx path AND the wire
    response shape matches the middleware-emitted envelope.
    """
    ctx = getattr(request.state, "call_context", None)
    if ctx is None:
        raise CallContextError(
            401,
            "callctx_missing",
            f"{CALLCTX_HEADER} header is required",
        )
    assert isinstance(ctx, CallContext)
    return ctx


__all__ = [
    "CALLCTX_HEADER",
    "CallContext",
    "CallContextError",
    "CallContextMiddleware",
    "DevShimDisabledInProductionError",
    "call_context_error_handler",
    "get_call_context",
]
