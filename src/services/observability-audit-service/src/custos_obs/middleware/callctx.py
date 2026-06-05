"""Call-context middleware for the Query API (OBS-IMPL-012).

Every authenticated internal request reaching the Observability and Audit
Service carries the API Gateway's EdDSA-signed call-context JWT in the
``x-custos-callctx`` header (COMP-001). This middleware verifies it via the
shared :mod:`custos_callctx` verifier and exposes the decoded
:class:`~custos_callctx.CallContext` on ``request.state.call_context`` so the
read-back routes (OBS-IMPL-013/014) authorize through :func:`get_call_context`
and :func:`require_permission`.

Two modes, steered by whether a verifier is supplied:

* **Production** — a :class:`~custos_callctx.CallContextVerifier` (built from the
  Auth Service JWKS URL) verifies the JWT signature + ``iss``/``aud``/``exp`` and
  returns the typed context. This is the design's trust model: receivers verify
  the gateway-minted token locally against the published JWKS, with no Auth
  Service round-trip.
* **Dev shim** — when no verifier is configured the middleware parses an
  *unsigned* JSON header so the remaining OBS-IMPL-* phases can exercise the
  authorized routes without a running Auth Service. The shim refuses to start in
  ``production`` (it would be an accidental auth bypass) and logs at WARNING so
  operators can see it is active. This mirrors the trigger-/catalog-service
  call-context middleware.

The two probe paths (``/healthz`` + ``/readyz``) bypass the middleware — they are
unauthenticated liveness/readiness checks.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Protocol

from custos_callctx import (
    CALLCTX_HEADER,
    CallContext,
    InvalidCallContextError,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from starlette.responses import Response
    from starlette.types import ASGIApp

logger = logging.getLogger("custos_obs.middleware.callctx")

#: Paths the middleware deliberately bypasses — the unauthenticated probes.
_BYPASS_PATHS: frozenset[str] = frozenset({"/healthz", "/readyz"})

#: Dev-shim placeholders for the JWT-bookkeeping claims the unsigned header does
#: not carry. They are never trusted in production (the shim is forbidden there).
_DEV_SHIM_ISSUER = "custos-dev-shim"
_DEV_SHIM_AUDIENCE = "custos.internal"
_DEV_SHIM_KID = "dev-shim"
_DEV_SHIM_COMPONENT = "api-gateway"


class CallContextVerifierProtocol(Protocol):
    """The narrow verify seam the middleware depends on.

    Structurally satisfied by :class:`custos_callctx.CallContextVerifier`; tests
    inject a fake without standing up a JWKS cache.
    """

    async def verify(self, *, metadata: Mapping[str, str]) -> CallContext: ...


class DevShimDisabledInProductionError(RuntimeError):
    """Raised when the call-context dev shim is activated in production.

    Production deployments must configure the Auth Service JWKS URL. Refusing to
    start (rather than silently accepting unsigned headers) guards against
    shipping an auth bypass by accident.
    """


class CallContextError(Exception):
    """Authorization failure rendered through the shared error envelope.

    Both the middleware (which returns a :class:`JSONResponse` directly because
    Starlette would otherwise convert a middleware-raised exception into a
    generic 500) and the FastAPI dependencies (:func:`get_call_context`,
    :func:`require_permission`) channel every 4xx auth failure through this type
    so HTTP clients observe one shape per logical failure mode.
    """

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


def _render_envelope(status_code: int, code: str, detail: str) -> JSONResponse:
    """Single source of truth for the call-context error envelope."""
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


async def call_context_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    """FastAPI exception handler that renders :class:`CallContextError`.

    Registered by :func:`custos_obs.create_app`; the signature accepts
    ``Exception`` so it matches :meth:`FastAPI.add_exception_handler` without an
    explicit ``# type: ignore``, then narrows back for attribute access.
    """
    assert isinstance(exc, CallContextError)
    return _render_envelope(exc.status_code, exc.code, exc.detail)


class CallContextMiddleware(BaseHTTPMiddleware):
    """Extracts and validates the call-context header on every request.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        verifier: The call-context JWT verifier. ``None`` enables the dev shim.
        environment: Deployment environment. ``"production"`` (case-insensitive)
            with no verifier raises :class:`DevShimDisabledInProductionError` at
            construction.

    Raises:
        DevShimDisabledInProductionError: When constructed with the dev shim
            active in a production environment.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        verifier: CallContextVerifierProtocol | None,
        environment: str,
    ) -> None:
        super().__init__(app)
        self._verifier = verifier
        self._dev_shim_active = verifier is None
        if self._dev_shim_active and environment.lower() == "production":
            raise DevShimDisabledInProductionError(
                "no call-context verifier configured but ENVIRONMENT=production; "
                "the call-context dev shim is forbidden in production. Configure "
                "the Auth Service JWKS URL or run a non-production environment."
            )

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.url.path in _BYPASS_PATHS:
            return await call_next(request)

        if self._verifier is not None:
            try:
                ctx = await self._verifier.verify(metadata=request.headers)
            except InvalidCallContextError as exc:
                logger.info("rejected call context: %s (%s)", exc.detail, exc.reason)
                return _render_envelope(401, "callctx_invalid", "invalid call context")
        else:
            raw_header = request.headers.get(CALLCTX_HEADER)
            if not raw_header:
                return _render_envelope(
                    401, "callctx_missing", f"{CALLCTX_HEADER} header is required"
                )
            try:
                ctx = self._parse_dev_shim_header(raw_header)
            except (ValueError, json.JSONDecodeError) as exc:
                return _render_envelope(400, "callctx_malformed", str(exc))
            logger.warning(
                "call-context dev shim active for %s %s (workspace=%s principal=%s) — "
                "configure the Auth Service JWKS URL to disable",
                request.method,
                request.url.path,
                ctx.workspace_id,
                ctx.acting_principal_id,
            )

        request.state.call_context = ctx
        return await call_next(request)

    @staticmethod
    def _parse_dev_shim_header(raw: str) -> CallContext:
        payload: Any = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("dev-shim header must decode to a JSON object")
        principal = payload.get("acting_principal_id") or payload.get("actingPrincipalId")
        if not isinstance(principal, str) or not principal:
            raise ValueError("dev-shim header requires a non-empty 'acting_principal_id'")
        workspace = payload.get("workspace_id", payload.get("workspaceId"))
        if workspace is not None and not isinstance(workspace, str):
            raise ValueError("dev-shim 'workspace_id' must be a string or null")
        raw_perms = payload.get("permissions", [])
        if not isinstance(raw_perms, list):
            raise ValueError("dev-shim 'permissions' must be a list")
        return CallContext(
            acting_principal_id=principal,
            workspace_id=workspace,
            caller_component=_DEV_SHIM_COMPONENT,
            jti="dev-shim",
            issued_at=0,
            expires_at=0,
            issuer=_DEV_SHIM_ISSUER,
            audience=_DEV_SHIM_AUDIENCE,
            kid=_DEV_SHIM_KID,
            permissions=frozenset(str(p) for p in raw_perms),
        )


async def get_call_context(request: Request) -> CallContext:
    """FastAPI dependency returning the parsed call context.

    Raises :class:`CallContextError` (rendered through
    :func:`call_context_error_handler`) when the middleware did not run — e.g. a
    route mounted outside the middleware stack — so handlers always observe a
    populated context on a 2xx path and the wire shape matches the
    middleware-emitted envelope.
    """
    ctx = getattr(request.state, "call_context", None)
    if ctx is None:
        raise CallContextError(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")
    assert isinstance(ctx, CallContext)
    return ctx


def require_permission(name: str) -> Callable[[Request], Awaitable[CallContext]]:
    """Build a FastAPI dependency that requires ``name`` on the call context."""

    async def _dep(request: Request) -> CallContext:
        ctx = await get_call_context(request)
        if not ctx.has_permission(name):
            raise CallContextError(403, "permission_denied", f"missing required permission: {name}")
        return ctx

    return _dep


__all__ = [
    "CALLCTX_HEADER",
    "CallContext",
    "CallContextError",
    "CallContextMiddleware",
    "CallContextVerifierProtocol",
    "DevShimDisabledInProductionError",
    "call_context_error_handler",
    "get_call_context",
    "require_permission",
]
