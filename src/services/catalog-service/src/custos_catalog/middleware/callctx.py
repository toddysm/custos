"""Call-context middleware + dev shim (CS-IMPL-004).

The API Gateway design (COMP-001) carries a signed call-context JWT in the
``x-custos-callctx`` header on every internal request. The Catalog Service
parses it, validates it, and exposes the result on ``request.state.call_context``
so route handlers can authorize via :func:`get_call_context` and
:func:`require_permission`.

The full validation path lives behind ``CAT_AUTHZ_ENDPOINT``; until the Auth
Service (COMP-002) ships per CS-IMPL-024 (#225) the middleware raises
:class:`NotImplementedError` whenever ``CAT_AUTHZ_ENDPOINT`` is set. With the
env var left empty the middleware activates a **dev shim** that parses an
unsigned JSON header so the rest of Phase B-H can proceed; the shim refuses
to start when ``ENVIRONMENT`` is ``production`` (case-insensitive).

Audit hook
----------

Every dev-shim request emits an ``auth.callctx.shim_used`` event through
:func:`custos_catalog.audit.emit_event`. CS-IMPL-019 will wire that hook to
the observability + audit pipeline; the stub here just logs at WARNING so the
operator can see the dev shim is active.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from custos_catalog.audit import emit_event

if TYPE_CHECKING:
    from starlette.types import ASGIApp

#: Wire header carrying the call-context document.
#: API Gateway design (COMP-001) pins the canonical lowercase form.
CALLCTX_HEADER: str = "x-custos-callctx"

#: Paths the middleware deliberately bypasses (no auth on probes).
_BYPASS_PATHS: frozenset[str] = frozenset({"/healthz", "/readyz"})

logger = logging.getLogger(__name__)


class DevShimDisabledInProductionError(RuntimeError):
    """Raised when the call-context dev shim is activated in production.

    Production deployments must set ``CAT_AUTHZ_ENDPOINT`` to a non-empty
    URL. Refusing to start (rather than silently accepting unsigned
    headers) is the design's guard against shipping a debug mode by
    accident.
    """


class CallContext(BaseModel):
    """Per-request authorization context attached to ``request.state``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    workspace_id: str = Field(..., min_length=1)
    principal_id: str = Field(..., min_length=1)
    tenant_id: str | None = None
    permissions: frozenset[str] = Field(default_factory=frozenset)
    issued_at: int | None = Field(default=None, alias="iat")
    expires_at: int | None = Field(default=None, alias="exp")

    def has_permission(self, name: str) -> bool:
        return name in self.permissions


def _error(status_code: int, code: str, detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


class CallContextMiddleware(BaseHTTPMiddleware):
    """Extracts and validates the call-context header on every request.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        authz_endpoint: Value of ``CAT_AUTHZ_ENDPOINT``. Empty enables
            the dev shim; non-empty surfaces a 500 with
            :class:`NotImplementedError` until CS-IMPL-024 wires the real
            Auth Service path.
        environment: Value of ``ENVIRONMENT``. ``"production"`` with an
            empty ``authz_endpoint`` raises
            :class:`DevShimDisabledInProductionError` at construction.

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
    ) -> None:
        super().__init__(app)
        self._authz_endpoint = authz_endpoint
        self._dev_shim_active = authz_endpoint == ""
        if self._dev_shim_active and environment.lower() == "production":
            raise DevShimDisabledInProductionError(
                "CAT_AUTHZ_ENDPOINT is empty but ENVIRONMENT=production; "
                "the call-context dev shim is forbidden in production. "
                "Set CAT_AUTHZ_ENDPOINT to the Auth Service URL or run a "
                "non-production environment."
            )

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.url.path in _BYPASS_PATHS:
            return await call_next(request)

        raw_header = request.headers.get(CALLCTX_HEADER)
        if not raw_header:
            return _error(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")

        if not self._dev_shim_active:
            # CS-IMPL-024 will replace this raise with the real call to the
            # Auth Service: sig-verify the JWT, parse claims, call /authorize.
            raise NotImplementedError(
                "Production call-context validation is not yet implemented; "
                "tracked by CS-IMPL-024 (#225). Unset CAT_AUTHZ_ENDPOINT to "
                "fall back to the dev shim in non-production environments."
            )

        try:
            ctx = self._parse_dev_shim_header(raw_header)
        except ValidationError as exc:
            return _error(400, "callctx_invalid", _summarize_validation_error(exc))
        except (ValueError, json.JSONDecodeError):
            return _error(400, "callctx_malformed", "header is not valid JSON")

        request.state.call_context = ctx
        logger.warning(
            "call-context dev shim active for %s %s "
            "(workspace=%s principal=%s) — set CAT_AUTHZ_ENDPOINT to disable",
            request.method,
            request.url.path,
            ctx.workspace_id,
            ctx.principal_id,
        )
        emit_event(
            "auth.callctx.shim_used",
            {
                "path": request.url.path,
                "method": request.method,
                "workspace_id": ctx.workspace_id,
                "principal_id": ctx.principal_id,
            },
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

    Raises an HTTP 401 if the middleware was not run (e.g. mis-mounted)
    so handlers always observe a populated context on a 2xx path.
    """
    from fastapi import HTTPException

    ctx = getattr(request.state, "call_context", None)
    if ctx is None:
        raise HTTPException(status_code=401, detail="call context not available")
    assert isinstance(ctx, CallContext)
    return ctx


def require_permission(
    name: str,
) -> Callable[[Request], Awaitable[CallContext]]:
    """Build a FastAPI dependency that requires ``name`` on the call context."""
    from fastapi import HTTPException

    async def _dep(request: Request) -> CallContext:
        ctx = await get_call_context(request)
        if not ctx.has_permission(name):
            raise HTTPException(
                status_code=403,
                detail=f"missing required permission: {name}",
            )
        return ctx

    return _dep


__all__ = [
    "CALLCTX_HEADER",
    "CallContext",
    "CallContextMiddleware",
    "DevShimDisabledInProductionError",
    "get_call_context",
    "require_permission",
]
