"""Call-context middleware + dev shim (TS-IMPL-003).

The API Gateway design (COMP-001) carries a signed call-context JWT in the
``x-custos-callctx`` header on every authenticated internal request. The
Trigger Service parses it, validates it, and exposes the result on
``request.state.call_context`` so route handlers (the workspace-scoped REST
CRUD surface landing in TS-IMPL-015) can authorize via
:func:`get_call_context` and :func:`require_permission`.

The full validation path is delegated to the Auth Service (COMP-002) and is
gated behind a non-empty authz endpoint. Until that wiring lands the
middleware activates a **dev shim** that parses an unsigned JSON header so
the rest of the TS-IMPL-* phases can proceed; the shim refuses to start when
``ENVIRONMENT`` is ``production`` (case-insensitive).

This mirrors the catalog-service middleware (CS-IMPL-004). The audit hook
that emits an ``auth.callctx.shim_used`` event lands with the Trigger Service
observability + audit pipeline in TS-IMPL-019; until then the dev shim logs
at WARNING so operators can see it is active.

Note: the webhook ingress path (``POST /v1/webhooks/{connectorInstanceId}``)
is an unauthenticated, gateway-forwarded route with no call-context — it is
not mounted here and would be added to the bypass set by the Generic Webhook
Receiver (TS-IMPL deferred to M2) rather than carrying a call-context header.
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

if TYPE_CHECKING:
    from starlette.types import ASGIApp

#: Wire header carrying the call-context document.
#: API Gateway design (COMP-001) pins the canonical lowercase form.
CALLCTX_HEADER: str = "x-custos-callctx"

#: Paths the middleware deliberately bypasses.
#:
#: ``/healthz`` + ``/readyz`` are unauthenticated probes. The two resume RPC
#: method paths are internal Dapr service-invocation routes the Workflow
#: Service calls (TS-IMPL-016); they are authenticated at the Dapr mesh layer
#: (mTLS + app-id allow-list), not via the call-context envelope — the caller
#: propagates no ``x-custos-callctx`` header — so they bypass here. The internal
#: receiver routes (TS-IMPL-017) are Dapr Pub/Sub control-plane surfaces — the
#: sidecar's subscription probe and topic deliveries carry no call-context
#: header either — so they bypass too. Keep these in sync with
#: ``custos_trigger.api.routes.rpc.REGISTER_RESUME_PATH`` / ``CANCEL_RESUME_PATH``
#: and ``custos_trigger.receivers.internal.DAPR_SUBSCRIBE_PATH`` /
#: ``INTERNAL_EVENTS_PATH``.
_BYPASS_PATHS: frozenset[str] = frozenset(
    {
        "/healthz",
        "/readyz",
        "/RegisterResumeSubscription",
        "/CancelResumeSubscription",
        "/dapr/subscribe",
        "/internal/events/workflow",
    }
)

logger = logging.getLogger(__name__)


class DevShimDisabledInProductionError(RuntimeError):
    """Raised when the call-context dev shim is activated in production.

    Production deployments must configure a non-empty authz endpoint.
    Refusing to start (rather than silently accepting unsigned headers) is
    the design's guard against shipping a debug mode by accident.
    """


class CallContextError(Exception):
    """Authorization failure rendered through the shared error envelope.

    Both the middleware (which returns a :class:`JSONResponse` directly
    because Starlette would otherwise convert middleware-raised exceptions
    to a generic 500) and the FastAPI dependencies
    (:func:`get_call_context` and :func:`require_permission`) channel every
    4xx auth failure through this type so HTTP clients see one shape for a
    given logical failure mode regardless of where it was detected.

    Pair this with :func:`call_context_error_handler` on the FastAPI
    application; :func:`custos_trigger.create_app` registers it
    automatically.
    """

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


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

    Registered by :func:`custos_trigger.create_app`; consumers mounting
    :class:`CallContextMiddleware` directly (e.g. tests) must also register
    this handler against :class:`CallContextError` so the dependency-side
    4xx responses match the middleware envelope.

    The signature accepts ``Exception`` so it matches
    :meth:`FastAPI.add_exception_handler` without an explicit ``# type:
    ignore``; the implementation narrows back to :class:`CallContextError`
    for the attribute access.
    """
    assert isinstance(exc, CallContextError)
    return _render_envelope(exc.status_code, exc.code, exc.detail)


class CallContextMiddleware(BaseHTTPMiddleware):
    """Extracts and validates the call-context header on every request.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        authz_endpoint: Auth Service URL. Empty enables the dev shim;
            non-empty surfaces a 500 with :class:`NotImplementedError`
            until the Auth Service (COMP-002) integration is wired.
        environment: Deployment environment. ``"production"`` with an empty
            ``authz_endpoint`` raises
            :class:`DevShimDisabledInProductionError` at construction.

    Raises:
        DevShimDisabledInProductionError: When constructed with the dev shim
            active in a production environment.
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
                "authz endpoint is empty but ENVIRONMENT=production; "
                "the call-context dev shim is forbidden in production. "
                "Configure the Auth Service URL or run a non-production "
                "environment."
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
            return _render_envelope(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")

        if not self._dev_shim_active:
            # The Auth Service (COMP-002) integration will replace this raise
            # with the real path: sig-verify the JWT, parse claims, authorize.
            raise NotImplementedError(
                "Production call-context validation is not yet implemented; "
                "it is delegated to the Auth Service (COMP-002). Unset the "
                "authz endpoint to fall back to the dev shim in non-production "
                "environments."
            )

        try:
            ctx = self._parse_dev_shim_header(raw_header)
        except ValidationError as exc:
            return _render_envelope(400, "callctx_invalid", _summarize_validation_error(exc))
        except (ValueError, json.JSONDecodeError):
            return _render_envelope(400, "callctx_malformed", "header is not valid JSON")

        request.state.call_context = ctx
        logger.warning(
            "call-context dev shim active for %s %s "
            "(workspace=%s principal=%s) — configure the authz endpoint to "
            "disable",
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
    :func:`call_context_error_handler`) when the middleware was not run —
    e.g. on a route mounted outside the middleware stack — so handlers
    always observe a populated context on a 2xx path AND so the wire
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


def require_permission(
    name: str,
) -> Callable[[Request], Awaitable[CallContext]]:
    """Build a FastAPI dependency that requires ``name`` on the call context."""

    async def _dep(request: Request) -> CallContext:
        ctx = await get_call_context(request)
        if not ctx.has_permission(name):
            raise CallContextError(
                403,
                "permission_denied",
                f"missing required permission: {name}",
            )
        return ctx

    return _dep


__all__ = [
    "CALLCTX_HEADER",
    "CallContext",
    "CallContextError",
    "CallContextMiddleware",
    "DevShimDisabledInProductionError",
    "call_context_error_handler",
    "get_call_context",
    "require_permission",
]
