"""Signed call-context verification and independently authenticated mesh routes.

The API Gateway design (COMP-001) carries a signed call-context JWT in the
``x-custos-callctx`` header on every authenticated internal request. The
Trigger Service parses it, validates it, and exposes the result on
``request.state.call_context`` so route handlers (the workspace-scoped REST
CRUD surface landing in TS-IMPL-015) can authorize via
:func:`get_call_context` and :func:`require_permission`.

The shared verifier validates signatures and claims against the Auth Service's
public keys. An empty authz endpoint retains the unsigned development shim,
which refuses to start in production. Mesh requests authenticate the local
sidecar's app token before trusting its Workflow caller identity.

This mirrors the catalog-service middleware (CS-IMPL-004). The audit hook
that emits an ``auth.callctx.shim_used`` event lands with the Trigger Service
observability + audit pipeline in TS-IMPL-019; until then the dev shim logs
at WARNING so operators can see it is active.

No webhook prefix is exempted: future ingress must authenticate through its
own exact route contract rather than a blanket middleware bypass.
"""

from __future__ import annotations

import hmac
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

    from custos_trigger.dependencies import WorkflowCallerVerifier

#: Wire header carrying the call-context document.
#: API Gateway design (COMP-001) pins the canonical lowercase form.
CALLCTX_HEADER: str = "x-custos-callctx"

TRIGGER_ADMIN_PERMISSION = "trigger:admin"
_PROBE_ROUTES = frozenset({("GET", "/healthz"), ("GET", "/readyz")})
_WORKFLOW_ROUTES = frozenset(
    {
        ("POST", "/RegisterResumeSubscription"),
        ("POST", "/CancelResumeSubscription"),
    }
)
_SIDECAR_ROUTES = frozenset(
    {
        ("GET", "/dapr/subscribe"),
        ("POST", "/internal/events/workflow"),
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


def _require_sidecar_token(request: Request, token: str | None) -> None:
    """Authenticate the app-side hop before accepting sidecar-provided routing metadata."""
    if token is None:
        raise CallContextError(503, "mesh_unavailable", "sidecar authentication is not configured")
    values = request.headers.getlist("dapr-api-token")
    if len(values) != 1 or not hmac.compare_digest(values[0].encode(), token.encode()):
        raise CallContextError(401, "mesh_unauthenticated", "invalid sidecar authentication")


class DaprWorkflowCallerVerifier:
    """Require the trusted sidecar hop and its overwritten Workflow caller application ID."""

    def __init__(self, token: str, app_id: str = "workflow-service") -> None:
        """Bind deployment-owned credentials, never a caller-supplied identity assertion."""
        if not token.strip() or not app_id.strip():
            raise ValueError("sidecar token and Workflow app ID must be nonempty")
        self._token, self._app_id = token, app_id

    async def __call__(self, request: Request) -> None:
        """Reject direct app-port spoofing and other authenticated mesh applications."""
        _require_sidecar_token(request, self._token)
        if request.headers.getlist("dapr-caller-app-id") != [self._app_id]:
            raise CallContextError(403, "mesh_caller_denied", "Workflow service identity required")


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
        """Match exact grants; administration never implies subscription permissions."""
        return name in self.permissions


def ensure_workspace(ctx: CallContext, workspace_id: str) -> None:
    """Keep the request's authoritative persistence scope bound to its verified context."""
    if ctx.workspace_id != workspace_id:
        raise CallContextError(
            status_code=403,
            code="workspace_mismatch",
            detail="call context workspace does not match the request path workspace",
        )


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
        authz_endpoint: Auth Service base URL; its public JWKS endpoint supplies
            verification keys. Empty enables the development shim.
        environment: Deployment environment. ``"production"`` with an empty
            ``authz_endpoint`` raises
            :class:`DevShimDisabledInProductionError` at construction.
        verifier: Optional shared verifier override for hermetic key-source tests.
        app_api_token: Token shared only with the local Dapr sidecar.
        workflow_caller_verifier: Trusted Workflow authenticator for exact resume routes.

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
        verifier: CallContextVerifier | None = None,
        app_api_token: str | None = None,
        workflow_caller_verifier: WorkflowCallerVerifier | None = None,
    ) -> None:
        """Configure independent user and mesh trust without making network calls."""
        super().__init__(app)
        self._verifier = verifier
        if self._verifier is None and authz_endpoint.strip():
            self._verifier = CallContextVerifier(
                jwks_url=authz_endpoint.strip().rstrip("/") + "/.well-known/jwks.json",
            )
        self._app_api_token = app_api_token
        self._workflow_caller_verifier = workflow_caller_verifier
        if self._workflow_caller_verifier is None and app_api_token is not None:
            self._workflow_caller_verifier = DaprWorkflowCallerVerifier(app_api_token)
        self._dev_shim_active = self._verifier is None
        if self._dev_shim_active and environment.strip().lower() == "production":
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
        """Verify the applicable trust domain before handlers can access tenant state."""
        route = (request.method, request.url.path)
        if route in _PROBE_ROUTES:
            return await call_next(request)
        if route in _WORKFLOW_ROUTES or route in _SIDECAR_ROUTES:
            try:
                await self._authenticate_mesh(request, workflow=route in _WORKFLOW_ROUTES)
            except CallContextError as exc:
                logger.warning("Rejected mesh request: %s", exc.code)
                return _render_envelope(exc.status_code, exc.code, exc.detail)
            return await call_next(request)

        raw_header = request.headers.get(CALLCTX_HEADER)
        if not raw_header:
            return _render_envelope(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")

        if self._verifier is not None:
            if len(request.headers.getlist(CALLCTX_HEADER)) != 1:
                return _render_envelope(401, "callctx_invalid", "one call context is required")
            try:
                verified = await self._verifier.verify(metadata=request.headers)
            except InvalidCallContextError as exc:
                logger.warning("Rejected signed call context: %s", exc.reason.value)
                if exc.reason is InvalidReason.JWKS_UNAVAILABLE:
                    return _render_envelope(503, "callctx_unavailable", "verification unavailable")
                return _render_envelope(401, "callctx_invalid", "invalid call context")
            if verified.workspace_id is None or not verified.workspace_id.strip():
                return _render_envelope(403, "workspace_required", "workspace scope is required")
            request.state.call_context = CallContext(
                workspace_id=verified.workspace_id,
                principal_id=verified.acting_principal_id,
                permissions=verified.permissions,
                iat=verified.issued_at,
                exp=verified.expires_at,
            )
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
            "(workspace=%s principal=%s) — configure the authz endpoint to "
            "disable",
            request.method,
            request.url.path,
            ctx.workspace_id,
            ctx.principal_id,
        )
        return await call_next(request)

    async def _authenticate_mesh(self, request: Request, *, workflow: bool) -> None:
        """Keep legacy development separate from authenticated production mesh traffic."""
        if workflow and self._workflow_caller_verifier is not None:
            await self._workflow_caller_verifier(request)
            request.state.workflow_caller_verified = True
            return
        if self._dev_shim_active and self._app_api_token is None:
            logger.warning("Unauthenticated mesh route enabled by the development shim")
            return
        if workflow:
            raise CallContextError(503, "mesh_unavailable", "Workflow authentication unavailable")
        _require_sidecar_token(request, self._app_api_token)
        if request.headers.getlist("dapr-caller-app-id"):
            raise CallContextError(
                403,
                "mesh_caller_denied",
                "direct sidecar callback required",
            )

    @staticmethod
    def _parse_dev_shim_header(raw: str) -> CallContext:
        """Parse the existing unsigned development wire shape, never a production token."""
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
    workspace_id = request.path_params.get("workspace_id")
    if workspace_id is not None:
        ensure_workspace(ctx, workspace_id)
    return ctx


def require_permission(
    name: str,
) -> Callable[[Request], Awaitable[CallContext]]:
    """Build a FastAPI dependency that requires ``name`` on the call context."""

    async def _dep(request: Request) -> CallContext:
        """Enforce workspace binding before checking the exact requested grant."""
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
    "TRIGGER_ADMIN_PERMISSION",
    "CallContext",
    "CallContextError",
    "CallContextMiddleware",
    "DaprWorkflowCallerVerifier",
    "DevShimDisabledInProductionError",
    "call_context_error_handler",
    "ensure_workspace",
    "get_call_context",
    "require_permission",
]
