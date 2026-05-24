"""Call-context middleware + dev shim for auth-service (AS-IMPL-005/006/007).

The API Gateway design (COMP-001) carries a signed call-context JWT in the
``x-custos-callctx`` header on every internal request. Auth Service is the
issuer of those JWTs (Phase G, AS-IMPL-017) **and** a consumer of them —
gateway routes back through auth-service for protected admin endpoints
(create tenant, mint service-account, etc.) once a bearer token has been
verified.

The full validation path lives behind ``CUSTOS_AUTH_CALLCTX_VERIFIER_URL``
(JWKS URL); until Phase G ships the signer (AS-IMPL-017), JWKS endpoint
(AS-IMPL-018), and verifier helper (AS-IMPL-019), the middleware raises
:class:`NotImplementedError` whenever the env var is set. With the env var
left empty the middleware activates a **dev shim** that parses an unsigned
JSON header so Phase C / D / E / F can proceed; the shim refuses to start
when ``ENVIRONMENT`` is ``production`` (case-insensitive) — the same guard
catalog-service uses for its own dev shim (CS-IMPL-004).

Wire format (dev shim)
----------------------

The dev-shim header is a JSON object with these fields::

    {
        "principal_id": "user-...",
        "tenant_id":    "tenant-..." | null,
        "workspace_id": "ws-..."     | null,
        "permissions":  ["platform.admin", "tenant.admin", ...]
    }

``principal_id`` is the only required field. ``tenant_id`` /
``workspace_id`` are optional because auth-service has both
platform-global endpoints (e.g. ``POST /v1/tenants``) and
workspace-scoped endpoints (e.g. ``GET /v1/workspaces/{id}``). Routes read
the tenant/workspace scope from :func:`get_call_context` and enforce any
required authorization with :func:`require_permission` where applicable,
rather than via the model alone.

Audit hook
----------

Unlike catalog-service, the auth-service middleware does **not** emit a
per-request ``auth.callctx.shim_used`` audit event — auth-service is the
sink for audit events, and round-tripping every shim request through its
own audit outbox would produce an audit-storm during local dev. A plain
``WARNING`` log line is sufficient operator signal that the dev shim is
active.
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

#: Paths the middleware deliberately bypasses (no auth on probes).
#:
#: * Health probes (``/healthz`` / ``/readyz``) — Kubernetes makes
#:   them without a call-context.
#: * Token verification (``/v1/auth/verify``) — the verify endpoint
#:   is *the source* of call-context for downstream services, so by
#:   construction the caller does not yet have one.
#: * Gateway hot-path (``/v1/authz/verify-and-authorize``) — same
#:   reasoning: the gateway calls this *before* it has a
#:   call-context, on every external request.
#: * OIDC callback (``/v1/auth/login/oidc/callback``) — external
#:   OIDC redirect from the IdP, bootstrapping a session; no
#:   internal call-context exists yet (AS-IMPL-024, Phase H lands
#:   the actual handler).
#: * JWKS endpoint — every component fetches the public key set
#:   anonymously to verify call-contexts locally; requiring a
#:   call-context here would be a chicken-and-egg deadlock.
#: * OpenAPI / docs endpoints — the spec is a public artefact used by
#:   client codegen, gateways, and external operators; gating it
#:   behind a call-context would mean nothing could discover the
#:   service surface without already speaking the call-context
#:   protocol.
_BYPASS_PATHS: frozenset[str] = frozenset(
    {
        "/healthz",
        "/readyz",
        "/v1/auth/verify",
        "/v1/auth/login/oidc/callback",
        "/v1/authz/verify-and-authorize",
        "/.well-known/jwks.json",
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
    }
)

logger = logging.getLogger(__name__)


class DevShimDisabledInProductionError(RuntimeError):
    """Raised when the call-context dev shim is activated in production.

    Production deployments must set ``CUSTOS_AUTH_CALLCTX_VERIFIER_URL``
    to a non-empty JWKS URL. Refusing to start (rather than silently
    accepting unsigned headers) is the design's guard against shipping a
    debug mode by accident.
    """


class CallContextError(Exception):
    """Authorization failure rendered through the shared error envelope.

    Both the middleware (which returns a :class:`JSONResponse` directly
    because Starlette would otherwise convert middleware-raised
    exceptions to a generic 500) and the FastAPI dependencies
    (:func:`get_call_context`, :func:`require_permission`, etc.) channel
    every 4xx auth failure through this type so HTTP clients see one
    shape for a given logical failure mode regardless of where it was
    detected.

    Pair this with :func:`call_context_error_handler` on the FastAPI
    application; :func:`custos_auth.create_app` registers it
    automatically.
    """

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


class CallContext(BaseModel):
    """Per-request authorization context attached to ``request.state``.

    ``workspace_id`` / ``tenant_id`` are optional because auth-service
    has platform-global endpoints (e.g. ``POST /v1/tenants``) as well as
    workspace-scoped ones. Endpoints that require a scope assertion
    enforce it via :func:`require_workspace_membership` /
    :func:`require_tenant_scope` rather than expressing it as a model
    invariant.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    principal_id: str = Field(..., min_length=1)
    tenant_id: str | None = None
    workspace_id: str | None = None
    permissions: frozenset[str] = Field(default_factory=frozenset)
    issued_at: int | None = Field(default=None, alias="iat")
    expires_at: int | None = Field(default=None, alias="exp")

    def has_permission(self, name: str) -> bool:
        return name in self.permissions

    def has_any_permission(self, *names: str) -> bool:
        return any(n in self.permissions for n in names)


def _render_envelope(status_code: int, code: str, detail: str) -> JSONResponse:
    """Single source of truth for the call-context error envelope."""
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


def _error(status_code: int, code: str, detail: str) -> JSONResponse:
    return _render_envelope(status_code, code, detail)


async def call_context_error_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """FastAPI exception handler that renders :class:`CallContextError`.

    Registered by :func:`custos_auth.create_app`; consumers mounting
    :class:`CallContextMiddleware` directly (e.g. tests) must also
    register this handler against :class:`CallContextError` so the
    dependency-side 4xx responses match the middleware envelope.

    The signature accepts ``Exception`` so it matches
    :meth:`FastAPI.add_exception_handler` without an explicit ``# type:
    ignore``; the implementation narrows back to
    :class:`CallContextError` for the attribute access.
    """
    assert isinstance(exc, CallContextError)
    return _render_envelope(exc.status_code, exc.code, exc.detail)


class CallContextMiddleware(BaseHTTPMiddleware):
    """Extracts and validates the call-context header on every request.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        verifier_url: Value of ``CUSTOS_AUTH_CALLCTX_VERIFIER_URL``.
            Empty enables the dev shim; non-empty surfaces a 500 with
            :class:`NotImplementedError` until Phase G (AS-IMPL-018/019)
            wires the real JWKS-based verifier path.
        environment: Value of ``ENVIRONMENT``. ``"production"`` with an
            empty ``verifier_url`` raises
            :class:`DevShimDisabledInProductionError` at construction.

    Raises:
        DevShimDisabledInProductionError: When constructed with the dev
            shim active in a production environment.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        verifier_url: str,
        environment: str,
    ) -> None:
        super().__init__(app)
        self._verifier_url = verifier_url
        self._dev_shim_active = verifier_url == ""
        if self._dev_shim_active and environment.lower() == "production":
            raise DevShimDisabledInProductionError(
                "CUSTOS_AUTH_CALLCTX_VERIFIER_URL is empty but "
                "ENVIRONMENT=production; the call-context dev shim is "
                "forbidden in production. Set "
                "CUSTOS_AUTH_CALLCTX_VERIFIER_URL to auth-service's "
                "JWKS URL (Phase G AS-IMPL-018) or run a non-production "
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
            return _error(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")

        if not self._dev_shim_active:
            # Phase G AS-IMPL-019 will replace this raise with the real
            # call to the JWKS verifier helper: sig-verify the JWT,
            # parse claims, populate request.state.call_context.
            raise NotImplementedError(
                "Production call-context verification is not yet "
                "implemented; tracked by AS-IMPL-019. Unset "
                "CUSTOS_AUTH_CALLCTX_VERIFIER_URL to fall back to the "
                "dev shim in non-production environments."
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
            "(principal=%s tenant=%s workspace=%s) — set "
            "CUSTOS_AUTH_CALLCTX_VERIFIER_URL to disable",
            request.method,
            request.url.path,
            ctx.principal_id,
            ctx.tenant_id,
            ctx.workspace_id,
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
    :func:`call_context_error_handler`) when the middleware was not run
    — e.g. on a route mounted outside the middleware stack — so
    handlers always observe a populated context on a 2xx path AND so
    the wire response shape matches the middleware-emitted envelope.
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
    *names: str,
) -> Callable[[Request], Awaitable[CallContext]]:
    """Build a FastAPI dependency that requires AT LEAST ONE of ``names``.

    Multiple permission names express the design's "or" semantics: e.g.
    ``GET /v1/tenants`` requires ``platform.admin`` **or** ``tenant.admin``.
    Pass a single name for a strict single-permission check.
    """
    if not names:
        raise ValueError("require_permission needs at least one permission name")

    async def _dep(request: Request) -> CallContext:
        ctx = await get_call_context(request)
        if not ctx.has_any_permission(*names):
            if len(names) == 1:
                detail = f"missing required permission: {names[0]}"
            else:
                detail = f"missing required permission: one of {', '.join(names)}"
            raise CallContextError(403, "permission_denied", detail)
        return ctx

    _dep.__name__ = f"require_permission[{','.join(names)}]"
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
