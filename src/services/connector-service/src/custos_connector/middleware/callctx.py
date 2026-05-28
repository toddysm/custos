"""Call-context middleware + dev shim (CONN-IMPL-004).

The API Gateway design (COMP-001) carries a signed call-context JWT in the
``x-custos-callctx`` header on every internal request. Connector Service
parses it, validates it, and exposes the result on
``request.state.call_context`` so route handlers can authorize via
:func:`get_call_context` and :func:`require_permission`.

The full validation path lives behind ``CONN_AUTHZ_ENDPOINT``; until the
Auth Service real-verifier wiring lands (mirrored from CS-IMPL-024 #225)
the middleware raises :class:`NotImplementedError` whenever
``CONN_AUTHZ_ENDPOINT`` is set. With the env var left empty the middleware
activates a **dev shim** that parses an unsigned JSON header so the rest
of Phases B-K can proceed; the shim refuses to start when ``ENVIRONMENT``
is ``production`` (case-insensitive).

Audit hook
----------

Every dev-shim request emits an ``auth.callctx.shim_used`` event and every
``require_permission`` decision emits an ``authz.decision`` event through
:func:`custos_connector.audit.emit_event`. The real audit pipeline lands
in CONN-IMPL-029; the stub here just logs at INFO so the operator can see
the dev shim is active and test fixtures can assert via ``caplog``.
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

from custos_connector.audit import audit_authz_decision, emit_event

if TYPE_CHECKING:
    from starlette.types import ASGIApp

#: Wire header carrying the call-context document.
#: API Gateway design (COMP-001) pins the canonical lowercase form.
CALLCTX_HEADER: str = "x-custos-callctx"

#: Paths the middleware deliberately bypasses (no auth on probes,
#: nor on the OpenAPI documentation surface — the schema is part of
#: the service's public-by-design REST contract per CONN-IMPL-026).
#: ``/metrics`` is the Prometheus exposition endpoint mounted by
#: CONN-IMPL-029; it MUST NOT require a call-context header so the
#: Helm-deployed Prometheus scraper can reach it without minting
#: an internal token. (Per the Phase K design the network policy
#: restricts the scrape to the in-cluster Prometheus pod; that is
#: the trust boundary, not the call-context middleware.)
_BYPASS_PATHS: frozenset[str] = frozenset(
    {"/healthz", "/readyz", "/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}
)

#: Path prefixes the middleware bypasses because they authenticate
#: externally (e.g. push webhook receivers verifying an HMAC signature
#: on the request body, not a call-context JWT). The push receiver
#: mounted at ``POST /v1/webhooks/connectors/{instance_id}/events``
#: (CONN-IMPL-025) enters the listen pipeline with its own
#: :class:`custos_connector.listen.signature.SignatureVerifier` and
#: explicitly does not have an internal call-context. The ``/metrics``
#: prefix covers the Prometheus mount whose internal path is
#: ``/metrics`` plus any trailing slash variant.
_BYPASS_PREFIXES: tuple[str, ...] = ("/v1/webhooks/", "/metrics")

logger = logging.getLogger(__name__)


class DevShimDisabledInProductionError(RuntimeError):
    """Raised when the call-context dev shim is activated in production.

    Production deployments must set ``CONN_AUTHZ_ENDPOINT`` to a non-empty
    URL. Refusing to start (rather than silently accepting unsigned
    headers) is the design's guard against shipping a debug mode by
    accident.
    """


class CallContextError(Exception):
    """Authorization failure rendered through the shared error envelope.

    Both the middleware (which returns a :class:`JSONResponse` directly
    because Starlette would otherwise convert middleware-raised
    exceptions to a generic 500) and the FastAPI dependencies
    (:func:`get_call_context` and :func:`require_permission`) channel
    every 4xx auth failure through this type so HTTP clients see one
    shape for a given logical failure mode regardless of where it was
    detected.

    Pair this with :func:`call_context_error_handler` on the FastAPI
    application; :func:`custos_connector.create_app` registers it
    automatically.
    """

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


class CallContext(BaseModel):
    """Per-request authorization context attached to ``request.state``.

    Mirrors the catalog-service dev-shim shape so the cross-service test
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

    def has_permission(self, name: str) -> bool:
        return name in self.permissions


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

    Registered by :func:`custos_connector.create_app`; consumers mounting
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
        authz_endpoint: Value of ``CONN_AUTHZ_ENDPOINT``. Empty enables
            the dev shim; non-empty surfaces a 500 with
            :class:`NotImplementedError` until the real-verifier wiring
            lands (mirroring catalog-service CS-IMPL-024 #225).
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
                "CONN_AUTHZ_ENDPOINT is empty but ENVIRONMENT=production; "
                "the call-context dev shim is forbidden in production. "
                "Set CONN_AUTHZ_ENDPOINT to the Auth Service URL or run a "
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
            return _error(401, "callctx_missing", f"{CALLCTX_HEADER} header is required")

        if not self._dev_shim_active:
            # The real-verifier follow-up (mirroring catalog-service
            # CS-IMPL-024 #225) will replace this raise with the real call
            # to the Auth Service: sig-verify the JWT, parse claims, call
            # /authorize.
            raise NotImplementedError(
                "Production call-context validation is not yet implemented "
                "for connector-service (mirrors catalog-service CS-IMPL-024 "
                "#225). Unset CONN_AUTHZ_ENDPOINT to fall back to the dev "
                "shim in non-production environments."
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
            "(workspace=%s principal=%s) — set CONN_AUTHZ_ENDPOINT to disable",
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
    name: str,
) -> Callable[[Request], Awaitable[CallContext]]:
    """Build a FastAPI dependency that requires ``name`` on the call context.

    Every authorization decision (allow or deny) emits an
    ``authz.decision`` audit event so the operator can trace exactly
    which permission gated which route. CONN-IMPL-029 (Phase K)
    promoted this emission onto the typed SPL audit pipeline: when the
    FastAPI application's ``Providers`` are reachable from
    ``request.app.state``, the dependency writes through
    :func:`audit_authz_decision`; the legacy log-only
    :func:`emit_event` line still fires alongside so dev / test paths
    without a metadata store keep their structured log signal.
    """

    async def _dep(request: Request) -> CallContext:
        ctx = await get_call_context(request)
        allowed = ctx.has_permission(name)
        emit_event(
            "authz.decision",
            {
                "path": request.url.path,
                "method": request.method,
                "permission": name,
                "allowed": allowed,
                "workspace_id": ctx.workspace_id,
                "principal_id": ctx.principal_id,
            },
        )
        providers = getattr(request.app.state, "providers", None)
        metadata_store = getattr(providers, "metadata_store", None) if providers else None
        if metadata_store is not None:
            await audit_authz_decision(
                metadata_store,
                workspace_id=ctx.workspace_id,
                actor=ctx.principal_id,
                principal_id=ctx.principal_id,
                path=request.url.path,
                method=request.method,
                permission=name,
                allowed=allowed,
            )
        if not allowed:
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
