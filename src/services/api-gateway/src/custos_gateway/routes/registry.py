"""M1 route registry for the Custos API Gateway (AGW-IMPL-013).

The gateway owns no domain logic: its public REST surface is the *union* of
every downstream component's externally-facing contract, mounted under ``/v1/``
(see ``design/components/api-gateway/design.md`` § "Public Interface" /
"Route registry (M1 contract set)"). This module makes that contract
**declarative**: :data:`M1_ROUTE_REGISTRY` is a frozen table of
:class:`RouteSpec` rows — one per external route — each carrying the four
cross-cutting attributes the design pins:

* ``required_permission`` — the Auth Service permission the route enforces. It
  is declared on the mounted route via
  :func:`custos_gateway.middleware.auth.require_permission`, so it participates
  in the startup registry cross-check (AGW-IMPL-008): a permission name that
  drifts from the Auth Service registry is a loud boot failure, not a per-request
  surprise.
* ``requires_idempotency_key`` — write endpoints default to ``True``.
* ``max_body_bytes`` — 1 MiB default; workflow/template *publish* routes are
  raised to 5 MiB (per :func:`custos_gateway.middleware.validate.is_publish_route`).
* ``rate_limit_class`` — ``write`` | ``read`` | ``webhook`` | ``auth``.

:func:`build_registry_router` materializes the table onto an
:class:`fastapi.APIRouter`: every route is mounted with its
``require_permission`` dependency and a thin pass-through endpoint that forwards
the request to the owning component over Dapr via the lifespan-owned
:class:`custos_gateway.router.DownstreamRouter` and shapes the reply.

Scope boundary (M1): this module declares the registry and the per-route
``require_permission`` + forwarding seam. The surrounding ASGI middleware stack
— CORS, correlation id, body/content-type validation, idempotency, rate limiting
and call-context minting — is assembled in request-pipeline order by
:func:`custos_gateway.app.create_app` (AGW-IMPL-016), which also binds the
:class:`~custos_gateway.router.DownstreamRouter` to ``app.state``.

The exact per-component routes are owned by each downstream component's design;
the registry mirrors their *external* surface only. Internal Dapr RPC routes
(``/rpc/*``, ``/internal/v1/*``), the gateway-owned webhook pass-through
(AGW-IMPL-014), the device-code flow (AGW-IMPL-015) and the anonymous
auth-bootstrap routes are intentionally **out** of this table. Auth-management
routes that authorize on an OR of two permissions, or on authentication only,
are likewise deferred — the gateway's single-permission ``require_permission``
cannot yet express those — and are tracked for a follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, cast

from fastapi import APIRouter, Depends, Request, Response

from custos_gateway._telemetry import (
    instrument_downstream,
    record_idempotency_replay,
    record_rate_limit_denial,
    request_telemetry,
)
from custos_gateway.clients.auth import AUTH_APP_ID
from custos_gateway.errors import GatewayError
from custos_gateway.middleware.auth import (
    AUTH_STATE_ATTR,
    AuthorizedCaller,
    require_permission,
)
from custos_gateway.middleware.callctx_mint import (
    OUTBOUND_METADATA_STATE_ATTR,
    mint_call_context,
)
from custos_gateway.middleware.idempotency import (
    IDEMPOTENCY_KEY_HEADER,
    IdempotencyCoordinator,
    IdempotencyKey,
    ReplayReservation,
    compute_request_hash,
    is_idempotent_method,
    resolve_idempotency_key,
)
from custos_gateway.middleware.ratelimit import (
    Allow,
    Deny,
    is_rate_limited_method,
    rate_limit_denied_error,
    rate_limit_headers,
)
from custos_gateway.middleware.validate import (
    classify_route,
    enforce_body_size,
    enforce_content_type,
    is_publish_route,
)
from custos_gateway.middleware.workspace import resolve_workspace
from custos_gateway.router import DownstreamCall
from custos_gateway.routes._forwarding import (
    DOWNSTREAM_ROUTER_STATE_ATTR,
    get_downstream_router,
    get_idempotency_store,
    get_rate_limiter,
    response_from_snapshot,
    response_snapshot,
    shaped_response,
)
from custos_gateway.settings import (
    DEFAULT_BODY_MAX_BYTES_DEFAULT,
    DEFAULT_BODY_MAX_BYTES_PUBLISH,
    Settings,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from custos_gateway.router import DownstreamResponse

__all__ = [
    "AUTH_APP_ID",
    "CATALOG_APP_ID",
    "CONNECTOR_APP_ID",
    "DOWNSTREAM_ROUTER_STATE_ATTR",
    "M1_ROUTE_REGISTRY",
    "OBSERVABILITY_APP_ID",
    "TRIGGER_APP_ID",
    "WORKFLOW_APP_ID",
    "WRITE_METHODS",
    "RateLimitClass",
    "RouteSpec",
    "build_registry_router",
    "registry_required_permissions",
]

#: Dapr app ids of the owning downstream components. ``AUTH_APP_ID`` is reused
#: from the Auth Service client so the two never drift; the rest mirror each
#: service's Helm chart name (``dapr.io/app-id``).
CATALOG_APP_ID: Final[str] = "catalog-service"
WORKFLOW_APP_ID: Final[str] = "workflow-service"
TRIGGER_APP_ID: Final[str] = "trigger-service"
CONNECTOR_APP_ID: Final[str] = "connector-service"
OBSERVABILITY_APP_ID: Final[str] = "observability-audit-service"

#: Methods that mutate state: they default to requiring an idempotency key and
#: to the ``write`` rate-limit class.
WRITE_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class RateLimitClass(StrEnum):
    """The rate-limit bucket a route is billed against (design § Public Interface)."""

    WRITE = "write"
    READ = "read"
    WEBHOOK = "webhook"
    AUTH = "auth"


@dataclass(frozen=True, slots=True)
class RouteSpec:
    """One external route in the gateway's M1 contract surface.

    ``path`` is the gateway-side FastAPI template; the workspace segment is named
    ``{workspaceId}`` so the workspace resolver can extract it. The request is
    forwarded verbatim to ``app_id`` over Dapr, so the template need only *match*
    the incoming path — the gateway never rewrites it.
    """

    method: str
    path: str
    app_id: str
    required_permission: str
    requires_idempotency_key: bool
    max_body_bytes: int
    rate_limit_class: RateLimitClass

    def __post_init__(self) -> None:
        if not self.method.isalpha() or self.method != self.method.upper():
            raise ValueError(f"method must be an upper-case HTTP verb, got {self.method!r}")
        if not self.path.startswith("/"):
            raise ValueError(f"path must be absolute, got {self.path!r}")
        if not self.app_id:
            raise ValueError("app_id must be a non-empty Dapr app id")
        if not self.required_permission:
            raise ValueError("required_permission must be a non-empty permission name")
        if self.max_body_bytes <= 0:
            raise ValueError(f"max_body_bytes must be positive, got {self.max_body_bytes}")


def _route(
    method: str,
    path: str,
    app_id: str,
    permission: str,
    *,
    requires_idempotency_key: bool | None = None,
    max_body_bytes: int | None = None,
    rate_limit_class: RateLimitClass | None = None,
) -> RouteSpec:
    """Build a :class:`RouteSpec`, deriving the cross-cutting defaults.

    The defaults encode the design rules so the table below stays readable:
    write methods require an idempotency key and bill the ``write`` bucket;
    reads bill ``read``; the body cap is 1 MiB unless the route is a
    workflow/template publish path, which is raised to 5 MiB.
    """
    is_write = method.upper() in WRITE_METHODS
    if requires_idempotency_key is None:
        requires_idempotency_key = is_write
    if rate_limit_class is None:
        rate_limit_class = RateLimitClass.WRITE if is_write else RateLimitClass.READ
    if max_body_bytes is None:
        max_body_bytes = (
            DEFAULT_BODY_MAX_BYTES_PUBLISH
            if is_write and is_publish_route(path)
            else DEFAULT_BODY_MAX_BYTES_DEFAULT
        )
    return RouteSpec(
        method=method.upper(),
        path=path,
        app_id=app_id,
        required_permission=permission,
        requires_idempotency_key=requires_idempotency_key,
        max_body_bytes=max_body_bytes,
        rate_limit_class=rate_limit_class,
    )


# ---------------------------------------------------------------------------
# The M1 contract set.
#
# Ordering matters only where a literal path could also match a sibling's path
# parameter: the connector ``/audit/leases`` route is registered *before* the
# observability ``/audit/{eventId}`` route so the literal wins. Everywhere else
# the templates are mutually exclusive and order is cosmetic (grouped by owner).
# ---------------------------------------------------------------------------
M1_ROUTE_REGISTRY: Final[tuple[RouteSpec, ...]] = (
    # --- Auth Service (custos-auth): single-permission management routes only.
    _route("POST", "/v1/service-accounts", AUTH_APP_ID, "admin:service-account"),
    _route(
        "POST",
        "/v1/service-accounts/{principalId}/tokens",
        AUTH_APP_ID,
        "admin:service-account",
    ),
    _route(
        "GET",
        "/v1/service-accounts/{principalId}/tokens",
        AUTH_APP_ID,
        "admin:service-account",
    ),
    _route("DELETE", "/v1/tokens/{tokenId}", AUTH_APP_ID, "admin:service-account"),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/role-bindings",
        AUTH_APP_ID,
        "admin:role-binding",
    ),
    _route(
        "DELETE",
        "/v1/workspaces/{workspaceId}/role-bindings/{bindingId}",
        AUTH_APP_ID,
        "admin:role-binding",
    ),
    # --- Catalog Service (catalog-service): authoring + registry reads.
    # Concrete routes mirror the Catalog Service contract exactly — the only
    # workflow/template/connector-type sub-resource writes are the explicit
    # action suffixes (``:deprecate`` / ``:extractTemplate`` / ``:materialize``),
    # not a catch-all ``POST /{ref}``.
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/workflows",
        CATALOG_APP_ID,
        "catalog:workflows:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/workflows/{nameOrRef}",
        CATALOG_APP_ID,
        "catalog:workflows:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/workflows/{ref}:deprecate",
        CATALOG_APP_ID,
        "catalog:workflows:write",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/workflows/{ref}:extractTemplate",
        CATALOG_APP_ID,
        "catalog:workflows:write",
    ),
    # Workspaceless get-by-id resolves a workflow version by its canonical
    # triple-encoded id; authorized as a platform-scoped catalog read.
    _route(
        "GET",
        "/v1/workflows/{workflowVersionId:path}",
        CATALOG_APP_ID,
        "catalog:workflows:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/templates",
        CATALOG_APP_ID,
        "catalog:templates:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/templates/{ref}",
        CATALOG_APP_ID,
        "catalog:templates:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/templates/{ref}:materialize",
        CATALOG_APP_ID,
        "catalog:templates:write",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/activity-types",
        CATALOG_APP_ID,
        "catalog:activity-types:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/activity-types",
        CATALOG_APP_ID,
        "catalog:activity-types:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/activity-types/{ref:path}",
        CATALOG_APP_ID,
        "catalog:activity-types:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/activity-types/{ref:path}:deprecate",
        CATALOG_APP_ID,
        "catalog:activity-types:write",
    ),
    _route(
        "POST",
        "/v1/catalog/connector-types",
        CATALOG_APP_ID,
        "catalog:connector-types:write",
    ),
    _route(
        "GET",
        "/v1/catalog/connector-types",
        CATALOG_APP_ID,
        "catalog:connector-types:read",
    ),
    _route(
        "GET",
        "/v1/catalog/connector-types/{ref}",
        CATALOG_APP_ID,
        "catalog:connector-types:read",
    ),
    _route(
        "POST",
        "/v1/catalog/connector-types/{ref}:deprecate",
        CATALOG_APP_ID,
        "catalog:connector-types:write",
    ),
    # --- Workflow Service (workflow-service): run lifecycle.
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/runs",
        WORKFLOW_APP_ID,
        "workflow:execute",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs",
        WORKFLOW_APP_ID,
        "run:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs/{runId}",
        WORKFLOW_APP_ID,
        "run:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/runs/{runId}:cancel",
        WORKFLOW_APP_ID,
        "run:cancel",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs/{runId}/steps/{stepId}",
        WORKFLOW_APP_ID,
        "run:read",
    ),
    # --- Trigger Service (trigger-service): subscription CRUD + manual fire.
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/triggers",
        TRIGGER_APP_ID,
        "trigger:subscriptions:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/triggers/{subscriptionId}",
        TRIGGER_APP_ID,
        "trigger:subscriptions:read",
    ),
    _route(
        "PATCH",
        "/v1/workspaces/{workspaceId}/triggers/{subscriptionId}",
        TRIGGER_APP_ID,
        "trigger:subscriptions:write",
    ),
    _route(
        "DELETE",
        "/v1/workspaces/{workspaceId}/triggers/{subscriptionId}",
        TRIGGER_APP_ID,
        "trigger:subscriptions:delete",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/triggers/{subscriptionId}:fire",
        TRIGGER_APP_ID,
        "trigger:subscriptions:fire",
    ),
    # --- Connector Service (connector-service): instance lifecycle + lease admin.
    # ``/audit/leases`` is declared before the observability ``/audit/{eventId}``
    # route so the literal segment wins the match.
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/connectors",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "PATCH",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}:enable",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}:disable",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/health",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}:force-health-check",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/leases",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/cursor",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/cursor:rewind",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/pull-loop:pause",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/pull-loop:resume",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/connectors/{connectorId}/leases:revoke-all",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs/{runId}/leases",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/runs/{runId}/leases:revoke-all",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/leases/{leaseId}:revoke",
        CONNECTOR_APP_ID,
        "admin:connector",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/connector-types",
        CONNECTOR_APP_ID,
        "connector:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/audit/leases",
        CONNECTOR_APP_ID,
        "audit:read",
    ),
    # --- Observability & Audit (observability-audit-service): logs/metrics/audit.
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs/{runId}/logs/tail",
        OBSERVABILITY_APP_ID,
        "logs:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs/{runId}/logs",
        OBSERVABILITY_APP_ID,
        "logs:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/runs/{runId}/metrics",
        OBSERVABILITY_APP_ID,
        "metrics:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/audit",
        OBSERVABILITY_APP_ID,
        "audit:read",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/audit/{eventId}",
        OBSERVABILITY_APP_ID,
        "audit:read",
    ),
)


def registry_required_permissions() -> frozenset[str]:
    """Return the distinct permission names the registry declares.

    This is the set the startup cross-check (AGW-IMPL-008) validates against the
    Auth Service permission registry, and the set a table-driven test pins.
    """
    return frozenset(spec.required_permission for spec in M1_ROUTE_REGISTRY)


def _make_forwarder(spec: RouteSpec) -> Callable[[Request], Awaitable[Response]]:
    """Build the ingress endpoint that runs the write/read pipeline for ``spec``.

    The pipeline mirrors the design's ingress order *after* the route-level
    ``resolve_workspace`` → ``require_permission`` → ``mint_call_context``
    dependencies have run: body-size and content-type validation, per-principal/
    workspace rate limiting, write-path idempotency reserve/replay/complete, the
    Dapr forward (carrying the call-context the mint dependency staged), and
    response shaping. Each stage is skipped when it does not apply to the route
    (reads bypass rate limiting/idempotency) or when its backing resource is
    unbound (no limiter or metadata store wired), so the forwarder degrades to a
    plain pass-through in those configurations.
    """

    async def _forward(request: Request) -> Response:
        with request_telemetry(method=spec.method, route=spec.path) as telemetry:
            telemetry.set_correlation_id(getattr(request.state, "correlation_id", None))
            # The authorize dependency chain (resolve_workspace → require_permission
            # → mint_call_context) has already bound the caller to request.state, so
            # stamp the span's caller ids first: a validation GatewayError below
            # still produces an error span carrying the full attribute set.
            caller = cast("AuthorizedCaller", getattr(request.state, AUTH_STATE_ATTR))
            telemetry.set_caller(
                workspace_id=caller.workspace_id,
                principal_id=caller.principal_id,
                decision_audit_event_id=caller.audit_event_id,
            )
            try:
                body = await request.body()
                enforce_body_size(len(body), spec.max_body_bytes)
                enforce_content_type(
                    method=request.method,
                    content_type=request.headers.get("content-type"),
                    route_class=classify_route(request.url.path),
                )
                allow = _apply_rate_limit(request, caller, spec)

                coordinator, key = _idempotency_context(request, spec, caller)
                if coordinator is not None and key is not None:
                    outcome = await coordinator.reserve(
                        key, _request_hash(request, spec, caller, body)
                    )
                    if isinstance(outcome, ReplayReservation):
                        record_idempotency_replay(route=spec.path, method=spec.method)
                        response = _with_rate_limit_headers(
                            response_from_snapshot(outcome.response_snapshot), allow
                        )
                        telemetry.set_status(response.status_code)
                        return response

                reply = await _invoke_downstream(request, spec, body)
                if coordinator is not None and key is not None:
                    await coordinator.complete(key, response_snapshot(reply))
                response = _with_rate_limit_headers(shaped_response(reply), allow)
                telemetry.set_status(response.status_code)
                return response
            except GatewayError as exc:
                telemetry.set_status(exc.status)
                raise

    return _forward


def _apply_rate_limit(request: Request, caller: AuthorizedCaller, spec: RouteSpec) -> Allow | None:
    """Charge the principal/workspace token buckets, or skip when not applicable.

    Returns the :class:`Allow` decision (whose headers are surfaced on the
    response) for a rate-limited write when a limiter is bound, ``None`` when the
    method is exempt or no limiter is wired, and raises ``429 rate-limited`` on a
    :class:`Deny` after recording the denial in the rate-limit counter.
    """
    if not is_rate_limited_method(request.method):
        return None
    limiter = get_rate_limiter(request)
    if limiter is None:
        return None
    decision = limiter.check(principal_id=caller.principal_id, workspace_id=caller.workspace_id)
    if isinstance(decision, Deny):
        record_rate_limit_denial(route=spec.path, method=spec.method)
        raise rate_limit_denied_error(decision)
    return decision


def _idempotency_context(
    request: Request, spec: RouteSpec, caller: AuthorizedCaller
) -> tuple[IdempotencyCoordinator | None, IdempotencyKey | None]:
    """Build the coordinator/key pair for a write route, or ``(None, None)``.

    Idempotency only engages on routes that require an idempotency key, for
    idempotent (write) methods, and when a metadata store is bound; otherwise the
    forwarder forwards without reserving.
    """
    if not (spec.requires_idempotency_key and is_idempotent_method(request.method)):
        return None, None
    store = get_idempotency_store(request)
    if store is None:
        return None, None
    settings = cast("Settings", request.app.state.settings)
    key = IdempotencyKey(
        workspace_id=caller.workspace_id,
        principal_id=caller.principal_id,
        route=spec.path,
        idempotency_key=resolve_idempotency_key(request.headers.get(IDEMPOTENCY_KEY_HEADER)),
    )
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=settings.idempotency_ttl_seconds)
    return coordinator, key


def _request_hash(request: Request, spec: RouteSpec, caller: AuthorizedCaller, body: bytes) -> str:
    """Fingerprint the request for idempotency key-reuse detection."""
    return compute_request_hash(
        method=request.method,
        route=spec.path,
        workspace_id=caller.workspace_id,
        headers=request.headers,
        body=body,
    )


async def _invoke_downstream(request: Request, spec: RouteSpec, body: bytes) -> DownstreamResponse:
    """Forward the minted request to ``spec.app_id`` over Dapr.

    The forwarded headers carry the outbound metadata staged by the call-context
    minter plus the inbound ``Content-Type``; the original path and query string
    are preserved. The router masks server-side failures as
    ``503 downstream-unavailable``.
    """
    downstream = get_downstream_router(request)
    headers: dict[str, str] = {}
    outbound = getattr(request.state, OUTBOUND_METADATA_STATE_ATTR, None)
    if outbound:
        headers.update(outbound)
    content_type = request.headers.get("content-type")
    if content_type is not None:
        headers["content-type"] = content_type
    method_path = request.url.path.lstrip("/")
    if request.url.query:
        method_path = f"{method_path}?{request.url.query}"
    with instrument_downstream(app_id=spec.app_id):
        return await downstream.invoke(
            DownstreamCall(
                app_id=spec.app_id,
                http_method=request.method,
                method_path=method_path,
                headers=headers,
                body=body or None,
            )
        )


def _with_rate_limit_headers(response: Response, allow: Allow | None) -> Response:
    """Attach the rate-limit budget headers to ``response`` when one applies."""
    if allow is not None:
        for name, value in rate_limit_headers(allow).items():
            response.headers[name] = value
    return response


def build_registry_router() -> APIRouter:
    """Materialize :data:`M1_ROUTE_REGISTRY` onto a FastAPI router.

    Every route is mounted with its ``resolve_workspace`` → ``require_permission``
    → ``mint_call_context`` dependency chain — so the workspace is resolved before
    authorization, the declared permission participates in the startup registry
    check, and a signed call-context is staged for the forward — plus the pipeline
    endpoint that forwards to the owning component.
    """
    router = APIRouter()
    for spec in M1_ROUTE_REGISTRY:
        router.add_api_route(
            spec.path,
            _make_forwarder(spec),
            methods=[spec.method],
            dependencies=[
                Depends(resolve_workspace),
                Depends(require_permission(spec.required_permission)),
                Depends(mint_call_context),
            ],
            name=f"{spec.method.lower()}:{spec.path}",
        )
    return router
