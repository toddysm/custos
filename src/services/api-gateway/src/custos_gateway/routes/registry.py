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

from custos_gateway.clients.auth import AUTH_APP_ID
from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.auth import require_permission
from custos_gateway.middleware.callctx_mint import OUTBOUND_METADATA_STATE_ATTR
from custos_gateway.middleware.validate import is_publish_route
from custos_gateway.router import DownstreamCall, DownstreamRouter
from custos_gateway.settings import (
    DEFAULT_BODY_MAX_BYTES_DEFAULT,
    DEFAULT_BODY_MAX_BYTES_PUBLISH,
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

#: ``app.state`` attribute holding the lifespan-owned
#: :class:`~custos_gateway.router.DownstreamRouter`. Bound by
#: :func:`custos_gateway.app.create_app` (AGW-IMPL-016).
DOWNSTREAM_ROUTER_STATE_ATTR: Final[str] = "downstream_router"

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
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/workflows",
        CATALOG_APP_ID,
        "catalog:workflows:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/workflows",
        CATALOG_APP_ID,
        "catalog:workflows:read",
    ),
    _route(
        "POST",
        "/v1/workspaces/{workspaceId}/workflows/{ref:path}",
        CATALOG_APP_ID,
        "catalog:workflows:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/workflows/{ref:path}",
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
        "POST",
        "/v1/workspaces/{workspaceId}/templates/{ref:path}",
        CATALOG_APP_ID,
        "catalog:templates:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/templates/{ref:path}",
        CATALOG_APP_ID,
        "catalog:templates:read",
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
        "POST",
        "/v1/workspaces/{workspaceId}/activity-types/{ref:path}",
        CATALOG_APP_ID,
        "catalog:activity-types:write",
    ),
    _route(
        "GET",
        "/v1/workspaces/{workspaceId}/activity-types/{ref:path}",
        CATALOG_APP_ID,
        "catalog:activity-types:read",
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
        "POST",
        "/v1/catalog/connector-types/{ref:path}",
        CATALOG_APP_ID,
        "catalog:connector-types:write",
    ),
    _route(
        "GET",
        "/v1/catalog/connector-types/{ref:path}",
        CATALOG_APP_ID,
        "catalog:connector-types:read",
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


def _downstream_router(request: Request) -> DownstreamRouter:
    """Return the lifespan-owned downstream router, or fail with 503.

    The router is bound to ``app.state`` by the application factory
    (AGW-IMPL-016); its absence means the gateway is not ready to forward.
    """
    router = getattr(request.app.state, DOWNSTREAM_ROUTER_STATE_ATTR, None)
    if router is None:
        raise GatewayError(
            GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
            detail="The gateway is not ready to forward requests.",
        )
    return cast(DownstreamRouter, router)


def _shaped_response(reply: DownstreamResponse) -> Response:
    """Wrap a shaped downstream reply in a Starlette response.

    The downstream headers are already hop-by-hop-stripped and preserve repeated
    headers (e.g. ``Set-Cookie``); they are copied verbatim and ``content-length``
    is recomputed from the forwarded body.
    """
    response = Response(content=reply.body, status_code=reply.status_code)
    raw: list[tuple[bytes, bytes]] = [
        (name.encode("latin-1"), value.encode("latin-1")) for name, value in reply.headers
    ]
    raw.append((b"content-length", str(len(reply.body)).encode("latin-1")))
    response.raw_headers[:] = raw
    return response


def _make_forwarder(app_id: str) -> Callable[[Request], Awaitable[Response]]:
    """Build the pass-through endpoint that forwards a request to ``app_id``.

    The forwarded headers carry the outbound metadata staged by the call-context
    minter (when present) plus the inbound ``Content-Type``; the body is streamed
    through unmodified. The downstream reply is returned raw (the router masks
    server-side failures as ``503 downstream-unavailable``).
    """

    async def _forward(request: Request) -> Response:
        downstream = _downstream_router(request)
        body = await request.body()
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
        reply = await downstream.invoke(
            DownstreamCall(
                app_id=app_id,
                http_method=request.method,
                method_path=method_path,
                headers=headers,
                body=body or None,
            )
        )
        return _shaped_response(reply)

    return _forward


def build_registry_router() -> APIRouter:
    """Materialize :data:`M1_ROUTE_REGISTRY` onto a FastAPI router.

    Every route is mounted with its ``require_permission`` dependency — so its
    declared permission participates in the startup registry check — and a thin
    pass-through endpoint forwarding to the owning component.
    """
    router = APIRouter()
    for spec in M1_ROUTE_REGISTRY:
        router.add_api_route(
            spec.path,
            _make_forwarder(spec.app_id),
            methods=[spec.method],
            dependencies=[Depends(require_permission(spec.required_permission))],
            name=f"{spec.method.lower()}:{spec.path}",
        )
    return router
