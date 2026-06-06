"""Declarative route registry for the Custos API Gateway.

The gateway has no domain routes of its own: its public surface is the union of
every downstream component's externally-facing REST contract, mounted under
``/v1/`` and threaded through the cross-cutting middleware. This package holds
that declarative contract (:mod:`custos_gateway.routes.registry`), the anonymous
webhook pass-through (:mod:`custos_gateway.routes.webhook`), and the router
factories that materialize them onto a FastAPI app.
"""

from __future__ import annotations

from custos_gateway.routes.registry import (
    CATALOG_APP_ID,
    CONNECTOR_APP_ID,
    DOWNSTREAM_ROUTER_STATE_ATTR,
    M1_ROUTE_REGISTRY,
    OBSERVABILITY_APP_ID,
    TRIGGER_APP_ID,
    WORKFLOW_APP_ID,
    RateLimitClass,
    RouteSpec,
    build_registry_router,
    registry_required_permissions,
)
from custos_gateway.routes.webhook import (
    WEBHOOK_BODY_MAX_BYTES,
    WEBHOOK_PATH,
    build_webhook_router,
)

__all__ = [
    "CATALOG_APP_ID",
    "CONNECTOR_APP_ID",
    "DOWNSTREAM_ROUTER_STATE_ATTR",
    "M1_ROUTE_REGISTRY",
    "OBSERVABILITY_APP_ID",
    "TRIGGER_APP_ID",
    "WEBHOOK_BODY_MAX_BYTES",
    "WEBHOOK_PATH",
    "WORKFLOW_APP_ID",
    "RateLimitClass",
    "RouteSpec",
    "build_registry_router",
    "build_webhook_router",
    "registry_required_permissions",
]
