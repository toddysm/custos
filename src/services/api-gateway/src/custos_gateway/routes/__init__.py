"""Declarative route registry for the Custos API Gateway.

The gateway has no domain routes of its own: its public surface is the union of
every downstream component's externally-facing REST contract, mounted under
``/v1/`` and threaded through the cross-cutting middleware. This package holds
that declarative contract (:mod:`custos_gateway.routes.registry`) and the router
factory that materializes it onto a FastAPI app.
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

__all__ = [
    "CATALOG_APP_ID",
    "CONNECTOR_APP_ID",
    "DOWNSTREAM_ROUTER_STATE_ATTR",
    "M1_ROUTE_REGISTRY",
    "OBSERVABILITY_APP_ID",
    "TRIGGER_APP_ID",
    "WORKFLOW_APP_ID",
    "RateLimitClass",
    "RouteSpec",
    "build_registry_router",
    "registry_required_permissions",
]
