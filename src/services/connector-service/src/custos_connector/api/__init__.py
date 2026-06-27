"""Public REST surface for the Connector Service (CONN-IMPL-026).

This package wires every operator-facing route from
``design/components/connector-service/design.md`` § *Public Interface*
into FastAPI. The individual handlers come from earlier phases
(:class:`~custos_connector.instances.service.InstanceService` for
instance CRUD + lifecycle, :class:`~custos_connector.lease.service.LeaseManager`
for lease admin, :class:`~custos_connector.loader.registry.ConnectorTypeRegistry`
for connector-type listing, and the metadata store for audit queries);
this layer is the route table glue + permission enforcement + workspace-
mismatch guard.

The internal RPC surface lives in sibling routers — ``binding``,
``lease``, ``listen``, plus the CONN-IMPL-027 (Phase J) routers
``validate`` and ``subscribe`` exported from here — and is mounted
by :mod:`custos_connector` alongside the public routers.

Module layout
-------------

* :mod:`._common` — shared workspace-mismatch guard, error envelope,
  and ``app.state.providers`` dependency resolvers.
* :mod:`.connector_types` — ``GET /v1/workspaces/{ws}/connector-types``.
* :mod:`.instances` — instance CRUD + ``:enable``, ``:disable``,
  ``/health``, ``:force-health-check``.
* :mod:`.lease_admin` — admin live-state and revoke endpoints.
* :mod:`.audit` — ``GET /v1/workspaces/{ws}/audit/leases``.
* :mod:`.validate` — ``POST /internal/v1/connectors:validate``
  (CONN-IMPL-027).
* :mod:`.subscribe` — ``POST /internal/v1/events:subscribe``
  (CONN-IMPL-027).
"""

from __future__ import annotations

from custos_connector.api.audit import router as audit_router
from custos_connector.api.connector_register import router as connector_register_router
from custos_connector.api.connector_types import router as connector_types_router
from custos_connector.api.instances import router as instances_router
from custos_connector.api.lease_admin import router as lease_admin_router
from custos_connector.api.subscribe import router as subscribe_router
from custos_connector.api.validate import router as validate_router

__all__ = [
    "audit_router",
    "connector_register_router",
    "connector_types_router",
    "instances_router",
    "lease_admin_router",
    "subscribe_router",
    "validate_router",
]
