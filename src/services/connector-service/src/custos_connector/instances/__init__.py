"""Workspace-scoped connector instance management.

This package owns :class:`InstanceService` and its typed errors. The
service composes three SPL providers:

* :class:`custos_spl.ConnectorInstanceStoreProvider` — instance rows.
* :class:`custos_spl.CatalogStoreProvider` — type/version existence
  check at create-time.
* :class:`custos_spl.MetadataStoreProvider` — audit outbox (typed
  helpers in :mod:`custos_connector.audit`).

Activation, health probing, and lease leasing are intentionally
**not** in this module — they ship in CONN-IMPL-013 (activation
controller) and CONN-IMPL-027 (sidecar lease loop). What lives here
is the CRUD foundation those tickets build on.
"""

from __future__ import annotations

from custos_connector.instances.service import (
    ConnectorInstanceNotFound,
    ConnectorTypeNotRegistered,
    ImmutableFieldUpdate,
    InstanceService,
    InstanceServiceError,
    InvalidInstancePayload,
    InvalidLeaseTtl,
)

__all__ = [
    "ConnectorInstanceNotFound",
    "ConnectorTypeNotRegistered",
    "ImmutableFieldUpdate",
    "InstanceService",
    "InstanceServiceError",
    "InvalidInstancePayload",
    "InvalidLeaseTtl",
]
