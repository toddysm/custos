"""Workspace-scoped connector instance management.

This package owns :class:`InstanceService` and its typed errors. The
service composes three SPL providers:

* :class:`custos_spl.ConnectorInstanceStoreProvider` — instance rows.
* :class:`custos_spl.CatalogStoreProvider` — type/version existence
  check at create-time.
* :class:`custos_spl.MetadataStoreProvider` — audit outbox (typed
  helpers in :mod:`custos_connector.audit`).

Activation, health probing, and lease leasing are intentionally
live in this package's service layer. Lease leasing and sidecar token
loops remain outside this module and land in later phases.
"""

from __future__ import annotations

from custos_connector.instances.service import (
    ActivationProbeFailed,
    ConnectorInstanceNotFound,
    ConnectorTypeNotRegistered,
    ImmutableFieldUpdate,
    InstanceHealthSnapshot,
    InstanceService,
    InstanceServiceError,
    InvalidInstancePayload,
    InvalidLeaseTtl,
)
from custos_connector.instances.validator import (
    InstanceConfigCode,
    InstanceConfigIssue,
    InstanceConfigValidationError,
    validate_instance_config,
)

__all__ = [
    "ActivationProbeFailed",
    "ConnectorInstanceNotFound",
    "ConnectorTypeNotRegistered",
    "ImmutableFieldUpdate",
    "InstanceConfigCode",
    "InstanceConfigIssue",
    "InstanceConfigValidationError",
    "InstanceHealthSnapshot",
    "InstanceService",
    "InstanceServiceError",
    "InvalidInstancePayload",
    "InvalidLeaseTtl",
    "validate_instance_config",
]
