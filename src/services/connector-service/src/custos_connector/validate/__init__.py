"""ValidateConnector internal RPC (CONN-IMPL-027, Phase J).

Thin wrapper around
:func:`custos_connector.instances.validator.validate_instance_config`
that exposes two preflight surfaces over a service-to-service RPC so
sibling services do not have to re-implement the connector-manifest
validation rules:

* **Instance mode** — Catalog Service and Workflow Service hand in
  an existing ``ConnectorInstance`` ID plus the capabilities the
  caller intends to exercise. The service looks up the instance,
  re-resolves its pinned ``ConnectorTypeVersion`` manifest from the
  catalog store, and re-runs the validator. This catches the
  "manifest drift after activation" case the Phase G design § §
  Drift Posture spells out, and answers the binding-time precheck
  Workflow Service runs ahead of ``BindForStep``.

* **Manifest mode** — operator-supplied ``type`` + ``version`` plus
  the proposed ``targetConfig`` / ``credentialsAuthentication`` /
  ``usedCapabilities`` bag. No persistence write. Catalog Service
  drives this on the operator-facing "test before save" surface so
  the same validator answers a UI preflight.

Both modes converge on a single response shape so the caller does
not branch on mode:

* ``{ "ok": true }`` on success.
* ``{ "error": { "code", "detail", "issues": [...] } }`` with HTTP
  400 + ``connector.instance_config_invalid`` (same envelope as
  :func:`custos_connector.api.instances._validation_error_response`)
  on validation failure.

The internal RPC is gated by
:data:`custos_connector.permissions.CONNECTOR_VALIDATE` and is
workspace-scoped via the call context: instance mode uses the
workspace from the call context to scope the lookup, while manifest
mode also reads the workspace from the call context (the catalog
itself is platform-wide, but the audit log entry the future audit
hook lands under is workspace-bound).
"""

from __future__ import annotations

from custos_connector.validate.service import (
    ValidateConnectorService,
    ValidateInstanceRequest,
    ValidateManifestRequest,
    ValidateRequest,
    ValidateResult,
)

__all__ = [
    "ValidateConnectorService",
    "ValidateInstanceRequest",
    "ValidateManifestRequest",
    "ValidateRequest",
    "ValidateResult",
]
