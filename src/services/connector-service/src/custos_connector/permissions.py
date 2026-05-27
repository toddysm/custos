"""Connector Service permission names (CONN-IMPL-004).

The design § Operator Admin Surface → Permission model declares three
permission names that Phase J routes will enforce via
:func:`custos_connector.middleware.require_permission`:

* ``connector:read`` — All ``GET`` endpoints for live state (leases,
  cursor, health).
* ``audit:read`` — ``GET .../audit/leases`` and other audit-query endpoints.
* ``admin:connector`` — All revoke / revoke-all / pause / resume /
  force-health-check / rewind / enable / disable endpoints.

Phase G adds one further name for the internal RPC surface:

* ``connector:bind`` — ``POST /internal/v1/bind-for-step`` (CONN-IMPL-016).
  Held by the Workflow Service's service identity; never granted to
  human operators.

Phase H adds a second internal-RPC permission:

* ``connector:lease-mint`` — ``POST /internal/v1/leases:issue``,
  ``:refresh``, ``:release`` (CONN-IMPL-019). Held by the
  secret-bridge sidecar's service identity; never granted to human
  operators. The sidecar uses these RPCs to delegate the
  capacity-tracked lease bookkeeping (and audit emission) to the
  Connector Service while it mints upstream credentials locally.

Phase J (CONN-IMPL-027) adds two more internal-RPC permissions:

* ``connector:validate`` — ``POST /internal/v1/connectors:validate``.
  Held by Catalog Service and Workflow Service for the preflight
  capability + config validation surface; never granted to human
  operators.
* ``events:subscribe`` — ``POST /internal/v1/events:subscribe``.
  Held by the Trigger Service so it can discover the Dapr Pub/Sub
  subscription metadata (component name, topic, filter spec) for
  ``custos.connector.events``; never granted to human operators.

Connector Service enforces by *name only*. The role hierarchy and binding
rules belong to the Auth Service (COMP-002); this module is the single
source of truth for the permission strings so the names appearing in
route decorators and test fixtures cannot drift.
"""

from __future__ import annotations

from typing import Final

#: All ``GET`` endpoints for live state (leases, cursor, health).
CONNECTOR_READ: Final[str] = "connector:read"

#: Audit-query endpoints (``GET .../audit/leases`` and similar).
AUDIT_READ: Final[str] = "audit:read"

#: Operator admin actions (revoke, revoke-all, pause, resume,
#: force-health-check, rewind, enable, disable).
ADMIN_CONNECTOR: Final[str] = "admin:connector"

#: ``POST /internal/v1/bind-for-step`` (CONN-IMPL-016). Held by the
#: Workflow Service's service identity; never granted to human
#: operators.
CONNECTOR_BIND: Final[str] = "connector:bind"

#: ``POST /internal/v1/leases:issue|refresh|release`` (CONN-IMPL-019).
#: Held by the secret-bridge sidecar's service identity; never granted
#: to human operators.
CONNECTOR_LEASE_MINT: Final[str] = "connector:lease-mint"

#: ``POST /internal/v1/connectors:validate`` (CONN-IMPL-027). Held by
#: Catalog Service and Workflow Service for the preflight capability +
#: config validation surface; never granted to human operators.
CONNECTOR_VALIDATE: Final[str] = "connector:validate"

#: ``POST /internal/v1/events:subscribe`` (CONN-IMPL-027). Held by the
#: Trigger Service so it can discover the Dapr Pub/Sub subscription
#: metadata for ``custos.connector.events``; never granted to human
#: operators.
EVENTS_SUBSCRIBE: Final[str] = "events:subscribe"

#: The full set of permissions the design declares for this service. Useful
#: for tests that build an "admin" call-context header carrying everything.
ALL_PERMISSIONS: Final[tuple[str, ...]] = (
    ADMIN_CONNECTOR,
    AUDIT_READ,
    CONNECTOR_BIND,
    CONNECTOR_LEASE_MINT,
    CONNECTOR_READ,
    CONNECTOR_VALIDATE,
    EVENTS_SUBSCRIBE,
)


__all__ = [
    "ADMIN_CONNECTOR",
    "ALL_PERMISSIONS",
    "AUDIT_READ",
    "CONNECTOR_BIND",
    "CONNECTOR_LEASE_MINT",
    "CONNECTOR_READ",
    "CONNECTOR_VALIDATE",
    "EVENTS_SUBSCRIBE",
]
