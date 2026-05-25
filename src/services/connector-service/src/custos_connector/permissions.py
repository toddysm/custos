"""Connector Service permission names (CONN-IMPL-004).

The design § Operator Admin Surface → Permission model declares three
permission names that Phase J routes will enforce via
:func:`custos_connector.middleware.require_permission`:

* ``connector:read`` — All ``GET`` endpoints for live state (leases,
  cursor, health).
* ``audit:read`` — ``GET .../audit/leases`` and other audit-query endpoints.
* ``admin:connector`` — All revoke / revoke-all / pause / resume /
  force-health-check / rewind / enable / disable endpoints.

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

#: The full set of permissions the design declares for this service. Useful
#: for tests that build an "admin" call-context header carrying everything.
ALL_PERMISSIONS: Final[tuple[str, ...]] = (
    ADMIN_CONNECTOR,
    AUDIT_READ,
    CONNECTOR_READ,
)


__all__ = [
    "ADMIN_CONNECTOR",
    "ALL_PERMISSIONS",
    "AUDIT_READ",
    "CONNECTOR_READ",
]
