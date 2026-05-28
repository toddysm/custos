"""Operator-driven sidecar control-channel fan-out (CONN-IMPL-028).

The operator REST surface in :mod:`custos_connector.api.lease_admin`
revokes leases by:

1. Emitting ``lease.revoke-requested`` (one event per operator call).
2. Recording the terminal-revoke state in the SPL lease store via
   :meth:`~custos_connector.lease.service.LeaseManager.revoke_with_status`
   (which emits ``lease.revoked`` per affected lease).
3. **Best-effort** signalling each affected sidecar's
   ``POST /sidecar-admin/v1/revoke`` (port 9443, mTLS) so the
   sidecar's local revocation registry stops serving the lease to
   activities before the DB revoke would naturally propagate.

This module owns step 3. The Connector Service DB remains the
authoritative source of truth for lease state; sidecar fan-out is a
signalling optimization to short-circuit in-flight activity reads.
Transport errors and the sidecar's terminal-shutdown ``503`` are
both treated as "fan-out failed but DB is correct" — they are
logged and not surfaced to the operator response.

For the M1 cut without ARM in-cluster, the :class:`SidecarRegistry`
typically has zero entries; the fan-out becomes a no-op while the
DB-side revoke remains the single point of enforcement. When ARM
lands (CONN-IMPL-029 / Phase K), ARM populates the registry as it
starts and stops sidecars.
"""

from __future__ import annotations

from custos_connector.sidecar_admin.client import (
    SidecarAdminClient,
    SidecarRevokeAck,
)
from custos_connector.sidecar_admin.registry import (
    InMemorySidecarRegistry,
    SidecarRegistry,
)

__all__ = [
    "InMemorySidecarRegistry",
    "SidecarAdminClient",
    "SidecarRegistry",
    "SidecarRevokeAck",
]
