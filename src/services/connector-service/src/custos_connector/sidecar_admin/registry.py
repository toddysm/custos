"""Sidecar lookup registry for operator-driven revoke fan-out (CONN-IMPL-028).

Maps a ``lease_id`` to the control-channel base URL of the sidecar
that minted it. Populated by ARM as it starts and stops sidecars in
production; populated by the test harness in unit tests so the
operator-revoke fan-out path can be exercised end-to-end.

For the M1 cut without ARM in-cluster the registry is typically
empty: :meth:`SidecarRegistry.endpoint_for` returns ``None`` for
every lease and the operator route falls back to the DB-only
terminal-revoke path. This keeps single-node development working
while the production ARM integration lands in CONN-IMPL-029.

The registry is intentionally lease-scoped (not connector-instance-
scoped) because the sidecar lives per ``(runId, stepId, attempt)``
and may hold leases from multiple connector instances. Indexing on
``lease_id`` keeps the lookup O(1) and matches the persistence
boundary the lease manager already uses.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SidecarRegistry(Protocol):
    """Lookup contract for ``lease_id → sidecar control-channel URL``.

    Implementations MUST be safe to call from multiple coroutines
    concurrently (the operator routes do not serialize calls).

    The ``endpoint`` returned is the **base URL** of the sidecar's
    control-channel HTTPS listener (port 9443 by convention). The
    :class:`~custos_connector.sidecar_admin.client.SidecarAdminClient`
    appends ``/sidecar-admin/v1/revoke`` when dispatching.
    """

    def endpoint_for(self, lease_id: str) -> str | None:
        """Return the sidecar base URL for ``lease_id`` or ``None``.

        ``None`` means "no sidecar currently owns this lease" — the
        caller should fall back to the DB-only terminal-revoke path
        without raising. Production ARM populates entries on sidecar
        start and removes them on sidecar stop.
        """
        ...


class InMemorySidecarRegistry:
    """In-memory :class:`SidecarRegistry` for tests and the M1 cut.

    Production ARM (CONN-IMPL-029) will populate this registry as
    sidecars start and stop; for now it is also the production
    registry, just typically empty until ARM lands.

    The instance is shared across coroutines via the
    :class:`~custos_connector.providers.Providers` bundle, so
    ``register`` / ``unregister`` must be safe under interleaved
    coroutine execution. Python's GIL plus the dict's atomic
    single-key writes are sufficient — no explicit lock is needed
    for these single-operation mutations.
    """

    def __init__(self) -> None:
        self._endpoints: dict[str, str] = {}

    def register(self, lease_id: str, endpoint: str) -> None:
        """Associate ``lease_id`` with the sidecar control-channel URL.

        Re-registering an existing ``lease_id`` overwrites the
        previous endpoint silently; sidecars never reissue leases, so
        a duplicate registration in production indicates ARM is
        reassigning the lease to a new sidecar after a restart.
        """
        self._endpoints[lease_id] = endpoint

    def unregister(self, lease_id: str) -> None:
        """Remove ``lease_id`` from the registry. Idempotent."""
        self._endpoints.pop(lease_id, None)

    def endpoint_for(self, lease_id: str) -> str | None:
        return self._endpoints.get(lease_id)


__all__ = ["InMemorySidecarRegistry", "SidecarRegistry"]
