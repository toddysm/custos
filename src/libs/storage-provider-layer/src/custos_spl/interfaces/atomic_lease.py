"""Optional cross-replica lease admission required by production credential issuance."""

from datetime import datetime
from typing import Protocol, runtime_checkable

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.lease_store import Lease


@runtime_checkable
class AtomicLeaseStoreProvider(Protocol):
    """Reserve lease capacity and insert the lease in one database transaction."""

    async def put_lease_bounded(self, workspace_id: WorkspaceId, lease: Lease, limit: int) -> Lease:
        """Insert or raise LeaseBusy at capacity; never race count-then-put across replicas."""
        ...

    async def refresh_lease_bounded(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
        expires_at: datetime,
        limit: int,
    ) -> Lease | None:
        """Serialize with inserts; never revive an expired, released or revoked lease."""
        ...
