"""Memory parity for receiver progress; shared state is not production restart durability."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from custos_spl.errors import LeaseExpired, QueryUnsupported
from custos_spl.interfaces.receiver_progress import (
    INACTIVE_PROGRESS_RETENTION,
    DiscoveryScan,
    ReceiverScan,
    ScanClaim,
    ScanContinuation,
    ScanOperation,
    require_scan_claim,
    scan_delay,
    scan_duration,
    scan_identity,
    validate_discovery_page,
)
from custos_spl.interfaces.scheduler import SchedulerLease, require_scheduler_lease, utc_instant
from custos_spl.pagination import Cursor, query_page_limit


@dataclass
class _Discovery:
    """One memory discovery row; registration and position change together."""

    incarnation: str = field(default_factory=lambda: uuid4().hex)
    generation: int = 0
    revision: int = 0
    cursor: Cursor | None = None
    sweep: int = 1
    completed: int = 0
    claim: ScanClaim | None = None


@dataclass
class _Progress:
    """Provider-owned state, never returned as a mutable record to workers."""

    continuation: ScanContinuation
    ticket: int
    eligible: datetime
    last_seen: int
    updated: datetime
    incarnation: str = field(default_factory=lambda: uuid4().hex)
    generation: int = 0
    revision: int = 0
    claim: ScanClaim | None = None


@dataclass
class MemoryReceiverScanData:
    """State container that may be shared across test provider instances."""

    discovery: dict[str, _Discovery] = field(default_factory=dict)
    progress: dict[tuple[str, str, ScanOperation], _Progress] = field(default_factory=dict)
    ticket: int = 0

    def next_ticket(self) -> int:
        """Allocate never-reused service order, equivalent to a database sequence."""
        self.ticket += 1
        return self.ticket


class MemoryReceiverScanProgress:
    """Atomic no-await operations with injected storage time and scheduler authority."""

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
        data: MemoryReceiverScanData | None = None,
        scheduler_lookup: Callable[[str], SchedulerLease | None] | None = None,
    ) -> None:
        """Bind existing state for replica tests without inventing scheduler authority."""
        self._scan_now = now if now is not None else lambda: datetime.now(UTC)
        self._scan_data = data if data is not None else MemoryReceiverScanData()
        self._scan_scheduler = scheduler_lookup

    async def check_receiver_scan_progress_ready(self) -> None:
        """Memory is ready at construction; durable production guarantees require PostgreSQL."""

    async def claim_receiver_discovery(
        self,
        deployment_id: str,
        holder: str,
        *,
        lease_seconds: int = 30,
    ) -> DiscoveryScan | None:
        """Acquire discovery without resetting a live claim or committed cursor."""
        scan_identity(deployment_id)
        scan_identity(holder)
        duration = scan_duration(lease_seconds)
        now = utc_instant(self._scan_now())
        row = self._scan_data.discovery.setdefault(deployment_id, _Discovery())
        if row.claim is not None and row.claim.expires_at > now:
            return None
        row.generation += 1
        row.claim = ScanClaim(
            deployment_id,
            None,
            None,
            row.incarnation,
            row.generation,
            uuid4().hex,
            holder,
            now + duration,
            row.revision,
        )
        return DiscoveryScan(row.claim, row.cursor, row.sweep, row.completed)

    async def commit_receiver_discovery(
        self,
        scan: DiscoveryScan,
        workspaces: Sequence[str],
        next_cursor: Cursor | None,
    ) -> None:
        """Register all four operations and advance a fenced discovery page atomically."""
        now = utc_instant(self._scan_now())
        row = self._scan_data.discovery.get(scan.claim.deployment_id)
        require_scan_claim(row.claim if row else None, scan.claim, now)
        assert row is not None
        if (scan.cursor, scan.sweep, scan.completed_sweep) != (
            row.cursor,
            row.sweep,
            row.completed,
        ):
            raise LeaseExpired("receiver discovery position changed")
        validate_discovery_page(workspaces, row.cursor, next_cursor)
        for workspace in workspaces:
            for operation in ScanOperation:
                key = (scan.claim.deployment_id, workspace, operation)
                progress = self._scan_data.progress.get(key)
                if progress is None:
                    self._scan_data.progress[key] = _Progress(
                        ScanContinuation(operation),
                        self._scan_data.next_ticket(),
                        now,
                        row.sweep,
                        now,
                    )
                else:
                    progress.last_seen = row.sweep
        row.cursor = next_cursor
        row.revision += 1
        row.claim = None
        if next_cursor is None:
            row.completed = row.sweep
            row.sweep += 1

    async def claim_receiver_scan(
        self,
        deployment_id: str,
        operation: ScanOperation,
        holder: str,
        *,
        lease_seconds: int = 360,
        scheduler_lease: SchedulerLease | None = None,
    ) -> ReceiverScan | None:
        """Move the oldest eligible row to the tail before returning its immutable claim."""
        scan_identity(deployment_id)
        scan_identity(holder)
        if not isinstance(operation, ScanOperation):
            raise ValueError("invalid receiver operation")
        duration = scan_duration(lease_seconds)
        now = utc_instant(self._scan_now())
        if operation is ScanOperation.SCHEDULED:
            if self._scan_scheduler is None:
                raise QueryUnsupported("scheduled progress requires scheduler authority")
            if scheduler_lease is None or (
                scheduler_lease.deployment_id != deployment_id
                or scheduler_lease.holder_id != holder
            ):
                raise LeaseExpired("scheduled progress requires matching scheduler authority")
            require_scheduler_lease(self._scan_scheduler(deployment_id), scheduler_lease, now)
        eligible = [
            (key, row)
            for key, row in self._scan_data.progress.items()
            if key[0] == deployment_id
            and key[2] == operation
            and row.eligible <= now
            and (row.claim is None or row.claim.expires_at <= now)
        ]
        if not eligible:
            return None
        key, row = min(eligible, key=lambda pair: pair[1].ticket)
        row.generation += 1
        row.ticket = self._scan_data.next_ticket()
        row.claim = ScanClaim(
            deployment_id,
            key[1],
            operation,
            row.incarnation,
            row.generation,
            uuid4().hex,
            holder,
            now + duration,
            row.revision,
        )
        return ReceiverScan(row.claim, row.continuation, row.ticket)

    def _owned(self, scan: ReceiverScan, now: datetime) -> _Progress:
        """Check authoritative ownership independently of a caller's editable snapshot."""
        claim = scan.claim
        if claim.workspace_id is None or claim.operation is None:
            raise LeaseExpired("operation claim requires workspace and operation")
        row = self._scan_data.progress.get(
            (claim.deployment_id, claim.workspace_id, claim.operation)
        )
        require_scan_claim(row.claim if row else None, claim, now)
        assert row is not None
        return row

    async def checkpoint_receiver_scan(
        self,
        scan: ReceiverScan,
        continuation: ScanContinuation,
        *,
        interval_seconds: float = 1,
    ) -> None:
        """Commit validated next progress and release only the exact current owner."""
        delay = scan_delay(interval_seconds)
        continuation = ScanContinuation.from_json(continuation.to_json())
        now = utc_instant(self._scan_now())
        row = self._owned(scan, now)
        if continuation.operation != scan.claim.operation:
            raise ValueError("receiver continuation operation mismatch")
        row.continuation = continuation
        row.revision += 1
        row.claim = None
        row.eligible, row.updated = now + delay, now

    async def release_receiver_scan(self, scan: ReceiverScan) -> None:
        """Preserve provider-owned continuation; a failed page cannot submit new progress."""
        now = utc_instant(self._scan_now())
        row = self._owned(scan, now)
        row.claim = None
        row.eligible, row.updated = now + timedelta(seconds=5), now

    async def prune_receiver_scans(
        self,
        deployment_id: str,
        completed_sweep: int,
        *,
        limit: int = 50,
    ) -> int:
        """Prune only idle finished rows absent from a verified completed discovery sweep."""
        scan_identity(deployment_id)
        size = query_page_limit(limit)
        discovery = self._scan_data.discovery.get(deployment_id)
        if type(completed_sweep) is not int or completed_sweep < 1:
            raise ValueError("completed receiver sweep must be positive")
        if discovery is None or completed_sweep > discovery.completed:
            raise ValueError("receiver discovery sweep has not completed")
        now = utc_instant(self._scan_now())
        keys = [
            key
            for key, row in self._scan_data.progress.items()
            if key[0] == deployment_id
            and row.last_seen < completed_sweep
            and row.continuation.cursor is None
            and row.updated <= now - INACTIVE_PROGRESS_RETENTION
            and (row.claim is None or row.claim.expires_at <= now)
        ][:size]
        for key in keys:
            del self._scan_data.progress[key]
        return len(keys)
