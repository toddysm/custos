"""Transactional receiver discovery/checkpoints composed into PgMetadataAdapter.

Scheduler authority is locked before scheduled progress. Discovery and cleanup
lock the discovery row before progress rows. No transaction spans an external
effect; claims never substitute for dispatch or scheduler authorization.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from asyncpg import UndefinedTableError
from custos_spl.errors import LeaseExpired, MigrationRequired
from custos_spl.interfaces.receiver_progress import (
    RECEIVER_PROGRESS_SCHEMA_REVISION,
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
from custos_spl.interfaces.scheduler import SchedulerLease, require_scheduler_lease
from custos_spl.pagination import Cursor, query_page_limit

if TYPE_CHECKING:
    from asyncpg import Connection, Record


async def _now(conn: Connection) -> datetime:
    """Read database time after acquiring any contended row lock."""
    value: datetime = await conn.fetchval("SELECT clock_timestamp()")
    return value


def _claim(row: Record, *, discovery: bool = False) -> ScanClaim | None:
    """Decode authoritative ownership without assuming a released row is claimed."""
    if row["token"] is None:
        return None
    return ScanClaim(
        row["deployment_id"],
        None if discovery else row["workspace_id"],
        None if discovery else ScanOperation(row["operation"]),
        row["incarnation"],
        row["generation"],
        row["token"],
        row["holder"],
        row["expires_at"],
        row["revision"],
    )


def _receiver(row: Record) -> ReceiverScan:
    """Reject malformed persisted continuation rather than returning an empty scan."""
    claim = _claim(row)
    if claim is None:
        raise LeaseExpired("receiver progress row is not claimed")
    continuation = ScanContinuation.from_json(row["continuation"])
    if continuation.operation != claim.operation:
        raise ValueError("stored receiver continuation operation mismatch")
    return ReceiverScan(claim, continuation, row["service_ticket"])


class PgReceiverScanProgress:
    """Optional metadata capability using the parent adapter's owned transaction/error boundary."""

    def _delivery_transaction(self) -> AbstractAsyncContextManager[Connection]:
        """Supplied by PgMetadataAdapter; standalone instances cannot access a database."""
        raise NotImplementedError("receiver progress requires a metadata transaction")

    async def check_receiver_scan_progress_ready(self) -> None:
        """Require revision16 explicitly; do not auto-migrate or silently downgrade."""
        async with self._delivery_transaction() as conn:
            try:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM custos_meta.adapter_revisions "
                    "WHERE interface_name='MetadataStoreProvider' AND revision=$1)",
                    RECEIVER_PROGRESS_SCHEMA_REVISION,
                )
            except UndefinedTableError as exc:
                raise MigrationRequired(
                    [("MetadataStoreProvider", RECEIVER_PROGRESS_SCHEMA_REVISION)]
                ) from exc
            if not exists:
                raise MigrationRequired(
                    [("MetadataStoreProvider", RECEIVER_PROGRESS_SCHEMA_REVISION)]
                )

    async def claim_receiver_discovery(
        self,
        deployment_id: str,
        holder: str,
        *,
        lease_seconds: int = 30,
    ) -> DiscoveryScan | None:
        """Serialize discovery claims; preserve cursor/sweep on expiry and replica restart."""
        scan_identity(deployment_id)
        scan_identity(holder)
        duration = scan_duration(lease_seconds)
        async with self._delivery_transaction() as conn:
            await conn.execute(
                "INSERT INTO custos_state.receiver_discovery(deployment_id,incarnation) "
                "VALUES($1,$2) ON CONFLICT DO NOTHING",
                deployment_id,
                uuid4().hex,
            )
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.receiver_discovery WHERE deployment_id=$1 FOR UPDATE",
                deployment_id,
            )
            assert row is not None
            now = await _now(conn)
            if row["expires_at"] is not None and row["expires_at"] > now:
                return None
            row = await conn.fetchrow(
                "UPDATE custos_state.receiver_discovery SET holder=$2,token=$3,"
                "generation=generation+1,expires_at=$4 WHERE deployment_id=$1 RETURNING *",
                deployment_id,
                holder,
                uuid4().hex,
                now + duration,
            )
            assert row is not None
            claim = _claim(row, discovery=True)
            assert claim is not None
            return DiscoveryScan(
                claim,
                Cursor(row["cursor"]) if row["cursor"] else None,
                row["sweep"],
                row["completed_sweep"],
            )

    async def commit_receiver_discovery(
        self,
        scan: DiscoveryScan,
        workspaces: Sequence[str],
        next_cursor: Cursor | None,
    ) -> None:
        """Register a bounded page and advance discovery in the same fenced transaction."""
        if len(workspaces) > 50:
            raise ValueError("receiver discovery page exceeds 50 IDs")
        workspaces = tuple(workspaces)
        claim = scan.claim
        if claim.operation is not None or claim.workspace_id is not None:
            raise LeaseExpired("discovery claim has operation scope")
        validate_discovery_page(workspaces, scan.cursor, next_cursor)
        async with self._delivery_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.receiver_discovery WHERE deployment_id=$1 FOR UPDATE",
                claim.deployment_id,
            )
            now = await _now(conn)
            require_scan_claim(_claim(row, discovery=True) if row else None, claim, now)
            assert row is not None
            if (scan.cursor.token if scan.cursor else None, scan.sweep, scan.completed_sweep) != (
                row["cursor"],
                row["sweep"],
                row["completed_sweep"],
            ):
                raise LeaseExpired("receiver discovery position changed")
            for workspace in workspaces:
                for operation in ScanOperation:
                    await conn.execute(
                        "INSERT INTO custos_state.receiver_progress "
                        "(deployment_id,workspace_id,operation,incarnation,continuation,"
                        "next_eligible_at,last_seen_sweep,last_seen_at,updated_at) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7,$6,$6) "
                        "ON CONFLICT(deployment_id,workspace_id,operation) DO UPDATE "
                        "SET last_seen_sweep=EXCLUDED.last_seen_sweep,"
                        "last_seen_at=EXCLUDED.last_seen_at",
                        claim.deployment_id,
                        workspace,
                        operation.value,
                        uuid4().hex,
                        ScanContinuation(operation).to_json(),
                        now,
                        row["sweep"],
                    )
            updated = await conn.fetchval(
                "UPDATE custos_state.receiver_discovery SET cursor=$2,revision=revision+1,"
                "completed_sweep=CASE WHEN $2::text IS NULL THEN sweep ELSE completed_sweep END,"
                "sweep=sweep+CASE WHEN $2::text IS NULL THEN 1 ELSE 0 END,"
                "holder=NULL,token=NULL,expires_at=NULL "
                "WHERE deployment_id=$1 AND expires_at>clock_timestamp() RETURNING revision",
                claim.deployment_id,
                next_cursor.token if next_cursor else None,
            )
            if updated is None:
                raise LeaseExpired("receiver discovery expired before checkpoint")

    async def claim_receiver_scan(
        self,
        deployment_id: str,
        operation: ScanOperation,
        holder: str,
        *,
        lease_seconds: int = 360,
        scheduler_lease: SchedulerLease | None = None,
    ) -> ReceiverScan | None:
        """Claim one fair row; validate scheduled leadership before any queue mutation."""
        scan_identity(deployment_id)
        scan_identity(holder)
        if not isinstance(operation, ScanOperation):
            raise ValueError("invalid receiver operation")
        duration = scan_duration(lease_seconds)
        async with self._delivery_transaction() as conn:
            authority_expires: datetime | None = None
            if operation is ScanOperation.SCHEDULED:
                if scheduler_lease is None or (
                    scheduler_lease.deployment_id != deployment_id
                    or scheduler_lease.holder_id != holder
                ):
                    raise LeaseExpired("scheduled progress requires matching scheduler authority")
                authority = await conn.fetchrow(
                    "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id=$1 FOR UPDATE",
                    deployment_id,
                )
                now = await _now(conn)
                current = (
                    SchedulerLease(
                        deployment_id,
                        authority["holder_id"],
                        authority["epoch"],
                        authority["expires_at"],
                        now,
                    )
                    if authority
                    else None
                )
                require_scheduler_lease(current, scheduler_lease, now)
                assert current is not None
                authority_expires = current.expires_at
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.receiver_progress "
                "WHERE deployment_id=$1 AND operation=$2 AND next_eligible_at<=clock_timestamp() "
                "AND (expires_at IS NULL OR expires_at<=clock_timestamp()) "
                "ORDER BY service_ticket LIMIT 1 FOR UPDATE SKIP LOCKED",
                deployment_id,
                operation.value,
            )
            if row is None:
                return None
            now = await _now(conn)
            if operation is ScanOperation.SCHEDULED:
                assert scheduler_lease is not None
                require_scheduler_lease(current, scheduler_lease, now)
            ScanContinuation.from_json(row["continuation"])
            row = await conn.fetchrow(
                "UPDATE custos_state.receiver_progress SET holder=$4,token=$5,"
                "generation=generation+1,expires_at=$6,"
                "service_ticket=nextval('custos_state.receiver_service_ticket') "
                "WHERE deployment_id=$1 AND workspace_id=$2 AND operation=$3 "
                "AND ($7::timestamptz IS NULL OR $7>clock_timestamp()) RETURNING *",
                deployment_id,
                row["workspace_id"],
                operation.value,
                holder,
                uuid4().hex,
                now + duration,
                authority_expires,
            )
            if row is None:
                raise LeaseExpired("scheduler authority expired before progress claim")
            return _receiver(row)

    async def _finish(
        self,
        scan: ReceiverScan,
        continuation: ScanContinuation | None,
        seconds: float,
    ) -> None:
        """Lock and fence the exact owner; failure release never accepts caller progress."""
        delay = scan_delay(seconds)
        claim = scan.claim
        if claim.workspace_id is None or claim.operation is None:
            raise LeaseExpired("operation claim requires workspace and operation")
        if continuation is not None:
            continuation = ScanContinuation.from_json(continuation.to_json())
            if continuation.operation != claim.operation:
                raise ValueError("receiver continuation operation mismatch")
        async with self._delivery_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.receiver_progress WHERE deployment_id=$1 "
                "AND workspace_id=$2 AND operation=$3 FOR UPDATE",
                claim.deployment_id,
                claim.workspace_id,
                claim.operation.value,
            )
            now = await _now(conn)
            require_scan_claim(_claim(row) if row else None, claim, now)
            updated = await conn.fetchval(
                "UPDATE custos_state.receiver_progress SET "
                "continuation=COALESCE($4,continuation),revision=revision+$5,"
                "holder=NULL,token=NULL,expires_at=NULL,next_eligible_at=$6,updated_at=$7 "
                "WHERE deployment_id=$1 AND workspace_id=$2 AND operation=$3 "
                "AND expires_at>clock_timestamp() RETURNING revision",
                claim.deployment_id,
                claim.workspace_id,
                claim.operation.value,
                continuation.to_json() if continuation else None,
                int(continuation is not None),
                now + delay,
                now,
            )
            if updated is None:
                raise LeaseExpired("receiver progress expired before checkpoint")

    async def checkpoint_receiver_scan(
        self,
        scan: ReceiverScan,
        continuation: ScanContinuation,
        *,
        interval_seconds: float = 1,
    ) -> None:
        """Checkpoint the acknowledged next page with exact revision/claim fencing."""
        await self._finish(scan, continuation, interval_seconds)

    async def release_receiver_scan(self, scan: ReceiverScan) -> None:
        """Retain the stored position and enforce the five-second failure retry floor."""
        await self._finish(scan, None, 5)

    async def prune_receiver_scans(
        self,
        deployment_id: str,
        completed_sweep: int,
        *,
        limit: int = 50,
    ) -> int:
        """Serialize absence proof with discovery; prune finished idle rows in bounded batches."""
        scan_identity(deployment_id)
        size = query_page_limit(limit)
        if type(completed_sweep) is not int or completed_sweep < 1:
            raise ValueError("completed receiver sweep must be positive")
        async with self._delivery_transaction() as conn:
            completed = await conn.fetchval(
                "SELECT completed_sweep FROM custos_state.receiver_discovery "
                "WHERE deployment_id=$1 FOR UPDATE",
                deployment_id,
            )
            if completed is None or completed < completed_sweep:
                raise ValueError("receiver discovery sweep has not completed")
            rows = await conn.fetch(
                "SELECT * FROM custos_state.receiver_progress WHERE deployment_id=$1 "
                "AND last_seen_sweep<$2 AND continuation::jsonb->>'cursor' IS NULL "
                "AND updated_at<=clock_timestamp()-interval '30 days' "
                "AND (expires_at IS NULL OR expires_at<=clock_timestamp()) "
                "ORDER BY updated_at,workspace_id,operation LIMIT $3 FOR UPDATE SKIP LOCKED",
                deployment_id,
                completed_sweep,
                size,
            )
            for row in rows:
                ScanContinuation.from_json(row["continuation"])
                await conn.execute(
                    "DELETE FROM custos_state.receiver_progress WHERE deployment_id=$1 "
                    "AND workspace_id=$2 AND operation=$3 AND incarnation=$4",
                    deployment_id,
                    row["workspace_id"],
                    row["operation"],
                    row["incarnation"],
                )
            return len(rows)
