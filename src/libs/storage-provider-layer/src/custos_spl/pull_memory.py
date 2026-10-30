"""Memory parity for durable pull policy; storage is shared across workers, not restart durable."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, MutableMapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

from custos_spl.errors import ImmutableViolation, LeaseBusy, QueryUnsupported
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.dead_letters import validate_dispatch_identifier
from custos_spl.interfaces.metadata_store import AuditEvent, ConnectorCursor
from custos_spl.interfaces.pull_control import (
    MAX_PULL_LEASE_SECONDS,
    ConnectorPullControl,
    PullLease,
    collect_pull_demand,
    effective_pull_interval,
    pull_control_keys,
    pull_deadline,
    pull_interval,
    recover_pull_invocation,
    require_pull_invocation,
    require_pull_lease,
    require_pull_ready,
    validate_pause_audit,
)
from custos_spl.interfaces.scheduler import utc_instant
from custos_spl.pagination import (
    Cursor,
    Page,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
)

#: Providers project active starts and unexpired waits synchronously from one memory snapshot.
PullDemandReader = Callable[[WorkspaceId, datetime], tuple[Iterable[object], Iterable[object]]]


class MemoryPullControl:
    """Atomic no-await memory operations over shared controls, cursors and audit rows.

    A caller may bind existing cursor/audit containers so the legacy metadata
    reads see the same state. The demand reader must project only the requested
    workspace's active starts and unexpired pull waits.
    """

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
        cursor_rows: MutableMapping[tuple[str, str], ConnectorCursor] | None = None,
        audit_rows: list[tuple[str, AuditEvent]] | None = None,
        demand_reader: PullDemandReader | None = None,
    ) -> None:
        """Bind owned or existing state without treating missing demand support as no demand."""
        self._pull_now = now if now is not None else lambda: datetime.now(UTC)
        self._pull_controls: dict[tuple[str, str], ConnectorPullControl] = {}
        self._pull_cursor_rows = cursor_rows if cursor_rows is not None else {}
        self._pull_audit_rows = audit_rows if audit_rows is not None else []
        self._pull_demand_reader = demand_reader

    async def check_pull_control_ready(self) -> None:
        """Memory is ready at construction; this does not assert production restart durability."""

    async def pull_now(self) -> datetime:
        """Use the shared storage clock rather than a worker's local scheduling clock."""
        return utc_instant(self._pull_now())

    async def get_pull_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorPullControl | None:
        """Return only the requested immutable workspace/instance policy."""
        validate_dispatch_identifier(workspace_id)
        validate_dispatch_identifier(instance_id)
        return self._pull_controls.get((workspace_id, instance_id))

    def _new_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        actor: str,
        paused: bool,
    ) -> ConnectorPullControl:
        """Construct an initial policy with authoritative memory time and no implied demand."""
        now = utc_instant(self._pull_now())
        return ConnectorPullControl(
            workspace_id,
            instance_id,
            paused,
            1,
            now,
            now,
            actor,
        )

    async def read_pull_controls(
        self,
        workspace_id: WorkspaceId,
        instance_ids: Sequence[ConnectorInstanceId],
    ) -> tuple[ConnectorPullControl, ...]:
        """Return one current scoped batch, including inactive policy tombstones."""
        identifiers = pull_control_keys(workspace_id, instance_ids)
        return tuple(
            self._pull_controls[(workspace_id, identifier)]
            for identifier in identifiers
            if (workspace_id, identifier) in self._pull_controls
        )

    async def initialize_pull_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        actor: str,
    ) -> ConnectorPullControl:
        """Initialize a server-generated new identity without clearing any existing pause."""
        proposed = self._new_control(workspace_id, instance_id, actor=actor, paused=False)
        return self._pull_controls.setdefault((workspace_id, instance_id), proposed)

    async def set_pull_paused(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        paused: bool,
        audit: AuditEvent,
    ) -> ConnectorPullControl:
        """Validate everything, then publish audit and intent without an intervening await."""
        validate_pause_audit(workspace_id, instance_id, paused, audit)
        if any(
            ws == workspace_id and event.event_id == audit.event_id
            for ws, event in self._pull_audit_rows
        ):
            raise ImmutableViolation("pull-control audit already exists")
        captured = replace(
            audit,
            subject=json.loads(json.dumps(dict(audit.subject), allow_nan=False)),
            payload=json.loads(json.dumps(dict(audit.payload), allow_nan=False)),
        )
        key = (workspace_id, instance_id)
        previous = self._pull_controls.get(key)
        current = previous or self._new_control(
            workspace_id, instance_id, actor=audit.actor, paused=True
        )
        updated = replace(
            current,
            operator_paused=paused,
            revision=current.revision + 1,
            updated_at=utc_instant(self._pull_now()),
            updated_by=audit.actor,
            consecutive_failures=current.consecutive_failures if paused else 0,
        )
        self._pull_audit_rows.append((workspace_id, captured))
        self._pull_controls[key] = updated
        return updated

    async def reconcile_pull_demand(
        self,
        workspace_id: WorkspaceId,
        *,
        default_interval_seconds: int,
        minimum_interval_seconds: int,
    ) -> None:
        """Apply one current demand snapshot without clearing pause or interval reservations."""
        validate_dispatch_identifier(workspace_id)
        if self._pull_demand_reader is None:
            raise QueryUnsupported("pull demand projection is unavailable")
        now = utc_instant(self._pull_now())
        starts, resumes = self._pull_demand_reader(workspace_id, now)
        demand = collect_pull_demand(
            starts,
            resumes,
            default_interval_seconds=default_interval_seconds,
            minimum_interval_seconds=minimum_interval_seconds,
        )
        prepared: dict[tuple[str, str], ConnectorPullControl] = {}
        identifiers = set(demand) | {
            instance for (workspace, instance) in self._pull_controls if workspace == workspace_id
        }
        for identifier in identifiers:
            key = (workspace_id, identifier)
            current = self._pull_controls.get(key)
            if current is None:
                current = self._new_control(
                    workspace_id,
                    ConnectorInstanceId(identifier),
                    actor="trigger-service",
                    paused=True,
                )
            interval = demand.get(identifier)
            native = (
                max(current.native_interval_seconds, minimum_interval_seconds)
                if current.native_interval_seconds is not None
                else None
            )
            if (
                current.trigger_interval_seconds != interval
                or current.native_interval_seconds != native
            ):
                current = replace(
                    current,
                    trigger_interval_seconds=interval,
                    native_interval_seconds=native,
                    effective_interval_seconds=effective_pull_interval(native, interval),
                    revision=current.revision + 1,
                    updated_at=now,
                    updated_by="trigger-service",
                )
            prepared[key] = current
        self._pull_controls.update(prepared)

    async def set_pull_native_demand(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        interval_seconds: int | None,
        actor: str,
    ) -> ConnectorPullControl:
        """Share native listening with Trigger demand while preserving operator authority."""
        if interval_seconds is not None:
            pull_interval(interval_seconds)
        key = (workspace_id, instance_id)
        current = self._pull_controls.get(key) or self._new_control(
            workspace_id, instance_id, actor=actor, paused=True
        )
        updated = replace(
            current,
            native_interval_seconds=interval_seconds,
            effective_interval_seconds=effective_pull_interval(
                interval_seconds, current.trigger_interval_seconds
            ),
            revision=current.revision + 1,
            updated_at=utc_instant(self._pull_now()),
            updated_by=actor,
        )
        self._pull_controls[key] = updated
        return updated

    async def query_pull_controls(
        self,
        workspace_id: WorkspaceId,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorPullControl]:
        """Use scoped identifier keysets with the same bounded paging contract as PostgreSQL."""
        validate_dispatch_identifier(workspace_id)
        size = query_page_limit(limit)
        scope = ("pull-controls", workspace_id)
        after = decode_query_cursor(cursor, scope)
        rows = sorted(
            (
                row
                for (workspace, _), row in self._pull_controls.items()
                if workspace == workspace_id
                and row.effective_interval_seconds is not None
                and (after is None or row.instance_id > after)
            ),
            key=lambda row: row.instance_id,
        )
        selected = rows[:size]
        return Page(
            selected,
            encode_query_cursor(scope, selected[-1].instance_id) if len(rows) > size else None,
        )

    async def acquire_pull_tick(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        holder_id: str,
        *,
        ttl_seconds: int,
    ) -> tuple[ConnectorCursor, PullLease]:
        """Atomically reserve due time and a fresh cursor token, even for the same holder."""
        for value in (workspace_id, instance_id, holder_id):
            validate_dispatch_identifier(value)
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_PULL_LEASE_SECONDS:
            raise ValueError("pull lease duration must be from 1 to 300 seconds")
        now = utc_instant(self._pull_now())
        key = (workspace_id, instance_id)
        control = self._pull_controls.get(key)
        interval = require_pull_ready(control, now)
        assert control is not None
        current = self._pull_cursor_rows.get(key)
        if (
            current is not None
            and current.lease_holder is not None
            and current.lease_expires_at is not None
            and current.lease_expires_at > now
        ):
            raise LeaseBusy("pull cursor already has live ownership")
        lease = PullLease(
            workspace_id,
            instance_id,
            holder_id,
            "pull:" + uuid4().hex,
            pull_deadline(now, ttl_seconds),
            interval,
        )
        updated = replace(
            control,
            next_eligible_at=pull_deadline(now, interval),
            invocation_id=lease.token,
            invocation_owner=holder_id,
            revision=control.revision + 1,
            updated_at=now,
            updated_by=holder_id,
        )
        row = ConnectorCursor(
            workspace_id,
            instance_id,
            current.value if current else "",
            current.advanced_at if current else now,
            lease.token,
            lease.expires_at,
        )
        self._pull_controls[key], self._pull_cursor_rows[key] = updated, row
        return row, lease

    async def commit_pull_tick(
        self,
        workspace_id: WorkspaceId,
        lease: PullLease,
        new_value: str,
        new_advanced_at: datetime,
    ) -> ConnectorCursor:
        """Commit the published cursor and clear failures, retaining the shared interval."""
        now = utc_instant(self._pull_now())
        key = (workspace_id, lease.instance_id)
        current = self._pull_cursor_rows.get(key)
        require_pull_lease(workspace_id, lease, current, now)
        assert current is not None
        row = replace(
            current,
            value=new_value,
            advanced_at=utc_instant(new_advanced_at),
            lease_holder=None,
            lease_expires_at=None,
        )
        control = self._pull_controls[key]
        require_pull_invocation(control, lease)
        updated = replace(
            control,
            invocation_id=None,
            invocation_owner=None,
            consecutive_failures=0,
            revision=control.revision + 1,
            updated_at=now,
            updated_by=lease.holder_id,
        )
        self._pull_controls[key], self._pull_cursor_rows[key] = updated, row
        return row

    async def fail_pull_tick(
        self,
        workspace_id: WorkspaceId,
        lease: PullLease,
        *,
        backoff_cap_seconds: int,
    ) -> None:
        """Keep the old cursor, reserve shared backoff and release the owned lease."""
        cap = pull_interval(backoff_cap_seconds)
        now = utc_instant(self._pull_now())
        key = (workspace_id, lease.instance_id)
        row = self._pull_cursor_rows.get(key)
        require_pull_lease(workspace_id, lease, row, now, allow_expired=True)
        assert row is not None
        current = self._pull_controls[key]
        require_pull_invocation(current, lease)
        failures = min(30, current.consecutive_failures + 1)
        delay = max(lease.interval_seconds, min(cap, lease.interval_seconds * 2 ** (failures - 1)))
        updated = replace(
            current,
            invocation_id=None,
            invocation_owner=None,
            consecutive_failures=failures,
            revision=current.revision + 1,
            next_eligible_at=max(current.next_eligible_at, pull_deadline(now, delay)),
            updated_at=now,
            updated_by=lease.holder_id,
        )
        self._pull_controls[key] = updated
        self._pull_cursor_rows[key] = replace(row, lease_holder=None, lease_expires_at=None)

    async def recover_pull_invocation(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        invocation_id: str,
        audit: AuditEvent,
    ) -> ConnectorPullControl:
        """Audit and recover one expired invocation without shortening its reservation."""
        key = (workspace_id, instance_id)
        cursor = self._pull_cursor_rows.get(key)
        updated = recover_pull_invocation(
            self._pull_controls.get(key),
            cursor,
            workspace_id,
            instance_id,
            invocation_id,
            audit,
            self._pull_now(),
        )
        if any(
            ws == workspace_id and event.event_id == audit.event_id
            for ws, event in self._pull_audit_rows
        ):
            raise ImmutableViolation("pull recovery audit already exists")
        captured = replace(
            audit,
            subject=json.loads(json.dumps(dict(audit.subject), allow_nan=False)),
            payload=json.loads(json.dumps(dict(audit.payload), allow_nan=False)),
        )
        self._pull_audit_rows.append((workspace_id, captured))
        self._pull_controls[key] = updated
        if cursor is not None:
            self._pull_cursor_rows[key] = replace(cursor, lease_holder=None, lease_expires_at=None)
        return updated
