"""Shared in-process delivery capability for the Trigger and Workflow development providers."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime

from custos_spl.errors import ImmutableViolation, LeaseBusy
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import DeadLetterData, DeadLetterEntry, new_dead_letter
from custos_spl.interfaces.delivery import (
    DeliveryNamespace,
    DeliveryReceipt,
    WorkflowMirror,
    WorkflowStart,
    delivery_instant,
    delivery_result,
    require_delivery,
    reserve_delivery,
    validate_delivery_key,
    validate_failure_capture,
)
from custos_spl.interfaces.metadata_store import Run
from custos_spl.pagination import (
    Cursor,
    Page,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
)


class MemoryDeliveryState:
    """Single-loop atomic operations with deeply immutable stored values and an injected clock."""

    def __init__(self, *, now: Callable[[], datetime] | None = None) -> None:
        """Initialize optional delivery state; service providers retain their other rows."""
        self._delivery_clock = now if now is not None else lambda: datetime.now(UTC)
        self._delivery_rows: dict[tuple[str, str, str], DeliveryReceipt] = {}
        self._workflow_mirrors: dict[tuple[str, str], WorkflowMirror] = {}
        self._delivery_run_rows: dict[tuple[str, str], Run] = {}
        self._workflow_starts: dict[tuple[str, str], WorkflowStart] = {}
        self._delivery_dead_letters: dict[tuple[str, str], DeadLetterEntry] = {}

    async def capture_failed_delivery(
        self, claim: DeliveryReceipt, data: DeadLetterData
    ) -> DeadLetterEntry:
        """Publish capture and acknowledgement together, never on a stale producer's authority."""
        validate_failure_capture(claim, data)
        key = (claim.workspace_id, claim.namespace, claim.key)
        current = self._delivery_rows.get(key)
        now = self._delivery_clock()
        require_delivery(current, claim, now)
        assert current is not None
        entry_key = (data.workspace_id, data.entry_id)
        existing = self._delivery_dead_letters.get(entry_key)
        if existing is not None and (
            existing.data.idempotency_key != data.idempotency_key
            or existing.data.target != data.target
        ):
            raise ImmutableViolation("failure capture identity is already bound")
        entry = existing if existing is not None else new_dead_letter(data, now)
        result = delivery_result(json.dumps({"deadLetterId": data.entry_id}))
        self._delivery_dead_letters[entry_key] = entry
        self._delivery_rows[key] = replace(
            current, token=None, lease_until=None, result_json=result
        )
        return entry

    async def put_workflow_start(self, run: Run, start: WorkflowStart) -> WorkflowStart:
        """Commit both records without yielding, or recover the original immutable input."""
        key = (start.workspace_id, start.run_id)
        if run.workspace_id != start.workspace_id or run.run_id != start.run_id:
            raise ValueError("workflow start scope does not match Run")
        existing = self._workflow_starts.get(key)
        if existing is not None:
            if existing.request_hash != start.request_hash:
                raise ImmutableViolation("workflow start input is immutable")
            return existing
        if key in self._delivery_run_rows:
            raise ImmutableViolation("replay_unsafe: existing run has no versioned start snapshot")
        self._delivery_run_rows[key] = run
        self._workflow_starts[key] = start
        return start

    async def get_workflow_start(
        self, workspace_id: WorkspaceId, run_id: str
    ) -> WorkflowStart | None:
        """Read the immutable versioned input within one workspace."""
        return self._workflow_starts.get((workspace_id, run_id))

    async def check_delivery_ready(self) -> None:
        """Memory structures exist at construction; no restart durability is implied."""

    async def delivery_now(self) -> datetime:
        """Return injected provider time as an aware UTC instant."""
        return delivery_instant(self._delivery_clock())

    async def get_delivery(
        self,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
    ) -> DeliveryReceipt | None:
        """Read a scoped immutable receipt."""
        validate_delivery_key(workspace_id, namespace, key)
        return self._delivery_rows.get((workspace_id, namespace, key))

    async def reserve_delivery(
        self,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
        request_hash: str,
        retain_until: datetime,
        *,
        lease_seconds: int = 300,
    ) -> DeliveryReceipt:
        """Reserve without yielding between checking and replacing provider state."""
        identity = (workspace_id, namespace, key)
        current = self._delivery_rows.get(identity)
        try:
            row = reserve_delivery(
                current,
                workspace_id,
                namespace,
                key,
                request_hash,
                retain_until,
                self._delivery_clock(),
                lease_seconds,
            )
        except LeaseBusy:
            assert current is not None
            self._delivery_rows[identity] = replace(
                current, retain_until=max(current.retain_until, delivery_instant(retain_until))
            )
            raise
        self._delivery_rows[identity] = row
        return row

    async def complete_delivery(self, claim: DeliveryReceipt, result_json: str) -> DeliveryReceipt:
        """Preserve completed consumer results and reject stale pending producers."""
        key = (claim.workspace_id, claim.namespace, claim.key)
        current = self._delivery_rows.get(key)
        result = delivery_result(result_json)
        if current is not None and current.completed:
            if current.request_hash != claim.request_hash or current.result_json != result:
                raise ImmutableViolation("delivery result is immutable")
            return current
        require_delivery(current, claim, self._delivery_clock())
        assert current is not None
        completed = replace(current, token=None, lease_until=None, result_json=result)
        self._delivery_rows[key] = completed
        return completed

    async def release_delivery(self, claim: DeliveryReceipt) -> None:
        """Release only the current producer; completion is never rolled back."""
        key = (claim.workspace_id, claim.namespace, claim.key)
        current = self._delivery_rows.get(key)
        require_delivery(current, claim, self._delivery_clock())
        assert current is not None
        self._delivery_rows[key] = replace(current, token=None, lease_until=None)

    async def acknowledge_delivery(
        self,
        workspace_id: WorkspaceId,
        key: str,
        request_hash: str,
        result_json: str,
    ) -> DeliveryReceipt:
        """Record a consumed resume even when the producer's lease has expired."""
        identity = (workspace_id, "workflow-resume", key)
        current = self._delivery_rows.get(identity)
        result = delivery_result(result_json)
        if current is None or current.request_hash != request_hash:
            raise ImmutableViolation("resume acknowledgement does not match an accepted request")
        if current.completed and current.result_json != result:
            raise ImmutableViolation("delivery result is immutable")
        completed = replace(current, token=None, lease_until=None, result_json=result)
        self._delivery_rows[identity] = completed
        return completed

    async def put_workflow_mirror(self, mirror: WorkflowMirror) -> None:
        """Update metadata only within the original workspace/run/step/source association."""
        key = (mirror.workspace_id, mirror.mirror_id)
        previous = self._workflow_mirrors.get(key)
        if previous is not None and previous.identity_hash != mirror.identity_hash:
            raise ImmutableViolation("workflow mirror association cannot be rebound")
        self._workflow_mirrors[key] = mirror

    async def query_workflow_mirrors(
        self,
        workspace_id: WorkspaceId,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        expired_before: datetime | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowMirror]:
        """Apply the same scoped filter/keyset contract as PostgreSQL."""
        cutoff = delivery_instant(expired_before) if expired_before else None
        scope = (
            "workflow-mirrors",
            workspace_id,
            run_id,
            step_id,
            cutoff.isoformat() if cutoff else None,
        )
        after, size = decode_query_cursor(cursor, scope), query_page_limit(limit)
        rows = sorted(
            (
                row
                for (ws, key), row in self._workflow_mirrors.items()
                if ws == workspace_id
                and (after is None or key > after)
                and (run_id is None or row.run_id == run_id)
                and (step_id is None or row.step_id == step_id)
                and (cutoff is None or row.expires_at <= cutoff)
            ),
            key=lambda row: row.mirror_id,
        )
        return Page(
            rows[:size],
            encode_query_cursor(scope, rows[size - 1].mirror_id) if len(rows) > size else None,
        )

    async def delete_workflow_mirror(self, workspace_id: WorkspaceId, mirror_id: str) -> None:
        """Remove only one workspace's mirror; repeated cleanup is harmless."""
        self._workflow_mirrors.pop((workspace_id, mirror_id), None)
