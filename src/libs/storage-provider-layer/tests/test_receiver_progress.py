"""Memory reference contract: saturation fairness, fenced progress and strict serialization."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from custos_spl.errors import LeaseExpired
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.receiver_progress import (
    MAX_CONTINUATION_BYTES,
    ReceiverScanProgressProvider,
    ScanContinuation,
    ScanOperation,
)
from custos_spl.interfaces.receiver_workspaces import (
    ReceiverWorkspaceProvider,
    receiver_workspace_page,
)
from custos_spl.interfaces.scheduler import SchedulerLease
from custos_spl.pagination import Cursor
from custos_spl.pull_memory import MemoryPullControl
from custos_spl.receiver_memory import MemoryReceiverScanData, MemoryReceiverScanProgress

NOW = datetime(2026, 11, 5, tzinfo=UTC)


async def test_paused_controls_remain_discoverable_without_current_demand() -> None:
    """Extracted Connector discovery retains paused recovery scopes and opaque keysets."""
    store = MemoryPullControl()
    assert isinstance(store, ReceiverWorkspaceProvider)
    await store.initialize_pull_control(
        WorkspaceId("retained"),
        ConnectorInstanceId("instance"),
        actor="creator",
    )
    await store.check_receiver_workspaces_ready()
    assert (await store.query_receiver_workspaces()).items == ("retained",)
    with pytest.raises(ValueError):
        await store.query_receiver_workspaces(cursor=Cursor("invalid"))


async def register(
    store: ReceiverScanProgressProvider, count: int, deployment: str = "deployment"
) -> None:
    """Persist all discovery pages without waiting for any operation to complete."""
    names = [f"ws-{index:04}" for index in range(count)]
    while True:
        scan = await store.claim_receiver_discovery(deployment, "discovery")
        assert scan is not None
        page = receiver_workspace_page(names, cursor=scan.cursor, limit=50)
        await store.commit_receiver_discovery(scan, page.items, page.next_cursor)
        if page.next_cursor is None:
            break


@pytest.mark.parametrize("count", [51, 200, 1000])
@pytest.mark.parametrize("operation", list(ScanOperation))
async def test_every_continuing_workspace_gets_a_turn_without_cursor_eviction(
    count: int,
    operation: ScanOperation,
) -> None:
    """Exact N-claim rounds retain every committed position while all workspaces stay busy."""
    clock = [NOW]
    data = MemoryReceiverScanData()
    authority = SchedulerLease("deployment", "worker", 1, NOW + timedelta(days=1), NOW)
    store = MemoryReceiverScanProgress(
        now=lambda: clock[0],
        data=data,
        scheduler_lookup=lambda _: authority,
    )
    assert isinstance(store, ReceiverScanProgressProvider)
    await register(store, count)
    for turn in range(3):
        visited: set[str] = set()
        for _ in range(count):
            scan = await store.claim_receiver_scan(
                "deployment",
                operation,
                "worker",
                scheduler_lease=authority,
            )
            assert scan is not None and scan.claim.workspace_id is not None
            assert scan.claim.workspace_id not in visited
            visited.add(scan.claim.workspace_id)
            assert scan.continuation.cursor == (
                None
                if turn == 0 or operation is ScanOperation.PULL_DEMAND
                else Cursor(f"position-{turn}")
            )
            await store.checkpoint_receiver_scan(
                scan,
                ScanContinuation(
                    operation,
                    None
                    if operation is ScanOperation.PULL_DEMAND
                    else Cursor(f"position-{turn + 1}"),
                    NOW if operation is ScanOperation.SCHEDULED else None,
                ),
            )
            clock[0] += timedelta(seconds=1)
        assert len(visited) == count
        store = MemoryReceiverScanProgress(
            now=lambda: clock[0],
            data=data,
            scheduler_lookup=lambda _: authority,
        )
        await register(store, count)


async def test_discovery_refresh_and_failure_release_cannot_reset_committed_position() -> None:
    """Last-seen updates preserve a worker revision; failures retain provider-owned state."""
    clock = [NOW]
    store = MemoryReceiverScanProgress(now=lambda: clock[0])
    await register(store, 1)
    scan = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
    assert scan is not None
    await register(store, 1)
    position = ScanContinuation(ScanOperation.DELIVERY, Cursor("deep-target-999"))
    await store.checkpoint_receiver_scan(scan, position)
    with pytest.raises(LeaseExpired):
        await store.checkpoint_receiver_scan(scan, ScanContinuation(ScanOperation.DELIVERY))
    clock[0] += timedelta(seconds=1)
    next_scan = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "other")
    assert next_scan is not None and next_scan.continuation == position
    await store.release_receiver_scan(
        replace(next_scan, continuation=ScanContinuation(ScanOperation.DELIVERY))
    )
    assert await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker") is None
    clock[0] += timedelta(seconds=5)
    retry = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
    assert retry is not None and retry.continuation == position


async def test_standby_and_expired_owner_cannot_consume_scheduled_turns() -> None:
    """Provider checks live scheduler state, not a caller's fabricated lease expiry."""
    clock = [NOW]
    authority = SchedulerLease("deployment", "leader", 1, NOW + timedelta(seconds=30), NOW)
    leases = {"deployment": authority}
    store = MemoryReceiverScanProgress(now=lambda: clock[0], scheduler_lookup=leases.get)
    await register(store, 1)
    for token in (None, replace(authority, holder_id="standby"), replace(authority, epoch=2)):
        with pytest.raises(LeaseExpired):
            await store.claim_receiver_scan(
                "deployment",
                ScanOperation.SCHEDULED,
                "leader",
                scheduler_lease=token,
            )
    scan = await store.claim_receiver_scan(
        "deployment",
        ScanOperation.SCHEDULED,
        "leader",
        scheduler_lease=authority,
        lease_seconds=1,
    )
    assert scan is not None
    clock[0] += timedelta(seconds=2)
    with pytest.raises(LeaseExpired):
        await store.release_receiver_scan(
            replace(scan, claim=replace(scan.claim, expires_at=NOW + timedelta(days=1)))
        )
    successor = await store.claim_receiver_scan(
        "deployment",
        ScanOperation.SCHEDULED,
        "leader",
        scheduler_lease=authority,
    )
    assert successor is not None and successor.claim.generation > scan.claim.generation


async def test_pruning_requires_completed_absence_and_never_removes_unfinished_work() -> None:
    """Null finished rows expire only after a completed absent sweep and thirty idle days."""
    clock = [NOW]
    store = MemoryReceiverScanProgress(now=lambda: clock[0])
    await register(store, 1)
    scan = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
    assert scan is not None
    await store.checkpoint_receiver_scan(
        scan, ScanContinuation(ScanOperation.DELIVERY, Cursor("unfinished"))
    )
    with pytest.raises(ValueError):
        await store.prune_receiver_scans("deployment", 2)
    absent = await store.claim_receiver_discovery("deployment", "discovery")
    assert absent is not None
    await store.commit_receiver_discovery(absent, [], None)
    assert await store.prune_receiver_scans("deployment", 2) == 0
    clock[0] += timedelta(days=31)
    assert await store.prune_receiver_scans("deployment", 2, limit=2) == 2
    assert await store.prune_receiver_scans("deployment", 2) == 1
    retry = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
    assert retry is not None and retry.continuation.cursor == Cursor("unfinished")


@pytest.mark.parametrize("operation", list(ScanOperation))
def test_closed_serialization_and_exact_byte_bound(operation: ScanOperation) -> None:
    """Every tagged shape round-trips; unknown/duplicate fields and oversized positions fail."""
    initial = ScanContinuation(operation)
    assert ScanContinuation.from_json(initial.to_json()) == initial
    wire = json.loads(initial.to_json())
    for mutation in ({"version": True}, {"version": 2}, {"extra": "private"}, {"cursor": 42}):
        with pytest.raises((ValueError, TypeError)):
            ScanContinuation.from_json(json.dumps(wire | mutation))
    with pytest.raises(ValueError):
        ScanContinuation.from_json(
            initial.to_json().replace('"version":1', '"version":1,"version":1')
        )
    if operation in (ScanOperation.DELIVERY, ScanOperation.RETENTION):
        overhead = len(ScanContinuation(operation, Cursor("x")).to_json().encode()) - 1
        valid = ScanContinuation(operation, Cursor("x" * (MAX_CONTINUATION_BYTES - overhead)))
        assert len(valid.to_json().encode()) == MAX_CONTINUATION_BYTES
        with pytest.raises(ValueError, match="64 KiB"):
            replace(valid, cursor=Cursor(valid.cursor.token + "x"))  # type: ignore[union-attr]
