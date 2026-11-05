"""Real PostgreSQL saturation, restart, fencing, discovery and inactive-progress contracts."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from asyncpg.pool import Pool
from custos_spl.errors import LeaseExpired, MigrationRequired
from custos_spl.ids import SubscriptionId, WorkflowId, WorkspaceId
from custos_spl.interfaces.metadata_store import Subscription
from custos_spl.interfaces.receiver_progress import (
    ReceiverScanProgressProvider,
    ScanContinuation,
    ScanOperation,
)
from custos_spl.interfaces.receiver_workspaces import (
    ReceiverWorkspaceProvider,
    receiver_workspace_page,
)
from custos_spl.interfaces.source_delivery import SourceDeliverySelection
from custos_spl.pagination import Cursor

from custos_pg import PgMetadataAdapter

pytestmark = pytest.mark.integration


async def register(store: PgMetadataAdapter, count: int, deployment: str = "deployment") -> None:
    """Commit genuine scoped keyset pages without waiting for operation slots."""
    names = [f"ws-{i:04}" for i in range(count)]
    while True:
        discovery = await store.claim_receiver_discovery(deployment, "discovery")
        assert discovery is not None
        page = receiver_workspace_page(names, cursor=discovery.cursor, limit=50)
        await store.commit_receiver_discovery(discovery, page.items, page.next_cursor)
        if page.next_cursor is None:
            break


async def eligible(pool: Pool) -> None:
    """Move retry eligibility into the past without waiting on wall-clock time."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE custos_state.receiver_progress "
            "SET next_eligible_at=clock_timestamp()-interval '1 second'"
        )


@pytest.mark.parametrize("count", [51, 200, 1000])
async def test_saturated_fairness_and_restart_preserve_all_positions(
    pg_pool: Pool, count: int
) -> None:
    """All N continuing workspaces get a turn in N claims, with no in-memory state dependency."""
    store = PgMetadataAdapter(pool=pg_pool)
    assert isinstance(store, ReceiverScanProgressProvider)
    with pytest.raises(MigrationRequired):
        await store.check_receiver_scan_progress_ready()
    await store.apply_pending()
    await store.check_receiver_scan_progress_ready()
    await register(store, count)
    for turn in range(2):
        seen: set[str] = set()
        for _ in range(count):
            scan = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
            assert scan is not None and scan.claim.workspace_id not in seen
            assert scan.claim.workspace_id is not None
            seen.add(scan.claim.workspace_id)
            assert scan.continuation.cursor == (None if turn == 0 else Cursor("position-999"))
            await store.checkpoint_receiver_scan(
                scan,
                ScanContinuation(ScanOperation.DELIVERY, Cursor("position-999")),
            )
        assert len(seen) == count
        store = PgMetadataAdapter(pool=pg_pool)
        await register(store, count)
        await eligible(pg_pool)


async def test_discovery_is_bounded_and_resumes_native_keyset_after_restart(pg_pool: Pool) -> None:
    """Orphaned retained scopes and duplicate subscription rows cannot defeat keyset bounds."""
    store = PgMetadataAdapter(pool=pg_pool)
    assert isinstance(store, ReceiverWorkspaceProvider)
    await store.apply_pending()
    now = datetime.now(UTC)
    for index in range(205):
        ws = WorkspaceId(f"ws-{index:04}")
        for suffix in ("a", "b"):
            await store.put_subscription(
                ws,
                Subscription(ws, SubscriptionId(suffix), WorkflowId("wf"), "active", now, now),
            )
    first = await store.query_receiver_workspaces(limit=50)
    assert len(first.items) == 50 and first.next_cursor is not None
    discovery = await store.claim_receiver_discovery("deployment", "a")
    assert discovery is not None
    await store.commit_receiver_discovery(discovery, first.items, first.next_cursor)
    restarted = PgMetadataAdapter(pool=pg_pool)
    next_scan = await restarted.claim_receiver_discovery("deployment", "b")
    assert next_scan is not None and next_scan.cursor == first.next_cursor
    second = await restarted.query_receiver_workspaces(cursor=next_scan.cursor, limit=50)
    assert second.items[0] == "ws-0050" and len(second.items) == 50
    with pytest.raises(LeaseExpired):
        await store.commit_receiver_discovery(discovery, first.items, first.next_cursor)
    with pytest.raises(ValueError):
        await restarted.query_receiver_workspaces(cursor=Cursor("invalid"))
    assert len((await restarted.query_receiver_workspaces(limit=10000)).items) == 200
    await store.put_source_delivery(
        SourceDeliverySelection(
            WorkspaceId("orphaned"),
            "selection",
            "a" * 64,
            now + timedelta(days=1),
            (),
        )
    )
    assert (await store.query_receiver_workspaces(limit=1)).items == ("orphaned",)


async def test_concurrent_claim_and_checkpoint_fence_scope_and_revision(pg_pool: Pool) -> None:
    """Two real pooled connections cannot share ownership or overwrite acknowledged progress."""
    store, other = PgMetadataAdapter(pool=pg_pool), PgMetadataAdapter(pool=pg_pool)
    await store.apply_pending()
    await register(store, 1)
    claims = await asyncio.gather(
        store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "a"),
        other.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "b"),
    )
    assert sum(scan is not None for scan in claims) == 1
    scan = next(scan for scan in claims if scan is not None)
    await register(other, 1)
    position = ScanContinuation(ScanOperation.DELIVERY, Cursor("target-999"))
    await store.checkpoint_receiver_scan(scan, position)
    # Model an acknowledgement lost after a successful database commit.
    with pytest.raises(LeaseExpired):
        await store.checkpoint_receiver_scan(scan, ScanContinuation(ScanOperation.DELIVERY))
    await eligible(pg_pool)
    retry = await other.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "restart")
    assert retry is not None and retry.continuation == position
    for mutation in (
        {"workspace_id": "foreign"},
        {"deployment_id": "foreign"},
        {"operation": ScanOperation.RETENTION},
        {"holder": "foreign"},
        {"incarnation": "f" * 32},
        {"token": "f" * 32},
        {"revision": 99},
    ):
        with pytest.raises(LeaseExpired):
            await store.release_receiver_scan(
                replace(retry, claim=replace(retry.claim, **mutation))
            )
    await other.release_receiver_scan(retry)
    assert await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "a") is None


async def test_expired_takeover_preserves_cursor_and_rejects_late_checkpoint(pg_pool: Pool) -> None:
    """A fresh incarnation/generation, not caller expiry or payload, decides ownership."""
    store = PgMetadataAdapter(pool=pg_pool)
    await store.apply_pending()
    await register(store, 1)
    first = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "a")
    assert first is not None
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE custos_state.receiver_progress "
            "SET expires_at=clock_timestamp()-interval '1 second' "
            "WHERE token=$1",
            first.claim.token,
        )
    with pytest.raises(LeaseExpired):
        await store.release_receiver_scan(first)
    second = await PgMetadataAdapter(pool=pg_pool).claim_receiver_scan(
        "deployment",
        ScanOperation.DELIVERY,
        "b",
    )
    assert second is not None and second.claim.generation > first.claim.generation
    with pytest.raises(LeaseExpired):
        await store.checkpoint_receiver_scan(
            first, ScanContinuation(ScanOperation.DELIVERY, Cursor("lost"))
        )
    assert second.continuation.cursor is None


async def test_scheduled_claims_require_current_provider_authority(pg_pool: Pool) -> None:
    """Standbys cannot advance service order; expiry between preflight and claim is fenced."""
    store = PgMetadataAdapter(pool=pg_pool)
    await store.apply_pending()
    await register(store, 51)
    authority = await store.acquire_scheduler_lease("deployment", "leader", ttl_seconds=300)
    assert authority is not None
    for _ in range(3):
        for token in (None, replace(authority, holder_id="standby"), replace(authority, epoch=99)):
            with pytest.raises(LeaseExpired):
                await store.claim_receiver_scan(
                    "deployment",
                    ScanOperation.SCHEDULED,
                    "standby",
                    scheduler_lease=token,
                )
    seen = []
    for _ in range(51):
        scan = await store.claim_receiver_scan(
            "deployment",
            ScanOperation.SCHEDULED,
            "leader",
            scheduler_lease=authority,
        )
        assert scan is not None
        seen.append(scan.claim.workspace_id)
        await store.checkpoint_receiver_scan(scan, ScanContinuation(ScanOperation.SCHEDULED))
    assert seen == [f"ws-{i:04}" for i in range(51)]
    await store.release_scheduler_lease(authority)
    with pytest.raises(LeaseExpired):
        await store.claim_receiver_scan(
            "deployment",
            ScanOperation.SCHEDULED,
            "leader",
            scheduler_lease=authority,
        )


async def test_prune_preserves_live_unfinished_or_rediscovered_rows_and_prevents_aba(
    pg_pool: Pool,
) -> None:
    """Require completed absence and idle finished state before bounded deletion."""
    store = PgMetadataAdapter(pool=pg_pool)
    await store.apply_pending()
    await register(store, 1)
    scan = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
    assert scan is not None
    await store.checkpoint_receiver_scan(
        scan, ScanContinuation(ScanOperation.DELIVERY, Cursor("pending"))
    )
    absent = await store.claim_receiver_discovery("deployment", "discovery")
    assert absent is not None
    await store.commit_receiver_discovery(absent, [], None)
    assert await store.prune_receiver_scans("deployment", 2) == 0
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE custos_state.receiver_progress "
            "SET updated_at=clock_timestamp()-interval '31 days'"
        )
    assert await store.prune_receiver_scans("deployment", 2, limit=2) == 2
    assert await store.prune_receiver_scans("deployment", 2) == 1
    await register(store, 1)
    await eligible(pg_pool)
    retained = await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "restart")
    assert retained is not None and retained.continuation.cursor == Cursor("pending")
    assert await store.prune_receiver_scans("deployment", 2) == 0
    with pytest.raises(LeaseExpired):
        await store.release_receiver_scan(scan)


async def test_malformed_persisted_version_is_not_silently_reset(pg_pool: Pool) -> None:
    """Read-side validation rejects shapes that pass coarse database JSON constraints."""
    store = PgMetadataAdapter(pool=pg_pool)
    await store.apply_pending()
    await register(store, 1)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE custos_state.receiver_progress SET continuation=$1 WHERE operation='delivery'",
            json.dumps({"version": 1, "operation": "delivery", "cursor": 42, "dueCutoff": None}),
        )
    with pytest.raises(ValueError, match="cursor"):
        await store.claim_receiver_scan("deployment", ScanOperation.DELIVERY, "worker")
    async with pg_pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT generation FROM custos_state.receiver_progress WHERE operation='delivery'"
            )
            == 0
        )
        await conn.execute(
            "DELETE FROM custos_meta.adapter_revisions "
            "WHERE interface_name='MetadataStoreProvider' AND revision=16"
        )
    with pytest.raises(MigrationRequired):
        await store.check_receiver_scan_progress_ready()
