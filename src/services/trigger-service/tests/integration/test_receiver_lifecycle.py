"""Actual receiver drivers over memory/PostgreSQL: saturated discovery, fair pages and restart."""

import asyncio
from collections import defaultdict
from datetime import timedelta

import httpx
import pytest
from custos_pg import PgMetadataAdapter
from custos_spl.errors import MigrationRequired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.receiver_progress import ReceiverScan, ScanContinuation, ScanOperation
from custos_spl.interfaces.source_delivery import SourceDeliverySelection
from custos_spl.pagination import Cursor

from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.pull_demand import DaprPullDemandClient
from custos_trigger.clients.workflow import DaprEndpoint
from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SourceType, Subscription
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.pipeline.reliable import ReliableDispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.receiver_runtime import ReceiverRuntime
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import SubscriptionStore
from tests.integration.test_receiver_workspaces import Store, workspace_dsn, workspace_store

__all__ = ["workspace_dsn", "workspace_store"]


def worker(store: Store, client: httpx.AsyncClient) -> ReceiverRuntime:
    """Build the real lifecycle with borrowed transports and actual progress providers."""

    async def publish(event: NormalizedEvent) -> None:
        """Acknowledge synthetic scheduled output; effects have separate consumer coverage."""

    return ReceiverRuntime(
        store,
        ReliableDispatcher(FakeWorkflowServiceClient(), store, NoopAuditSink(), max_retries=0),
        DaprPullDemandClient(client, DaprEndpoint("127.0.0.1", 3500, "connector-service")),
        publish,
        NoopAuditSink(),
        deployment_id="lifecycle",
        interval_seconds=0.001,
        lease_seconds=300,
        renew_seconds=100,
    )


async def seed(store: Store, count: int) -> None:
    """Make synthetic workspaces discoverable through retained selections, not subscriptions."""
    now = await store.delivery_now()
    for index in range(count):
        await store.put_source_delivery(
            SourceDeliverySelection(
                WorkspaceId(f"ws-{index:04}"),
                "selection",
                "a" * 64,
                now + timedelta(days=1),
                (),
            )
        )


@pytest.mark.parametrize("count", [51, 200, 1000])
async def test_full_lifecycle_window_cannot_starve_later_scopes(
    workspace_store: Store,
    count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive discovery past saturation and verify N-turn fairness/restart for every operation."""
    await seed(workspace_store, count)
    seen: dict[ScanOperation, list[str]] = defaultdict(list)
    cursors: dict[tuple[ScanOperation, str], list[Cursor | None]] = defaultdict(list)

    async def page(scan: ReceiverScan) -> ScanContinuation:
        """Every continuing operation stays busy; retain each restored position for verification."""
        op, ws = scan.claim.operation, scan.claim.workspace_id
        assert op is not None and ws is not None
        seen[op].append(ws)
        cursors[op, ws].append(scan.continuation.cursor)
        return ScanContinuation(
            op,
            None if op is ScanOperation.PULL_DEMAND else Cursor("continued"),
            await workspace_store.scheduler_now() if op is ScanOperation.SCHEDULED else None,
        )

    async with httpx.AsyncClient() as client:
        first = worker(workspace_store, client)
        monkeypatch.setattr(first, "_page", page)
        # Discovery proceeds while the first50 remain unfinished, without storing a tenant queue.
        await first._discover()
        for _ in range(50):
            await first._run_operation(ScanOperation.DELIVERY)
        for _ in range((count - 1) // 50):
            await first._discover()
        assert not hasattr(first, "_workspaces")
        seen.clear()
        # Make every row eligible under the provider clock, without wall-clock sleeps.
        if isinstance(workspace_store, InMemoryTriggerMetadataStore):
            now = await workspace_store.scheduler_now()
            workspace_store._scan_now = lambda: now + timedelta(seconds=1)
        else:
            pool = await workspace_store._pool_ref()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.receiver_progress SET next_eligible_at=clock_timestamp()"
                )
        for op in ScanOperation:
            for _ in range(count):
                await first._run_operation(op)
            assert len(seen[op]) == len(set(seen[op])) == count
        await first.aclose()
        if isinstance(workspace_store, PgMetadataAdapter):
            restarted_store: Store = PgMetadataAdapter(pool=await workspace_store._pool_ref())
        else:
            restarted_store = workspace_store
            restarted_store._scan_now = lambda: now + timedelta(seconds=2)
        if isinstance(restarted_store, PgMetadataAdapter):
            async with (await restarted_store._pool_ref()).acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.receiver_progress SET next_eligible_at=clock_timestamp()"
                )
        restarted = worker(restarted_store, client)
        monkeypatch.setattr(restarted, "_page", page)
        seen.clear()
        for op in ScanOperation:
            for _ in range(count):
                await restarted._run_operation(op)
            assert len(seen[op]) == len(set(seen[op])) == count
            for ws in seen[op]:
                assert cursors[op, ws][-1] == (
                    None if op is ScanOperation.PULL_DEMAND else Cursor("continued")
                )
        await restarted.aclose()


async def test_discovery_task_keeps_running_while_all_operation_tasks_are_occupied(
    workspace_store: Store,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Start the actual five tasks, block all four operations and observe later discovery commit."""
    await seed(workspace_store, 51)
    entered: set[ScanOperation] = set()
    release = asyncio.Event()

    async def hold(scan: ReceiverScan) -> ScanContinuation:
        """Simulate bounded collaborators that retain a page until the test explicitly drains."""
        assert scan.claim.operation is not None
        entered.add(scan.claim.operation)
        await release.wait()
        return scan.continuation

    async with httpx.AsyncClient() as client:
        runtime = worker(workspace_store, client)
        monkeypatch.setattr(runtime, "_page", hold)
        await runtime.start()
        try:
            async with asyncio.timeout(5):
                while len(entered) < 4:
                    await asyncio.sleep(0.001)
                while True:
                    scan = await workspace_store.claim_receiver_scan(
                        "lifecycle",
                        ScanOperation.DELIVERY,
                        "observer",
                    )
                    if scan is None:
                        await asyncio.sleep(0.001)
                        continue
                    if scan.claim.workspace_id == "ws-0050":
                        break
                    await workspace_store.checkpoint_receiver_scan(
                        scan,
                        ScanContinuation(ScanOperation.DELIVERY),
                        interval_seconds=3600,
                    )
            assert len(runtime._tasks) == 5 and runtime.healthy
        finally:
            release.set()
            await runtime.aclose()


async def test_two_running_lifecycles_publish_all_workspaces_without_duplicate_fires(
    workspace_store: Store,
) -> None:
    """Run real discovery/schedule/retention/demand tasks with competing scheduler replicas."""
    now = await workspace_store.scheduler_now()
    subscriptions = SubscriptionStore(workspace_store)
    for index in range(51):
        ws = f"tenant-{index:02}"
        await subscriptions.create(
            Subscription(
                workspace_id=ws,
                subscription_id="schedule",
                workflow_id="workflow",
                target_workflow_version_id="version",
                source_type=SourceType.SCHEDULED,
                source_config=SourceConfig(interval_seconds=3600),
                selector="true",
                created_at=now - timedelta(hours=2),
                updated_at=now,
            )
        )
        await workspace_store.update_schedule_next_fire(
            WorkspaceId(ws),
            "schedule",
            now - timedelta(hours=1),
        )
    events: list[NormalizedEvent] = []
    completed = asyncio.Event()

    async def publish(event: NormalizedEvent) -> None:
        """Acknowledge every real occurrence and signal the last distinct workspace."""
        events.append(event)
        if len({event.data["workspace"] for event in events}) == 51:
            completed.set()

    def acknowledge(request: httpx.Request) -> httpx.Response:
        """Exercise actual bounded demand transport with a local synthetic acknowledgement."""
        return httpx.Response(200, json={"schemaVersion": "1", "registered": 0})

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        first, second = worker(workspace_store, client), worker(workspace_store, client)
        first.publish = second.publish = publish
        try:
            await first.start()
            await second.start()
            await asyncio.wait_for(completed.wait(), 30)
            assert first.healthy and second.healthy
        finally:
            await first.aclose()
            await second.aclose()
        assert (
            len(events)
            == len({(event.data["workspace"], event.event_id) for event in events})
            == 51
        )
        assert not client.is_closed


@pytest.mark.integration
async def test_actual_lifecycle_refuses_old_progress_schema(pg_pool: object) -> None:
    """Worker startup requires revision16 even when the remaining source schemas are present."""
    from asyncpg.pool import Pool

    assert isinstance(pg_pool, Pool)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM custos_meta.adapter_revisions "
            "WHERE interface_name='MetadataStoreProvider' AND revision=16"
        )
    store = PgMetadataAdapter(pool=pg_pool)
    async with httpx.AsyncClient() as client:
        runtime = worker(store, client)
        with pytest.raises(MigrationRequired):
            await runtime.start()
        assert not runtime._tasks and not runtime.healthy
