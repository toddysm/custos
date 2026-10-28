"""Identical fencing/outbox contracts against memory and real PostgreSQL."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

import pytest
from asyncpg.pool import Pool
from custos_pg import PgMetadataAdapter
from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    LeaseExpired,
    MigrationRequired,
)
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.scheduler import (
    PendingScheduleFire,
    SchedulerLease,
    SchedulerStateProvider,
)
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider

from custos_trigger.models import SourceType, Subscription, SubscriptionState
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores.base import TriggerMetadataStore
from custos_trigger.stores.subscriptions import SubscriptionStore

_WS = WorkspaceId("scheduler-test")


class Backend(TriggerMetadataStore, TriggerQueryProvider, SchedulerStateProvider, Protocol):
    """The production storage surfaces exercised by this contract."""


@dataclass
class Harness:
    """Provide deterministic memory time and real database fault injection."""

    store: Backend
    clock: list[datetime]
    pool: Pool | None

    async def expire(self, deployment: str) -> None:
        """Cross the exact memory boundary or make the database lease eligible without sleeps."""
        if self.pool is None:
            self.clock[0] += timedelta(seconds=30)
        else:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.scheduler_lease SET expires_at=clock_timestamp() "
                    "WHERE deployment_id=$1",
                    deployment,
                )


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def scheduler_dsn(request: pytest.FixtureRequest) -> str | None:
    """Reset the database before entering the test's event loop."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def harness(scheduler_dsn: str | None) -> AsyncIterator[Harness]:
    """Yield the same interface backed by either actual production provider."""
    clock = [datetime(2026, 10, 28, tzinfo=UTC)]
    if scheduler_dsn is None:
        yield Harness(InMemoryTriggerMetadataStore(now=lambda: clock[0]), clock, None)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=scheduler_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        adapter = PgMetadataAdapter(pool=pool)
        await adapter.apply_pending()
        yield Harness(adapter, clock, pool)
    finally:
        await pool.close()


async def _due(
    harness: Harness, identifier: str = "schedule", workspace: WorkspaceId = _WS
) -> Subscription:
    """Create real managed state and set an explicitly due instant through the maintenance API."""
    now = await harness.store.scheduler_now()
    store = SubscriptionStore(harness.store, now=lambda: now)
    subscription = await store.create(
        Subscription(
            workspace_id=workspace,
            subscription_id=identifier,
            workflow_id="workflow",
            source_type=SourceType.SCHEDULED,
            source_config=SourceConfig(interval_seconds=30),
            created_at=now - timedelta(minutes=1),
            updated_at=now,
        )
    )
    await harness.store.update_schedule_next_fire(
        workspace, identifier, now - timedelta(seconds=30)
    )
    return subscription


async def _advance(
    harness: Harness, subscription: Subscription, lease: SchedulerLease
) -> PendingScheduleFire:
    """Advance the captured due instant via the fenced transaction."""
    schedule = await harness.store.get_schedule(
        WorkspaceId(subscription.workspace_id), subscription.subscription_id
    )
    assert schedule is not None
    now = await harness.store.scheduler_now()
    return await harness.store.advance_schedule(
        WorkspaceId(subscription.workspace_id),
        subscription.subscription_id,
        lease,
        expected_next_fire_at=schedule.next_fire_at,
        expected_revision=subscription.store_revision,
        planned_fire_at=schedule.next_fire_at,
        next_fire_at=now + timedelta(minutes=1),
    )


async def test_competing_acquisition_and_stable_renewal(harness: Harness) -> None:
    """Only one replica acquires; renewal and same-holder acquire retries retain the epoch."""
    await harness.store.check_scheduler_ready()
    results = await asyncio.gather(
        *(
            harness.store.acquire_scheduler_lease("deployment", holder, ttl_seconds=30)
            for holder in ("a", "b")
        )
    )
    winners = [lease for lease in results if lease is not None]
    assert len(winners) == 1
    first = winners[0]
    assert first.expires_at - first.observed_at == timedelta(seconds=30)
    retry = await harness.store.acquire_scheduler_lease(
        "deployment", first.holder_id, ttl_seconds=30
    )
    assert retry is not None and retry.epoch == first.epoch and retry.expires_at == first.expires_at
    renewed = await harness.store.renew_scheduler_lease(first, ttl_seconds=30)
    assert renewed.epoch == first.epoch and renewed.expires_at >= first.expires_at
    independent = await harness.store.acquire_scheduler_lease(
        "other-deployment", "b", ttl_seconds=30
    )
    assert independent is not None and independent.epoch == 1


async def test_expiry_handover_and_stale_mutations(harness: Harness) -> None:
    """The first eligible scan acquires a higher epoch and every old mutation is fenced out."""
    subscription = await _due(harness)
    first = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert first is not None
    await harness.expire("d")
    second = await harness.store.acquire_scheduler_lease("d", "b", ttl_seconds=30)
    assert second is not None and second.epoch == first.epoch + 1
    for token in (first, replace(second, holder_id="a"), replace(second, epoch=first.epoch)):
        with pytest.raises(LeaseExpired):
            await harness.store.renew_scheduler_lease(token, ttl_seconds=30)
        with pytest.raises(LeaseExpired):
            await harness.store.release_scheduler_lease(token)
        with pytest.raises(LeaseExpired):
            await _advance(harness, subscription, token)
    await harness.store.release_scheduler_lease(second)
    third = await harness.store.acquire_scheduler_lease("d", "b", ttl_seconds=30)
    assert third is not None and third.epoch == second.epoch + 1


async def test_competing_advancement_creates_one_durable_fire(harness: Harness) -> None:
    """Overlapping attempts cannot advance the same occurrence twice."""
    subscription = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    schedule = await harness.store.get_schedule(_WS, "schedule")
    assert schedule is not None
    now = await harness.store.scheduler_now()
    following = now + timedelta(minutes=1)

    async def advance() -> PendingScheduleFire:
        """Race identical captured input, not a second refreshed due read."""
        return await harness.store.advance_schedule(
            _WS,
            "schedule",
            lease,
            expected_next_fire_at=schedule.next_fire_at,
            expected_revision=subscription.store_revision,
            planned_fire_at=schedule.next_fire_at,
            next_fire_at=following,
        )

    results = await asyncio.gather(
        advance(),
        advance(),
        return_exceptions=True,
    )
    assert sum(isinstance(result, ImmutableViolation) for result in results) == 1
    page = await harness.store.query_pending_schedule_fires(_WS, "d")
    assert len(page.items) == 1 and page.items[0] in results
    assert page.items[0].planned_fire_at == schedule.next_fire_at
    updated = await harness.store.get_schedule(_WS, "schedule")
    assert updated is not None and updated.next_fire_at == following
    if harness.pool is not None:
        restarted = PgMetadataAdapter(pool=harness.pool)
        await restarted.check_scheduler_ready()
        assert (await restarted.query_pending_schedule_fires(_WS, "d")).items == page.items
    await harness.expire("d")
    replacement = await harness.store.acquire_scheduler_lease("d", "b", ttl_seconds=30)
    assert replacement is not None
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == page.items
    with pytest.raises(LeaseExpired):
        await harness.store.acknowledge_schedule_fire(_WS, page.items[0].fire_id, lease)
    await harness.store.acknowledge_schedule_fire(_WS, page.items[0].fire_id, replacement)
    await harness.store.acknowledge_schedule_fire(_WS, page.items[0].fire_id, replacement)
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == ()


@pytest.mark.parametrize("state", [SubscriptionState.PAUSED, SubscriptionState.EXPIRED])
async def test_changed_eligibility_cannot_advance(
    harness: Harness, state: SubscriptionState
) -> None:
    """Eligibility and configuration revisions are checked under transaction locks."""
    original = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    await SubscriptionStore(harness.store).set_state(_WS, "schedule", state)
    with pytest.raises(ImmutableViolation):
        await _advance(harness, original, lease)
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == ()


async def test_active_configuration_revision_is_fenced(harness: Harness) -> None:
    """A still-due active schedule cannot fire with a superseded configuration snapshot."""
    original = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    before = await harness.store.get_schedule(_WS, "schedule")
    await SubscriptionStore(harness.store).update(
        original.model_copy(update={"selector": "event.data.key == 'new'"}),
        expected_revision=original.store_revision,
        preserve_state=True,
    )
    with pytest.raises(ImmutableViolation):
        await _advance(harness, original, lease)
    assert await harness.store.get_schedule(_WS, "schedule") == before
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == ()


async def test_pending_pages_and_workspace_isolation(harness: Harness) -> None:
    """Recovery cursors and acknowledgment cannot cross a workspace or scheduler deployment."""
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    for identifier in ("a", "b"):
        await _advance(harness, await _due(harness, identifier), lease)
    foreign = await _advance(harness, await _due(harness, "a", WorkspaceId("other")), lease)
    page = await harness.store.query_pending_schedule_fires(_WS, "d", limit=1)
    assert len(page.items) == 1 and page.next_cursor is not None
    last = await harness.store.query_pending_schedule_fires(
        _WS, "d", limit=1, cursor=page.next_cursor
    )
    assert len(last.items) == 1 and last.next_cursor is None
    for workspace, deployment in ((WorkspaceId("other"), "d"), (_WS, "other")):
        with pytest.raises(ValueError):
            await harness.store.query_pending_schedule_fires(
                workspace, deployment, cursor=page.next_cursor
            )
    with pytest.raises(ImmutableViolation):
        await harness.store.acknowledge_schedule_fire(
            WorkspaceId("missing"), foreign.fire_id, lease
        )
    assert (
        len((await harness.store.query_pending_schedule_fires(WorkspaceId("other"), "d")).items)
        == 1
    )


@pytest.mark.integration
async def test_scheduler_schema_is_additive_and_required(pg_pool: Pool) -> None:
    """Older metadata consumers keep their minimum; scheduler readiness requires revision six."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.check_scheduler_ready()
    assert adapter.SCHEMA_REVISION == 4
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM custos_meta.adapter_revisions "
            "WHERE interface_name='MetadataStoreProvider' AND revision=6"
        )
    with pytest.raises(MigrationRequired):
        await adapter.check_scheduler_ready()
    await adapter.apply_pending()
    await adapter.check_scheduler_ready()


@pytest.mark.integration
async def test_pending_fire_rolls_back_if_schedule_write_fails(pg_pool: Pool) -> None:
    """Failure after outbox insertion cannot lose a fire or advance the schedule."""
    store = PgMetadataAdapter(pool=pg_pool)
    harness = Harness(store, [], pg_pool)
    subscription = await _due(harness)
    lease = await store.acquire_scheduler_lease("rollback", "a", ttl_seconds=30)
    assert lease is not None
    before = await store.get_schedule(_WS, "schedule")
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "ALTER TABLE custos_state.schedule ADD CONSTRAINT test_reject_advance "
            "CHECK (next_fire_at <= clock_timestamp())"
        )
    try:
        with pytest.raises(BackendUnavailable):
            await _advance(harness, subscription, lease)
        assert await store.get_schedule(_WS, "schedule") == before
        assert (await store.query_pending_schedule_fires(_WS, "rollback")).items == ()
    finally:
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE custos_state.schedule DROP CONSTRAINT test_reject_advance"
            )


@pytest.mark.integration
async def test_clock_is_rechecked_after_waiting_for_schedule_locks(pg_pool: Pool) -> None:
    """A transaction begun while live must fail if its subscription lock arrives after expiry."""
    from asyncpg import LockNotAvailableError

    store = PgMetadataAdapter(pool=pg_pool)
    harness = Harness(store, [], pg_pool)
    subscription = await _due(harness)
    lease = await store.acquire_scheduler_lease("blocked", "a", ttl_seconds=2)
    assert lease is not None
    before = await store.get_schedule(_WS, "schedule")
    async with pg_pool.acquire() as blocker:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.fetchrow(
            "SELECT subscription_id FROM custos_state.subscription "
            "WHERE workspace_id=$1 AND subscription_id='schedule' FOR UPDATE",
            _WS,
        )
        task = asyncio.create_task(_advance(harness, subscription, lease))
        try:

            async def waiting_with_lease_lock() -> None:
                """Observe the operation's acquired lease lock, rather than assuming it started."""
                while True:
                    try:
                        async with pg_pool.acquire() as probe, probe.transaction():
                            await probe.fetchrow(
                                "SELECT * FROM custos_state.scheduler_lease "
                                "WHERE deployment_id='blocked' FOR UPDATE NOWAIT"
                            )
                    except LockNotAvailableError:
                        return
                    await asyncio.sleep(0)

            await asyncio.wait_for(waiting_with_lease_lock(), timeout=1)
            async with pg_pool.acquire() as clock:
                assert await clock.fetchval(
                    "SELECT expires_at > clock_timestamp() FROM custos_state.scheduler_lease "
                    "WHERE deployment_id='blocked'"
                )
                await clock.execute(
                    "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM "
                    "expires_at-clock_timestamp())) + 0.02) FROM custos_state.scheduler_lease "
                    "WHERE deployment_id='blocked'"
                )
        finally:
            await transaction.rollback()
            if not task.done():
                with suppress(LeaseExpired):
                    await asyncio.wait_for(task, timeout=5)
        with pytest.raises(LeaseExpired):
            await task
    assert await store.get_schedule(_WS, "schedule") == before
    assert (await store.query_pending_schedule_fires(_WS, "blocked")).items == ()
