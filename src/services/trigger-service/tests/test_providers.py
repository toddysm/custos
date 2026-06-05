"""Provider wiring + in-memory backend semantics (TS-IMPL-008)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.ids import WorkflowId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    DedupDuplicate,
    DedupReserved,
)
from custos_spl.interfaces.metadata_store import (
    Schedule as SplSchedule,
)

from custos_trigger.providers import (
    InMemoryTriggerMetadataStore,
    Providers,
    is_memory_dsn,
    load_providers,
)
from custos_trigger.stores.base import TriggerMetadataStore

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now


# --- is_memory_dsn / load_providers ------------------------------------------


@pytest.mark.parametrize(
    "dsn",
    ["", "   ", "memory", "MEMORY", "in-memory", "inmemory", "memory://local"],
)
def test_is_memory_dsn_true(dsn: str) -> None:
    assert is_memory_dsn(dsn) is True


@pytest.mark.parametrize(
    "dsn",
    ["postgres://u@h/db", "postgresql://u@h/db", "host=localhost dbname=custos"],
)
def test_is_memory_dsn_false(dsn: str) -> None:
    assert is_memory_dsn(dsn) is False


def test_load_providers_defaults_to_in_memory() -> None:
    providers = load_providers("")
    assert isinstance(providers, Providers)
    assert isinstance(providers.metadata_store, InMemoryTriggerMetadataStore)


def test_load_providers_memory_sentinel() -> None:
    providers = load_providers("memory://")
    assert isinstance(providers.metadata_store, InMemoryTriggerMetadataStore)


def test_load_providers_postgres_dsn_binds_pg_adapter() -> None:
    from custos_pg import PgMetadataAdapter

    providers = load_providers("postgresql://user@localhost:5432/custos")
    # LazyPool defers connection, so construction opens no socket.
    assert isinstance(providers.metadata_store, PgMetadataAdapter)


def test_in_memory_store_satisfies_protocol() -> None:
    assert isinstance(InMemoryTriggerMetadataStore(), TriggerMetadataStore)


# --- Dedup semantics ---------------------------------------------------------


async def test_put_dedup_key_reserves_then_duplicates() -> None:
    clock = _Clock(_NOW)
    store = InMemoryTriggerMetadataStore(now=clock)
    ws = WorkspaceId("ws-1")

    first = await store.put_dedup_key(ws, "evt-1", ttl_seconds=60)
    assert isinstance(first, DedupReserved)
    assert first.key.expires_at == _NOW + timedelta(seconds=60)
    assert store.dedup_key("ws-1", "evt-1") is not None

    second = await store.put_dedup_key(ws, "evt-1", ttl_seconds=60)
    assert isinstance(second, DedupDuplicate)
    assert second.existing.key == "evt-1"


async def test_put_dedup_key_reserves_again_after_expiry() -> None:
    clock = _Clock(_NOW)
    store = InMemoryTriggerMetadataStore(now=clock)
    ws = WorkspaceId("ws-1")

    await store.put_dedup_key(ws, "evt-1", ttl_seconds=60)
    clock.now = _NOW + timedelta(seconds=61)

    again = await store.put_dedup_key(ws, "evt-1", ttl_seconds=60)
    assert isinstance(again, DedupReserved)


# --- Schedule next-fire updates ----------------------------------------------


async def test_update_schedule_next_fire() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    ws = WorkspaceId("ws-1")
    schedule = SplSchedule(
        workspace_id=ws,
        schedule_id="sched-1",
        workflow_id=WorkflowId("wf-1"),
        cron="0 * * * *",
        next_fire_at=_NOW,
        enabled=True,
    )
    await store.put_schedule(ws, schedule)
    new_fire = _NOW + timedelta(hours=1)

    updated = await store.update_schedule_next_fire(ws, "sched-1", new_fire)

    assert updated.next_fire_at == new_fire
    assert store.schedule("ws-1", "sched-1").next_fire_at == new_fire  # type: ignore[union-attr]


async def test_update_schedule_next_fire_unknown_raises() -> None:
    store = InMemoryTriggerMetadataStore()
    with pytest.raises(ValueError, match="unknown schedule"):
        await store.update_schedule_next_fire(WorkspaceId("ws-1"), "missing", _NOW)


async def test_in_memory_store_defaults_clock_to_utcnow() -> None:
    # The default (no injected clock) stamps ``updated_at`` from the wall
    # clock — exercise that branch via a real state transition.
    from custos_spl.ids import SubscriptionId
    from custos_spl.interfaces.metadata_store import Subscription as SplSubscription

    store = InMemoryTriggerMetadataStore()
    ws = WorkspaceId("ws-1")
    before = datetime.now(UTC)
    await store.put_subscription(
        ws,
        SplSubscription(
            workspace_id=ws,
            subscription_id=SubscriptionId("sub-1"),
            workflow_id=WorkflowId("wf-1"),
            state="active",
            created_at=before,
            updated_at=before,
        ),
    )

    updated = await store.update_subscription_state(ws, SubscriptionId("sub-1"), "paused")

    assert updated.state == "paused"
    assert updated.updated_at >= before
