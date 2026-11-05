"""Memory/PostgreSQL parity for bounded receiver discovery independent of live subscriptions."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from asyncpg.pool import Pool
from custos_pg import PgMetadataAdapter
from custos_spl.errors import MigrationRequired
from custos_spl.ids import SubscriptionId, WorkflowId, WorkspaceId
from custos_spl.interfaces.dead_letters import DeadLetterFilter
from custos_spl.interfaces.metadata_store import Subscription
from custos_spl.interfaces.receiver_workspaces import ReceiverWorkspaceProvider
from custos_spl.interfaces.source_delivery import SourceDeliverySelection
from custos_spl.pagination import Cursor, encode_query_cursor

from custos_trigger.providers import InMemoryTriggerMetadataStore
from tests.integration.test_dead_letter_state import capture

NOW = datetime(2026, 11, 2, tzinfo=UTC)
Store = InMemoryTriggerMetadataStore | PgMetadataAdapter


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def workspace_dsn(request: pytest.FixtureRequest) -> str | None:
    """Resolve the reset database before the test's event loop owns its pool."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def workspace_store(workspace_dsn: str | None) -> AsyncIterator[Store]:
    """Exercise the same public capability on both actual providers."""
    if workspace_dsn is None:
        yield InMemoryTriggerMetadataStore(now=lambda: NOW)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=workspace_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        yield PgMetadataAdapter(pool=pool)
    finally:
        await pool.close()


async def test_keyset_pages_are_distinct_bounded_and_scope_checked(workspace_store: Store) -> None:
    """Duplicate intent rows cannot duplicate workspaces or change the 200-ID upper bound."""
    assert isinstance(workspace_store, ReceiverWorkspaceProvider)
    await workspace_store.check_receiver_workspaces_ready()
    for index in range(205):
        ws = WorkspaceId(f"workspace-{index:03}")
        for identifier in ("first", "second"):
            await workspace_store.put_subscription(
                ws,
                Subscription(ws, SubscriptionId(identifier), WorkflowId("wf"), "active", NOW, NOW),
            )
    first = await workspace_store.query_receiver_workspaces(limit=10_000)
    assert len(first.items) == 200 and first.next_cursor is not None
    second = await workspace_store.query_receiver_workspaces(cursor=first.next_cursor, limit=200)
    assert len(second.items) == 5 and second.next_cursor is None
    assert list(first.items + second.items) == [f"workspace-{i:03}" for i in range(205)]
    for invalid in (Cursor("not-base64"), encode_query_cursor(("other-query",), "workspace-010")):
        with pytest.raises(ValueError):
            await workspace_store.query_receiver_workspaces(cursor=invalid)
    with pytest.raises(ValueError):
        await workspace_store.query_receiver_workspaces(limit=0)


async def test_recovery_only_workspaces_do_not_require_current_subscriptions(
    workspace_store: Store,
) -> None:
    """Retained work drives discovery even when no subscription remains."""
    now = await workspace_store.delivery_now()
    selection_ws = WorkspaceId("selection-only")
    await workspace_store.put_source_delivery(
        SourceDeliverySelection(selection_ws, "selection", "a" * 64, now + timedelta(days=1), ())
    )
    await workspace_store.put_dead_letter(capture(workspace=WorkspaceId("dead-letter-only")))
    await workspace_store.begin_bulk_replay(
        WorkspaceId("command-only"),
        "command",
        "actor",
        DeadLetterFilter(),
        cursor=None,
        limit=10,
        retention_seconds=300,
    )
    await workspace_store.reserve_delivery(
        WorkspaceId("receipt-only"), "trigger", "request", "b" * 64, now + timedelta(days=1)
    )
    page = await workspace_store.query_receiver_workspaces()
    assert page.items == ("command-only", "dead-letter-only", "receipt-only", "selection-only")
    assert page.next_cursor is None


@pytest.mark.integration
async def test_old_recovery_schema_blocks_discovery_readiness(pg_pool: object) -> None:
    """Do not start a scan against a deployment missing retained source selections."""
    assert isinstance(pg_pool, Pool)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM custos_meta.adapter_revisions "
            "WHERE interface_name='MetadataStoreProvider' AND revision=13"
        )
    with pytest.raises(MigrationRequired):
        await PgMetadataAdapter(pool=pg_pool).check_receiver_workspaces_ready()
