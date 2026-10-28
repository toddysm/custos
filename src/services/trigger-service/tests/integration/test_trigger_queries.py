"""Identical trigger-query/store contracts against memory and real PostgreSQL.

Memory cases run in the default suite. PostgreSQL cases use the existing
integration database fixture and run in the hosted PostgreSQL service job.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

import pytest
from custos_pg import PgMetadataAdapter
from custos_spl.errors import QueryUnsupported
from custos_spl.ids import RunId, StepId, SubscriptionId, WorkspaceId
from custos_spl.interfaces.metadata_store import ResumeSubscription
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider
from custos_spl.pagination import Cursor

from custos_trigger.models import SourceType, Subscription, SubscriptionState
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.stores import ResumeReadUnsupportedError, ResumeSubscriptionStore
from custos_trigger.stores.base import TriggerMetadataStore
from custos_trigger.stores.schedules import ScheduleStore
from custos_trigger.stores.subscriptions import SubscriptionStore

_NOW = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
_WS = WorkspaceId("query-ws")


class QueryStore(TriggerMetadataStore, TriggerQueryProvider, Protocol):
    """The write and read surfaces exercised by the shared contract tests."""


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def query_dsn(request: pytest.FixtureRequest) -> str | None:
    """Resolve the database before entering the async test's event loop."""
    if request.param == "memory":
        return None
    return cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def backend(query_dsn: str | None) -> AsyncIterator[QueryStore]:
    """Yield either production provider against the same query assertions."""
    if query_dsn is None:
        yield InMemoryTriggerMetadataStore(now=lambda: _NOW)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=query_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        adapter = PgMetadataAdapter(pool=pool)
        await adapter.apply_pending()
        assert adapter.declared_revisions["MetadataStoreProvider"] == frozenset({1, 2, 3, 4, 5})
        yield adapter
    finally:
        await pool.close()


def _subscription(identifier: str, workspace: str = _WS) -> Subscription:
    """Make a stable subscription whose selector can be reauthored independently."""
    return Subscription(
        workspace_id=workspace,
        subscription_id=identifier,
        workflow_id="workflow",
        source_type=SourceType.MANUAL,
        selector="event.data.accepted == true",
        created_at=_NOW,
        updated_at=_NOW,
    )


async def test_subscription_snapshot_and_missing_workspace(backend: QueryStore) -> None:
    """Point reads use append order and never leak a colliding tenant identifier."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    original = _subscription("same")
    await store.create(original)
    await store.create(_subscription("same", "other"))
    changed = original.model_copy(update={"selector": "event.data.accepted == false"})
    earlier_clock = SubscriptionStore(backend, now=lambda: _NOW - timedelta(days=1))
    await earlier_clock.reauthor_selector(changed)
    assert await store.get(_WS, "same") == changed
    assert await store.get("other", "same") == _subscription("same", "other")
    assert await store.get("missing", "same") is None
    assert await store.get(_WS, "missing") is None


async def test_subscription_pages_and_state_filter(backend: QueryStore) -> None:
    """Scoped cursors preserve stable ordering, final-page shape and state filtering."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    for identifier in ("c", "a", "b"):
        await store.create(_subscription(identifier))
    await store.create(_subscription("a", "other"))
    await store.set_state(_WS, "b", SubscriptionState.PAUSED)
    first = await store.query(_WS, limit=1)
    assert [row.subscription_id for row in first.items] == ["a"]
    assert first.next_cursor is not None
    second = await store.query(_WS, limit=1, cursor=first.next_cursor)
    assert [row.subscription_id for row in second.items] == ["b"]
    third = await store.query(_WS, limit=1, cursor=second.next_cursor)
    assert [row.subscription_id for row in third.items] == ["c"]
    assert third.next_cursor is None
    active = await store.query(_WS, state=SubscriptionState.ACTIVE)
    assert [row.subscription_id for row in active.items] == ["a", "c"]
    assert all(row.selector == "event.data.accepted == true" for row in active.items)
    assert [row.subscription_id for row in await store.list_in_workspace(_WS)] == ["a", "b", "c"]
    assert (await store.query("missing")).items == ()
    for workspace, state in (("other", None), (_WS, SubscriptionState.ACTIVE)):
        with pytest.raises(ValueError, match="cursor"):
            await store.query(workspace, state=state, cursor=first.next_cursor)


async def test_subscription_enumeration_drains_provider_pages(backend: QueryStore) -> None:
    """The legacy internal receiver sees all candidates beyond the default page."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    for index in range(52):
        await store.create(_subscription(f"sub-{index:03d}"))
    assert len((await store.query(_WS)).items) == 50
    assert len(await store.list_in_workspace(_WS)) == 52


async def test_subscription_without_selector(backend: QueryStore) -> None:
    """A base row written before its first selector retains the historical defaults."""
    from custos_trigger.models import to_spl_subscription

    await backend.put_subscription(_WS, to_spl_subscription(_subscription("empty")))
    snapshot = await backend.get_subscription(_WS, SubscriptionId("empty"))
    assert snapshot is not None and snapshot.selector is None
    result = await SubscriptionStore(backend).get(_WS, "empty")
    assert result is not None and result.selector is None


async def _put_resume(
    backend: QueryStore,
    identifier: str,
    *,
    workspace: WorkspaceId = _WS,
    connector: str = "registry",
    modes: object = None,
    expires_at: datetime = _NOW + timedelta(days=1),
    legacy: bool = False,
) -> None:
    """Seed provider rows, including source associations awaiting task 023's wire model."""
    payload: dict[str, object] = {"eventKey": "event-key"}
    if not legacy:
        payload["source"] = {
            "connectorInstanceId": connector,
            "deliveryModes": ["push", "pull"] if modes is None else modes,
        }
    await backend.put_resume_subscription(
        workspace,
        ResumeSubscription(
            workspace,
            identifier,
            RunId("run"),
            StepId("step"),
            expires_at,
            payload,
        ),
    )


async def test_resume_source_filter_pagination_and_expiry(backend: QueryStore) -> None:
    """Only unexpired waits for the exact workspace, instance and delivery mode match."""
    for identifier in ("b", "a"):
        await _put_resume(backend, identifier)
    await _put_resume(backend, "a", workspace=WorkspaceId("other"))
    await _put_resume(backend, "wrong", connector="other-registry")
    await _put_resume(backend, "legacy", legacy=True)
    await _put_resume(backend, "push-only", modes=["push"])
    await _put_resume(backend, "malformed-modes", modes="pull")
    await _put_resume(backend, "expired", expires_at=_NOW)
    store = ResumeSubscriptionStore(backend)
    first = await store.query_for_source(
        _WS,
        connector_instance_id="registry",
        delivery_mode="pull",
        as_of=_NOW,
        limit=1,
    )
    assert [row.resume_id for row in first.items] == ["a"]
    assert first.next_cursor is not None
    second = await store.query_for_source(
        _WS,
        connector_instance_id="registry",
        delivery_mode="pull",
        as_of=_NOW,
        limit=1,
        cursor=first.next_cursor,
    )
    assert [row.resume_id for row in second.items] == ["b"]
    assert second.next_cursor is None
    assert await store.get("missing", "a") is None
    expired = await store.get(_WS, "expired")
    assert expired is not None and expired.expires_at == _NOW
    with pytest.raises(ValueError, match="cursor"):
        await store.query_for_source(
            _WS,
            connector_instance_id="registry",
            delivery_mode="push",
            as_of=_NOW,
            cursor=first.next_cursor,
        )
    with pytest.raises(ValueError, match="cursor"):
        await store.query_for_source(
            "other",
            connector_instance_id="registry",
            delivery_mode="pull",
            as_of=_NOW,
            cursor=first.next_cursor,
        )
    await store.cancel(_WS, "a")
    assert await store.get(_WS, "a") is None
    assert await store.get("other", "a") is not None


async def test_due_schedule_reads_and_scoped_pagination(backend: QueryStore) -> None:
    """Due enumeration excludes future/disabled rows, including exact-cutoff boundaries."""
    store = ScheduleStore(backend)
    for identifier, due, enabled in (
        ("b", _NOW, True),
        ("a", _NOW - timedelta(days=1), True),
        ("disabled", _NOW, False),
        ("future", _NOW + timedelta(seconds=1), True),
    ):
        await store.put(
            workspace_id=_WS,
            schedule_id=identifier,
            workflow_id="workflow",
            cron="0 * * * *",
            next_fire_at=due,
            enabled=enabled,
        )
    await store.put(
        workspace_id="other",
        schedule_id="a",
        workflow_id="other-workflow",
        cron="0 * * * *",
        next_fire_at=_NOW,
    )
    first = await store.query_due(_WS, due_at=_NOW, limit=1)
    assert [row.schedule_id for row in first.items] == ["a"]
    second = await store.query_due(_WS, due_at=_NOW, limit=1, cursor=first.next_cursor)
    assert [row.schedule_id for row in second.items] == ["b"]
    assert second.next_cursor is None
    assert await store.get("missing", "a") is None
    disabled = await store.get(_WS, "disabled")
    assert disabled is not None and not disabled.enabled
    with pytest.raises(ValueError, match="cursor"):
        await store.query_due("other", due_at=_NOW, cursor=first.next_cursor)
    await backend.update_schedule_next_fire(_WS, "a", _NOW + timedelta(hours=1))
    assert [row.schedule_id for row in (await store.query_due(_WS, due_at=_NOW)).items] == ["b"]


async def test_query_validation(backend: QueryStore) -> None:
    """Malformed cursors, invalid page sizes and naive timestamps fail explicitly."""
    for token in ("not-base64!", "e30=", "WzEsW10sMF0="):
        with pytest.raises(ValueError, match="cursor"):
            await backend.query_subscriptions(_WS, cursor=Cursor(token))
    with pytest.raises(ValueError, match="positive"):
        await backend.query_subscriptions(_WS, limit=0)
    with pytest.raises(ValueError, match="positive"):
        await backend.query_due_schedules(_WS, due_at=_NOW, limit=-1)
    with pytest.raises(ValueError, match="timezone-aware"):
        await backend.query_due_schedules(_WS, due_at=_NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timezone-aware"):
        await backend.query_resume_subscriptions(
            _WS,
            connector_instance_id="registry",
            delivery_mode="pull",
            as_of=_NOW.replace(tzinfo=None),
        )
    with pytest.raises(ValueError, match="connector"):
        await backend.query_resume_subscriptions(
            _WS,
            connector_instance_id="",
            delivery_mode="pull",
            as_of=_NOW,
        )


async def test_write_only_providers_fail_explicitly() -> None:
    """Store query methods never turn unsupported capabilities into empty success."""
    write_only = cast(TriggerMetadataStore, object())
    with pytest.raises(ResumeReadUnsupportedError):
        await ResumeSubscriptionStore(write_only).query_for_source(
            _WS,
            connector_instance_id="registry",
            delivery_mode="push",
            as_of=_NOW,
        )
    with pytest.raises(QueryUnsupported):
        await ScheduleStore(write_only).get(_WS, "schedule")
    with pytest.raises(QueryUnsupported):
        await ScheduleStore(write_only).query_due(_WS, due_at=_NOW)
