"""Identical trigger-query/store contracts against memory and real PostgreSQL.

Memory cases run in the default suite. PostgreSQL cases use the existing
integration database fixture and run in the hosted PostgreSQL service job.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import httpx
import pytest
from asyncpg import CheckViolationError
from asyncpg.pool import Pool
from custos_pg import PgConnectorInstanceAdapter, PgMetadataAdapter
from custos_spl.errors import ImmutableViolation, QueryUnsupported
from custos_spl.ids import (
    ConnectorInstanceId,
    RunId,
    StepId,
    SubscriptionId,
    WorkflowId,
    WorkspaceId,
)
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from custos_spl.interfaces.metadata_store import ResumeSubscription, Schedule
from custos_spl.interfaces.trigger_queries import ResumeRegistrationProvider, TriggerQueryProvider
from custos_spl.pagination import Cursor

from custos_trigger.api.routes import rpc
from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.dependencies import WorkflowCallerVerifier, get_resume_subscription_store
from custos_trigger.models import ResumeRegistration, SourceType, Subscription, SubscriptionState
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import ResumeReadUnsupportedError, ResumeSubscriptionStore
from custos_trigger.stores.base import TriggerMetadataStore
from custos_trigger.stores.resume import StoredResumeRegistration
from custos_trigger.stores.schedules import ScheduleStore
from custos_trigger.stores.subscriptions import SubscriptionStore

_NOW = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
_WS = WorkspaceId("query-ws")


class QueryStore(TriggerMetadataStore, TriggerQueryProvider, ResumeRegistrationProvider, Protocol):
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
        assert adapter.declared_revisions["MetadataStoreProvider"] == frozenset(
            {1, 2, 3, 4, 5, 6, 7}
        )
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


@pytest.mark.parametrize(
    "config", [SourceConfig(interval_seconds=30), SourceConfig(cron="*/5 * * * *", timezone="UTC")]
)
async def test_managed_schedule_lifecycle_is_atomic(
    backend: QueryStore, config: SourceConfig
) -> None:
    """Configuration, state and schedule eligibility move together without losing progress."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    original = await store.create(
        _subscription("scheduled").model_copy(
            update={"source_type": SourceType.SCHEDULED, "source_config": config}
        )
    )
    scheduled = await backend.get_schedule(_WS, original.subscription_id)
    assert scheduled is not None
    assert scheduled.cron == (config.cron or "")
    assert scheduled.enabled
    progress = replace(scheduled, next_fire_at=_NOW + timedelta(days=3))
    await backend.update_schedule_next_fire(_WS, progress.schedule_id, progress.next_fire_at)
    paused = await store.set_state(_WS, original.subscription_id, SubscriptionState.PAUSED)
    assert paused.source_config == config
    assert await backend.get_schedule(_WS, original.subscription_id) == replace(
        progress, enabled=False
    )
    active = await store.set_state(_WS, original.subscription_id, SubscriptionState.ACTIVE)
    assert await backend.get_schedule(_WS, original.subscription_id) == progress
    changed = await store.update(
        active.model_copy(update={"source_config": SourceConfig(interval_seconds=60)}),
        expected_revision=active.store_revision,
        reset_schedule=True,
    )
    schedule = await backend.get_schedule(_WS, original.subscription_id)
    assert schedule is not None and schedule.cron == ""
    assert schedule.next_fire_at == _NOW + timedelta(seconds=60)
    await store.set_state(_WS, original.subscription_id, SubscriptionState.EXPIRED)
    assert await backend.get_schedule(_WS, original.subscription_id) == replace(
        schedule, enabled=False
    )
    persisted = await store.get(_WS, original.subscription_id)
    assert persisted is not None and persisted.store_revision == changed.store_revision + 1
    assert persisted.source_config == changed.source_config


async def test_concurrent_subscription_updates_reject_stale_revision(backend: QueryStore) -> None:
    """A row-lock waiter must re-read the committed selector, not its old statement snapshot."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    original = await store.create(_subscription("concurrent"))
    results = await asyncio.gather(
        *(
            store.update(
                original.model_copy(update={"selector": selector}),
                expected_revision=original.store_revision,
            )
            for selector in ("true", "false")
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, ImmutableViolation) for result in results) == 1
    winners = [result for result in results if isinstance(result, Subscription)]
    assert len(winners) == 1
    assert await store.get(_WS, "concurrent") == winners[0]
    assert winners[0].store_revision == 2


@pytest.mark.parametrize("scheduled", [False, True])
async def test_metadata_patch_preserves_legacy_pause_and_delete(
    backend: QueryStore, scheduled: bool
) -> None:
    """A legacy base-row change cannot be overwritten by a metadata-only atomic patch."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    candidate = _subscription("legacy-state")
    if scheduled:
        candidate = candidate.model_copy(
            update={
                "source_type": SourceType.SCHEDULED,
                "source_config": SourceConfig(interval_seconds=30),
            }
        )
    stale = await store.create(candidate)
    for state in ("paused", "expired"):
        await backend.update_subscription_state(_WS, SubscriptionId("legacy-state"), state)
        updated = await store.update(
            stale.model_copy(update={"selector": f"event.data.state == '{state}'"}),
            expected_revision=stale.store_revision,
            preserve_state=True,
        )
        assert updated.state.value == state
        assert await store.get(_WS, "legacy-state") == updated
        if scheduled:
            schedule = await backend.get_schedule(_WS, "legacy-state")
            assert schedule is not None and not schedule.enabled
        stale = updated
    with pytest.raises(ImmutableViolation, match="terminal"):
        await store.set_state(_WS, "legacy-state", SubscriptionState.ACTIVE)


@pytest.mark.parametrize("workflow", ["workflow", "unrelated"])
async def test_unrelated_schedule_cannot_be_adopted(backend: QueryStore, workflow: str) -> None:
    """An existing schedule ID must not be overwritten even when its workflow happens to match."""
    unrelated = Schedule(_WS, "collision", WorkflowId(workflow), "0 * * * *", _NOW, True)
    await backend.put_schedule(_WS, unrelated)
    store = SubscriptionStore(backend, now=lambda: _NOW)
    with pytest.raises(ImmutableViolation, match="schedule"):
        await store.create(
            _subscription("collision").model_copy(
                update={
                    "source_type": SourceType.SCHEDULED,
                    "source_config": SourceConfig(interval_seconds=30),
                }
            )
        )
    assert await store.get(_WS, "collision") is None
    assert await backend.get_schedule(_WS, "collision") == unrelated


@pytest.mark.integration
async def test_schedule_failure_rolls_back_base_state_and_selector(pg_pool: Pool) -> None:
    """A PostgreSQL failure at the final write rolls back all earlier lifecycle changes."""
    backend = PgMetadataAdapter(pool=pg_pool)
    store = SubscriptionStore(backend, now=lambda: _NOW)
    original = await store.create(
        _subscription("rollback").model_copy(
            update={
                "source_type": SourceType.SCHEDULED,
                "source_config": SourceConfig(interval_seconds=30),
            }
        )
    )
    schedule = await backend.get_schedule(_WS, "rollback")
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "ALTER TABLE custos_state.schedule ADD CONSTRAINT test_reject_pause CHECK (enabled)"
        )
    try:
        with pytest.raises(CheckViolationError):
            await store.set_state(_WS, "rollback", SubscriptionState.PAUSED)
        assert await store.get(_WS, "rollback") == original
        assert await backend.get_schedule(_WS, "rollback") == schedule
    finally:
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE custos_state.schedule DROP CONSTRAINT test_reject_pause"
            )


@pytest.mark.integration
async def test_postgres_connector_reference_scoping(pg_pool: Pool) -> None:
    """Reference reads distinguish neither missing nor cross-tenant connector IDs."""
    reader = PgConnectorInstanceAdapter(pool=pg_pool)
    await reader.apply_pending()
    instance = ConnectorInstance(
        workspace_id=_WS,
        instance_id=ConnectorInstanceId("registry"),
        type="registry",
        version="1",
        name=None,
        lease_ttl_seconds=None,
        enabled=True,
        status="active",
        health_status=None,
        target_config={},
        credentials_authentication={},
        used_capabilities=None,
        created_at=_NOW,
        updated_at=_NOW,
    )
    await reader.put_connector_instance(_WS, instance)
    assert await reader.has_connector_instance(_WS, instance.instance_id)
    assert not await reader.has_connector_instance(WorkspaceId("other"), instance.instance_id)
    assert not await reader.has_connector_instance(_WS, ConnectorInstanceId("missing"))
    await reader.put_connector_instance(
        WorkspaceId("other"), replace(instance, workspace_id=WorkspaceId("other"))
    )
    assert await reader.has_connector_instance(WorkspaceId("other"), instance.instance_id)


async def test_subscription_snapshot_and_missing_workspace(backend: QueryStore) -> None:
    """Point reads use append order and never leak a colliding tenant identifier."""
    store = SubscriptionStore(backend, now=lambda: _NOW)
    original = await store.create(_subscription("same"))
    await store.create(_subscription("same", "other"))
    changed = original.model_copy(
        update={"selector": "event.data.accepted == false", "updated_at": _NOW - timedelta(days=1)}
    )
    earlier_clock = SubscriptionStore(backend, now=lambda: _NOW - timedelta(days=1))
    await earlier_clock.reauthor_selector(changed)
    same = await store.get(_WS, "same")
    other = await store.get("other", "same")
    assert same is not None and same.model_dump() == changed.model_dump()
    assert same.store_revision == 2
    assert other is not None and other.model_dump() == _subscription("same", "other").model_dump()
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
    connector: object = "registry",
    modes: object = None,
    expires_at: datetime = _NOW + timedelta(days=1),
    legacy: bool = False,
) -> None:
    """Seed valid associations plus malformed legacy shapes for query filtering."""
    payload: dict[str, object] = {
        "eventKey": "event-key",
        "selector": "event.data.accepted == true",
    }
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


async def test_resume_source_requires_string_identifier(backend: QueryStore) -> None:
    """Malformed JSON source values must not coerce into another connector's ID."""
    await _put_resume(backend, "numeric", connector=123)
    await _put_resume(backend, "string", connector="123")
    page = await backend.query_resume_subscriptions(
        _WS, connector_instance_id="123", delivery_mode="pull", as_of=_NOW
    )
    assert [row.resume_id for row in page.items] == ["string"]


async def test_resume_expiry_uses_instants_during_repeated_hour(backend: QueryStore) -> None:
    """A first-fold expiry is past even when its wall time is later than the cutoff."""
    zone = ZoneInfo("America/New_York")
    cutoff = datetime(2026, 11, 1, 1, 15, tzinfo=zone, fold=1)
    await _put_resume(
        backend,
        "expired-first-fold",
        expires_at=datetime(2026, 11, 1, 1, 45, tzinfo=zone, fold=0),
    )
    await _put_resume(
        backend,
        "live-second-fold",
        expires_at=datetime(2026, 11, 1, 1, 30, tzinfo=zone, fold=1),
    )
    page = await backend.query_resume_subscriptions(
        _WS, connector_instance_id="registry", delivery_mode="pull", as_of=cutoff
    )
    assert [row.resume_id for row in page.items] == ["live-second-fold"]


async def test_schedule_due_uses_instants_during_repeated_hour(backend: QueryStore) -> None:
    """The memory scheduler's due set matches PostgreSQL across a repeated local hour."""
    zone = ZoneInfo("America/New_York")
    cutoff = datetime(2026, 11, 1, 1, 15, tzinfo=zone, fold=1)
    store = ScheduleStore(backend)
    for identifier, due in (
        ("due-first-fold", datetime(2026, 11, 1, 1, 45, tzinfo=zone, fold=0)),
        ("future-second-fold", datetime(2026, 11, 1, 1, 30, tzinfo=zone, fold=1)),
    ):
        await store.put(
            workspace_id=_WS,
            schedule_id=identifier,
            workflow_id="workflow",
            cron="0 * * * *",
            next_fire_at=due,
        )
    page = await store.query_due(_WS, due_at=cutoff)
    assert [row.schedule_id for row in page.items] == ["due-first-fold"]


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
    with pytest.raises(ResumeReadUnsupportedError, match="atomic"):
        await ResumeSubscriptionStore(write_only).register_or_get(
            ResumeRegistration(run_id="run", step_id="step", event_key="approval"),
            workspace_id=_WS,
            resume_id="registration",
            expires_at=_NOW,
            as_of=_NOW,
        )


class _ConcurrentResumeStore(ResumeSubscriptionStore):
    """Force two requests to observe the same initial row before either writes."""

    def __init__(self, backend: TriggerMetadataStore) -> None:
        """Create a two-request rendezvous around the production read method."""
        super().__init__(backend)
        self.barrier = asyncio.Barrier(2)

    async def get(self, workspace_id: str, resume_id: str) -> StoredResumeRegistration | None:
        """Return the captured initial snapshot only after the other request has read it."""
        existing = await super().get(workspace_id, resume_id)
        await asyncio.wait_for(self.barrier.wait(), 5)
        return existing


class _ResumeAudit:
    """Capture divergent-selector events from the real HTTP registration route."""

    def __init__(self, workspace_id: str = rpc.RESUME_WORKSPACE) -> None:
        """Start with no observed divergence."""
        self.events: list[Mapping[str, object]] = []
        self.workspace_id = workspace_id

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, object]
    ) -> None:
        """Verify and record the registration conflict audit."""
        assert event_name == rpc.AUDIT_RESUME_DIVERGENT
        assert workspace_id == self.workspace_id
        self.events.append(attributes)


class _SourceReferences:
    """Expose only the two scoped instances used by concurrent registration tests."""

    async def has_connector_instance(
        self, workspace_id: WorkspaceId, instance_id: ConnectorInstanceId
    ) -> bool:
        """Validate the test's exact tenant and identifiers without credentials."""
        return workspace_id == _WS and instance_id in ("a", "b")


@pytest.mark.integration
@pytest.mark.parametrize("ttl", ["PT1H", "PT0.5S", "PT1.5S"])
async def test_workflow_client_round_trips_real_trigger_routes(
    backend: QueryStore,
    monkeypatch: pytest.MonkeyPatch,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
    ttl: str,
) -> None:
    """Run the production Workflow transport through real Trigger routes and provider reads."""
    from custos_spl.interfaces.trigger_source import ResumeSource
    from custos_workflow.clients._dapr_invoke import DaprInvokeEndpoint
    from custos_workflow.clients.trigger import (
        CancelResumeSubscriptionRequest,
        DaprTriggerServiceClient,
        RegisterResumeSubscriptionRequest,
    )
    from custos_workflow.runs.wait import parse_wait_duration

    monkeypatch.setattr(rpc, "_now", lambda: _NOW)
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=backend, connector_references=_SourceReferences()),
        dispatcher=Dispatcher(FakeWorkflowServiceClient(), Deduplicator(backend)),
        receiver_configuration_enabled=True,
        workflow_caller_verifier=workflow_auth[0],
    )
    transport = httpx.ASGITransport(app=app)

    async def forward(request: httpx.Request) -> httpx.Response:
        """Forward a bare method path as Dapr does, rather than retaining an ASGI mount prefix."""
        prefix = "/v1.0/invoke/trigger-service/method"
        assert request.url.path.startswith(prefix + "/")
        forwarded = httpx.Request(
            request.method,
            request.url.copy_with(path=request.url.path.removeprefix(prefix)),
            headers=request.headers,
            content=await request.aread(),
        )
        return await transport.handle_async_request(forwarded)

    source = ResumeSource("a", ("push", "pull"), 10)
    store = ResumeSubscriptionStore(backend)
    async with (
        app.router.lifespan_context(app),
        transport,
        httpx.AsyncClient(transport=httpx.MockTransport(forward), headers=workflow_auth[1]) as http,
    ):
        client = DaprTriggerServiceClient(
            http_client=http,
            endpoint=DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="trigger-service"),
        )
        response = await client.register_resume_subscription(
            RegisterResumeSubscriptionRequest(
                run_id="contract-run",
                step_id="wait",
                event_key="event",
                ttl=ttl,
                selector="event.data.key == 'one'",
                workspace_id=_WS,
                source=source,
            )
        )
        saved = await store.get(_WS, response.ts_subscription_id)
        assert saved is not None and saved.registration.source is not None
        assert saved.registration.source.to_source() == source
        assert saved.registration.workspace_id == _WS
        assert saved.expires_at == _NOW + parse_wait_duration("wait", ttl)
        matches = await backend.query_resume_subscriptions(
            _WS, connector_instance_id="a", delivery_mode="pull", as_of=_NOW
        )
        assert len(matches.items) == 1
        await client.cancel_resume_subscription(
            CancelResumeSubscriptionRequest(
                run_id="contract-run", step_id="wait", event_key="event", workspace_id=_WS
            )
        )
        assert await store.get(_WS, response.ts_subscription_id) is None


@pytest.mark.parametrize("expired", [False, True])
async def test_concurrent_external_source_rebinding_is_rejected(
    backend: QueryStore,
    monkeypatch: pytest.MonkeyPatch,
    expired: bool,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> None:
    """Racing sources cannot overwrite one another, even when refreshing an expired key."""
    monkeypatch.setattr(rpc, "_now", lambda: _NOW)
    triple = {"runId": "external-race", "stepId": "step", "eventKey": "event"}
    identifier = rpc.compute_resume_id("external-race", "step", "event")
    body = {**triple, "workspaceId": str(_WS), "selector": "event.data.key == 'one'"}
    source_a = {"connectorInstanceId": "a", "deliveryModes": ["push"]}
    plain = ResumeSubscriptionStore(backend)
    if expired:
        await plain.register(
            ResumeRegistration.model_validate({**body, "source": source_a}),
            workspace_id=_WS,
            resume_id=identifier,
            expires_at=_NOW,
        )
    store = _ConcurrentResumeStore(backend)
    audit = _ResumeAudit(_WS)
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=backend, connector_references=_SourceReferences()),
        audit_sink=audit,
        dispatcher=Dispatcher(FakeWorkflowServiceClient(), Deduplicator(backend)),
        receiver_configuration_enabled=True,
        workflow_caller_verifier=workflow_auth[0],
    )
    app.dependency_overrides[get_resume_subscription_store] = lambda: store
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger.test",
            headers=workflow_auth[1],
        ) as client,
    ):
        responses = await asyncio.gather(
            client.post(rpc.REGISTER_RESUME_PATH, json={**body, "source": source_a}),
            client.post(
                rpc.REGISTER_RESUME_PATH,
                json={
                    **body,
                    "source": {"connectorInstanceId": "b", "deliveryModes": ["push"]},
                },
            ),
        )
    assert sorted(response.status_code for response in responses) == [200, 409], [
        response.text for response in responses
    ]
    saved = await plain.get(_WS, identifier)
    assert saved is not None and saved.registration.source is not None
    if expired:
        assert saved.registration.source.connector_instance_id == "a"
    assert len(audit.events) == 1
    assert audit.events[0]["reason"] == "source_rebinding"
    assert audit.events[0]["originalSource"] == saved.registration.source.to_source().to_wire()


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("divergent", [False, True])
async def test_concurrent_resume_registration_is_original_wins(
    backend: QueryStore, monkeypatch: pytest.MonkeyPatch, expired: bool, divergent: bool
) -> None:
    """Concurrent new/expired requests return one handle and preserve the winning selector."""
    monkeypatch.setattr(rpc, "_now", lambda: _NOW)
    resume_id = rpc.compute_resume_id("race-run", "step", "approval")
    plain_store = ResumeSubscriptionStore(backend)
    if expired:
        await plain_store.register(
            ResumeRegistration(run_id="race-run", step_id="step", event_key="approval"),
            workspace_id=rpc.RESUME_WORKSPACE,
            resume_id=resume_id,
            expires_at=_NOW,
        )
    store = _ConcurrentResumeStore(backend)
    audit = _ResumeAudit()
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=backend),
        audit_sink=audit,
        dispatcher=Dispatcher(FakeWorkflowServiceClient(), Deduplicator(backend)),
    )
    app.dependency_overrides[get_resume_subscription_store] = lambda: store
    selector = "event.data.region == 'emea'"
    other_selector = "event.data.region == 'apac'" if divergent else selector
    body = {"runId": "race-run", "stepId": "step", "eventKey": "approval", "ttl": "PT1H"}
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://trigger.test"
        ) as client,
    ):
        responses = await asyncio.gather(
            client.post(rpc.REGISTER_RESUME_PATH, json={**body, "selector": selector}),
            client.post(rpc.REGISTER_RESUME_PATH, json={**body, "selector": other_selector}),
            return_exceptions=True,
        )
    for response in responses:
        assert isinstance(response, httpx.Response), repr(response)
        assert response.status_code == 200, response.text
        assert response.json() == {"subscriptionId": resume_id}
    stored = await plain_store.get(rpc.RESUME_WORKSPACE, resume_id)
    assert stored is not None
    assert stored.expires_at == _NOW + timedelta(hours=1)
    assert stored.registration.selector in (selector, other_selector)
    assert len(audit.events) == int(divergent)
    if divergent:
        assert audit.events[0]["originalSelector"] == stored.registration.selector
        assert audit.events[0]["replaySelector"] != stored.registration.selector


async def test_atomic_registration_retains_live_expiry(backend: QueryStore) -> None:
    """An idempotent retry cannot extend a live wait or replace its selector."""
    store = ResumeSubscriptionStore(backend)
    original = ResumeRegistration(
        run_id="run",
        step_id="step",
        event_key="approval",
        selector="event.data.region == 'emea'",
    )
    first = await store.register_or_get(
        original,
        workspace_id=_WS,
        resume_id="stable",
        expires_at=_NOW + timedelta(hours=1),
        as_of=_NOW,
    )
    replay = await store.register_or_get(
        original.model_copy(update={"selector": "event.data.region == 'apac'"}),
        workspace_id=_WS,
        resume_id="stable",
        expires_at=_NOW + timedelta(days=1),
        as_of=_NOW,
    )
    assert replay == first
    assert replay.expires_at == _NOW + timedelta(hours=1)
    with pytest.raises(ValueError, match="timezone-aware"):
        await store.register_or_get(
            original,
            workspace_id=_WS,
            resume_id="stable",
            expires_at=_NOW,
            as_of=_NOW.replace(tzinfo=None),
        )
