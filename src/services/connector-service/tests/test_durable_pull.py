"""Cross-replica demand, operator intent and publish-before-commit behavior in durable mode."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import timedelta
from typing import Any, NoReturn
from unittest.mock import AsyncMock

import httpx
import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.pull_control import ConnectorPullControl, PullTickBlocked
from custos_spl.pagination import Cursor, Page

from custos_connector import create_app
from custos_connector._telemetry import CURSOR_LAG_REGISTRY
from custos_connector.audit import EVENT_PULL_LOOP_PAUSED
from custos_connector.cursor.service import CursorInstanceUnavailable
from custos_connector.providers import wire_durable_pull_runtime
from custos_connector.runtime import (
    HookRunResult,
    PluginCleanupFailed,
    PluginInvoker,
    invocation_container_name,
)
from custos_connector.settings import SettingsError, load_settings
from tests._pull import (
    INSTANCE,
    MESH_HEADERS,
    WS,
    PullClock,
    memory_runtime,
    runner,
    seed_instance,
    settings,
    start_demand,
)


@pytest.mark.parametrize("resume_only", [False, True])
async def test_staggered_replicas_share_interval_after_fast_cursor_commit(
    resume_only: bool,
) -> None:
    """Fast completion cannot let a second replica poll again inside the same interval."""
    clock = PullClock()
    first, metadata, plugin_a, publisher = memory_runtime(clock)
    await seed_instance(first)
    if resume_only:
        metadata.pull_resumes[(WS, "wait")] = (
            clock() + timedelta(hours=1),
            {
                "source": {
                    "connectorInstanceId": INSTANCE,
                    "deliveryModes": ["pull"],
                    "pollIntervalSeconds": 10,
                }
            },
        )
    else:
        start_demand(metadata)
    plugin_b = runner()
    second = wire_durable_pull_runtime(first, settings(), plugin_invoker=PluginInvoker(plugin_b))
    for replica in (first, second):
        assert replica.pull_demand_reconciler is not None
        assert await replica.pull_demand_reconciler.reconcile(WS) == 1
    assert first.pull_loop_scheduler is not None and second.pull_loop_scheduler is not None
    assert (await first.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"
    clock.advance(1)
    assert not (await second.pull_loop_scheduler.tick_once()).outcomes
    assert len(plugin_a.calls) == 1 and not plugin_b.calls
    clock.advance(9)
    assert (await second.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"
    assert len(plugin_b.calls) == 1 and len(publisher.published) == 4


async def test_partial_publish_failure_preserves_cursor_and_shared_backoff() -> None:
    """A different replica retries the unchanged cursor only after the reserved failure interval."""
    clock = PullClock()
    first, metadata, plugin, publisher = memory_runtime(clock)
    await seed_instance(first)
    start_demand(metadata)
    assert first.pull_demand_reconciler is not None and first.pull_loop_scheduler is not None
    await first.pull_demand_reconciler.reconcile(WS)
    publisher.fail_at = 2
    assert (await first.pull_loop_scheduler.tick_once()).outcomes[0].status == "failed"
    cursor = await metadata.read_cursor(WS, INSTANCE)
    assert cursor is not None and cursor.value == "" and cursor.lease_holder is None
    replica = wire_durable_pull_runtime(first, settings(), plugin_invoker=PluginInvoker(plugin))
    assert replica.pull_demand_reconciler is not None and replica.pull_loop_scheduler is not None
    await replica.pull_demand_reconciler.reconcile(WS)
    assert not (await replica.pull_loop_scheduler.tick_once()).outcomes
    publisher.fail_at = None
    clock.advance(10)
    assert (await replica.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"
    cursor = await metadata.read_cursor(WS, INSTANCE)
    assert cursor is not None and cursor.value == '"cursor-2"'
    assert [event.event_id for event in publisher.published] == ["first", "first", "second"]
    assert (
        plugin.calls[0]["payload"]["input"]["cursor"]
        == plugin.calls[1]["payload"]["input"]["cursor"]
    )


async def test_reconciliation_tracks_pause_delete_expiry_and_native_listening() -> None:
    """Consumer mutation preserves operator authority and other registration owners."""
    clock = PullClock()
    providers, metadata, _, _ = memory_runtime(clock)
    instance = await seed_instance(providers)
    await seed_instance(providers, workspace="other")
    start_demand(metadata, interval=60)
    start_demand(metadata, identifier="faster", interval=20)
    start_demand(metadata, workspace="other")
    metadata.pull_resumes[(WS, "wait")] = (
        clock() + timedelta(seconds=5),
        {
            "source": {
                "connectorInstanceId": INSTANCE,
                "deliveryModes": ["pull"],
                "pollIntervalSeconds": 10,
            }
        },
    )
    reconciler, controls = providers.pull_demand_reconciler, providers.pull_control
    assert reconciler is not None and controls is not None
    assert await reconciler.reconcile(WS) == 1
    row = await controls.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    assert row is not None and row.effective_interval_seconds == 10
    clock.advance(5)
    await reconciler.reconcile(WS)
    row = await controls.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    assert row is not None and row.effective_interval_seconds == 20
    start_demand(metadata, identifier="faster", interval=20, state="paused")
    await reconciler.reconcile(WS)
    row = await controls.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    assert row is not None and row.effective_interval_seconds == 60
    assert providers.listen_manager is not None and providers.pull_loop_scheduler is not None
    await providers.listen_manager.activate(instance)
    metadata.pull_subscriptions.pop((WS, "start"))
    await reconciler.reconcile(WS)
    assert providers.pull_loop_scheduler.schedules()
    await providers.listen_manager.deactivate(WS, INSTANCE)
    assert not providers.pull_loop_scheduler.schedules()
    assert all(workspace == WS for workspace, _ in providers.pull_loop_scheduler.schedules())


async def test_private_reconciliation_and_atomic_pause_before_local_registration() -> None:
    """Pause persists without a scheduler, survives new replicas and requires explicit resume."""
    clock = PullClock()
    providers, metadata, plugin, _ = memory_runtime(clock)
    await seed_instance(providers, initialize=False)
    no_local_scheduler = replace(providers, pull_loop_scheduler=None)
    app = create_app(settings=settings(), providers=no_local_scheduler)
    context = {
        "x-custos-callctx": json.dumps(
            {
                "workspace_id": WS,
                "principal_id": "operator",
                "permissions": ["admin:connector"],
            }
        )
    }
    path = f"/v1/workspaces/{WS}/connectors/{INSTANCE}/pull-loop"
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://connector",
        ) as client,
    ):
        assert (await client.post(path + ":pause", headers=context)).status_code == 200
        row = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
        assert row is not None and row.operator_paused
        assert (
            sum(
                event.event_type == EVENT_PULL_LOOP_PAUSED
                for _, event in metadata.append_audit_calls
            )
            == 1
        )
        start_demand(metadata)
        denied = await client.post(
            "/ReconcilePullDemand",
            headers={
                **MESH_HEADERS,
                "dapr-caller-app-id": "api-gateway",
            },
            json={"schemaVersion": "1", "workspaceId": WS},
        )
        assert denied.status_code == 403
        refreshed = await client.post(
            "/ReconcilePullDemand",
            headers=MESH_HEADERS,
            json={"schemaVersion": "1", "workspaceId": WS},
        )
        assert refreshed.json() == {"schemaVersion": "1", "registered": 1}
        assert providers.pull_loop_scheduler is not None
        assert not (await providers.pull_loop_scheduler.tick_once()).outcomes and not plugin.calls
        restarted = wire_durable_pull_runtime(
            providers, settings(), plugin_invoker=PluginInvoker(plugin)
        )
        assert (
            restarted.pull_demand_reconciler is not None
            and restarted.pull_loop_scheduler is not None
        )
        await restarted.pull_demand_reconciler.reconcile(WS)
        assert not (await restarted.pull_loop_scheduler.tick_once()).outcomes
        assert (await client.post(path + ":resume", headers=context)).status_code == 200
        assert (await restarted.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"


async def test_failed_control_commit_never_acknowledges_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A storage failure cannot mutate local policy or return a success-shaped response."""
    clock = PullClock()
    providers, metadata, _, _ = memory_runtime(clock)
    await seed_instance(providers)
    before = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    monkeypatch.setattr(
        metadata, "set_pull_paused", AsyncMock(side_effect=BackendUnavailable("down"))
    )
    app = create_app(settings=settings(), providers=providers)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://connector",
        ) as client,
    ):
        response = await client.post(
            f"/v1/workspaces/{WS}/connectors/{INSTANCE}/pull-loop:pause",
            headers={
                "x-custos-callctx": json.dumps(
                    {
                        "workspace_id": WS,
                        "principal_id": "operator",
                        "permissions": ["admin:connector"],
                    }
                )
            },
        )
    assert response.status_code == 503
    assert await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE)) == before


@pytest.mark.parametrize(
    "body,status",
    [
        (b"{}", 400),
        (b'{"schemaVersion":"2","workspaceId":"pull-a"}', 400),
        (b'{"schemaVersion":"1","workspaceId":" "}', 400),
        (b'{"schemaVersion":"1","workspaceId":"pull-a","workspaceId":"other"}', 400),
        (b"x" * 16385, 413),
    ],
)
async def test_private_reconciliation_rejects_bad_commands(body: bytes, status: int) -> None:
    """Malformed commands never become successful empty demand updates."""
    providers, _, plugin, _ = memory_runtime(PullClock())
    app = create_app(settings=settings(), providers=providers)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://connector",
        ) as client,
    ):
        response = await client.post("/ReconcilePullDemand", content=body, headers=MESH_HEADERS)
    assert response.status_code == status and not plugin.calls


@pytest.mark.parametrize("change", ["flag", "publisher", "identity"])
def test_durable_mode_rejects_incomplete_configuration(change: str) -> None:
    """Invalid flags, no-op publication and absent identity fail before resources open."""
    env = {
        "CONN_CATALOG_STORE": "postgresql://unused/unused",
        "CONN_METADATA_STORE": "postgresql://unused/unused",
        "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
        "CONN_DURABLE_PULL_ENABLED": "true",
        "CONN_DAPR_HTTP_ENDPOINT": "http://127.0.0.1:3500",
        "APP_API_TOKEN": "unit-sidecar",
        "CONN_TRIGGER_NAMESPACE": "custos",
    }
    if change == "flag":
        env["CONN_DURABLE_PULL_ENABLED"] = "invalid"
    elif change == "publisher":
        env.pop("CONN_DAPR_HTTP_ENDPOINT")
    else:
        env.pop("CONN_TRIGGER_NAMESPACE")
    with pytest.raises(SettingsError):
        load_settings(env)


async def test_configured_floor_can_exceed_default_backoff_cap() -> None:
    """A valid longer floor is honored instead of rejected by the five-minute default cap."""
    clock = PullClock()
    providers, metadata, plugin, _ = memory_runtime(clock)
    await seed_instance(providers)
    start_demand(metadata)
    configured = replace(settings(), pull_loop_min_interval_sec=600)
    runtime = wire_durable_pull_runtime(providers, configured, plugin_invoker=PluginInvoker(plugin))
    assert runtime.pull_demand_reconciler is not None and runtime.pull_loop_scheduler is not None
    await runtime.pull_demand_reconciler.reconcile(WS)
    assert (await runtime.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"
    control = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    assert control is not None and control.next_eligible_at == clock() + timedelta(seconds=600)


def test_enabled_app_rejects_missing_injected_control_authority() -> None:
    """Injected compatibility providers cannot silently downgrade an enabled durable deployment."""
    providers, _, _, _ = memory_runtime(PullClock())
    with pytest.raises(ValueError, match="bound control"):
        create_app(settings=settings(), providers=replace(providers, pull_control=None))


async def test_lag_series_follows_remaining_shared_demand() -> None:
    """Native removal preserves an active Trigger series; final demand removal clears it."""
    scope = "lag-source"
    clock = PullClock()
    providers, metadata, _, _ = memory_runtime(clock)
    instance = await seed_instance(providers, workspace=scope)
    start_demand(metadata, workspace=scope)
    assert providers.pull_demand_reconciler is not None
    assert providers.listen_manager is not None and providers.pull_loop_scheduler is not None
    await providers.pull_demand_reconciler.reconcile(scope)
    await providers.listen_manager.activate(instance)
    CURSOR_LAG_REGISTRY.record_advanced(
        workspace_id=scope, instance_id=INSTANCE, advanced_at=clock()
    )
    try:
        await providers.listen_manager.deactivate(scope, INSTANCE)
        assert (scope, INSTANCE) in CURSOR_LAG_REGISTRY.snapshot()
        metadata.pull_subscriptions.pop((scope, "start"))
        await providers.pull_demand_reconciler.reconcile(scope)
        assert (scope, INSTANCE) not in CURSOR_LAG_REGISTRY.snapshot()
    finally:
        providers.pull_loop_scheduler.unregister(scope, INSTANCE)
        CURSOR_LAG_REGISTRY.forget(workspace_id=scope, instance_id=INSTANCE)


async def test_old_reconciliation_cannot_remove_new_native_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native registration newer than an enumeration survives that enumeration's cleanup."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    instance = await seed_instance(providers)
    start_demand(metadata)
    reconciler, scheduler, manager = (
        providers.pull_demand_reconciler,
        providers.pull_loop_scheduler,
        providers.listen_manager,
    )
    assert reconciler is not None and scheduler is not None and manager is not None
    await reconciler.reconcile(WS)
    metadata.pull_subscriptions.clear()
    query = metadata.query_pull_controls

    async def activate_after_snapshot(
        workspace_id: WorkspaceId,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorPullControl]:
        """Create newer durable/native intent after the old no-demand snapshot is read."""
        page = await query(workspace_id, cursor=cursor, limit=limit)
        await manager.activate(instance)
        return page

    monkeypatch.setattr(metadata, "query_pull_controls", activate_after_snapshot)
    await reconciler.reconcile(WS)
    assert (WS, INSTANCE) in scheduler.schedules()
    assert (await scheduler.tick_once()).outcomes[0].status == "ticked"


async def test_stale_refresh_cannot_remove_newer_native_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delayed no-demand revision cannot erase a newly applied active revision."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    instance = await seed_instance(providers)
    scheduler, manager = providers.pull_loop_scheduler, providers.listen_manager
    assert scheduler is not None and manager is not None
    await manager.activate(instance)
    await metadata.set_pull_native_demand(
        WorkspaceId(WS), ConnectorInstanceId(INSTANCE), interval_seconds=None, actor="another-pod"
    )
    read = metadata.read_pull_controls

    async def delayed_read(
        workspace_id: WorkspaceId,
        instance_ids: Sequence[ConnectorInstanceId],
    ) -> tuple[ConnectorPullControl, ...]:
        """Deliver an older row only after the activating caller has projected its new revision."""
        rows = await read(workspace_id, instance_ids)
        await manager.activate(instance)
        return rows

    monkeypatch.setattr(metadata, "read_pull_controls", delayed_read)
    assert (await scheduler.tick_once()).outcomes[0].status == "ticked"
    assert (WS, INSTANCE) in scheduler.schedules()


async def test_removed_registration_retains_its_revision_fence() -> None:
    """A stale active row cannot resurrect a registration after its newer removal."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    instance = await seed_instance(providers)
    scheduler, manager = providers.pull_loop_scheduler, providers.listen_manager
    assert scheduler is not None and manager is not None
    await manager.activate(instance)
    older = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    assert older is not None
    await manager.deactivate(WS, INSTANCE)
    scheduler.apply_pull_control(older)
    assert (WS, INSTANCE) not in scheduler.schedules()


async def test_refresh_batches_registered_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two hundred five registered controls require three bounded reads, not 205 transactions."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    scheduler = providers.pull_loop_scheduler
    assert scheduler is not None
    for index in range(205):
        row = await metadata.set_pull_native_demand(
            WorkspaceId(WS),
            ConnectorInstanceId(f"instance-{index:03}"),
            interval_seconds=10,
            actor="fixture",
        )
        scheduler.apply_pull_control(row)
    read = AsyncMock(wraps=metadata.read_pull_controls)
    point_read = AsyncMock(side_effect=AssertionError("per-instance refresh is forbidden"))
    monkeypatch.setattr(metadata, "read_pull_controls", read)
    monkeypatch.setattr(metadata, "get_pull_control", point_read)
    assert not (await scheduler.tick_once()).outcomes
    assert read.await_count == 3 and point_read.await_count == 0
    assert [len(call.args[1]) for call in read.await_args_list] == [100, 100, 5]


async def test_reconciliation_pages_instance_eligibility(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two hundred five controls require three eligibility pages, not 205 point lookups."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    for index in range(205):
        identifier = f"instance-{index:03}"
        await seed_instance(providers, identifier=identifier)
        await metadata.set_pull_native_demand(
            WorkspaceId(WS), ConnectorInstanceId(identifier), interval_seconds=10, actor="fixture"
        )
    instances = providers.instance_store
    paged = AsyncMock(wraps=instances.list_connector_instances)
    point = AsyncMock(side_effect=AssertionError("per-instance eligibility reads are forbidden"))
    monkeypatch.setattr(instances, "list_connector_instances", paged)
    monkeypatch.setattr(instances, "get_connector_instance", point)
    assert providers.pull_demand_reconciler is not None
    assert await providers.pull_demand_reconciler.reconcile(WS) == 205
    assert paged.await_count == 3 and point.await_count == 0


async def test_old_unavailable_result_cannot_remove_new_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old dispatched attempt cannot delete a registration refreshed while it was awaiting."""
    providers, _, _, _ = memory_runtime(PullClock())
    instance = await seed_instance(providers)
    scheduler, manager, cursor = (
        providers.pull_loop_scheduler,
        providers.listen_manager,
        providers.cursor_service,
    )
    assert scheduler is not None and manager is not None and cursor is not None
    await manager.activate(instance)
    tick = cursor.tick

    async def obsolete_result(workspace: str, identifier: str) -> NoReturn:
        """Publish a new registration before an obsolete lookup failure returns."""
        await manager.activate(instance)
        raise CursorInstanceUnavailable(identifier, "obsolete lookup")

    monkeypatch.setattr(cursor, "tick", obsolete_result)
    assert (await scheduler.tick_once()).outcomes[0].status == "unavailable"
    assert (WS, INSTANCE) in scheduler.schedules()
    monkeypatch.setattr(cursor, "tick", tick)
    assert (await scheduler.tick_once()).outcomes[0].status == "ticked"


@pytest.mark.parametrize("pause_failure", [False, True])
async def test_unconfirmed_cleanup_blocks_or_retains_ownership(
    monkeypatch: pytest.MonkeyPatch,
    pause_failure: bool,
) -> None:
    """An unconfirmed invocation is never followed by an unguarded ownership release."""
    clock = PullClock()
    providers, metadata, plugin, _ = memory_runtime(clock)
    await seed_instance(providers)
    start_demand(metadata)
    monkeypatch.setattr(plugin, "_exc", PluginCleanupFailed("unit-owned-container"))
    if pause_failure:
        monkeypatch.setattr(
            metadata, "set_pull_paused", AsyncMock(side_effect=BackendUnavailable("unavailable"))
        )
    assert (
        providers.pull_demand_reconciler is not None and providers.pull_loop_scheduler is not None
    )
    await providers.pull_demand_reconciler.reconcile(WS)
    assert (await providers.pull_loop_scheduler.tick_once()).outcomes[0].status == "failed"
    control = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
    cursor = await metadata.read_cursor(WS, INSTANCE)
    assert control is not None and cursor is not None
    assert control.operator_paused is not pause_failure
    assert cursor.lease_holder is not None and control.invocation_id == cursor.lease_holder
    clock.advance(61)
    with pytest.raises(PullTickBlocked, match="invocation_unconfirmed"):
        await metadata.acquire_pull_tick(
            WorkspaceId(WS), ConnectorInstanceId(INSTANCE), "new-replica", ttl_seconds=60
        )
    assert len(plugin.calls) == 1


async def test_remaining_lease_budget_reaches_plugin_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist the fence and bound the hook before any upstream invocation."""
    providers, metadata, plugin, _ = memory_runtime(PullClock())
    await seed_instance(providers)
    start_demand(metadata)
    run = plugin.run

    async def observe_claim(
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
        invocation_id: str | None = None,
    ) -> HookRunResult:
        """Inspect durable state at the actual runner boundary."""
        control = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
        assert control is not None and invocation_id is not None
        assert control.invocation_id == invocation_id
        return await run(
            image_ref=image_ref,
            hook=hook,
            payload=payload,
            timeout_seconds=timeout_seconds,
            invocation_id=invocation_id,
        )

    monkeypatch.setattr(plugin, "run", observe_claim)
    runtime = wire_durable_pull_runtime(
        providers, settings(), plugin_invoker=PluginInvoker(plugin, timeout_seconds=120)
    )
    assert runtime.pull_demand_reconciler is not None and runtime.pull_loop_scheduler is not None
    await runtime.pull_demand_reconciler.reconcile(WS)
    assert (await runtime.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"
    assert 0 < plugin.calls[0]["timeout_seconds"] <= 50


async def test_cancellation_retains_cursor_until_invocation_stops() -> None:
    """Ownership cannot be released while the runner is still completing cancellation."""
    clock = PullClock()
    providers, metadata, _, _ = memory_runtime(clock)
    await seed_instance(providers)
    start_demand(metadata)
    started, cleaning, stopped, allow_cleanup = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    class CancellingRunner:
        """Expose a deterministic cancellation barrier like the Docker runner's owned cleanup."""

        async def run(
            self,
            *,
            image_ref: str,
            hook: str,
            payload: Mapping[str, Any],
            timeout_seconds: float,
            invocation_id: str | None = None,
        ) -> NoReturn:
            """Do not propagate cancellation until the invocation has actually stopped."""
            started.set()
            try:
                await asyncio.Event().wait()
                raise AssertionError("unexpected invocation completion")
            except asyncio.CancelledError:
                cleaning.set()
                await allow_cleanup.wait()
                stopped.set()
                raise

    runtime = wire_durable_pull_runtime(
        providers, settings(), plugin_invoker=PluginInvoker(CancellingRunner())
    )
    assert runtime.pull_demand_reconciler is not None and runtime.cursor_service is not None
    await runtime.pull_demand_reconciler.reconcile(WS)
    task = asyncio.create_task(runtime.cursor_service.tick(WS, INSTANCE))
    try:
        async with asyncio.timeout(2):
            await started.wait()
            task.cancel()
            await cleaning.wait()
        cursor = await metadata.read_cursor(WS, INSTANCE)
        assert cursor is not None and cursor.lease_holder is not None and not stopped.is_set()
        allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        cursor = await metadata.read_cursor(WS, INSTANCE)
        assert stopped.is_set() and cursor is not None and cursor.lease_holder is None
        control = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
        assert control is not None and control.invocation_id is None
    finally:
        allow_cleanup.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_explicit_recovery_is_scoped_confirmed_and_generation_checked() -> None:
    """The existing admin endpoint exposes and recovers only the observed expired invocation."""
    clock = PullClock()
    providers, metadata, _, _ = memory_runtime(clock)
    await seed_instance(providers)
    start_demand(metadata)
    assert providers.pull_demand_reconciler is not None
    await providers.pull_demand_reconciler.reconcile(WS)
    _, lease = await metadata.acquire_pull_tick(
        WorkspaceId(WS), ConnectorInstanceId(INSTANCE), "old-executor", ttl_seconds=30
    )
    app = create_app(settings=settings(), providers=providers)
    context = {
        "x-custos-callctx": json.dumps(
            {
                "workspace_id": WS,
                "principal_id": "operator",
                "permissions": ["admin:connector", "connector:read"],
            }
        )
    }
    recovery = {
        "invocationId": lease.token,
        "confirmedStopped": True,
        "reason": "old executor terminated",
    }
    prefix = f"/v1/workspaces/{WS}/connectors/{INSTANCE}"
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://connector",
        ) as client,
    ):
        response = await client.get(prefix + "/cursor", headers=context)
        assert response.status_code == 200
        assert (await client.post(prefix + "/pull-loop:resume", json=recovery)).status_code == 401
        assert (
            await client.post(
                prefix.replace(WS, "other") + "/pull-loop:resume", json=recovery, headers=context
            )
        ).status_code == 403
        assert response.json()["pullControl"]["containerName"] == invocation_container_name(
            lease.token
        )
        assert response.json()["pullControl"]["invocationOwner"] == "old-executor"
        assert (
            await client.post(prefix + "/pull-loop:resume", json=recovery, headers=context)
        ).status_code == 409
        clock.advance(31)
        assert (await client.post(prefix + "/pull-loop:resume", headers=context)).status_code == 200
        with pytest.raises(PullTickBlocked, match="invocation_unconfirmed"):
            await metadata.acquire_pull_tick(
                WorkspaceId(WS), ConnectorInstanceId(INSTANCE), "new-executor", ttl_seconds=30
            )
        for invalid in ({**recovery, "confirmedStopped": False}, {"invocationId": lease.token}):
            assert (
                await client.post(prefix + "/pull-loop:resume", json=invalid, headers=context)
            ).status_code == 422
        assert (
            await client.post(prefix + "/pull-loop:resume", json=recovery, headers=context)
        ).status_code == 200
        _, newer = await metadata.acquire_pull_tick(
            WorkspaceId(WS), ConnectorInstanceId(INSTANCE), "new-executor", ttl_seconds=30
        )
        assert (
            await client.post(prefix + "/pull-loop:resume", json=recovery, headers=context)
        ).status_code == 409
        current = await metadata.get_pull_control(WorkspaceId(WS), ConnectorInstanceId(INSTANCE))
        assert current is not None and current.invocation_id == newer.token
