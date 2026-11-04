"""Lifespan polling ownership, graceful drain, failed readiness and cancellation joining."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import ConnectorInstanceId, WorkspaceId

from custos_connector import create_app
from custos_connector.pull_runtime import PullRuntime
from custos_connector.runtime import HookRunResult
from custos_connector.settings import SettingsError
from tests._pull import PullClock, memory_runtime, seed_instance, settings, start_demand


def test_production_worker_cannot_select_a_noop_publisher() -> None:
    """Reject silent source-event loss before loading any production runtime resources."""
    with pytest.raises(SettingsError, match="real Dapr publisher"):
        replace(
            settings(),
            environment="production",
            pull_worker_enabled=True,
            dapr_http_endpoint="",
        )


async def test_real_runtime_rediscovers_and_polls_persisted_demand() -> None:
    """Starting the owner requires no manual registration or second poll timer."""
    providers, metadata, runner, _ = memory_runtime(PullClock())
    await seed_instance(providers)
    start_demand(metadata)
    assert providers.pull_loop_scheduler is not None
    assert providers.pull_demand_reconciler is not None
    runtime = PullRuntime(
        metadata,
        providers.pull_loop_scheduler,
        providers.pull_demand_reconciler,
        interval_seconds=0.01,
    )
    try:
        await runtime.start()
        async with asyncio.timeout(2):
            while not runner.calls:
                await asyncio.sleep(0.01)
        assert runtime.healthy
    finally:
        await runtime.aclose()
    assert not runtime.healthy and len(runner.calls) == 1
    with pytest.raises(RuntimeError, match="restarted"):
        await runtime.start()


async def test_shutdown_joins_every_concurrent_hook_before_closing_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fast cancelled hook must not detach a sibling still completing owned cleanup."""
    providers, metadata, runner, _ = memory_runtime(PullClock())
    assert providers.pull_demand_reconciler is not None
    for workspace in ("source-a", "source-b"):
        await seed_instance(providers, workspace=workspace)
        start_demand(metadata, workspace=workspace)
        await providers.pull_demand_reconciler.reconcile(workspace)
    entered, fast_done, slow_cleaning, release_slow, slow_done = (asyncio.Event() for _ in range(5))
    calls = 0
    order: list[str] = []

    async def hook(**kwargs: object) -> HookRunResult:
        """Model concurrent hooks with different cancellation cleanup times."""
        nonlocal calls
        slot = calls
        calls += 1
        if calls == 2:
            entered.set()
        try:
            await asyncio.Event().wait()
            raise AssertionError("hook should be cancelled")
        except asyncio.CancelledError:
            if slot == 0:
                fast_done.set()
            else:
                slow_cleaning.set()
                await release_slow.wait()
                order.append("slow-cleanup")
                slow_done.set()
            raise

    async def close_http() -> None:
        """Record whether borrowed transports outlive both dispatch children."""
        order.append("http")

    monkeypatch.setattr(runner, "run", hook)
    http = AsyncMock()
    http.aclose.side_effect = close_http
    app = create_app(
        settings=replace(settings(), pull_worker_enabled=True),
        providers=replace(providers, dapr_http_client=http),
    )
    lifetime = app.router.lifespan_context(app)
    await lifetime.__aenter__()
    app.state.pull_runtime.drain_seconds = 0.01
    shutdown: asyncio.Task[bool | None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        shutdown = asyncio.create_task(lifetime.__aexit__(None, None, None))
        await asyncio.wait_for(fast_done.wait(), 1)
        await asyncio.wait_for(slow_cleaning.wait(), 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(shutdown), 0.05)
        assert not order
        release_slow.set()
        await shutdown
        assert order == ["slow-cleanup", "http"]
    finally:
        release_slow.set()
        if shutdown is not None:
            await asyncio.gather(shutdown, return_exceptions=True)
            await asyncio.wait_for(slow_done.wait(), 1)
        else:
            await lifetime.__aexit__(None, None, None)


@pytest.mark.parametrize("force_cancel", [False, True])
async def test_shutdown_owns_drain_before_http_transport_closes(
    monkeypatch: pytest.MonkeyPatch, force_cancel: bool
) -> None:
    """Cancellation cleanup stays joined even when the caller cancels shutdown again."""
    providers, _, _, _ = memory_runtime(PullClock())
    assert providers.pull_loop_scheduler is not None
    entered, finish, cleaning = asyncio.Event(), asyncio.Event(), asyncio.Event()
    closed: list[str] = []

    async def tick() -> None:
        """Model an admitted hook whose cancellation still owns asynchronous cleanup."""
        entered.set()
        try:
            await finish.wait()
        except asyncio.CancelledError:
            cleaning.set()
            await finish.wait()
            closed.append("cleanup")
            raise
        closed.append("drained")

    async def close_http() -> None:
        """Record whether the lifespan kept transport ownership until work was joined."""
        closed.append("http")

    monkeypatch.setattr(providers.pull_loop_scheduler, "tick_once", tick)
    http = AsyncMock()
    http.aclose.side_effect = close_http
    providers = replace(providers, dapr_http_client=http)
    app = create_app(settings=replace(settings(), pull_worker_enabled=True), providers=providers)
    lifetime = app.router.lifespan_context(app)
    await lifetime.__aenter__()
    await entered.wait()
    runtime = app.state.pull_runtime
    assert isinstance(runtime, PullRuntime)
    runtime.drain_seconds = 0.01 if force_cancel else 5
    shutdown = asyncio.create_task(lifetime.__aexit__(None, None, None))
    try:
        if force_cancel:
            await asyncio.wait_for(cleaning.wait(), timeout=1)
            shutdown.cancel()
            await asyncio.sleep(0)
            shutdown.cancel()
            await asyncio.sleep(0)
        assert not shutdown.done() and "http" not in closed
        finish.set()
        if force_cancel:
            with pytest.raises(asyncio.CancelledError):
                await shutdown
        else:
            await shutdown
        assert closed == ["cleanup" if force_cancel else "drained", "http"]
    finally:
        finish.set()
        await asyncio.gather(shutdown, return_exceptions=True)


async def test_readiness_failure_never_starts_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing discovery capability/schema fails startup without an upstream effect."""
    providers, metadata, runner, _ = memory_runtime(PullClock())
    monkeypatch.setattr(
        metadata,
        "check_receiver_workspaces_ready",
        AsyncMock(side_effect=BackendUnavailable("storage unavailable")),
    )
    app = create_app(settings=replace(settings(), pull_worker_enabled=True), providers=providers)
    with pytest.raises(BackendUnavailable):
        async with app.router.lifespan_context(app):
            pytest.fail("unready runtime started")
    assert not runner.calls


async def test_worker_failure_is_unhealthy_and_surfaced_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected failures are neither silently retried nor lost in detached tasks."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    assert providers.pull_loop_scheduler is not None
    assert providers.pull_demand_reconciler is not None
    monkeypatch.setattr(
        providers.pull_loop_scheduler, "tick_once", AsyncMock(side_effect=ValueError("private"))
    )
    runtime = PullRuntime(metadata, providers.pull_loop_scheduler, providers.pull_demand_reconciler)
    await runtime.start()
    await asyncio.sleep(0)
    assert not runtime.healthy
    with pytest.raises(RuntimeError, match="receiver task failed"):
        await runtime.aclose()


def test_worker_activation_requires_durable_storage_and_positive_drain() -> None:
    """Authoring/control enablement and timer activation are separate explicit gates."""
    assert not settings().pull_worker_enabled
    with pytest.raises(SettingsError, match="durable pull"):
        replace(settings(), durable_pull_enabled=False, pull_worker_enabled=True)
    with pytest.raises(SettingsError, match="DRAIN"):
        replace(settings(), receiver_drain_seconds=0)


async def test_failed_workspace_does_not_starve_other_discovered_work(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Move past one provider failure while retaining its durable state for the next scan."""
    providers, metadata, _, _ = memory_runtime(PullClock())
    assert providers.pull_loop_scheduler is not None
    assert providers.pull_demand_reconciler is not None
    for workspace in ("a-unavailable", "b-ready"):
        await metadata.initialize_pull_control(
            WorkspaceId(workspace), ConnectorInstanceId("instance"), actor="creator"
        )
    reached = asyncio.Event()

    async def reconcile(workspace: str) -> int:
        """Fail only one workspace with a deliberately private backend diagnostic."""
        if workspace == "a-unavailable":
            raise BackendUnavailable("synthetic-private-diagnostic")
        reached.set()
        return 0

    monkeypatch.setattr(providers.pull_demand_reconciler, "reconcile", reconcile)
    worker = PullRuntime(
        metadata,
        providers.pull_loop_scheduler,
        providers.pull_demand_reconciler,
        interval_seconds=0.001,
    )
    try:
        await worker.start()
        await asyncio.wait_for(reached.wait(), timeout=1)
    finally:
        await worker.aclose()
    assert "synthetic-private-diagnostic" not in caplog.text
