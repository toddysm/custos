"""Owned receiver lifecycle, finite scan continuation and explicit failure/activation contracts."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.source_delivery import SourceDeliverySelection
from custos_spl.pagination import Cursor

from custos_trigger._telemetry import DurableAuditSink
from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.pull_demand import DaprPullDemandClient
from custos_trigger.clients.workflow import DaprEndpoint
from custos_trigger.dependencies import WorkflowCallerVerifier
from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SourceType, Subscription
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.pipeline.reliable import ReliableDispatcher, SourceReconciliationPage
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.receiver_runtime import ReceiverRuntime
from custos_trigger.settings import (
    ReceiverWorkerSettings,
    SettingsError,
    read_receiver_worker_settings,
)
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import SubscriptionStore
from tests._source_helpers import NOW, dispatcher


def runtime(
    store: InMemoryTriggerMetadataStore, client: httpx.AsyncClient, published: list[NormalizedEvent]
) -> ReceiverRuntime:
    """Bind the real scheduler and durable services with an explicitly borrowed mock HTTP client."""

    async def publish(event: NormalizedEvent) -> None:
        """Record acknowledged publication without performing downstream effects in this fixture."""
        published.append(event)

    return ReceiverRuntime(
        store,
        ReliableDispatcher(FakeWorkflowServiceClient(), store, NoopAuditSink(), max_retries=0),
        DaprPullDemandClient(client, DaprEndpoint("127.0.0.1", 3500, "connector-service")),
        publish,
        NoopAuditSink(),
        deployment_id="unit-receivers",
        interval_seconds=0.001,
    )


def acknowledge(_: httpx.Request) -> httpx.Response:
    """Acknowledge only the synthetic reconciliation command used by these tests."""
    return httpx.Response(200, json={"schemaVersion": "1", "registered": 0})


def test_production_receivers_cannot_omit_execution_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not start a production source path that can never obtain fresh scoped authority."""
    monkeypatch.delenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", raising=False)
    monkeypatch.delenv("CUSTOS_EXECUTION_BINDINGS", raising=False)
    with pytest.raises(ValueError, match="execution identities"):
        create_app(environment="production", source_receivers_enabled=True)


async def test_scheduled_worker_advances_past_the_first_due_page() -> None:
    """More than 50 due schedules finish instead of repeatedly restarting at the first page."""
    clock = [NOW]
    store = InMemoryTriggerMetadataStore(now=lambda: clock[0])
    subscriptions = SubscriptionStore(store, now=lambda: clock[0])
    for index in range(55):
        await subscriptions.create(
            Subscription(
                workspace_id="workspace",
                subscription_id=f"schedule-{index:02}",
                workflow_id="workflow",
                target_workflow_version_id="version",
                source_type=SourceType.SCHEDULED,
                source_config=SourceConfig(interval_seconds=30),
                selector="true",
                created_at=NOW,
                updated_at=NOW,
            )
        )
    clock[0] += timedelta(seconds=31)
    published: list[NormalizedEvent] = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, published)
        try:
            await worker.start()
            async with asyncio.timeout(3):
                while len(published) < 55:
                    await asyncio.sleep(0.01)
            assert len({event.event_id for event in published}) == 55
            assert worker.healthy
        finally:
            await worker.aclose()
        assert not worker.healthy and not client.is_closed


async def test_pending_delivery_continuation_is_retained_and_null_only_ends_a_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain within-selection progress and revisit pending work after a complete scan."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    await store.put_source_delivery(
        SourceDeliverySelection(
            WorkspaceId("workspace"), "selection", "a" * 64, NOW + timedelta(days=1), ()
        )
    )
    calls: list[Cursor | None] = []
    continued = asyncio.Event()

    async def reconcile(
        workspace: str, *, cursor: Cursor | None, limit: int
    ) -> SourceReconciliationPage:
        """Keep a target pending across the end-of-pass boundary."""
        assert workspace == "workspace" and limit == 50
        calls.append(cursor)
        if len(calls) == 1:
            return SourceReconciliationPage(1, 0, 1, Cursor("continuation"))
        if len(calls) >= 3:
            continued.set()
        return SourceReconciliationPage(1, 0, 1, None)

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        monkeypatch.setattr(worker.reliable, "reconcile_pending", reconcile)
        try:
            await worker.start()
            await asyncio.wait_for(continued.wait(), timeout=2)
        finally:
            await worker.aclose()
    assert calls[:3] == [None, Cursor("continuation"), None]


async def test_audit_sink_does_not_persist_raw_diagnostics() -> None:
    """Keep identifiers/counts without persisting exception text or credentials."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    await DurableAuditSink(store).emit(
        "trigger.dispatch.failed",
        workspace_id="workspace",
        attributes={
            "eventId": "event",
            "reason": "synthetic-private-diagnostic",
            "credential": "synthetic-private-token",
            "raw": {"body": "private"},
            "depth": 3,
        },
    )
    assert len(store._dead_letter_audits) == 1
    event = store._dead_letter_audits[0]
    assert event.payload == {"eventId": "event", "reason": "dispatch-failure", "depth": 3}


def test_worker_settings_are_separate_default_off_and_strict() -> None:
    """Configuration authoring is not activation; malformed gates never enable workers."""
    assert not read_receiver_worker_settings({}).enabled
    for env in (
        {"TRIGGER_RECEIVER_WORKERS_ENABLED": "unknown"},
        {"TRIGGER_RECEIVER_WORKERS_ENABLED": "true"},
        {"TRIGGER_RECEIVER_DRAIN_SECONDS": "0"},
    ):
        with pytest.raises(SettingsError):
            read_receiver_worker_settings(env)


async def test_factory_closes_owned_storage_on_early_readiness_failure(
    monkeypatch: pytest.MonkeyPatch,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> None:
    """Failure before worker/HTTP creation must still close the factory-created pool."""
    from custos_trigger import app as app_module

    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    close = AsyncMock()
    monkeypatch.setattr(app_module, "load_providers", lambda _: Providers(store, close=close))
    monkeypatch.setattr(
        store, "check_delivery_ready", AsyncMock(side_effect=BackendUnavailable("unavailable"))
    )
    app = create_app(
        dispatcher=dispatcher(store, FakeWorkflowServiceClient()),
        receiver_configuration_enabled=True,
        source_receivers_enabled=True,
        workflow_caller_verifier=workflow_auth[0],
        app_api_token="unit-sidecar",
        receiver_worker_settings=ReceiverWorkerSettings(enabled=True, deployment_id="unit"),
    )
    with pytest.raises(BackendUnavailable):
        async with app.router.lifespan_context(app):
            pytest.fail("unready worker was admitted")
    close.assert_awaited_once()
    assert not app.state.ready


async def test_repeated_shutdown_cancellation_joins_owned_delivery_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep borrowed clients alive until cancelled delivery cleanup finishes."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    await store.put_source_delivery(
        SourceDeliverySelection(
            WorkspaceId("workspace"), "selection", "a" * 64, NOW + timedelta(days=1), ()
        )
    )
    entered, cleaning, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def hold(
        workspace: str, *, cursor: Cursor | None, limit: int
    ) -> SourceReconciliationPage:
        """Retain ownership after cancellation until the simulated transport cleanup completes."""
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleaning.set()
            await finished.wait()
            raise
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        worker.drain_seconds = 0.01
        monkeypatch.setattr(worker.reliable, "reconcile_pending", hold)
        await worker.start()
        await asyncio.wait_for(entered.wait(), timeout=1)
        shutdown = asyncio.create_task(worker.aclose())
        try:
            await asyncio.wait_for(cleaning.wait(), timeout=1)
            for _ in range(2):
                shutdown.cancel()
                await asyncio.sleep(0)
            assert not shutdown.done() and not client.is_closed
            finished.set()
            with pytest.raises(asyncio.CancelledError):
                await shutdown
            assert all(task.done() for task in worker._tasks)
        finally:
            finished.set()
            await asyncio.gather(shutdown, return_exceptions=True)


async def test_unexpected_worker_exit_is_not_reported_ready(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Fatal faults are observed and surfaced without logging exception payloads."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    monkeypatch.setattr(
        store,
        "query_receiver_workspaces",
        AsyncMock(side_effect=ValueError("synthetic-private-diagnostic")),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        await worker.start()
        await asyncio.sleep(0)
        assert not worker.healthy
        with pytest.raises(RuntimeError, match="receiver task failed"):
            await worker.aclose()
    assert "synthetic-private-diagnostic" not in caplog.text
