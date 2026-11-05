"""Owned receiver lifecycle, finite scan continuation and explicit failure/activation contracts."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from custos_spl.errors import BackendUnavailable, MigrationRequired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.receiver_progress import ReceiverScan, ScanContinuation, ScanOperation
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
    origin, monotonic = store._now(), time.monotonic()
    store._scan_now = lambda: origin + timedelta(seconds=time.monotonic() - monotonic)

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


async def test_busy_workspace_yields_while_its_continuation_is_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One endless backlog cannot block other workspaces, including beyond a discovery page."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    names = ["a-busy", *(f"z-{index:02}" for index in range(54))]
    for workspace in names:
        await store.put_source_delivery(
            SourceDeliverySelection(
                WorkspaceId(workspace),
                "selection",
                "a" * 64,
                NOW + timedelta(days=1),
                (),
            )
        )
    visited: set[str] = set()
    cursors: list[Cursor | None] = []
    completed = asyncio.Event()

    async def reconcile(
        workspace: str,
        *,
        cursor: Cursor | None,
        limit: int,
    ) -> SourceReconciliationPage:
        """Retain a perpetual continuation only for the first workspace in keyset order."""
        assert limit == 50
        if workspace == "a-busy":
            cursors.append(cursor)
            return SourceReconciliationPage(1, 0, 1, Cursor("busy-continuation"))
        visited.add(workspace)
        if len(visited) == 54:
            completed.set()
        return SourceReconciliationPage(1, 0, 0, None)

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        monkeypatch.setattr(worker.reliable, "reconcile_pending", reconcile)
        try:
            await worker.start()
            await asyncio.wait_for(completed.wait(), 2)
        finally:
            await worker.aclose()
    assert cursors[0] is None
    assert len(cursors) > 1 and all(cursor == Cursor("busy-continuation") for cursor in cursors[1:])


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


async def test_progress_schema_failure_blocks_startup_before_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revision15 is insufficient for activated workers; never fall back to a volatile queue."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    monkeypatch.setattr(
        store,
        "check_receiver_scan_progress_ready",
        AsyncMock(side_effect=MigrationRequired([("MetadataStoreProvider", 16)])),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        with pytest.raises(MigrationRequired):
            await worker.start()
        assert not worker._tasks and not worker.healthy


async def test_checkpoint_acknowledgement_loss_preserves_committed_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncertain checkpoint must not replace the new cursor with stale caller state."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    await store.put_source_delivery(
        SourceDeliverySelection(
            WorkspaceId("workspace"),
            "selection",
            "a" * 64,
            NOW + timedelta(days=1),
            (),
        )
    )
    saved = ScanContinuation(ScanOperation.DELIVERY, Cursor("committed-target"))
    checkpoint = store.checkpoint_receiver_scan

    async def lost(
        scan: ReceiverScan, continuation: ScanContinuation, *, interval_seconds: float = 1
    ) -> None:
        """Commit for real, then model a lost network acknowledgement."""
        await checkpoint(scan, continuation, interval_seconds=interval_seconds)
        raise BackendUnavailable("synthetic-private-response")

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        await worker._discover()
        monkeypatch.setattr(worker, "_page", AsyncMock(return_value=saved))
        monkeypatch.setattr(store, "checkpoint_receiver_scan", lost)
        with pytest.raises(BackendUnavailable):
            await worker._run_operation(ScanOperation.DELIVERY)
        monkeypatch.setattr(store, "checkpoint_receiver_scan", checkpoint)
        store._scan_now = lambda: NOW + timedelta(seconds=10)
        successor = await store.claim_receiver_scan(
            "unit-receivers",
            ScanOperation.DELIVERY,
            "successor",
        )
        assert successor is not None and successor.continuation == saved


async def test_failed_page_releases_unchanged_progress_with_retry_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No effect failure can acknowledge a page, reset progress or be retried immediately."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    await store.put_source_delivery(
        SourceDeliverySelection(
            WorkspaceId("workspace"),
            "selection",
            "a" * 64,
            NOW + timedelta(days=1),
            (),
        )
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        store._scan_now = lambda: NOW
        await worker._discover()
        monkeypatch.setattr(worker, "_page", AsyncMock(side_effect=TimeoutError))
        with pytest.raises(TimeoutError):
            await worker._run_operation(ScanOperation.DELIVERY)
        assert (
            await store.claim_receiver_scan("unit-receivers", ScanOperation.DELIVERY, "next")
            is None
        )
        store._scan_now = lambda: NOW + timedelta(seconds=5)
        retry = await store.claim_receiver_scan("unit-receivers", ScanOperation.DELIVERY, "next")
        assert retry is not None and retry.continuation.cursor is None


async def test_failure_release_is_joined_through_repeated_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The driver cannot detach database cleanup when a shutdown caller cancels again."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    await store.put_source_delivery(
        SourceDeliverySelection(
            WorkspaceId("workspace"),
            "selection",
            "a" * 64,
            NOW + timedelta(days=1),
            (),
        )
    )
    entered, finish = asyncio.Event(), asyncio.Event()
    release = store.release_receiver_scan

    async def hold_release(scan: ReceiverScan) -> None:
        """Hold actual failure release until cancellation ownership has been checked."""
        entered.set()
        await finish.wait()
        await release(scan)

    async with httpx.AsyncClient(transport=httpx.MockTransport(acknowledge)) as client:
        worker = runtime(store, client, [])
        await worker._discover()
        monkeypatch.setattr(worker, "_page", AsyncMock(side_effect=TimeoutError))
        monkeypatch.setattr(store, "release_receiver_scan", hold_release)
        task = asyncio.create_task(worker._run_operation(ScanOperation.DELIVERY))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            store._scan_now = lambda: NOW + timedelta(seconds=10)
            assert (
                await store.claim_receiver_scan(
                    "unit-receivers",
                    ScanOperation.DELIVERY,
                    "successor",
                )
                is not None
            )
        finally:
            finish.set()
            await asyncio.gather(task, return_exceptions=True)
