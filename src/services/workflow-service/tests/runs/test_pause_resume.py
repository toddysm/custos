"""Tests for :meth:`RunController.pause_run` and
:meth:`RunController.resume_run` (WF-IMPL-039).

Mirrors the style of ``test_cancel_run.py``: in-process Run Store layered
on :class:`FakeMetadataStoreProvider`, recording workflow client / catalog
client / lifecycle publisher, ``FixedClock`` for deterministic timestamps.
The pause / resume operations do not use a poll loop, so no sleeper.
"""

from __future__ import annotations

import textwrap
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, NamedTuple, cast

import pytest
from custos_cel import FixedClock
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.document import WorkflowDocument
from custos_workflow.runs import (
    LIFECYCLE_KIND_WORKFLOW_PAUSED,
    LIFECYCLE_KIND_WORKFLOW_RESUMED,
    CatalogClient,
    InMemoryLifecycleEventPublisher,
    InProcessRunStore,
    LifecycleEvent,
    LifecycleEventPublisher,
    RunController,
    RunNotFoundError,
    RunRecord,
    RunRef,
    RunStateConflictError,
    RunStatus,
    RunStore,
    WorkflowRuntimeUnavailableError,
    WorkflowVersion,
    derive_run_id,
)
from custos_workflow.runs.ids import RunId
from custos_workflow.runtime._common import (
    PauseRunRequest,
    ResumeRunRequest,
    ScheduleWorkflowRequest,
)
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW_VERSION_ID = "wfv-001"
WORKFLOW_ID = "wf-alpha"
IDEMPOTENCY_KEY = "client-key-pause-resume"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
RUN_ID: RunId = derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)


def _doc_yaml() -> str:
    return textwrap.dedent(
        """\
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata: {name: pipeline, workspace: ws}
        spec:
          steps:
            - id: a
              let: {x: '${{ true }}'}
        """
    )


def _workflow_version() -> WorkflowVersion:
    doc = WorkflowDocument.model_validate(__import__("yaml").safe_load(_doc_yaml()))
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


class _RecordingCatalogClient:
    """Minimal :class:`CatalogClient` stub.

    ``pause_run`` / ``resume_run`` never touch the catalog; this exists
    purely to satisfy the controller's constructor.
    """

    def __init__(self, version: WorkflowVersion) -> None:
        self._version = version
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        return self._version


@dataclass
class _RecordingWorkflowClient:
    """Recording in-memory :class:`_WorkflowClient`-compatible double.

    ``pause_raise`` / ``resume_raise`` let tests force runtime failures.
    The terminate / get_state / schedule entry points are stubs because
    pause / resume never call them.
    """

    pause_raise: Exception | None = None
    resume_raise: Exception | None = None
    on_pause_success: Callable[[], Awaitable[None]] | None = None
    on_resume_success: Callable[[], Awaitable[None]] | None = None

    pause_requests: list[PauseRunRequest] = field(default_factory=list)
    resume_requests: list[ResumeRunRequest] = field(default_factory=list)

    async def schedule_new_workflow(
        self, request: ScheduleWorkflowRequest
    ) -> str:  # pragma: no cover
        raise NotImplementedError("pause/resume never call schedule_new_workflow")

    async def terminate_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("pause/resume never call terminate_workflow")

    async def get_workflow_state(self, request: Any) -> Any:  # pragma: no cover
        raise NotImplementedError("pause/resume never call get_workflow_state")

    async def pause_workflow(self, request: PauseRunRequest) -> None:
        self.pause_requests.append(request)
        if self.pause_raise is not None:
            raise self.pause_raise
        if self.on_pause_success is not None:
            await self.on_pause_success()

    async def resume_workflow(self, request: ResumeRunRequest) -> None:
        self.resume_requests.append(request)
        if self.resume_raise is not None:
            raise self.resume_raise
        if self.on_resume_success is not None:
            await self.on_resume_success()


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


_PATHS_FROM_QUEUED: dict[RunStatus, tuple[RunStatus, ...]] = {
    RunStatus.QUEUED: (),
    RunStatus.RUNNING: (RunStatus.RUNNING,),
    RunStatus.PAUSING: (RunStatus.RUNNING, RunStatus.PAUSING),
    RunStatus.PAUSED: (RunStatus.RUNNING, RunStatus.PAUSING, RunStatus.PAUSED),
    RunStatus.CANCELLING: (RunStatus.CANCELLING,),
    RunStatus.CANCELLED: (RunStatus.CANCELLING, RunStatus.CANCELLED),
    RunStatus.SUCCEEDED: (RunStatus.RUNNING, RunStatus.SUCCEEDED),
    RunStatus.FAILED: (RunStatus.FAILED,),
}


async def _seed_run(
    store: InProcessRunStore,
    *,
    status: RunStatus,
) -> RunRecord:
    """Persist a :class:`RunRecord` and walk it to *status*."""
    record = RunRecord(
        workspace_id=WORKSPACE,
        run_id=RUN_ID,
        workflow_id=WORKFLOW_ID,
        workflow_version=WORKFLOW_VERSION_ID,
        status=RunStatus.QUEUED,
        reason=None,
        started_at=FIXED_NOW,
        updated_at=FIXED_NOW,
        compiled_graph=None,
    )
    await store.put_run(record)
    for next_status in _PATHS_FROM_QUEUED[status]:
        await store.update_run_status(WORKSPACE, RUN_ID, next_status)
    stored = await store.get_run(WORKSPACE, RUN_ID)
    assert stored is not None
    return stored


class _Fixture(NamedTuple):
    controller: RunController
    catalog: _RecordingCatalogClient
    store: InProcessRunStore
    workflow_client: _RecordingWorkflowClient
    publisher: InMemoryLifecycleEventPublisher


def _make_controller(
    *,
    store: InProcessRunStore | None = None,
    workflow_client: _RecordingWorkflowClient | None = None,
    publisher: InMemoryLifecycleEventPublisher | None = None,
    clock: FixedClock | None = None,
) -> _Fixture:
    catalog = _RecordingCatalogClient(_workflow_version())
    store = store or _store()
    workflow_client = workflow_client or _RecordingWorkflowClient()
    publisher = publisher or InMemoryLifecycleEventPublisher()
    clock = clock or FixedClock(FIXED_NOW)
    controller = RunController(
        catalog=catalog,
        store=store,
        workflow_client=workflow_client,
        activity_registry=InMemoryActivityTypeRegistry({}),
        lifecycle_publisher=publisher,
        clock=clock,
    )
    return _Fixture(
        controller=controller,
        catalog=catalog,
        store=store,
        workflow_client=workflow_client,
        publisher=publisher,
    )


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    """The recording double exposes the structural Workflow-client
    surface the controller depends on for pause / resume."""

    def test_recording_workflow_client_has_pause_and_resume(self) -> None:
        client = _RecordingWorkflowClient()
        assert callable(client.pause_workflow)
        assert callable(client.resume_workflow)

    def test_in_memory_publisher_satisfies_lifecycle_publisher_protocol(self) -> None:
        assert isinstance(InMemoryLifecycleEventPublisher(), LifecycleEventPublisher)

    def test_recording_catalog_satisfies_protocol(self) -> None:
        assert isinstance(_RecordingCatalogClient(_workflow_version()), CatalogClient)

    def test_in_process_run_store_satisfies_protocol(self) -> None:
        assert isinstance(_store(), RunStore)


# ---------------------------------------------------------------------------
# RunNotFound
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRunNotFound:
    """Unknown ``run_id`` surfaces :class:`RunNotFoundError`."""

    async def test_pause_on_unknown_id_raises_run_not_found(self) -> None:
        fx = _make_controller()
        with pytest.raises(RunNotFoundError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert str(RUN_ID) in str(excinfo.value)
        assert fx.workflow_client.pause_requests == []
        assert fx.publisher.events == []

    async def test_resume_on_unknown_id_raises_run_not_found(self) -> None:
        fx = _make_controller()
        with pytest.raises(RunNotFoundError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert str(RUN_ID) in str(excinfo.value)
        assert fx.workflow_client.resume_requests == []
        assert fx.publisher.events == []


# ---------------------------------------------------------------------------
# pause_run happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPauseHappyPath:
    """A run in ``running`` pauses cleanly through to ``paused``."""

    async def test_returns_runref_with_paused_status(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        ref = await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert isinstance(ref, RunRef)
        assert ref.workspace_id == WORKSPACE
        assert ref.run_id == RUN_ID
        assert ref.workflow_version_id == WORKFLOW_VERSION_ID
        assert ref.status == RunStatus.PAUSED

    async def test_persists_pausing_then_paused_status_transitions(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.PAUSED

    async def test_calls_pause_workflow_once_with_run_id(self) -> None:
        client = _RecordingWorkflowClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(client.pause_requests) == 1
        assert client.pause_requests[0].instance_id == str(RUN_ID)

    async def test_emits_paused_lifecycle_event(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(fx.publisher.events) == 1
        event = fx.publisher.events[0]
        assert event.kind == LIFECYCLE_KIND_WORKFLOW_PAUSED
        assert event.workspace_id == WORKSPACE
        assert event.run_id == RUN_ID
        assert event.workflow_version_id == WORKFLOW_VERSION_ID
        assert event.occurred_at == FIXED_NOW
        assert event.extra == {}


# ---------------------------------------------------------------------------
# resume_run happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestResumeHappyPath:
    """A run in ``paused`` resumes cleanly through to ``running``."""

    async def test_returns_runref_with_running_status(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        ref = await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert isinstance(ref, RunRef)
        assert ref.workspace_id == WORKSPACE
        assert ref.run_id == RUN_ID
        assert ref.workflow_version_id == WORKFLOW_VERSION_ID
        assert ref.status == RunStatus.RUNNING

    async def test_persists_running_status(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.RUNNING

    async def test_calls_resume_workflow_once_with_run_id(self) -> None:
        client = _RecordingWorkflowClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(client.resume_requests) == 1
        assert client.resume_requests[0].instance_id == str(RUN_ID)

    async def test_emits_resumed_lifecycle_event(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(fx.publisher.events) == 1
        event = fx.publisher.events[0]
        assert event.kind == LIFECYCLE_KIND_WORKFLOW_RESUMED
        assert event.workspace_id == WORKSPACE
        assert event.run_id == RUN_ID
        assert event.workflow_version_id == WORKFLOW_VERSION_ID
        assert event.occurred_at == FIXED_NOW
        assert event.extra == {}


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestIdempotentPause:
    """``pause_run`` is idempotent on ``paused`` and ``pausing``."""

    async def test_re_pause_on_paused_is_noop(self) -> None:
        client = _RecordingWorkflowClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        # First pause: full flow.
        await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(client.pause_requests) == 1
        assert len(fx.publisher.events) == 1
        # Second pause on a now-``paused`` row: pure no-op.
        ref = await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert ref.status == RunStatus.PAUSED
        assert len(client.pause_requests) == 1  # not re-called
        assert len(fx.publisher.events) == 1  # not re-emitted

    async def test_re_pause_on_pausing_is_noop(self) -> None:
        client = _RecordingWorkflowClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.PAUSING)
        ref = await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert ref.status == RunStatus.PAUSING
        assert client.pause_requests == []
        assert fx.publisher.events == []


@pytest.mark.asyncio
class TestIdempotentResume:
    """``resume_run`` is idempotent on ``running``."""

    async def test_re_resume_on_running_is_noop(self) -> None:
        client = _RecordingWorkflowClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        ref = await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert ref.status == RunStatus.RUNNING
        assert client.resume_requests == []
        assert fx.publisher.events == []

    async def test_resume_then_re_resume_full_then_noop(self) -> None:
        client = _RecordingWorkflowClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        # First resume: full flow.
        await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(client.resume_requests) == 1
        assert len(fx.publisher.events) == 1
        # Second resume on a now-``running`` row: pure no-op.
        ref = await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert ref.status == RunStatus.RUNNING
        assert len(client.resume_requests) == 1
        assert len(fx.publisher.events) == 1


# ---------------------------------------------------------------------------
# Illegal source state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPauseIllegalTransition:
    """Pause from a status other than ``running`` / ``pausing`` /
    ``paused`` raises :class:`RunStateConflictError`."""

    async def test_pause_on_queued_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.QUEUED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.QUEUED.value
        assert excinfo.value.attempted_status == RunStatus.PAUSING.value
        assert fx.workflow_client.pause_requests == []
        assert fx.publisher.events == []

    async def test_pause_after_cancel_raises_state_conflict(self) -> None:
        """Per the acceptance criteria: pause-after-cancel must raise."""
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.CANCELLING)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.CANCELLING.value
        assert excinfo.value.attempted_status == RunStatus.PAUSING.value
        assert fx.workflow_client.pause_requests == []
        assert fx.publisher.events == []

    async def test_pause_on_cancelled_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.CANCELLED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.CANCELLED.value
        assert excinfo.value.attempted_status == RunStatus.PAUSING.value

    async def test_pause_on_succeeded_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.SUCCEEDED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.SUCCEEDED.value
        assert excinfo.value.attempted_status == RunStatus.PAUSING.value

    async def test_pause_on_failed_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.FAILED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.FAILED.value
        assert excinfo.value.attempted_status == RunStatus.PAUSING.value


@pytest.mark.asyncio
class TestResumeIllegalTransition:
    """Resume from a status other than ``paused`` / ``running`` raises
    :class:`RunStateConflictError` — validated BEFORE any Dapr call
    because there is no transitional ``resuming`` state to fall back on."""

    async def test_resume_on_queued_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.QUEUED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.QUEUED.value
        assert excinfo.value.attempted_status == RunStatus.RUNNING.value
        assert fx.workflow_client.resume_requests == []
        assert fx.publisher.events == []

    async def test_resume_on_pausing_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.PAUSING)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.PAUSING.value
        assert excinfo.value.attempted_status == RunStatus.RUNNING.value
        assert fx.workflow_client.resume_requests == []
        assert fx.publisher.events == []

    async def test_resume_on_cancelling_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.CANCELLING)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.CANCELLING.value
        assert excinfo.value.attempted_status == RunStatus.RUNNING.value

    async def test_resume_on_cancelled_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.CANCELLED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.CANCELLED.value
        assert excinfo.value.attempted_status == RunStatus.RUNNING.value

    async def test_resume_on_succeeded_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.SUCCEEDED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.SUCCEEDED.value
        assert excinfo.value.attempted_status == RunStatus.RUNNING.value

    async def test_resume_on_failed_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.FAILED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.current_status == RunStatus.FAILED.value
        assert excinfo.value.attempted_status == RunStatus.RUNNING.value


# ---------------------------------------------------------------------------
# Runtime unavailable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRuntimeUnavailable:
    """Dapr failures surface as :class:`WorkflowRuntimeUnavailableError`."""

    async def test_pause_workflow_failure_raises_runtime_unavailable(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(pause_raise=ConnectionError("boom"))
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.cause == "boom"
        assert str(RUN_ID) in str(excinfo.value)
        assert fx.publisher.events == []

    async def test_pause_failure_leaves_record_in_pausing(self) -> None:
        """Operator must see a coherent ``pausing`` row to reconcile."""
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(pause_raise=ConnectionError("boom"))
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError):
            await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.PAUSING

    async def test_resume_workflow_failure_raises_runtime_unavailable(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(resume_raise=ConnectionError("kapow"))
        )
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.cause == "kapow"
        assert str(RUN_ID) in str(excinfo.value)
        assert fx.publisher.events == []

    async def test_resume_failure_leaves_record_in_paused(self) -> None:
        """Resume validates source BEFORE Dapr, so a Dapr failure must
        leave the row in ``paused`` — operator can simply retry."""
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(resume_raise=ConnectionError("kapow"))
        )
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        with pytest.raises(WorkflowRuntimeUnavailableError):
            await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.PAUSED


# ---------------------------------------------------------------------------
# Concurrent lifecycle race — final transition reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestConcurrentCancelRace:
    """A concurrent ``cancel_run`` can win the race between the runtime
    pause / resume call and the final store transition. The controller
    MUST reconcile by surfacing the winning operation's :class:`RunRef`
    rather than raising :class:`RunStateConflictError` and leaving the
    runtime and store in an unrecoverable disagreement (Copilot review
    feedback)."""

    async def test_pause_run_reconciles_when_concurrent_cancel_wins(self) -> None:
        # Hook: between ``pause_workflow`` returning and the final
        # ``pausing -> paused`` transition, a concurrent ``cancel_run``
        # walks the row ``pausing -> cancelling``.
        store = _store()

        async def _concurrent_cancel() -> None:
            await store.update_run_status(WORKSPACE, RUN_ID, RunStatus.CANCELLING)

        client = _RecordingWorkflowClient(on_pause_success=_concurrent_cancel)
        fx = _make_controller(store=store, workflow_client=client)
        await _seed_run(store, status=RunStatus.RUNNING)

        ref = await fx.controller.pause_run(workspace_id=WORKSPACE, run_id=RUN_ID)

        # The runtime was paused (Dapr side-effect honoured).
        assert len(client.pause_requests) == 1
        # The row reflects the winning concurrent cancel.
        assert ref.status == RunStatus.CANCELLING
        record = await store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.CANCELLING
        # No ``workflow.paused`` event — the winner owns the lifecycle
        # event chain.
        assert fx.publisher.events == []

    async def test_resume_run_reconciles_when_concurrent_cancel_wins(self) -> None:
        # Hook: between ``resume_workflow`` returning and the final
        # ``paused -> running`` transition, a concurrent ``cancel_run``
        # walks the row ``paused -> cancelling``.
        store = _store()

        async def _concurrent_cancel() -> None:
            await store.update_run_status(WORKSPACE, RUN_ID, RunStatus.CANCELLING)

        client = _RecordingWorkflowClient(on_resume_success=_concurrent_cancel)
        fx = _make_controller(store=store, workflow_client=client)
        await _seed_run(store, status=RunStatus.PAUSED)

        ref = await fx.controller.resume_run(workspace_id=WORKSPACE, run_id=RUN_ID)

        # The runtime was resumed (Dapr side-effect honoured).
        assert len(client.resume_requests) == 1
        # The row reflects the winning concurrent cancel.
        assert ref.status == RunStatus.CANCELLING
        record = await store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.CANCELLING
        # No ``workflow.resumed`` event — the winner owns the lifecycle
        # event chain.
        assert fx.publisher.events == []


# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


class TestModuleSurface:
    """The ``paused`` / ``resumed`` lifecycle constants are part of the
    public surface."""

    def test_lifecycle_kind_constants_in_controller_all(self) -> None:
        from custos_workflow.runs import controller as controller_module

        assert "LIFECYCLE_KIND_WORKFLOW_PAUSED" in controller_module.__all__
        assert "LIFECYCLE_KIND_WORKFLOW_RESUMED" in controller_module.__all__

    def test_lifecycle_kind_values_are_wire_stable(self) -> None:
        assert LIFECYCLE_KIND_WORKFLOW_PAUSED == "workflow.paused"
        assert LIFECYCLE_KIND_WORKFLOW_RESUMED == "workflow.resumed"

    def test_lifecycle_event_carries_paused_kind(self) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_WORKFLOW_PAUSED,
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
            occurred_at=FIXED_NOW,
        )
        envelope = event.to_dict()
        assert envelope["kind"] == "workflow.paused"

    def test_lifecycle_event_carries_resumed_kind(self) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_WORKFLOW_RESUMED,
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
            occurred_at=FIXED_NOW,
        )
        envelope = event.to_dict()
        assert envelope["kind"] == "workflow.resumed"

    def test_package_re_exports_lifecycle_kinds(self) -> None:
        from custos_workflow import runs as runs_pkg

        assert "LIFECYCLE_KIND_WORKFLOW_PAUSED" in runs_pkg.__all__
        assert "LIFECYCLE_KIND_WORKFLOW_RESUMED" in runs_pkg.__all__
