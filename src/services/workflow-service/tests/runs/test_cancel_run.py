"""Tests for :meth:`RunController.cancel_run` (WF-IMPL-038).

Mirrors the style of ``test_start_run.py``: in-process Run Store layered
on :class:`FakeMetadataStoreProvider`, recording workflow client / catalog
client / lifecycle publisher, ``FixedClock`` for deterministic timestamps,
and a no-op sleeper for the terminate poll loop.
"""

from __future__ import annotations

import textwrap
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, NamedTuple, cast

import pytest
from custos_cel import FixedClock
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.document import WorkflowDocument
from custos_workflow.runs import (
    LIFECYCLE_KIND_WORKFLOW_CANCELLED,
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
    GetRunStateRequest,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
)
from custos_workflow.runtime._common import (
    RunState as RuntimeRunState,
)
from custos_workflow.runtime._common import (
    RunStatus as RuntimeRunStatus,
)
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW_VERSION_ID = "wfv-001"
WORKFLOW_ID = "wf-alpha"
IDEMPOTENCY_KEY = "client-key-cancel"
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

    ``cancel_run`` never touches the catalog, so this exists purely to
    satisfy the constructor.
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

    ``terminate_raise`` / ``get_state_raise`` let tests force runtime
    failures. ``state_sequence`` drives the poll loop: each
    ``get_workflow_state`` call pops the next entry; once the list is
    exhausted the last value is repeated.
    """

    terminate_raise: Exception | None = None
    get_state_raise: Exception | None = None
    state_sequence: list[RuntimeRunState | None] = field(default_factory=list)

    schedule_requests: list[ScheduleWorkflowRequest] = field(default_factory=list)
    terminate_requests: list[TerminateRunRequest] = field(default_factory=list)
    state_requests: list[GetRunStateRequest] = field(default_factory=list)

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        self.schedule_requests.append(request)
        return request.instance_id or ""

    async def terminate_workflow(self, request: TerminateRunRequest) -> None:
        self.terminate_requests.append(request)
        if self.terminate_raise is not None:
            raise self.terminate_raise

    async def get_workflow_state(self, request: GetRunStateRequest) -> RuntimeRunState | None:
        self.state_requests.append(request)
        if self.get_state_raise is not None:
            raise self.get_state_raise
        if not self.state_sequence:
            return None
        if len(self.state_requests) <= len(self.state_sequence):
            return self.state_sequence[len(self.state_requests) - 1]
        return self.state_sequence[-1]

    async def pause_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("cancel_run never calls pause_workflow")

    async def resume_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("cancel_run never calls resume_workflow")

    async def raise_workflow_event(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("test double does not implement raise_workflow_event")


@dataclass
class _RecordingSleeper:
    """Async sleeper that records durations without ever blocking.

    Tests assert that the poll loop's per-poll budget is honoured
    without invoking real wall-clock sleep.
    """

    durations: list[float] = field(default_factory=list)

    async def __call__(self, seconds: float) -> None:
        self.durations.append(seconds)


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


def _runtime_state(status: RuntimeRunStatus) -> RuntimeRunState:
    """Build a minimal :class:`RuntimeRunState` carrying *status*."""
    return RuntimeRunState(
        instance_id=str(RUN_ID),
        name="custos.workflow.run",
        status=status,
        created_at=FIXED_NOW,
        last_updated_at=FIXED_NOW,
        serialized_input=None,
        serialized_output=None,
    )


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
    reason: str | None = None,
    workflow_version_id: str = WORKFLOW_VERSION_ID,
) -> RunRecord:
    """Persist a :class:`RunRecord` and walk it to *status*.

    Always inserts as :class:`RunStatus.QUEUED` (the only legal initial
    status) and then drives ``update_run_status`` through the documented
    transition table to reach *status*.
    """
    record = RunRecord(
        workspace_id=WORKSPACE,
        run_id=RUN_ID,
        workflow_id=WORKFLOW_ID,
        workflow_version=workflow_version_id,
        status=RunStatus.QUEUED,
        reason=None,
        started_at=FIXED_NOW,
        updated_at=FIXED_NOW,
        compiled_graph=None,
    )
    await store.put_run(record)
    for next_status in _PATHS_FROM_QUEUED[status]:
        await store.update_run_status(
            WORKSPACE, RUN_ID, next_status, reason=reason if next_status == status else None
        )
    stored = await store.get_run(WORKSPACE, RUN_ID)
    assert stored is not None
    return stored


class _Fixture(NamedTuple):
    controller: RunController
    catalog: _RecordingCatalogClient
    store: InProcessRunStore
    workflow_client: _RecordingWorkflowClient
    publisher: InMemoryLifecycleEventPublisher
    sleeper: _RecordingSleeper


def _make_controller(
    *,
    store: InProcessRunStore | None = None,
    workflow_client: _RecordingWorkflowClient | None = None,
    publisher: InMemoryLifecycleEventPublisher | None = None,
    clock: FixedClock | None = None,
    sleeper: _RecordingSleeper | None = None,
    terminate_poll_attempts: int = 5,
    terminate_poll_interval_seconds: float = 0.1,
) -> _Fixture:
    catalog = _RecordingCatalogClient(_workflow_version())
    store = store or _store()
    workflow_client = workflow_client or _RecordingWorkflowClient()
    publisher = publisher or InMemoryLifecycleEventPublisher()
    clock = clock or FixedClock(FIXED_NOW)
    sleeper = sleeper or _RecordingSleeper()
    controller = RunController(
        catalog=catalog,
        store=store,
        workflow_client=workflow_client,
        activity_registry=InMemoryActivityTypeRegistry({}),
        lifecycle_publisher=publisher,
        clock=clock,
        terminate_poll_attempts=terminate_poll_attempts,
        terminate_poll_interval_seconds=terminate_poll_interval_seconds,
        sleep=cast(Callable[[float], Any], sleeper),
    )
    return _Fixture(
        controller=controller,
        catalog=catalog,
        store=store,
        workflow_client=workflow_client,
        publisher=publisher,
        sleeper=sleeper,
    )


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    """The recording double satisfies the controller's structural
    Workflow-client Protocol (terminate + get_state methods)."""

    def test_recording_workflow_client_has_terminate(self) -> None:
        client = _RecordingWorkflowClient()
        # Structural check via attribute presence (the Protocol is private).
        assert callable(client.terminate_workflow)
        assert callable(client.get_workflow_state)
        assert callable(client.schedule_new_workflow)

    def test_recording_workflow_client_protocol_for_in_memory_publisher(self) -> None:
        assert isinstance(InMemoryLifecycleEventPublisher(), LifecycleEventPublisher)

    def test_recording_catalog_satisfies_protocol(self) -> None:
        assert isinstance(_RecordingCatalogClient(_workflow_version()), CatalogClient)

    def test_in_process_run_store_satisfies_protocol(self) -> None:
        assert isinstance(_store(), RunStore)


# ---------------------------------------------------------------------------
# Constructor argument validation
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    """Constructor refuses non-sensical poll budgets."""

    def test_zero_terminate_poll_attempts_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="terminate_poll_attempts must be >= 1"):
            _make_controller(terminate_poll_attempts=0)

    def test_negative_terminate_poll_interval_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="terminate_poll_interval_seconds must be >= 0"):
            _make_controller(terminate_poll_interval_seconds=-0.1)


# ---------------------------------------------------------------------------
# RunNotFound
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRunNotFound:
    """Unknown ``run_id`` surfaces :class:`RunNotFoundError`."""

    async def test_cancel_run_on_unknown_id_raises_run_not_found(self) -> None:
        fx = _make_controller()
        with pytest.raises(RunNotFoundError) as excinfo:
            await fx.controller.cancel_run(
                workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
            )
        assert str(RUN_ID) in str(excinfo.value)
        # No side effects.
        assert fx.workflow_client.terminate_requests == []
        assert fx.workflow_client.state_requests == []
        assert fx.publisher.events == []


# ---------------------------------------------------------------------------
# Happy path: RUNNING -> CANCELLING -> CANCELLED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestHappyPath:
    """A run in ``running`` cancels cleanly through to ``cancelled``."""

    async def test_returns_runref_with_cancelled_status(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        ref = await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
        )
        assert isinstance(ref, RunRef)
        assert ref.workspace_id == WORKSPACE
        assert ref.run_id == RUN_ID
        assert ref.workflow_version_id == WORKFLOW_VERSION_ID
        assert ref.status == RunStatus.CANCELLED

    async def test_persists_cancelling_then_cancelled_status_transitions(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.CANCELLED
        assert record.reason == "user-cancel"

    async def test_calls_terminate_workflow_once_with_run_id(self) -> None:
        client = _RecordingWorkflowClient(
            state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
        )
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        assert len(client.terminate_requests) == 1
        assert client.terminate_requests[0].instance_id == str(RUN_ID)

    async def test_polls_get_workflow_state_until_terminated(self) -> None:
        client = _RecordingWorkflowClient(
            state_sequence=[
                _runtime_state(RuntimeRunStatus.RUNNING),
                _runtime_state(RuntimeRunStatus.RUNNING),
                _runtime_state(RuntimeRunStatus.TERMINATED),
            ]
        )
        sleeper = _RecordingSleeper()
        fx = _make_controller(workflow_client=client, sleeper=sleeper)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        # Three polls: RUNNING, RUNNING, TERMINATED.
        assert len(client.state_requests) == 3
        # Two sleeps between the three polls (skipped on the final poll).
        assert sleeper.durations == [0.1, 0.1]

    async def test_terminal_completed_state_also_short_circuits(self) -> None:
        """Any runtime-terminal status (not just TERMINATED) stops the poll."""
        client = _RecordingWorkflowClient(
            state_sequence=[_runtime_state(RuntimeRunStatus.COMPLETED)]
        )
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        assert len(client.state_requests) == 1

    async def test_terminal_failed_state_also_short_circuits(self) -> None:
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.FAILED)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        assert len(client.state_requests) == 1

    async def test_dapr_forgot_instance_short_circuits_poll(self) -> None:
        """``get_workflow_state`` returning ``None`` is treated as terminal."""
        client = _RecordingWorkflowClient(state_sequence=[None])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        assert len(client.state_requests) == 1

    async def test_emits_cancelled_lifecycle_event_with_reason(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        assert len(fx.publisher.events) == 1
        event = fx.publisher.events[0]
        assert event.kind == LIFECYCLE_KIND_WORKFLOW_CANCELLED
        assert event.workspace_id == WORKSPACE
        assert event.run_id == RUN_ID
        assert event.workflow_version_id == WORKFLOW_VERSION_ID
        assert event.occurred_at == FIXED_NOW
        assert event.extra == {"reason": "user-cancel"}

    async def test_none_reason_omits_reason_from_lifecycle_extra(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason=None)
        assert fx.publisher.events[0].extra == {}

    async def test_cancel_from_queued_succeeds(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.QUEUED)
        ref = await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="quota")
        assert ref.status == RunStatus.CANCELLED

    async def test_cancel_from_paused_succeeds(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.PAUSED)
        ref = await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="cleanup"
        )
        assert ref.status == RunStatus.CANCELLED

    async def test_cancel_from_pausing_succeeds(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(
                state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
            )
        )
        await _seed_run(fx.store, status=RunStatus.PAUSING)
        ref = await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="cleanup"
        )
        assert ref.status == RunStatus.CANCELLED


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestIdempotentReCancel:
    """``cancel_run`` is idempotent on ``cancelled`` and ``cancelling``."""

    async def test_re_cancel_on_cancelled_is_noop(self) -> None:
        client = _RecordingWorkflowClient(
            state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
        )
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        # First cancel: full flow.
        await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel")
        assert len(client.terminate_requests) == 1
        assert len(fx.publisher.events) == 1
        # Second cancel on a now-``cancelled`` row: pure no-op.
        ref = await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
        )
        assert ref.status == RunStatus.CANCELLED
        assert len(client.terminate_requests) == 1  # not re-called
        assert len(fx.publisher.events) == 1  # not re-emitted

    async def test_re_cancel_on_cancelling_is_noop(self) -> None:
        client = _RecordingWorkflowClient(
            state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
        )
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.CANCELLING)
        ref = await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
        )
        assert ref.status == RunStatus.CANCELLING
        # No new terminate, no event.
        assert client.terminate_requests == []
        assert client.state_requests == []
        assert fx.publisher.events == []

    async def test_re_cancel_returns_persisted_reason_unchanged(self) -> None:
        """An idempotent re-cancel does NOT overwrite the persisted reason."""
        client = _RecordingWorkflowClient(
            state_sequence=[_runtime_state(RuntimeRunStatus.TERMINATED)]
        )
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="original-reason"
        )
        await fx.controller.cancel_run(
            workspace_id=WORKSPACE, run_id=RUN_ID, reason="different-reason"
        )
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.reason == "original-reason"


# ---------------------------------------------------------------------------
# Illegal source state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestIllegalTransition:
    """Cancel from a terminal-non-cancel status raises state conflict."""

    async def test_cancel_on_succeeded_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.SUCCEEDED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.cancel_run(
                workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
            )
        assert excinfo.value.current_status == RunStatus.SUCCEEDED.value
        assert excinfo.value.attempted_status == RunStatus.CANCELLING.value
        # No Dapr side-effect.
        assert fx.workflow_client.terminate_requests == []
        assert fx.publisher.events == []

    async def test_cancel_on_failed_raises_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.FAILED)
        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.cancel_run(workspace_id=WORKSPACE, run_id=RUN_ID, reason=None)
        assert excinfo.value.current_status == RunStatus.FAILED.value
        assert excinfo.value.attempted_status == RunStatus.CANCELLING.value


# ---------------------------------------------------------------------------
# Runtime unavailable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRuntimeUnavailable:
    """Dapr failures surface as :class:`WorkflowRuntimeUnavailableError`."""

    async def test_terminate_failure_raises_runtime_unavailable_error(self) -> None:
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(terminate_raise=ConnectionError("boom"))
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.cancel_run(
                workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
            )
        assert excinfo.value.cause == "boom"
        # No poll, no event.
        assert fx.workflow_client.state_requests == []
        assert fx.publisher.events == []

    async def test_terminate_failure_leaves_record_in_cancelling(self) -> None:
        """Operator must see a coherent ``cancelling`` row to retry."""
        fx = _make_controller(
            workflow_client=_RecordingWorkflowClient(terminate_raise=ConnectionError("boom"))
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError):
            await fx.controller.cancel_run(
                workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
            )
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.CANCELLING

    async def test_poll_budget_exhausted_raises_runtime_unavailable(self) -> None:
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.RUNNING)])
        sleeper = _RecordingSleeper()
        fx = _make_controller(
            workflow_client=client,
            sleeper=sleeper,
            terminate_poll_attempts=3,
            terminate_poll_interval_seconds=0.5,
        )
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.cancel_run(
                workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
            )
        assert "after 3 attempts" in str(excinfo.value)
        assert len(client.state_requests) == 3
        assert sleeper.durations == [0.5, 0.5]
        # No CANCELLED transition, no event.
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.CANCELLING
        assert fx.publisher.events == []

    async def test_get_state_failure_wraps_as_runtime_unavailable(self) -> None:
        """``get_workflow_state`` exceptions must be wrapped — the public
        Run Controller surface is the single frozen error taxonomy,
        so raw runtime exceptions must NEVER bubble out of the poll
        loop (Copilot review feedback)."""
        client = _RecordingWorkflowClient(get_state_raise=ConnectionError("kapow"))
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.cancel_run(
                workspace_id=WORKSPACE, run_id=RUN_ID, reason="user-cancel"
            )
        assert excinfo.value.cause == "kapow"
        assert "poll workflow runtime" in str(excinfo.value)
        # Only one poll attempted; the row stays cancelling; no event.
        assert len(client.state_requests) == 1
        record = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert record is not None
        assert record.status == RunStatus.CANCELLING
        assert fx.publisher.events == []


# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


class TestModuleSurface:
    """The ``cancelled`` lifecycle constant is part of the public surface."""

    def test_lifecycle_cancelled_kind_constant_in_controller_all(self) -> None:
        from custos_workflow.runs import controller as controller_module

        assert "LIFECYCLE_KIND_WORKFLOW_CANCELLED" in controller_module.__all__

    def test_lifecycle_cancelled_kind_value_is_wire_stable(self) -> None:
        assert LIFECYCLE_KIND_WORKFLOW_CANCELLED == "workflow.cancelled"

    def test_lifecycle_event_carries_cancelled_kind(self) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED,
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
            occurred_at=FIXED_NOW,
            extra={"reason": "user"},
        )
        envelope = event.to_dict()
        assert envelope["kind"] == "workflow.cancelled"
        assert envelope["extra"] == {"reason": "user"}

    def test_package_re_exports_terminate_poll_defaults(self) -> None:
        """The poll-budget defaults are part of the documented public
        surface (the PR description advertises them) and MUST be
        re-exported by ``custos_workflow.runs`` (Copilot review
        feedback)."""
        from custos_workflow import runs as runs_pkg

        assert "DEFAULT_TERMINATE_POLL_ATTEMPTS" in runs_pkg.__all__
        assert "DEFAULT_TERMINATE_POLL_INTERVAL_S" in runs_pkg.__all__
        assert runs_pkg.DEFAULT_TERMINATE_POLL_ATTEMPTS == 30
        assert runs_pkg.DEFAULT_TERMINATE_POLL_INTERVAL_S == 1.0
