"""Tests for :meth:`RunController.raise_external_event` (WF-IMPL-068, #454).

Covers the Trigger-Service ``RaiseExternalEvent`` bridge:

* happy-path dispatch into ``WorkflowClient.raise_workflow_event``;
* in-process TTL dedup on
  ``(workspace_id, run_id, step_id, event_name, idempotency_key)``;
* TTL-expiry re-dispatch;
* opt-out when ``idempotency_key`` is ``None`` / empty / whitespace;
* unknown-run → :class:`RunNotFoundError`;
* terminal-state run → :class:`RunStateConflictError`;
* Dapr runtime failure → :class:`WorkflowRuntimeUnavailableError`
  with the dedup reservation rolled back;
* argument validation (``workspace_id`` / ``event_name`` / ``step_id``
  non-empty).

The fixture mirrors :mod:`tests.runs.test_cancel_run` so the
recording doubles stay in lock-step with the rest of the
controller test suite.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple, cast

import pytest
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.document import WorkflowDocument
from custos_workflow.runs import (
    InMemoryLifecycleEventPublisher,
    InProcessRunStore,
    RunController,
    RunNotFoundError,
    RunRecord,
    RunStateConflictError,
    RunStatus,
    WorkflowRuntimeUnavailableError,
    WorkflowVersion,
    derive_run_id,
)
from custos_workflow.runs.ids import RunId
from custos_workflow.runtime._common import (
    GetRunStateRequest,
    PauseRunRequest,
    RaiseRunEventRequest,
    ResumeRunRequest,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
)
from custos_workflow.runtime._common import (
    RunState as RuntimeRunState,
)
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW_VERSION_ID = "wfv-001"
WORKFLOW_ID = "wf-alpha"
IDEMPOTENCY_KEY = "client-key-raise"
STEP_ID = "approve"
EVENT_NAME = "approval.received"
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

    ``raise_external_event`` never touches the catalog, so this
    exists purely to satisfy the constructor.
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
    """Recording in-memory ``_WorkflowClient``-compatible double.

    ``raise_event_raise`` forces a runtime failure on the next
    ``raise_workflow_event`` call.
    """

    raise_event_raise: Exception | None = None

    raise_event_requests: list[RaiseRunEventRequest] = field(default_factory=list)

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        raise NotImplementedError("raise_external_event never schedules")

    async def terminate_workflow(self, request: TerminateRunRequest) -> None:
        raise NotImplementedError("raise_external_event never terminates")

    async def get_workflow_state(self, request: GetRunStateRequest) -> RuntimeRunState | None:
        raise NotImplementedError("raise_external_event never polls state")

    async def pause_workflow(self, request: PauseRunRequest) -> None:
        raise NotImplementedError("raise_external_event never pauses")

    async def resume_workflow(self, request: ResumeRunRequest) -> None:
        raise NotImplementedError("raise_external_event never resumes")

    async def raise_workflow_event(self, request: RaiseRunEventRequest) -> None:
        self.raise_event_requests.append(request)
        if self.raise_event_raise is not None:
            raise self.raise_event_raise


@dataclass
class _AdvanceableClock:
    """:class:`Clock`-compatible clock the tests can advance manually.

    :class:`FixedClock` is immutable; the TTL-expiry tests need to
    move time forward without rebuilding the controller.
    """

    now_value: datetime

    def now(self) -> datetime:
        return self.now_value

    def advance(self, delta: timedelta) -> None:
        self.now_value = self.now_value + delta


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


class _Fixture(NamedTuple):
    controller: RunController
    catalog: _RecordingCatalogClient
    store: InProcessRunStore
    workflow_client: _RecordingWorkflowClient
    publisher: InMemoryLifecycleEventPublisher
    clock: _AdvanceableClock


def _make_controller(
    *,
    store: InProcessRunStore | None = None,
    workflow_client: _RecordingWorkflowClient | None = None,
    publisher: InMemoryLifecycleEventPublisher | None = None,
    clock: _AdvanceableClock | None = None,
    event_dispatch_ttl: timedelta | None = None,
) -> _Fixture:
    catalog = _RecordingCatalogClient(_workflow_version())
    store = store or _store()
    workflow_client = workflow_client or _RecordingWorkflowClient()
    publisher = publisher or InMemoryLifecycleEventPublisher()
    clock = clock or _AdvanceableClock(FIXED_NOW)
    controller = RunController(
        catalog=catalog,
        store=store,
        workflow_client=workflow_client,
        activity_registry=InMemoryActivityTypeRegistry({}),
        lifecycle_publisher=publisher,
        clock=cast(Any, clock),
        event_dispatch_ttl=event_dispatch_ttl,
    )
    return _Fixture(
        controller=controller,
        catalog=catalog,
        store=store,
        workflow_client=workflow_client,
        publisher=publisher,
        clock=clock,
    )


async def _seed_run(
    store: InProcessRunStore,
    *,
    status: RunStatus,
    reason: str | None = None,
) -> RunRecord:
    """Persist a :class:`RunRecord` with the given *status*."""
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
    paths: dict[RunStatus, tuple[RunStatus, ...]] = {
        RunStatus.QUEUED: (),
        RunStatus.RUNNING: (RunStatus.RUNNING,),
        RunStatus.PAUSING: (RunStatus.RUNNING, RunStatus.PAUSING),
        RunStatus.PAUSED: (RunStatus.RUNNING, RunStatus.PAUSING, RunStatus.PAUSED),
        RunStatus.CANCELLING: (RunStatus.CANCELLING,),
        RunStatus.CANCELLED: (RunStatus.CANCELLING, RunStatus.CANCELLED),
        RunStatus.SUCCEEDED: (RunStatus.RUNNING, RunStatus.SUCCEEDED),
        RunStatus.FAILED: (RunStatus.FAILED,),
    }
    for next_status in paths[status]:
        await store.update_run_status(
            WORKSPACE,
            RUN_ID,
            next_status,
            reason=reason if next_status == status else None,
        )
    stored = await store.get_run(WORKSPACE, RUN_ID)
    assert stored is not None
    return stored


# ---------------------------------------------------------------------------
# Constructor argument validation
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    """The controller rejects non-positive event-dispatch TTLs."""

    def test_zero_event_dispatch_ttl_raises(self) -> None:
        with pytest.raises(ValueError, match="event_dispatch_ttl must be positive"):
            _make_controller(event_dispatch_ttl=timedelta(0))

    def test_negative_event_dispatch_ttl_raises(self) -> None:
        with pytest.raises(ValueError, match="event_dispatch_ttl must be positive"):
            _make_controller(event_dispatch_ttl=timedelta(seconds=-1))


# ---------------------------------------------------------------------------
# Argument validation on the method itself
# ---------------------------------------------------------------------------


class TestArgumentValidation:
    """``raise_external_event`` refuses obviously broken arguments."""

    async def test_empty_workspace_id_raises_value_error(self) -> None:
        fx = _make_controller()
        with pytest.raises(ValueError, match="workspace_id must be non-empty"):
            await fx.controller.raise_external_event(
                workspace_id="",
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
            )

    async def test_empty_event_name_raises_value_error(self) -> None:
        fx = _make_controller()
        with pytest.raises(ValueError, match="event_name must be non-empty"):
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name="",
                payload={},
            )

    async def test_empty_step_id_raises_value_error(self) -> None:
        fx = _make_controller()
        with pytest.raises(ValueError, match="step_id must be non-empty"):
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id="",
                event_name=EVENT_NAME,
                payload={},
            )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    """Happy-path dispatch into ``raise_workflow_event``."""

    async def test_dispatches_to_workflow_client_for_running_run(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name=EVENT_NAME,
            payload={"approved": True},
            idempotency_key=None,
        )

        assert len(fx.workflow_client.raise_event_requests) == 1
        request = fx.workflow_client.raise_event_requests[0]
        assert request.instance_id == str(RUN_ID)
        assert request.event_name == EVENT_NAME
        assert request.data == {"approved": True}

    async def test_dispatches_for_each_documented_non_terminal_status(self) -> None:
        for status in (
            RunStatus.QUEUED,
            RunStatus.RUNNING,
            RunStatus.PAUSING,
            RunStatus.PAUSED,
            RunStatus.CANCELLING,
        ):
            fx = _make_controller()
            await _seed_run(fx.store, status=status)

            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={"status": status.value},
            )
            assert len(fx.workflow_client.raise_event_requests) == 1, (
                f"expected dispatch for status {status.value}"
            )

    async def test_payload_defaults_to_none_when_omitted(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name=EVENT_NAME,
        )

        assert fx.workflow_client.raise_event_requests[0].data is None


# ---------------------------------------------------------------------------
# Dedup ledger
# ---------------------------------------------------------------------------


class TestDedupLedger:
    """In-process TTL ledger keyed on ``(ws, run, step, event, key)``."""

    async def test_replay_within_ttl_window_does_not_redispatch(self) -> None:
        fx = _make_controller(event_dispatch_ttl=timedelta(minutes=10))
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        for _ in range(3):
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={"n": 1},
                idempotency_key="abc",
            )

        assert len(fx.workflow_client.raise_event_requests) == 1

    async def test_replay_after_ttl_eviction_redispatches(self) -> None:
        fx = _make_controller(event_dispatch_ttl=timedelta(seconds=5))
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name=EVENT_NAME,
            payload={"n": 1},
            idempotency_key="abc",
        )
        # Advance well past the TTL window.
        fx.clock.advance(timedelta(seconds=30))
        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name=EVENT_NAME,
            payload={"n": 1},
            idempotency_key="abc",
        )

        assert len(fx.workflow_client.raise_event_requests) == 2

    async def test_distinct_event_names_dispatch_independently(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name="event.a",
            payload={},
            idempotency_key="key",
        )
        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name="event.b",
            payload={},
            idempotency_key="key",
        )

        assert [r.event_name for r in fx.workflow_client.raise_event_requests] == [
            "event.a",
            "event.b",
        ]

    async def test_distinct_step_ids_dispatch_independently(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id="step-a",
            event_name=EVENT_NAME,
            payload={},
            idempotency_key="key",
        )
        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id="step-b",
            event_name=EVENT_NAME,
            payload={},
            idempotency_key="key",
        )

        assert len(fx.workflow_client.raise_event_requests) == 2

    async def test_no_idempotency_key_always_dispatches(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        for _ in range(3):
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
                idempotency_key=None,
            )

        assert len(fx.workflow_client.raise_event_requests) == 3

    async def test_empty_string_idempotency_key_opts_out(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        for key in ("", "   "):
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
                idempotency_key=key,
            )

        assert len(fx.workflow_client.raise_event_requests) == 2


# ---------------------------------------------------------------------------
# Run-state preconditions
# ---------------------------------------------------------------------------


class TestRunStatePreconditions:
    """Unknown / terminal-state runs surface the locked errors."""

    async def test_unknown_run_raises_run_not_found(self) -> None:
        fx = _make_controller()
        # Store deliberately empty.

        with pytest.raises(RunNotFoundError) as excinfo:
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
            )

        assert excinfo.value.run_id == str(RUN_ID)
        assert fx.workflow_client.raise_event_requests == []

    async def test_terminal_succeeded_raises_run_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.SUCCEEDED)

        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
            )

        assert excinfo.value.run_id == str(RUN_ID)
        assert excinfo.value.current_status == RunStatus.SUCCEEDED.value
        assert excinfo.value.attempted_status == "raise_event"
        assert fx.workflow_client.raise_event_requests == []

    async def test_terminal_failed_raises_run_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.FAILED)

        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
            )

        assert excinfo.value.current_status == RunStatus.FAILED.value

    async def test_terminal_cancelled_raises_run_state_conflict(self) -> None:
        fx = _make_controller()
        await _seed_run(fx.store, status=RunStatus.CANCELLED)

        with pytest.raises(RunStateConflictError) as excinfo:
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
            )

        assert excinfo.value.current_status == RunStatus.CANCELLED.value


# ---------------------------------------------------------------------------
# Runtime failure handling
# ---------------------------------------------------------------------------


class TestRuntimeFailures:
    """Dapr runtime failures wrap into ``WorkflowRuntimeUnavailableError``."""

    async def test_dispatch_failure_is_wrapped(self) -> None:
        client = _RecordingWorkflowClient(raise_event_raise=RuntimeError("dapr down"))
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
                idempotency_key="abc",
            )

        assert excinfo.value.run_id == str(RUN_ID)
        assert "dapr down" in (excinfo.value.cause or "")

    async def test_dispatch_failure_releases_dedup_reservation(self) -> None:
        client = _RecordingWorkflowClient(raise_event_raise=RuntimeError("dapr down"))
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)

        with pytest.raises(WorkflowRuntimeUnavailableError):
            await fx.controller.raise_external_event(
                workspace_id=WORKSPACE,
                run_id=RUN_ID,
                step_id=STEP_ID,
                event_name=EVENT_NAME,
                payload={},
                idempotency_key="abc",
            )

        # Retry after the runtime recovers: the dedup reservation
        # must have been rolled back so the retry actually fires.
        client.raise_event_raise = None
        await fx.controller.raise_external_event(
            workspace_id=WORKSPACE,
            run_id=RUN_ID,
            step_id=STEP_ID,
            event_name=EVENT_NAME,
            payload={},
            idempotency_key="abc",
        )
        assert len(client.raise_event_requests) == 2
