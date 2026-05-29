"""Tests for :meth:`RunController.get_run` and
:meth:`RunController.list_runs` (WF-IMPL-040).

Mirrors the style of ``test_cancel_run.py`` and
``test_pause_resume.py``: in-process Run Store layered on
:class:`FakeMetadataStoreProvider`, recording workflow client /
catalog client / lifecycle publisher, ``FixedClock`` for deterministic
timestamps. ``get_run`` / ``list_runs`` are read-side projections, so
the publisher and sleeper are not exercised.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, NamedTuple, cast

import pytest
from custos_cel import FixedClock
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.pagination import Cursor, Page

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.document import WorkflowDocument
from custos_workflow.runs import (
    CatalogClient,
    InMemoryLifecycleEventPublisher,
    InProcessRunStore,
    LifecycleEventPublisher,
    RunController,
    RunNotFoundError,
    RunRecord,
    RunRef,
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
IDEMPOTENCY_KEY = "client-key-get-list"
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
    """Minimal :class:`CatalogClient` stub; never called by get / list."""

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

    ``get_state_raise`` drives the runtime-unavailable path on
    :meth:`get_workflow_state`. ``state_sequence`` lets tests inject
    successive snapshots — the test never hits more than one call per
    ``get_run`` invocation, but the sequence pattern is consistent with
    the sibling test modules.
    """

    get_state_raise: Exception | None = None
    state_sequence: list[RuntimeRunState | None] = field(default_factory=list)

    state_requests: list[GetRunStateRequest] = field(default_factory=list)

    async def schedule_new_workflow(
        self, request: ScheduleWorkflowRequest
    ) -> str:  # pragma: no cover
        raise NotImplementedError("get_run / list_runs never call schedule_new_workflow")

    async def terminate_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("get_run / list_runs never call terminate_workflow")

    async def pause_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("get_run / list_runs never call pause_workflow")

    async def resume_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError("get_run / list_runs never call resume_workflow")

    async def get_workflow_state(self, request: GetRunStateRequest) -> RuntimeRunState | None:
        self.state_requests.append(request)
        if self.get_state_raise is not None:
            raise self.get_state_raise
        if not self.state_sequence:
            return None
        if len(self.state_requests) <= len(self.state_sequence):
            return self.state_sequence[len(self.state_requests) - 1]
        return self.state_sequence[-1]


class _ExplodingGetStateClient:
    """Workflow client whose ``get_workflow_state`` always raises.

    Used in the terminal-status path assertion: ``get_run`` must NOT
    call the runtime when the persisted status is terminal — a real
    Dapr round-trip would surface as ``AssertionError`` immediately.
    """

    async def schedule_new_workflow(
        self, request: ScheduleWorkflowRequest
    ) -> str:  # pragma: no cover
        raise NotImplementedError

    async def terminate_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError

    async def pause_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError

    async def resume_workflow(self, request: Any) -> None:  # pragma: no cover
        raise NotImplementedError

    async def get_workflow_state(self, request: GetRunStateRequest) -> RuntimeRunState | None:
        raise AssertionError("get_workflow_state must not be called for a terminal-status run")


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


def _runtime_state(status: RuntimeRunStatus, *, instance_id: str = str(RUN_ID)) -> RuntimeRunState:
    return RuntimeRunState(
        instance_id=instance_id,
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
    run_id: RunId | None = None,
    workspace_id: str = WORKSPACE,
) -> RunRecord:
    """Persist a :class:`RunRecord` and walk it to *status*."""
    rid = run_id or RUN_ID
    record = RunRecord(
        workspace_id=workspace_id,
        run_id=rid,
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
        await store.update_run_status(workspace_id, rid, next_status)
    stored = await store.get_run(workspace_id, rid)
    assert stored is not None
    return stored


class _Fixture(NamedTuple):
    controller: RunController
    catalog: _RecordingCatalogClient
    store: InProcessRunStore
    workflow_client: Any
    publisher: InMemoryLifecycleEventPublisher


def _make_controller(
    *,
    store: InProcessRunStore | None = None,
    workflow_client: Any | None = None,
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
    """The recording doubles satisfy the structural Protocols."""

    def test_recording_workflow_client_has_get_state(self) -> None:
        client = _RecordingWorkflowClient()
        assert callable(client.get_workflow_state)

    def test_exploding_client_has_get_state(self) -> None:
        client = _ExplodingGetStateClient()
        assert callable(client.get_workflow_state)

    def test_in_memory_publisher_satisfies_lifecycle_publisher_protocol(self) -> None:
        assert isinstance(InMemoryLifecycleEventPublisher(), LifecycleEventPublisher)

    def test_recording_catalog_satisfies_protocol(self) -> None:
        assert isinstance(_RecordingCatalogClient(_workflow_version()), CatalogClient)

    def test_in_process_run_store_satisfies_protocol(self) -> None:
        assert isinstance(_store(), RunStore)


# ---------------------------------------------------------------------------
# get_run: RunNotFound
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGetRunNotFound:
    """Unknown ``run_id`` surfaces :class:`RunNotFoundError`."""

    async def test_get_on_unknown_id_raises_run_not_found(self) -> None:
        fx = _make_controller()
        with pytest.raises(RunNotFoundError) as excinfo:
            await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert str(RUN_ID) in str(excinfo.value)
        # No side effects.
        assert fx.workflow_client.state_requests == []


# ---------------------------------------------------------------------------
# get_run: terminal-status pure-store path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGetRunTerminalStatus:
    """A terminal-status read NEVER calls the runtime
    (acceptance criterion: terminal-status reads never call
    ``WorkflowClient.get_workflow_state``)."""

    async def test_terminal_succeeded_does_not_call_runtime(self) -> None:
        client = _ExplodingGetStateClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.SUCCEEDED)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.SUCCEEDED

    async def test_terminal_failed_does_not_call_runtime(self) -> None:
        client = _ExplodingGetStateClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.FAILED)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.FAILED

    async def test_terminal_cancelled_does_not_call_runtime(self) -> None:
        client = _ExplodingGetStateClient()
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.CANCELLED)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.CANCELLED


# ---------------------------------------------------------------------------
# get_run: in-flight overlay path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGetRunOverlay:
    """Non-terminal persisted status triggers a single runtime overlay
    call. The persisted row MUST NOT be mutated (read-through
    projection — acceptance criterion: store snapshot before / after
    is byte-equal)."""

    async def test_overlay_invokes_runtime_exactly_once(self) -> None:
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.RUNNING)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.QUEUED)
        await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert len(client.state_requests) == 1
        assert client.state_requests[0].instance_id == str(RUN_ID)

    async def test_overlay_returns_runtime_status(self) -> None:
        # Persisted is QUEUED, runtime is RUNNING — overlay wins.
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.RUNNING)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.QUEUED)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.RUNNING

    async def test_overlay_never_mutates_persisted_row(self) -> None:
        """The persisted snapshot before and after must be byte-equal."""
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.RUNNING)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.QUEUED)
        before = await fx.store.get_run(WORKSPACE, RUN_ID)
        await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        after = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert before == after
        assert before is not None
        assert before.status == RunStatus.QUEUED

    async def test_overlay_returned_record_carries_overlaid_status(self) -> None:
        """Every other field on the returned record mirrors the row."""
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.RUNNING)])
        fx = _make_controller(workflow_client=client)
        seeded = await _seed_run(fx.store, status=RunStatus.QUEUED)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        # Status is overlaid; every other field mirrors the seeded row.
        assert record.status == RunStatus.RUNNING
        assert record == replace(seeded, status=RunStatus.RUNNING)

    @pytest.mark.parametrize(
        ("runtime", "persisted_before", "expected"),
        [
            (RuntimeRunStatus.PENDING, RunStatus.QUEUED, RunStatus.QUEUED),
            (RuntimeRunStatus.RUNNING, RunStatus.QUEUED, RunStatus.RUNNING),
            (RuntimeRunStatus.SUSPENDED, RunStatus.RUNNING, RunStatus.PAUSED),
            (RuntimeRunStatus.COMPLETED, RunStatus.RUNNING, RunStatus.SUCCEEDED),
            (RuntimeRunStatus.FAILED, RunStatus.RUNNING, RunStatus.FAILED),
            (RuntimeRunStatus.TERMINATED, RunStatus.RUNNING, RunStatus.CANCELLED),
        ],
    )
    async def test_overlay_translates_runtime_status_to_persisted_status(
        self,
        runtime: RuntimeRunStatus,
        persisted_before: RunStatus,
        expected: RunStatus,
    ) -> None:
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(runtime)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=persisted_before)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == expected

    async def test_overlay_unknown_runtime_status_falls_back_to_persisted(self) -> None:
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.UNKNOWN)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.RUNNING

    async def test_overlay_stalled_runtime_status_falls_back_to_persisted(self) -> None:
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.STALLED)])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.RUNNING

    async def test_overlay_runtime_none_falls_back_to_persisted(self) -> None:
        """Dapr-side instance gone (``None``) — surface persisted row."""
        client = _RecordingWorkflowClient(state_sequence=[None])
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record.status == RunStatus.RUNNING

    async def test_overlay_runtime_matches_persisted_returns_record_verbatim(self) -> None:
        """No spurious replace() when runtime status matches the row."""
        client = _RecordingWorkflowClient(state_sequence=[_runtime_state(RuntimeRunStatus.RUNNING)])
        fx = _make_controller(workflow_client=client)
        seeded = await _seed_run(fx.store, status=RunStatus.RUNNING)
        record = await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert record == seeded


# ---------------------------------------------------------------------------
# get_run: runtime unavailable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGetRunRuntimeUnavailable:
    """Runtime failures surface as :class:`WorkflowRuntimeUnavailableError`."""

    async def test_get_state_failure_raises_runtime_unavailable(self) -> None:
        client = _RecordingWorkflowClient(get_state_raise=ConnectionError("boom"))
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        with pytest.raises(WorkflowRuntimeUnavailableError) as excinfo:
            await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        assert excinfo.value.cause == "boom"
        assert str(RUN_ID) in str(excinfo.value)

    async def test_get_state_failure_leaves_persisted_row_unchanged(self) -> None:
        client = _RecordingWorkflowClient(get_state_raise=ConnectionError("boom"))
        fx = _make_controller(workflow_client=client)
        await _seed_run(fx.store, status=RunStatus.RUNNING)
        before = await fx.store.get_run(WORKSPACE, RUN_ID)
        with pytest.raises(WorkflowRuntimeUnavailableError):
            await fx.controller.get_run(workspace_id=WORKSPACE, run_id=RUN_ID)
        after = await fx.store.get_run(WORKSPACE, RUN_ID)
        assert before == after


# ---------------------------------------------------------------------------
# list_runs: pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestListRuns:
    """``list_runs`` delegates to ``RunStore.list_runs`` and maps each
    :class:`RunRecord` to a :class:`RunRef`. Never calls the runtime."""

    async def _seed_workspace(self, store: InProcessRunStore, count: int) -> list[RunId]:
        ids: list[RunId] = []
        for i in range(count):
            rid = derive_run_id(WORKSPACE, f"{IDEMPOTENCY_KEY}-{i}")
            await _seed_run(store, status=RunStatus.RUNNING, run_id=rid)
            ids.append(rid)
        return ids

    async def test_empty_workspace_returns_empty_page(self) -> None:
        fx = _make_controller(workflow_client=_ExplodingGetStateClient())
        page = await fx.controller.list_runs(workspace_id=WORKSPACE)
        assert isinstance(page, Page)
        assert page.items == ()
        assert page.next_cursor is None

    async def test_returns_page_of_runrefs(self) -> None:
        fx = _make_controller(workflow_client=_ExplodingGetStateClient())
        await self._seed_workspace(fx.store, 3)
        page = await fx.controller.list_runs(workspace_id=WORKSPACE)
        assert len(page.items) == 3
        for ref in page.items:
            assert isinstance(ref, RunRef)
            assert ref.workspace_id == WORKSPACE
            assert ref.workflow_version_id == WORKFLOW_VERSION_ID
            assert ref.status == RunStatus.RUNNING

    async def test_never_calls_runtime(self) -> None:
        """``list_runs`` must NOT issue per-item overlay calls."""
        client = _ExplodingGetStateClient()  # raises if invoked
        fx = _make_controller(workflow_client=client)
        await self._seed_workspace(fx.store, 5)
        # The exploding client raises AssertionError on any get_state call.
        page = await fx.controller.list_runs(workspace_id=WORKSPACE)
        assert len(page.items) == 5

    async def test_cursor_round_trip_preserves_order(self) -> None:
        fx = _make_controller(workflow_client=_ExplodingGetStateClient())
        ids = await self._seed_workspace(fx.store, 5)

        # Page 1: limit=2 → first two runs, next cursor present.
        page_1 = await fx.controller.list_runs(workspace_id=WORKSPACE, limit=2)
        assert [r.run_id for r in page_1.items] == ids[0:2]
        assert page_1.next_cursor is not None

        # Page 2: cursor from page 1, limit=2 → next two runs.
        page_2 = await fx.controller.list_runs(
            workspace_id=WORKSPACE, cursor=page_1.next_cursor, limit=2
        )
        assert [r.run_id for r in page_2.items] == ids[2:4]
        assert page_2.next_cursor is not None

        # Page 3: cursor from page 2, limit=2 → final run, terminal page.
        page_3 = await fx.controller.list_runs(
            workspace_id=WORKSPACE, cursor=page_2.next_cursor, limit=2
        )
        assert [r.run_id for r in page_3.items] == ids[4:5]
        assert page_3.next_cursor is None

    async def test_explicit_none_cursor_starts_first_page(self) -> None:
        fx = _make_controller(workflow_client=_ExplodingGetStateClient())
        ids = await self._seed_workspace(fx.store, 3)
        page = await fx.controller.list_runs(workspace_id=WORKSPACE, cursor=None, limit=10)
        assert [r.run_id for r in page.items] == ids
        assert page.next_cursor is None

    async def test_other_workspace_runs_are_not_returned(self) -> None:
        fx = _make_controller(workflow_client=_ExplodingGetStateClient())
        await self._seed_workspace(fx.store, 2)
        await _seed_run(
            fx.store,
            status=RunStatus.RUNNING,
            run_id=derive_run_id("ws-002", IDEMPOTENCY_KEY),
            workspace_id="ws-002",
        )
        page = await fx.controller.list_runs(workspace_id=WORKSPACE)
        assert len(page.items) == 2
        assert all(r.workspace_id == WORKSPACE for r in page.items)

    async def test_cursor_type_round_trips_through_controller(self) -> None:
        """The opaque :class:`Cursor` token is passed through unchanged
        — callers MUST be able to round-trip it without inspection."""
        fx = _make_controller(workflow_client=_ExplodingGetStateClient())
        await self._seed_workspace(fx.store, 3)
        page_1 = await fx.controller.list_runs(workspace_id=WORKSPACE, limit=1)
        assert isinstance(page_1.next_cursor, Cursor)
        # Token is treated as opaque — we re-feed it as-is.
        page_2 = await fx.controller.list_runs(
            workspace_id=WORKSPACE, cursor=page_1.next_cursor, limit=1
        )
        # Distinct row from page 1.
        assert page_1.items[0].run_id != page_2.items[0].run_id
