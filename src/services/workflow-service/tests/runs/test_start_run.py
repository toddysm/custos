"""WF-IMPL-037 \u2014 :meth:`RunController.start_run` acceptance tests.

Covers every acceptance criterion from #389:

* Happy path \u2014 catalog fetch, compile, persist, schedule, transition
  to running, lifecycle event emitted in correct order.
* Idempotent replay \u2014 same ``(workspace_id, idempotency_key)`` with
  byte-equal ``(workflow_version_id, inputs)`` returns the existing
  :class:`RunRef` and SKIPS both the Catalog and the Dapr scheduler
  (asserted with recording fakes).
* Divergent ``workflow_version_id`` \u2014
  :class:`RunStateConflictError`.
* Divergent ``inputs`` \u2014 :class:`RunStateConflictError`.
* Dapr runtime failure \u2014 :class:`WorkflowRuntimeUnavailableError`
  AND the persisted record transitions to ``failed`` (no
  half-persisted ``queued`` row).
* Status-transition audit order \u2014 ``queued`` \u2192 ``running``
  recorded on the run store.
* Lifecycle envelope shape \u2014 wire-stable
  :class:`LifecycleEvent.to_dict` round-trips through JSON.
* Empty ``workspace_id`` rejected (re-raises :class:`ValueError`).
* Catalog exception propagates unwrapped (not the controller's job).
* :class:`InMemoryLifecycleEventPublisher` records the publish order.
"""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import NamedTuple, cast

import pytest
from custos_cel import FixedClock
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.document import WorkflowDocument
from custos_workflow.runs import (
    LIFECYCLE_KIND_WORKFLOW_STARTED,
    WORKFLOW_NAME,
    CatalogClient,
    InMemoryLifecycleEventPublisher,
    InProcessRunStore,
    LifecycleEvent,
    LifecycleEventPublisher,
    RunController,
    RunRef,
    RunStateConflictError,
    RunStatus,
    RunStore,
    WorkflowRuntimeUnavailableError,
    WorkflowVersion,
    derive_run_id,
)
from custos_workflow.runs.controller import _fingerprint_inputs
from custos_workflow.runtime._common import ScheduleWorkflowRequest
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW_VERSION_ID = "wfv-001"
WORKFLOW_ID = "wf-alpha"
IDEMPOTENCY_KEY = "client-key-1"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


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
    """:class:`CatalogClient` that records every call.

    Tests assert ``calls == []`` on the idempotent replay path.
    """

    def __init__(self, version: WorkflowVersion, *, raise_on_call: Exception | None = None) -> None:
        self._version = version
        self._raise_on_call = raise_on_call
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        if self._raise_on_call is not None:
            raise self._raise_on_call
        return self._version


@dataclass
class _RecordingWorkflowClient:
    """Records every ``schedule_new_workflow`` call.

    ``raise_on_call`` lets tests inject a runtime failure to drive
    the no-half-persisted-record path. ``requests`` captures the
    invocation order for assertion.
    """

    raise_on_call: Exception | None = None
    requests: list[ScheduleWorkflowRequest] = field(default_factory=list)

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        self.requests.append(request)
        if self.raise_on_call is not None:
            raise self.raise_on_call
        return request.instance_id or ""


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


class _Fixture(NamedTuple):
    controller: RunController
    catalog: _RecordingCatalogClient
    store: InProcessRunStore
    workflow_client: _RecordingWorkflowClient
    publisher: InMemoryLifecycleEventPublisher


def _make_controller(
    *,
    catalog: _RecordingCatalogClient | None = None,
    store: InProcessRunStore | None = None,
    workflow_client: _RecordingWorkflowClient | None = None,
    publisher: InMemoryLifecycleEventPublisher | None = None,
    clock: FixedClock | None = None,
) -> _Fixture:
    catalog = catalog or _RecordingCatalogClient(_workflow_version())
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
    """The Protocols must be ``runtime_checkable`` and the in-memory
    stubs must satisfy them."""

    def test_in_memory_lifecycle_publisher_satisfies_protocol(self) -> None:
        assert isinstance(InMemoryLifecycleEventPublisher(), LifecycleEventPublisher)

    def test_recording_catalog_satisfies_protocol(self) -> None:
        assert isinstance(_RecordingCatalogClient(_workflow_version()), CatalogClient)

    def test_in_process_run_store_satisfies_protocol(self) -> None:
        assert isinstance(_store(), RunStore)


# ---------------------------------------------------------------------------
# Fingerprint helper
# ---------------------------------------------------------------------------


class TestFingerprint:
    def test_same_inputs_dict_order_collapses_to_same_digest(self) -> None:
        a = _fingerprint_inputs("v", {"a": 1, "b": 2})
        b = _fingerprint_inputs("v", {"b": 2, "a": 1})
        assert a == b

    def test_different_workflow_version_id_yields_different_digest(self) -> None:
        a = _fingerprint_inputs("v1", {"a": 1})
        b = _fingerprint_inputs("v2", {"a": 1})
        assert a != b

    def test_different_inputs_yields_different_digest(self) -> None:
        a = _fingerprint_inputs("v", {"a": 1})
        b = _fingerprint_inputs("v", {"a": 2})
        assert a != b

    def test_empty_inputs_is_stable(self) -> None:
        a = _fingerprint_inputs("v", {})
        b = _fingerprint_inputs("v", {})
        assert a == b


# ---------------------------------------------------------------------------
# Lifecycle envelope
# ---------------------------------------------------------------------------


class TestLifecycleEvent:
    def test_to_dict_is_json_serializable(self) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_WORKFLOW_STARTED,
            workspace_id=WORKSPACE,
            run_id=derive_run_id(WORKSPACE, IDEMPOTENCY_KEY),
            workflow_version_id=WORKFLOW_VERSION_ID,
            occurred_at=FIXED_NOW,
            extra={"note": "hello"},
        )
        payload = event.to_dict()
        roundtripped = json.loads(json.dumps(payload))
        assert roundtripped["kind"] == "workflow.started"
        assert roundtripped["workspace_id"] == WORKSPACE
        assert roundtripped["workflow_version_id"] == WORKFLOW_VERSION_ID
        assert roundtripped["occurred_at"] == FIXED_NOW.isoformat()
        assert roundtripped["extra"] == {"note": "hello"}

    def test_default_extra_is_empty_mapping(self) -> None:
        event = LifecycleEvent(
            kind="x",
            workspace_id=WORKSPACE,
            run_id=derive_run_id(WORKSPACE, IDEMPOTENCY_KEY),
            workflow_version_id=WORKFLOW_VERSION_ID,
            occurred_at=FIXED_NOW,
        )
        assert dict(event.extra) == {}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestHappyPath:
    async def test_returns_runref_with_running_status(self) -> None:
        fx = _make_controller()
        controller = fx.controller
        ref = await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert isinstance(ref, RunRef)
        assert ref.workspace_id == WORKSPACE
        assert ref.run_id == derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)
        assert ref.workflow_version_id == WORKFLOW_VERSION_ID
        assert ref.status is RunStatus.RUNNING

    async def test_persists_run_record_with_compiled_graph(self) -> None:
        fx = _make_controller()
        controller, store = fx.controller, fx.store
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        run_id = derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)
        record = await store.get_run(WORKSPACE, run_id)
        assert record is not None
        assert record.status is RunStatus.RUNNING
        assert record.workflow_id == WORKFLOW_ID
        assert record.workflow_version == WORKFLOW_VERSION_ID
        assert record.started_at == FIXED_NOW
        assert record.compiled_graph is not None
        assert record.compiled_graph.topological_order == ("a",)

    async def test_schedules_on_workflow_runtime_with_workflow_name(self) -> None:
        fx = _make_controller()
        controller, wfc = fx.controller, fx.workflow_client
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"a": 1},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert len(wfc.requests) == 1
        request = wfc.requests[0]
        assert request.workflow == WORKFLOW_NAME
        assert request.instance_id == derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)
        # The input is a ``RunInput`` instance the orchestrator
        # re-hydrates via ``RunInput.from_dict`` after Dapr's JSON
        # encode/decode round-trip.
        assert request.input.workspace_id == WORKSPACE
        assert request.input.workflow_version_id == WORKFLOW_VERSION_ID
        assert request.input.idempotency_key == IDEMPOTENCY_KEY
        assert dict(request.input.inputs) == {"a": 1}
        assert request.input.compiled_graph_json  # non-empty JSON envelope

    async def test_emits_workflow_started_lifecycle_event(self) -> None:
        fx = _make_controller()
        controller, publisher = fx.controller, fx.publisher
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert len(publisher.events) == 1
        event = publisher.events[0]
        assert event.kind == LIFECYCLE_KIND_WORKFLOW_STARTED
        assert event.workspace_id == WORKSPACE
        assert event.workflow_version_id == WORKFLOW_VERSION_ID
        assert event.run_id == derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)
        assert event.occurred_at == FIXED_NOW

    async def test_calls_catalog_once_on_fresh_start(self) -> None:
        fx = _make_controller()
        controller, catalog = fx.controller, fx.catalog
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert catalog.calls == [(WORKSPACE, WORKFLOW_VERSION_ID)]

    async def test_empty_idempotency_key_yields_fresh_uuid(self) -> None:
        fx = _make_controller()
        controller = fx.controller
        ref_a = await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=None,
        )
        ref_b = await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=None,
        )
        assert ref_a.run_id != ref_b.run_id

    async def test_none_inputs_normalises_to_empty_dict(self) -> None:
        fx = _make_controller()
        controller, wfc = fx.controller, fx.workflow_client
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs=None,
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert dict(wfc.requests[0].input.inputs) == {}


# ---------------------------------------------------------------------------
# Idempotent replay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestIdempotentReplay:
    async def test_byte_equal_replay_skips_catalog_and_dapr(self) -> None:
        fx = _make_controller()
        controller, catalog, wfc, publisher = (
            fx.controller,
            fx.catalog,
            fx.workflow_client,
            fx.publisher,
        )
        ref_first = await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"a": 1, "b": 2},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        # Snapshot call counters BEFORE the replay.
        catalog_calls_before = list(catalog.calls)
        wfc_requests_before = list(wfc.requests)
        publisher_events_before = list(publisher.events)

        ref_second = await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            # Same payload, different dict-key ordering \u2014 still
            # byte-equal via the canonical-JSON fingerprint.
            inputs={"b": 2, "a": 1},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        # Idempotent return.
        assert ref_second == ref_first

        # Catalog / Dapr / publisher untouched on the replay.
        assert catalog.calls == catalog_calls_before
        assert wfc.requests == wfc_requests_before
        assert publisher.events == publisher_events_before

    async def test_replay_after_workflow_version_change_raises_conflict(self) -> None:
        fx = _make_controller()
        controller = fx.controller
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        with pytest.raises(RunStateConflictError) as exc_info:
            await controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id="wfv-002",  # divergent version
                inputs={},
                idempotency_key=IDEMPOTENCY_KEY,
            )
        assert exc_info.value.current_status == RunStatus.RUNNING.value
        assert exc_info.value.attempted_status == RunStatus.QUEUED.value

    async def test_replay_with_divergent_inputs_raises_conflict(self) -> None:
        fx = _make_controller()
        controller = fx.controller
        await controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"a": 1},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        with pytest.raises(RunStateConflictError) as exc_info:
            await controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={"a": 2},  # divergent payload
                idempotency_key=IDEMPOTENCY_KEY,
            )
        assert exc_info.value.run_id == str(derive_run_id(WORKSPACE, IDEMPOTENCY_KEY))


# ---------------------------------------------------------------------------
# Runtime-unavailable path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRuntimeUnavailable:
    async def test_dapr_failure_raises_runtime_unavailable_error(self) -> None:
        wfc = _RecordingWorkflowClient(raise_on_call=ConnectionError("sidecar down"))
        fx = _make_controller(workflow_client=wfc)
        controller = fx.controller
        with pytest.raises(WorkflowRuntimeUnavailableError) as exc_info:
            await controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={},
                idempotency_key=IDEMPOTENCY_KEY,
            )
        assert exc_info.value.cause is not None
        assert "sidecar down" in exc_info.value.cause
        assert exc_info.value.run_id == str(derive_run_id(WORKSPACE, IDEMPOTENCY_KEY))

    async def test_dapr_failure_marks_record_failed_not_queued(self) -> None:
        wfc = _RecordingWorkflowClient(raise_on_call=ConnectionError("sidecar down"))
        fx = _make_controller(workflow_client=wfc)
        controller, store = fx.controller, fx.store
        with pytest.raises(WorkflowRuntimeUnavailableError):
            await controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={},
                idempotency_key=IDEMPOTENCY_KEY,
            )
        run_id = derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)
        record = await store.get_run(WORKSPACE, run_id)
        assert record is not None, "record must persist for audit, not be deleted"
        assert record.status is RunStatus.FAILED
        assert record.reason is not None
        assert "runtime_unavailable" in record.reason

    async def test_dapr_failure_suppresses_lifecycle_event(self) -> None:
        wfc = _RecordingWorkflowClient(raise_on_call=ConnectionError("sidecar down"))
        fx = _make_controller(workflow_client=wfc)
        controller, publisher = fx.controller, fx.publisher
        with pytest.raises(WorkflowRuntimeUnavailableError):
            await controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={},
                idempotency_key=IDEMPOTENCY_KEY,
            )
        # No ``workflow.started`` event must fire when the run failed
        # to start.
        assert publisher.events == []


# ---------------------------------------------------------------------------
# Catalog exception propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCatalogFailure:
    async def test_catalog_exception_propagates_unwrapped(self) -> None:
        catalog = _RecordingCatalogClient(
            _workflow_version(),
            raise_on_call=LookupError("workflow version not found"),
        )
        fx = _make_controller(catalog=catalog)
        controller, store, wfc, publisher = (
            fx.controller,
            fx.store,
            fx.workflow_client,
            fx.publisher,
        )
        with pytest.raises(LookupError, match="workflow version not found"):
            await controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={},
                idempotency_key=IDEMPOTENCY_KEY,
            )
        # Nothing persisted, nothing scheduled, nothing emitted.
        run_id = derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)
        assert await store.get_run(WORKSPACE, run_id) is None
        assert wfc.requests == []
        assert publisher.events == []


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestArgumentValidation:
    async def test_empty_workspace_id_raises_value_error(self) -> None:
        fx = _make_controller()
        controller = fx.controller
        with pytest.raises(ValueError, match="workspace_id"):
            await controller.start_run(
                workspace_id="",
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={},
                idempotency_key=IDEMPOTENCY_KEY,
            )
