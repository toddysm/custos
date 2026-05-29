"""Shape + import-safety tests for the real Dapr adapter (WF-IMPL-029).

These tests intentionally do **not** require a live Dapr sidecar. They
exercise three guarantees:

1. Importing :mod:`custos_workflow.runtime` and constructing
   :class:`WorkflowRuntime` / :class:`WorkflowClient` does not open a
   gRPC connection or attempt to reach the sidecar.
2. Every :class:`WorkflowClient` method delegates to the underlying
   :class:`dapr.ext.workflow.DaprWorkflowClient` with the expected
   args, and returns whatever the SDK returns (transformed to a
   :class:`RunState` for :meth:`get_workflow_state`).
3. Every :class:`WorkflowRuntime` method delegates to the underlying
   :class:`dapr.ext.workflow.WorkflowRuntime` and respects the
   idempotent start / shutdown contract.

The underlying SDK is swapped with :class:`unittest.mock.MagicMock`
fakes so the test suite remains hermetic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from dapr.ext.workflow import WorkflowStatus

from custos_workflow.runtime import (
    GetRunStateRequest,
    PauseRunRequest,
    RaiseRunEventRequest,
    ResumeRunRequest,
    RunStatus,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
    WorkflowClient,
    WorkflowRuntime,
)
from custos_workflow.runtime.dapr import dapr_status_to_run_status

# ---------------------------------------------------------------------------
# Import safety
# ---------------------------------------------------------------------------


def test_import_does_not_open_sidecar_connection() -> None:
    """Importing the module and constructing the adapter is side-effect free."""

    # The module imports symbols from ``dapr.ext.workflow`` but does
    # not instantiate :class:`DaprWorkflowClient` or
    # :class:`WorkflowRuntime` at module load. Constructing the
    # adapter with no injected client must also be inert.
    client = WorkflowClient()
    runtime = WorkflowRuntime()
    assert client._client is None
    assert runtime._runtime is None
    assert runtime.is_started is False


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dapr_status", "expected"),
    [
        (WorkflowStatus.UNKNOWN, RunStatus.UNKNOWN),
        (WorkflowStatus.PENDING, RunStatus.PENDING),
        (WorkflowStatus.RUNNING, RunStatus.RUNNING),
        (WorkflowStatus.SUSPENDED, RunStatus.SUSPENDED),
        (WorkflowStatus.COMPLETED, RunStatus.COMPLETED),
        (WorkflowStatus.FAILED, RunStatus.FAILED),
        (WorkflowStatus.TERMINATED, RunStatus.TERMINATED),
        (WorkflowStatus.STALLED, RunStatus.STALLED),
    ],
)
def test_dapr_status_translation_covers_every_member(
    dapr_status: WorkflowStatus, expected: RunStatus
) -> None:
    assert dapr_status_to_run_status(dapr_status) is expected


def test_dapr_status_translation_falls_back_to_unknown_for_unmapped() -> None:
    # A sentinel that isn't in the mapping dict — exercises the
    # ``.get(..., RunStatus.UNKNOWN)`` fallback. Cast through ``Any``
    # because mypy will reject the literal type mismatch.
    sentinel: Any = object()
    assert dapr_status_to_run_status(sentinel) is RunStatus.UNKNOWN


# ---------------------------------------------------------------------------
# WorkflowClient delegation
# ---------------------------------------------------------------------------


async def test_schedule_new_workflow_delegates_with_request_fields() -> None:
    inner = MagicMock()
    inner.schedule_new_workflow.return_value = "inst-1"
    client = WorkflowClient(client=inner)

    start_at = datetime(2026, 5, 28, tzinfo=UTC)
    req = ScheduleWorkflowRequest(
        workflow="my_wf",
        input={"k": "v"},
        instance_id="inst-1",
        start_at=start_at,
    )
    instance_id = await client.schedule_new_workflow(req)
    assert instance_id == "inst-1"
    inner.schedule_new_workflow.assert_called_once_with(
        "my_wf",
        input={"k": "v"},
        instance_id="inst-1",
        start_at=start_at,
    )


async def test_get_workflow_state_returns_none_when_sdk_returns_none() -> None:
    inner = MagicMock()
    inner.get_workflow_state.return_value = None
    client = WorkflowClient(client=inner)

    result = await client.get_workflow_state(GetRunStateRequest(instance_id="missing"))
    assert result is None
    inner.get_workflow_state.assert_called_once_with("missing", fetch_payloads=True)


async def test_get_workflow_state_maps_dapr_state_to_run_state() -> None:
    state = MagicMock()
    state.instance_id = "inst-7"
    state.name = "wf"
    state.runtime_status = WorkflowStatus.RUNNING
    state.created_at = datetime(2026, 5, 1, tzinfo=UTC)
    state.last_updated_at = datetime(2026, 5, 2, tzinfo=UTC)
    state.serialized_input = '{"a":1}'
    state.serialized_output = None
    state.failure_details = None

    inner = MagicMock()
    inner.get_workflow_state.return_value = state
    client = WorkflowClient(client=inner)

    snapshot = await client.get_workflow_state(
        GetRunStateRequest(instance_id="inst-7", fetch_payloads=False)
    )
    assert snapshot is not None
    assert snapshot.instance_id == "inst-7"
    assert snapshot.name == "wf"
    assert snapshot.status is RunStatus.RUNNING
    assert snapshot.serialized_input == '{"a":1}'
    assert snapshot.serialized_output is None
    assert snapshot.failure_message is None
    inner.get_workflow_state.assert_called_once_with("inst-7", fetch_payloads=False)


async def test_get_workflow_state_includes_failure_details_when_present() -> None:
    failure = MagicMock()
    failure.message = "boom"
    failure.error_type = "RuntimeError"

    state = MagicMock()
    state.instance_id = "inst-8"
    state.name = "wf"
    state.runtime_status = WorkflowStatus.FAILED
    state.created_at = None
    state.last_updated_at = None
    state.serialized_input = None
    state.serialized_output = None
    state.failure_details = failure

    inner = MagicMock()
    inner.get_workflow_state.return_value = state
    client = WorkflowClient(client=inner)

    snapshot = await client.get_workflow_state(GetRunStateRequest(instance_id="inst-8"))
    assert snapshot is not None
    assert snapshot.status is RunStatus.FAILED
    assert snapshot.failure_message == "boom"
    assert snapshot.failure_type == "RuntimeError"


async def test_terminate_workflow_passes_output_and_recursive() -> None:
    inner = MagicMock()
    client = WorkflowClient(client=inner)
    await client.terminate_workflow(
        TerminateRunRequest(instance_id="i", output={"reason": "user"}, recursive=False)
    )
    inner.terminate_workflow.assert_called_once_with(
        "i", output={"reason": "user"}, recursive=False
    )


async def test_pause_workflow_delegates() -> None:
    inner = MagicMock()
    client = WorkflowClient(client=inner)
    await client.pause_workflow(PauseRunRequest(instance_id="i"))
    inner.pause_workflow.assert_called_once_with("i")


async def test_resume_workflow_delegates() -> None:
    inner = MagicMock()
    client = WorkflowClient(client=inner)
    await client.resume_workflow(ResumeRunRequest(instance_id="i"))
    inner.resume_workflow.assert_called_once_with("i")


async def test_raise_workflow_event_delegates_with_data_keyword() -> None:
    inner = MagicMock()
    client = WorkflowClient(client=inner)
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id="i", event_name="ev", data={"a": 1})
    )
    inner.raise_workflow_event.assert_called_once_with("i", "ev", data={"a": 1})


async def test_aclose_releases_underlying_client_and_is_idempotent() -> None:
    inner = MagicMock()
    client = WorkflowClient(client=inner)
    await client.aclose()
    inner.close.assert_called_once()
    # Second close is a no-op (client was released).
    inner.close.reset_mock()
    await client.aclose()
    inner.close.assert_not_called()


async def test_lazy_client_construction_on_first_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Constructing :class:`WorkflowClient` with no ``client=`` should lazily
    instantiate the real :class:`DaprWorkflowClient` on first method call."""

    constructed: list[MagicMock] = []

    class _StubDaprWorkflowClient:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []
            constructed.append(self)  # type: ignore[arg-type]

        def schedule_new_workflow(
            self,
            workflow: Any,
            *,
            input: Any = None,
            instance_id: Any = None,
            start_at: Any = None,
        ) -> str:
            self.calls.append((workflow, input, instance_id, start_at))
            return "auto-generated-id"

    monkeypatch.setattr("custos_workflow.runtime.dapr.DaprWorkflowClient", _StubDaprWorkflowClient)
    client = WorkflowClient()
    assert client._client is None  # not constructed yet
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="wf"))
    assert instance_id == "auto-generated-id"
    assert len(constructed) == 1
    assert client._client is constructed[0]


# ---------------------------------------------------------------------------
# WorkflowRuntime delegation
# ---------------------------------------------------------------------------


async def test_runtime_registers_and_starts_idempotently() -> None:
    inner = MagicMock()
    runtime = WorkflowRuntime(runtime=inner)

    def wf(_ctx: Any, _i: Any) -> Any:
        return None

    def act(_ctx: Any, _i: Any) -> Any:
        return None

    runtime.register_workflow(wf, name="my_wf")
    runtime.register_activity(act, name="my_act")
    inner.register_workflow.assert_called_once_with(wf, name="my_wf")
    inner.register_activity.assert_called_once_with(act, name="my_act")

    await runtime.start()
    inner.start.assert_called_once()
    assert runtime.is_started is True

    # Second start is a no-op.
    await runtime.start()
    inner.start.assert_called_once()


async def test_runtime_shutdown_is_safe_before_start_and_idempotent() -> None:
    inner = MagicMock()
    runtime = WorkflowRuntime(runtime=inner)
    # shutdown before start does nothing.
    await runtime.shutdown()
    inner.shutdown.assert_not_called()

    await runtime.start()
    await runtime.shutdown()
    inner.shutdown.assert_called_once()
    assert runtime.is_started is False
    # Second shutdown does nothing.
    await runtime.shutdown()
    inner.shutdown.assert_called_once()


async def test_runtime_wait_for_worker_ready_delegates_timeout() -> None:
    inner = MagicMock()
    inner.wait_for_worker_ready.return_value = True
    runtime = WorkflowRuntime(runtime=inner)
    result = await runtime.wait_for_worker_ready(timeout=1.5)
    assert result is True
    inner.wait_for_worker_ready.assert_called_once_with(1.5)


async def test_runtime_lazy_construction_on_first_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed: list[MagicMock] = []

    class _StubRuntime:
        def __init__(self) -> None:
            self.registered_workflows: list[Any] = []
            self.registered_activities: list[Any] = []
            self.started = 0
            self.shut = 0
            constructed.append(self)  # type: ignore[arg-type]

        def register_workflow(self, fn: Any, *, name: Any = None) -> None:
            self.registered_workflows.append((fn, name))

        def register_activity(self, fn: Any, *, name: Any = None) -> None:
            self.registered_activities.append((fn, name))

        def start(self) -> None:
            self.started += 1

        def shutdown(self) -> None:
            self.shut += 1

        def wait_for_worker_ready(self, timeout: float) -> bool:
            del timeout
            return True

    monkeypatch.setattr("custos_workflow.runtime.dapr._DaprWorkflowRuntime", _StubRuntime)
    runtime = WorkflowRuntime()
    assert runtime._runtime is None

    def wf(_ctx: Any, _i: Any) -> Any:
        return None

    runtime.register_workflow(wf, name="lazy")
    assert len(constructed) == 1
    assert runtime._runtime is constructed[0]
    assert constructed[0].registered_workflows == [(wf, "lazy")]


# ---------------------------------------------------------------------------
# Signature smoke — proves the adapter exposes the methods later
# Run Controller tasks (WF-IMPL-037 ... WF-IMPL-040) expect.
# ---------------------------------------------------------------------------


def test_workflow_client_exposes_full_lifecycle_surface() -> None:
    client = WorkflowClient()
    for name in (
        "schedule_new_workflow",
        "get_workflow_state",
        "terminate_workflow",
        "pause_workflow",
        "resume_workflow",
        "raise_workflow_event",
        "aclose",
    ):
        assert hasattr(client, name), f"WorkflowClient is missing {name}"


def test_workflow_runtime_exposes_full_lifecycle_surface() -> None:
    runtime = WorkflowRuntime()
    for name in (
        "register_workflow",
        "register_activity",
        "start",
        "shutdown",
        "wait_for_worker_ready",
        "is_started",
    ):
        assert hasattr(runtime, name), f"WorkflowRuntime is missing {name}"
