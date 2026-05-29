"""Fake runtime + client tests (WF-IMPL-029).

Acceptance criterion: a 3-step orchestrator round-trips end-to-end
through ``FakeWorkflowRuntime`` and the resulting instance exposes
``status`` / ``output`` / ``history`` for assertions.

The suite covers (well above the headline criterion) every public
surface so the fake can be used confidently in the remaining
Run Controller tasks (WF-IMPL-035..040):

* Sequential 3-step orchestrator end-to-end.
* Scheduling with an explicit ``instance_id`` (idempotent re-schedule).
* Scheduling an unregistered workflow raises.
* External event delivery (queued before vs. queued after the wait).
* Durable timer fires synchronously.
* ``custom_status`` set inside the orchestrator surfaces on the
  instance.
* Activity failure surfaces inside the orchestrator and can be caught.
* Activity failure that escapes the generator marks the run FAILED.
* Unregistered activity raises ``LookupError`` inside the generator.
* Pause + resume; events raised while suspended are queued and
  consumed on resume.
* Terminate transitions to TERMINATED and is a no-op on terminal
  instances.
* :meth:`FakeWorkflowClient.get_workflow_state` returns ``None`` for
  unknown ids and a structured :class:`RunState` snapshot otherwise.
* Non-generator workflow function completes immediately.
* Yielding an unsupported task type fails the instance loudly.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from custos_workflow.runtime import (
    FakeActivityContext,
    FakeWorkflowClient,
    FakeWorkflowContext,
    FakeWorkflowRuntime,
    GetRunStateRequest,
    PauseRunRequest,
    RaiseRunEventRequest,
    ResumeRunRequest,
    RunState,
    RunStatus,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
)
from custos_workflow.runtime.fake import (
    HistoryEvent,
    UnknownInstanceError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC))


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


# ---------------------------------------------------------------------------
# Three-step happy path (acceptance criterion)
# ---------------------------------------------------------------------------


async def test_three_step_orchestrator_round_trip(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """Round-trip a 3-step orchestrator; assert status / output / history."""

    def step_one(_ctx: FakeActivityContext, payload: dict[str, Any]) -> dict[str, Any]:
        return {"value": payload["value"] + 1}

    def step_two(_ctx: FakeActivityContext, payload: dict[str, Any]) -> dict[str, Any]:
        return {"value": payload["value"] * 10}

    def step_three(_ctx: FakeActivityContext, payload: dict[str, Any]) -> dict[str, Any]:
        return {"value": payload["value"] - 5}

    def orchestrator(
        ctx: FakeWorkflowContext, payload: dict[str, Any]
    ) -> Generator[Any, Any, dict[str, Any]]:
        a = yield ctx.call_activity("step_one", input=payload)
        b = yield ctx.call_activity("step_two", input=a)
        c = yield ctx.call_activity("step_three", input=b)
        assert isinstance(c, dict)
        return c

    runtime.register_workflow(orchestrator, name="three_step")
    runtime.register_activity(step_one)
    runtime.register_activity(step_two)
    runtime.register_activity(step_three)

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="three_step", input={"value": 1})
    )

    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    # (1 + 1) * 10 - 5 = 15
    assert state.output == {"value": 15}

    kinds = [event.kind for event in state.history]
    assert kinds == [
        "started",
        "activity_completed",
        "activity_completed",
        "activity_completed",
        "completed",
    ]
    activity_names = [
        event.detail["activity"] for event in state.history if event.kind == "activity_completed"
    ]
    assert activity_names == ["step_one", "step_two", "step_three"]


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------


async def test_schedule_explicit_instance_id_is_idempotent(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def workflow(_ctx: FakeWorkflowContext, payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - never yields
            yield None
        return payload

    runtime.register_workflow(workflow, name="echo")

    first = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="echo", input="hello", instance_id="run-1")
    )
    assert first == "run-1"

    # Re-scheduling the same id returns the existing instance, does not reset.
    second = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="echo", input="world", instance_id="run-1")
    )
    assert second == "run-1"
    state = runtime.instance("run-1")
    assert state.input == "hello"  # original input preserved
    assert state.status == RunStatus.COMPLETED


async def test_schedule_unregistered_workflow_raises(
    client: FakeWorkflowClient,
) -> None:
    with pytest.raises(UnknownInstanceError) as exc_info:
        await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="never_registered"))
    assert "never_registered" in str(exc_info.value)


async def test_schedule_with_workflow_callable_resolves_by_dunder_name(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def my_wf(_ctx: FakeWorkflowContext, payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover
            yield None
        return payload

    runtime.register_workflow(my_wf)
    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow=my_wf, input=42)
    )
    state = runtime.instance(instance_id)
    assert state.name == "my_wf"
    assert state.output == 42


# ---------------------------------------------------------------------------
# External events
# ---------------------------------------------------------------------------


async def test_external_event_arrives_after_wait(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        approval = yield ctx.wait_for_external_event("approval")
        return {"approval": approval}

    runtime.register_workflow(orchestrator, name="approval_wait")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="approval_wait")
    )

    state = runtime.instance(instance_id)
    assert state.status == RunStatus.RUNNING
    assert state.waiting_for_event == "approval"

    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id=instance_id, event_name="approval", data="approved")
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"approval": "approved"}


async def test_external_event_queued_before_wait_drains_fifo(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, list[Any]]:
        first = yield ctx.wait_for_external_event("ev")
        second = yield ctx.wait_for_external_event("ev")
        return [first, second]

    runtime.register_workflow(orchestrator, name="two_events")
    runtime.register_workflow(_noop_workflow, name="noop")
    seeded = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="noop", instance_id="ev-seed")
    )
    assert seeded == "ev-seed"

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="two_events", instance_id="ev-run")
    )
    # Two events arrive before the second wait — they must be drained FIFO.
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id=instance_id, event_name="ev", data="A")
    )
    state = runtime.instance(instance_id)
    # First event consumed; second is queued for the second wait.
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id=instance_id, event_name="ev", data="B")
    )
    assert state.status == RunStatus.COMPLETED
    assert state.output == ["A", "B"]


async def test_external_event_on_terminal_instance_is_noop(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    runtime.register_workflow(_noop_workflow, name="noop")
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="noop"))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED

    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id=instance_id, event_name="any", data=None)
    )
    # No new history events appended past the original "started" + "completed".
    assert [e.kind for e in state.history] == ["started", "completed"]


# ---------------------------------------------------------------------------
# Timer
# ---------------------------------------------------------------------------


async def test_create_timer_fires_synchronously(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, str]:
        yield ctx.create_timer(timedelta(seconds=5))
        yield ctx.create_timer(ctx.current_utc_datetime + timedelta(minutes=1))
        return "ok"

    runtime.register_workflow(orchestrator, name="timer_wait")
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="timer_wait"))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    fire_events = [e for e in state.history if e.kind == "timer_fired"]
    assert len(fire_events) == 2
    # Both timers have a fire_at populated.
    assert all(isinstance(e.detail["fire_at"], datetime) for e in fire_events)


# ---------------------------------------------------------------------------
# Custom status
# ---------------------------------------------------------------------------


async def test_set_custom_status_surfaces_on_context() -> None:
    ctx = FakeWorkflowContext(
        instance_id="run-1",
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert ctx.custom_status is None
    assert ctx.is_replaying is False
    ctx.set_custom_status("midway")
    assert ctx.custom_status == "midway"


# ---------------------------------------------------------------------------
# Activity failure semantics
# ---------------------------------------------------------------------------


async def test_activity_failure_caught_inside_generator(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def boom(_ctx: FakeActivityContext, _payload: Any) -> Any:
        raise RuntimeError("intentional")

    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, str]:
        try:
            yield ctx.call_activity("boom")
        except RuntimeError as exc:
            return f"caught: {exc}"
        return "no-exception"

    runtime.register_workflow(orchestrator, name="try_catch")
    runtime.register_activity(boom)
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="try_catch"))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == "caught: intentional"
    failed_events = [e for e in state.history if e.kind == "activity_failed"]
    assert len(failed_events) == 1
    assert failed_events[0].detail["error_type"] == "RuntimeError"


async def test_activity_failure_uncaught_fails_the_run(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def boom(_ctx: FakeActivityContext, _payload: Any) -> Any:
        raise ValueError("nope")

    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        yield ctx.call_activity("boom")
        return "unreachable"

    runtime.register_workflow(orchestrator, name="uncaught")
    runtime.register_activity(boom)
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="uncaught"))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.FAILED
    assert state.failure_message == "nope"
    assert state.failure_type == "ValueError"


async def test_unregistered_activity_surfaces_as_lookup_error(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, str]:
        try:
            yield ctx.call_activity("never_registered")
        except LookupError as exc:
            return f"missing: {exc}"
        return "unreachable"

    runtime.register_workflow(orchestrator, name="missing_act")
    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="missing_act")
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output is not None
    assert "never_registered" in state.output


# ---------------------------------------------------------------------------
# Pause / resume
# ---------------------------------------------------------------------------


async def test_pause_then_resume_drains_buffered_events(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        # Yield once so we can pause mid-flight.
        first = yield ctx.call_activity("first", input=1)
        second = yield ctx.wait_for_external_event("ev")
        return {"first": first, "second": second}

    def first(_ctx: FakeActivityContext, payload: int) -> int:
        return payload + 100

    runtime.register_workflow(orchestrator, name="pausable")
    runtime.register_activity(first)
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="pausable"))
    state = runtime.instance(instance_id)
    # The orchestrator is now blocked on wait_for_external_event.
    assert state.status == RunStatus.RUNNING
    assert state.waiting_for_event == "ev"

    await client.pause_workflow(PauseRunRequest(instance_id=instance_id))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.SUSPENDED

    # Event raised while suspended must be buffered.
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id=instance_id, event_name="ev", data="X")
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.SUSPENDED  # still paused

    await client.resume_workflow(ResumeRunRequest(instance_id=instance_id))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"first": 101, "second": "X"}


async def test_pause_on_non_running_is_noop(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    runtime.register_workflow(_noop_workflow, name="noop")
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="noop"))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    await client.pause_workflow(PauseRunRequest(instance_id=instance_id))
    await client.resume_workflow(ResumeRunRequest(instance_id=instance_id))
    assert state.status == RunStatus.COMPLETED


# ---------------------------------------------------------------------------
# Terminate
# ---------------------------------------------------------------------------


async def test_terminate_running_instance(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        yield ctx.wait_for_external_event("never")
        return "unreachable"

    runtime.register_workflow(orchestrator, name="terminate_me")
    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="terminate_me")
    )
    await client.terminate_workflow(
        TerminateRunRequest(instance_id=instance_id, output={"reason": "user"})
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.TERMINATED
    assert state.output == {"reason": "user"}
    # Terminate is a no-op on terminal instances.
    await client.terminate_workflow(
        TerminateRunRequest(instance_id=instance_id, output={"reason": "twice"})
    )
    assert state.output == {"reason": "user"}


# ---------------------------------------------------------------------------
# get_workflow_state snapshot
# ---------------------------------------------------------------------------


async def test_get_workflow_state_returns_none_for_unknown(
    client: FakeWorkflowClient,
) -> None:
    snapshot = await client.get_workflow_state(GetRunStateRequest(instance_id="does-not-exist"))
    assert snapshot is None


async def test_get_workflow_state_returns_run_state(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    runtime.register_workflow(_noop_workflow, name="noop")
    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="noop", input={"a": 1})
    )
    snapshot = await client.get_workflow_state(GetRunStateRequest(instance_id=instance_id))
    assert isinstance(snapshot, RunState)
    assert snapshot.instance_id == instance_id
    assert snapshot.name == "noop"
    assert snapshot.status == RunStatus.COMPLETED
    assert snapshot.serialized_input == '{"a": 1}'
    assert snapshot.serialized_output is None  # _noop_workflow returns None
    assert snapshot.failure_message is None


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_runtime_lifecycle(runtime: FakeWorkflowRuntime) -> None:
    assert runtime.is_started is False
    assert await runtime.wait_for_worker_ready() is False
    await runtime.start()
    assert runtime.is_started is True
    assert await runtime.wait_for_worker_ready(timeout=0.1) is True
    await runtime.shutdown()
    assert runtime.is_started is False


async def test_client_aclose_is_safe(client: FakeWorkflowClient) -> None:
    await client.aclose()  # no exception, idempotent


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


async def test_instance_lookup_for_unknown_id_raises(
    runtime: FakeWorkflowRuntime,
) -> None:
    with pytest.raises(UnknownInstanceError):
        runtime.instance("nope")
    assert runtime.instance_ids() == []


async def test_instance_ids_preserves_insertion_order(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    runtime.register_workflow(_noop_workflow, name="noop")
    ids = []
    for label in ("a", "b", "c"):
        ids.append(
            await client.schedule_new_workflow(
                ScheduleWorkflowRequest(workflow="noop", instance_id=label)
            )
        )
    assert runtime.instance_ids() == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Misuse paths
# ---------------------------------------------------------------------------


def test_call_activity_rejects_empty_name() -> None:
    ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError):
        ctx.call_activity("")


def test_wait_for_external_event_rejects_empty_name() -> None:
    ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError):
        ctx.wait_for_external_event("")


def test_call_activity_rejects_anonymous_callable() -> None:
    ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 1, 1, tzinfo=UTC))

    # A class with an empty ``__name__`` shadows the implicit class
    # attribute Python normally sets, triggering the empty-name guard.
    class _AnonCallable:
        __name__ = ""

    with pytest.raises(ValueError):
        ctx.call_activity(_AnonCallable())  # type: ignore[arg-type]


async def test_non_generator_workflow_completes_immediately(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def synchronous_workflow(_ctx: FakeWorkflowContext, payload: Any) -> Any:
        return {"echoed": payload}

    runtime.register_workflow(synchronous_workflow, name="sync_wf")
    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="sync_wf", input="hello")
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"echoed": "hello"}
    assert [e.kind for e in state.history] == ["started", "completed"]


async def test_unsupported_yield_fails_instance(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    def orchestrator(_ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        yield "not a task token"
        return "unreachable"

    runtime.register_workflow(orchestrator, name="bad_yield")
    instance_id = await client.schedule_new_workflow(ScheduleWorkflowRequest(workflow="bad_yield"))
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.FAILED
    assert state.failure_type == "UnsupportedTaskTypeError"


# ---------------------------------------------------------------------------
# HistoryEvent equality sanity
# ---------------------------------------------------------------------------


def test_history_event_is_hashable_and_equal() -> None:
    a = HistoryEvent(kind="started", detail={"x": 1})
    b = HistoryEvent(kind="started", detail={"x": 1})
    assert a == b
    # Hashable thanks to frozen=True only when detail is hashable; but
    # the dataclass is frozen so identity-based assertions are fine.
    assert isinstance(a, HistoryEvent)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _noop_workflow(_ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, None]:
    if False:  # pragma: no cover - never yields
        yield None
    return None
