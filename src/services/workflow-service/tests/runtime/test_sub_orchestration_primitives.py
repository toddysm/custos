"""Sub-Orchestration runtime primitives (WF-IMPL-084).

Covers the three context primitives the Sub-Orchestration Manager
spawns and awaits child workflow instances with:

* :meth:`FakeWorkflowContext.call_child_workflow` — single-yield child
  spawn/await, deterministic + explicit instance ids, failure
  propagation as :class:`ChildWorkflowError`.
* :meth:`FakeWorkflowContext.when_all` — fan-in over children /
  activities, spawn-order result list, first-listed failure
  propagation, empty list, and the un-ready guard.
* :meth:`FakeWorkflowContext.when_any` — first-listed ready winner
  (timer, buffered event, or completed child), the ``get_result`` /
  identity contract, and the no-ready guard.
* :class:`DaprWorkflowContextAdapter` — delegation to the wrapped real
  context plus ``when_all`` / ``when_any`` forwarding to the Dapr SDK
  module-level combinators, with zero behaviour change to
  ``call_activity``.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from custos_workflow.runs.step_handler import WorkflowContext
from custos_workflow.runtime import (
    ChildWorkflowError,
    DaprWorkflowContextAdapter,
    FakeActivityContext,
    FakeWorkflowClient,
    FakeWorkflowContext,
    FakeWorkflowRuntime,
    RaiseRunEventRequest,
    RunStatus,
    ScheduleWorkflowRequest,
)
from custos_workflow.runtime import dapr as dapr_module

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC))


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


def _register_doubler(runtime: FakeWorkflowRuntime) -> None:
    """Register a ``child`` workflow that doubles ``payload["n"]``."""

    def child(ctx: FakeWorkflowContext, payload: dict[str, Any]) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - generator with no durable yields
            yield None
        return {"n": payload["n"] * 2}

    runtime.register_workflow(child, name="child")


# ---------------------------------------------------------------------------
# call_child_workflow — single yield
# ---------------------------------------------------------------------------


async def test_call_child_workflow_spawns_and_awaits(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """Parent spawns a child, awaits it, and receives the child output."""

    _register_doubler(runtime)

    def parent(ctx: FakeWorkflowContext, payload: dict[str, Any]) -> Generator[Any, Any, Any]:
        result = yield ctx.call_child_workflow("child", input={"n": payload["n"]})
        return {"doubled": result["n"]}

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input={"n": 21}, instance_id="parent-1")
    )

    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"doubled": 42}
    # Deterministic child id derived from the parent.
    assert state.spawned_children == ["parent-1:child:1"]
    child_state = runtime.instance("parent-1:child:1")
    assert child_state.status == RunStatus.COMPLETED
    kinds = [event.kind for event in state.history]
    assert kinds == ["started", "child_workflow_started", "child_workflow_completed", "completed"]


async def test_call_child_workflow_accepts_explicit_instance_id(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """An explicit ``instance_id`` pins the spawned child instance id."""

    _register_doubler(runtime)

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        result = yield ctx.call_child_workflow("child", input={"n": 5}, instance_id="fixed-child")
        return result

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None, instance_id="parent-1")
    )

    state = runtime.instance(instance_id)
    assert state.spawned_children == ["fixed-child"]
    assert runtime.instance("fixed-child").output == {"n": 10}


async def test_call_child_workflow_accepts_callable_reference(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """Passing the workflow callable resolves its ``__name__``."""

    def child(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - no durable yields
            yield None
        return "ok"

    runtime.register_workflow(child)

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        return (yield ctx.call_child_workflow(child))

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == "ok"


async def test_call_child_workflow_failure_propagates_as_child_error(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """A failing child raises :class:`ChildWorkflowError` into the parent."""

    def child(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - no durable yields
            yield None
        raise ValueError("boom")

    runtime.register_workflow(child, name="child")

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        try:
            yield ctx.call_child_workflow("child")
        except ChildWorkflowError as exc:
            return {"caught": exc.instance_id, "error_type": exc.error_type}
        return {"caught": None}  # pragma: no cover - failure path always taken

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None, instance_id="parent-1")
    )

    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"caught": "parent-1:child:1", "error_type": "ValueError"}
    kinds = [event.kind for event in state.history]
    assert "child_workflow_failed" in kinds


# ---------------------------------------------------------------------------
# when_all
# ---------------------------------------------------------------------------


async def test_when_all_returns_results_in_spawn_order(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """``when_all`` resolves to a list of child results in spawn order."""

    def child(ctx: FakeWorkflowContext, payload: dict[str, Any]) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - no durable yields
            yield None
        return payload["n"] * 10

    runtime.register_workflow(child, name="child")

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        tasks = [ctx.call_child_workflow("child", input={"n": n}) for n in (1, 2, 3)]
        return (yield ctx.when_all(tasks))

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None, instance_id="p")
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == [10, 20, 30]
    assert state.spawned_children == ["p:child:1", "p:child:2", "p:child:3"]


async def test_when_all_over_activities(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """``when_all`` also fans in over activity tasks."""

    def a(_ctx: FakeActivityContext, payload: int) -> int:
        return payload + 1

    def b(_ctx: FakeActivityContext, payload: int) -> int:
        return payload + 2

    runtime.register_activity(a)
    runtime.register_activity(b)

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        return (
            yield ctx.when_all([ctx.call_activity("a", input=10), ctx.call_activity("b", input=20)])
        )

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == [11, 22]


async def test_when_all_over_unregistered_activity_propagates_lookup_error(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """A missing activity inside ``when_all`` surfaces as ``LookupError``."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        try:
            yield ctx.when_all([ctx.call_activity("missing", input=1)])
        except LookupError as exc:
            return {"error": str(exc)}
        return {"error": None}  # pragma: no cover - failure path taken

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == {"error": "activity 'missing' is not registered"}


async def test_when_all_over_failing_activity_propagates_error(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """An activity that raises inside ``when_all`` propagates its exception."""

    def boom(_ctx: FakeActivityContext, _payload: Any) -> int:
        raise ValueError("activity blew up")

    runtime.register_activity(boom)

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        try:
            yield ctx.when_all([ctx.call_activity("boom", input=1)])
        except ValueError as exc:
            return {"error": str(exc)}
        return {"error": None}  # pragma: no cover - failure path taken

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == {"error": "activity blew up"}


async def test_when_all_empty_resolves_to_empty_list(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """``when_all([])`` completes immediately with an empty list."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        return (yield ctx.when_all([]))

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == []


async def test_when_all_propagates_first_listed_failure(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """A failing child in ``when_all`` propagates as ``ChildWorkflowError``."""

    def good(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - no durable yields
            yield None
        return "ok"

    def bad(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - no durable yields
            yield None
        raise RuntimeError("nope")

    runtime.register_workflow(good, name="good")
    runtime.register_workflow(bad, name="bad")

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        try:
            yield ctx.when_all([ctx.call_child_workflow("bad"), ctx.call_child_workflow("good")])
        except ChildWorkflowError as exc:
            return {"failed_child": exc.instance_id}
        return {"failed_child": None}  # pragma: no cover - failure path taken

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None, instance_id="p")
    )
    state = runtime.instance(instance_id)
    assert state.output == {"failed_child": "p:child:1"}


async def test_when_all_unready_task_fails_instance(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """``when_all`` over an un-queued external event fails the instance loudly."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        return (yield ctx.when_all([ctx.wait_for_external_event("never")]))

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.FAILED
    assert state.failure_type == "UnsupportedWhenAllError"


# ---------------------------------------------------------------------------
# when_any
# ---------------------------------------------------------------------------


async def test_when_any_timer_wins_when_no_event_queued(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """With no event queued, the always-ready timer is the first-listed winner."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        event = ctx.wait_for_external_event("approval")
        timer = ctx.create_timer(timedelta(minutes=5))
        winner = yield ctx.when_any([timer, event])
        return {"timed_out": winner is timer, "value": winner.get_result()}

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"timed_out": True, "value": None}


async def test_when_any_buffered_event_wins_over_timer(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """A first-listed event that is already buffered wins over a ready timer."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        # Block until the test buffers the approval event and releases us.
        yield ctx.wait_for_external_event("go")
        event = ctx.wait_for_external_event("approval")
        timer = ctx.create_timer(timedelta(minutes=5))
        winner = yield ctx.when_any([event, timer])
        return {"approved": winner is event, "value": winner.get_result()}

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None, instance_id="p")
    )
    # Instance is blocked on "go"; buffer the approval, then release.
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id="p", event_name="approval", data={"by": "alice"})
    )
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id="p", event_name="go", data=None)
    )

    state = runtime.instance(instance_id)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"approved": True, "value": {"by": "alice"}}


async def test_when_any_skips_unready_event_to_ready_timer(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """A first-listed un-ready event is skipped; the next ready task wins."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        event = ctx.wait_for_external_event("approval")
        timer = ctx.create_timer(timedelta(minutes=1))
        winner = yield ctx.when_any([event, timer])
        return {"winner_is_timer": winner is timer}

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == {"winner_is_timer": True}


async def test_when_any_winner_get_result_reraises_child_failure(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """When the winner is a failed child, ``get_result`` re-raises the failure."""

    def bad(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - no durable yields
            yield None
        raise ValueError("kaboom")

    runtime.register_workflow(bad, name="bad")

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        timer = ctx.create_timer(timedelta(minutes=1))
        winner = yield ctx.when_any([ctx.call_child_workflow("bad"), timer])
        try:
            winner.get_result()
        except ChildWorkflowError:
            return {"caught": True}
        return {"caught": False}  # pragma: no cover - failure path taken

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    assert runtime.instance(instance_id).output == {"caught": True}


async def test_when_any_no_ready_task_fails_instance(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """``when_any`` with no ready task fails the instance loudly."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        return (yield ctx.when_any([ctx.wait_for_external_event("never")]))

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.FAILED
    assert state.failure_type == "UnsupportedWhenAnyError"


async def test_when_any_empty_list_raises_in_generator(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """``when_any([])`` raises ``ValueError`` inside the generator."""

    def parent(ctx: FakeWorkflowContext, _payload: Any) -> Generator[Any, Any, Any]:
        if False:  # pragma: no cover - raises before any yield
            yield None
        return ctx.when_any([])

    runtime.register_workflow(parent, name="parent")

    instance_id = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=None)
    )
    state = runtime.instance(instance_id)
    assert state.status == RunStatus.FAILED
    assert state.failure_type == "ValueError"


# ---------------------------------------------------------------------------
# FakeWorkflowContext unit-level guards
# ---------------------------------------------------------------------------


def test_call_child_workflow_requires_a_name() -> None:
    """A workflow reference without a ``__name__`` is rejected."""

    ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 6, 1, tzinfo=UTC))
    with pytest.raises(ValueError, match="non-empty workflow name"):
        ctx.call_child_workflow(object())  # type: ignore[arg-type]


def test_child_task_get_result_before_completion_raises() -> None:
    """Reading a child task's result before resolution raises ``RuntimeError``."""

    ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 6, 1, tzinfo=UTC))
    token = ctx.call_child_workflow("child")
    with pytest.raises(RuntimeError, match="has not completed"):
        token.get_result()


def test_fake_context_satisfies_workflow_context_protocol() -> None:
    """The fake context structurally satisfies the :class:`WorkflowContext` Protocol."""

    ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 6, 1, tzinfo=UTC))
    assert isinstance(ctx, WorkflowContext)


# ---------------------------------------------------------------------------
# DaprWorkflowContextAdapter
# ---------------------------------------------------------------------------


def test_adapter_satisfies_workflow_context_protocol() -> None:
    """The real-ctx adapter structurally satisfies the Protocol."""

    assert isinstance(DaprWorkflowContextAdapter(MagicMock()), WorkflowContext)


def test_adapter_delegates_passthrough_calls() -> None:
    """Every non-combinator surface delegates to the wrapped context unchanged."""

    ctx = MagicMock()
    when = datetime(2026, 6, 1, tzinfo=UTC)
    ctx.instance_id = "inst-1"
    ctx.current_utc_datetime = when
    ctx.is_replaying = True

    adapter = DaprWorkflowContextAdapter(ctx)

    assert adapter.unwrap is ctx
    assert adapter.instance_id == "inst-1"
    assert adapter.current_utc_datetime is when
    assert adapter.is_replaying is True

    adapter.set_custom_status("running")
    ctx.set_custom_status.assert_called_once_with("running")

    adapter.call_activity("act", input={"k": 1})
    ctx.call_activity.assert_called_once_with("act", input={"k": 1})

    adapter.wait_for_external_event("evt")
    ctx.wait_for_external_event.assert_called_once_with("evt")

    adapter.create_timer(when)
    ctx.create_timer.assert_called_once_with(when)

    adapter.call_child_workflow("wf", input=7, instance_id="c1")
    ctx.call_child_workflow.assert_called_once_with("wf", input=7, instance_id="c1")


def test_adapter_when_all_forwards_to_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """``when_all`` forwards a materialised list to the SDK combinator."""

    captured: dict[str, Any] = {}

    def fake_when_all(tasks: list[Any]) -> str:
        captured["tasks"] = tasks
        return "ALL"

    monkeypatch.setattr(dapr_module, "_dapr_when_all", fake_when_all)
    adapter = DaprWorkflowContextAdapter(MagicMock())

    assert adapter.when_all((1, 2, 3)) == "ALL"
    assert captured["tasks"] == [1, 2, 3]


def test_adapter_when_any_forwards_to_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """``when_any`` forwards a materialised list to the SDK combinator."""

    captured: dict[str, Any] = {}

    def fake_when_any(tasks: list[Any]) -> str:
        captured["tasks"] = tasks
        return "ANY"

    monkeypatch.setattr(dapr_module, "_dapr_when_any", fake_when_any)
    adapter = DaprWorkflowContextAdapter(MagicMock())

    assert adapter.when_any((9, 8)) == "ANY"
    assert captured["tasks"] == [9, 8]
