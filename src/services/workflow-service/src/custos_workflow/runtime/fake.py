"""In-memory :class:`WorkflowRuntime` / :class:`WorkflowClient` doubles.

The fake drives the same generator-based orchestrator contract that
``dapr-ext-workflow`` expects, but resolves activities synchronously
in-process and surfaces deterministic, replay-friendly state for test
assertions. The acceptance criterion for WF-IMPL-029 is that a 3-step
orchestrator function round-trips end-to-end through
:class:`FakeWorkflowRuntime` and exposes ``status`` / ``output`` /
``history`` for assertions; the implementation here supports
:meth:`FakeWorkflowContext.call_activity`,
:meth:`FakeWorkflowContext.wait_for_external_event`, and
:meth:`FakeWorkflowContext.create_timer`, plus paused / terminated
states, so subsequent Run Controller tasks (WF-IMPL-035..040) can
drive the real orchestrator under the fake without further changes.

Determinism contract:

* Activity dispatch order is the yield order of the generator.
* Timers auto-fire instantly (no wall-clock delay) and append a
  ``timer_fired`` history event.
* External events are queued FIFO per event name. If the orchestrator
  yields :meth:`FakeWorkflowContext.wait_for_external_event` and no
  matching event is queued, the instance records
  :attr:`_InstanceState.waiting_for_event` and the driver returns —
  the test then calls :meth:`FakeWorkflowClient.raise_workflow_event`
  to unblock it.
* The fake never raises out of :meth:`FakeWorkflowClient.schedule_new_workflow`;
  activity failures are caught and either re-thrown into the generator
  (so workflow code can catch them) or, if the generator does not
  catch, recorded on the instance as :attr:`RunStatus.FAILED`.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

from custos_workflow.runtime._common import (
    GetRunStateRequest,
    PauseRunRequest,
    RaiseRunEventRequest,
    ResumeRunRequest,
    RunState,
    RunStatus,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
)
from custos_workflow.runtime.dapr_activities import (
    BIND_FOR_STEP_ACTIVITY_NAME,
    SCHEDULE_ACTIVITY_ACTIVITY_NAME,
    BindForStepCallToken,
    FakeDaprActivityDispatcher,
    ScheduleActivityCallToken,
    build_arm_schedule_activity,
    build_connector_bind_for_step_activity,
)

if TYPE_CHECKING:
    from custos_workflow.clients.activity_runtime import ActivityRuntimeClient
    from custos_workflow.clients.connector import ConnectorClient

__all__ = [
    "FakeActivityContext",
    "FakeActivityFn",
    "FakeWorkflowClient",
    "FakeWorkflowContext",
    "FakeWorkflowFn",
    "FakeWorkflowRuntime",
    "HistoryEvent",
    "UnknownInstanceError",
]


# ---------------------------------------------------------------------------
# Public protocol-equivalent types
# ---------------------------------------------------------------------------


#: A workflow function: ``(ctx, input) -> generator``.
FakeWorkflowFn = Callable[["FakeWorkflowContext", Any], Generator[Any, Any, Any]]

#: An activity callable: ``(ctx, input) -> result``.
FakeActivityFn = Callable[["FakeActivityContext", Any], Any]


# Internal task tokens yielded by FakeWorkflowContext.* — analogous to
# durabletask.task.Task subclasses but inert (the driver in
# :class:`FakeWorkflowRuntime` inspects them by ``isinstance``).


@dataclass(frozen=True)
class _ActivityTask:
    """Token yielded by :meth:`FakeWorkflowContext.call_activity`."""

    activity_name: str
    input: Any


@dataclass(frozen=True)
class _ExternalEventTask:
    """Token yielded by :meth:`FakeWorkflowContext.wait_for_external_event`."""

    event_name: str


@dataclass(frozen=True)
class _TimerTask:
    """Token yielded by :meth:`FakeWorkflowContext.create_timer`."""

    fire_at: datetime


# ---------------------------------------------------------------------------
# History events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HistoryEvent:
    """A single event recorded on a fake instance's history log.

    The ``kind`` value is a stable string. Currently emitted kinds:
    ``"started"``, ``"activity_completed"``, ``"activity_failed"``,
    ``"external_event"``, ``"timer_fired"``, ``"completed"``,
    ``"failed"``, ``"terminated"``, ``"suspended"``, ``"resumed"``.
    Tests assert against this log to verify replay determinism.
    """

    kind: str
    detail: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Workflow & activity contexts
# ---------------------------------------------------------------------------


class FakeActivityContext:
    """Minimal stand-in for :class:`dapr.ext.workflow.WorkflowActivityContext`.

    The only fields the Run Controller is documented to consume are
    :attr:`workflow_id` and :attr:`task_id`; both are exposed here.
    """

    def __init__(self, *, workflow_id: str, task_id: str) -> None:
        self._workflow_id = workflow_id
        self._task_id = task_id

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    @property
    def task_id(self) -> str:
        return self._task_id


class FakeWorkflowContext:
    """Minimal stand-in for :class:`dapr.ext.workflow.DaprWorkflowContext`.

    Surfaces the methods the Run Controller orchestrator (WF-IMPL-035)
    will use: :meth:`call_activity`, :meth:`wait_for_external_event`,
    :meth:`create_timer`, :meth:`set_custom_status`, plus the
    :attr:`instance_id` and :attr:`current_utc_datetime` properties.
    """

    def __init__(self, *, instance_id: str, now: datetime) -> None:
        self._instance_id = instance_id
        self._now = now
        self._custom_status: str | None = None

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def current_utc_datetime(self) -> datetime:
        return self._now

    @property
    def is_replaying(self) -> bool:
        # The fake never replays; deterministic single-pass execution.
        return False

    @property
    def custom_status(self) -> str | None:
        return self._custom_status

    def set_custom_status(self, custom_status: str) -> None:
        self._custom_status = custom_status

    def call_activity(
        self, activity: Callable[..., Any] | str, *, input: Any = None
    ) -> _ActivityTask:
        """Yield-target for invoking an activity by name (or callable)."""

        name = activity if isinstance(activity, str) else getattr(activity, "__name__", "")
        if not name:
            raise ValueError("call_activity requires a non-empty activity name")
        return _ActivityTask(activity_name=name, input=input)

    def wait_for_external_event(self, name: str) -> _ExternalEventTask:
        """Yield-target for blocking on an externally-raised event."""

        if not name:
            raise ValueError("wait_for_external_event requires a non-empty name")
        return _ExternalEventTask(event_name=name)

    def create_timer(self, fire_at: datetime | timedelta) -> _TimerTask:
        """Yield-target for scheduling a durable timer.

        Accepts either an absolute :class:`datetime` (consistent with
        the Dapr API) or a :class:`timedelta` relative to the context's
        :attr:`current_utc_datetime`.
        """

        absolute = self._now + fire_at if isinstance(fire_at, timedelta) else fire_at
        return _TimerTask(fire_at=absolute)


# ---------------------------------------------------------------------------
# Internal instance state
# ---------------------------------------------------------------------------


@dataclass
class _InstanceState:
    """Live state for one fake workflow instance.

    Public attribute conventions match the acceptance criterion:
    ``status``, ``output``, ``history`` are all readable on the
    object returned by :meth:`FakeWorkflowRuntime.instance`.
    """

    instance_id: str
    name: str
    input: Any
    created_at: datetime
    status: RunStatus = RunStatus.PENDING
    output: Any = None
    failure_message: str | None = None
    failure_type: str | None = None
    history: list[HistoryEvent] = field(default_factory=list)
    custom_status: str | None = None
    last_updated_at: datetime | None = None
    # Internal fields below.
    generator: Generator[Any, Any, Any] | None = None
    ctx: FakeWorkflowContext | None = None
    next_result: Any = None
    pending_exception: BaseException | None = None
    waiting_for_event: str | None = None
    pending_events: dict[str, deque[Any]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Fake runtime
# ---------------------------------------------------------------------------


_DEFAULT_EPOCH: Final[datetime] = datetime(2026, 1, 1, tzinfo=UTC)

_TERMINAL_STATUSES: Final[frozenset[RunStatus]] = frozenset(
    {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED}
)


class UnknownInstanceError(LookupError):
    """Raised when a request references an instance the fake has never seen."""


class FakeWorkflowRuntime:
    """In-memory test substitute for :class:`WorkflowRuntime`.

    Holds the registry of workflow + activity functions and the live
    set of instances. A :class:`FakeWorkflowClient` connected to the
    same runtime is obtained via :meth:`client`.

    The :attr:`now` attribute is a controllable clock: tests can
    advance it before raising events to verify timestamp-derived
    behaviour. The default starts at 2026-01-01T00:00:00Z so durations
    in test history events are predictable.
    """

    def __init__(
        self,
        *,
        now: datetime | None = None,
        activity_dispatcher: FakeDaprActivityDispatcher | None = None,
        activity_runtime_client: ActivityRuntimeClient | None = None,
        connector_client: ConnectorClient | None = None,
    ) -> None:
        self._workflows: dict[str, FakeWorkflowFn] = {}
        self._activities: dict[str, FakeActivityFn] = {}
        self._registered_activity_order: list[str] = []
        self._instances: dict[str, _InstanceState] = {}
        self._started = False
        # Mirrors :attr:`custos_workflow.runtime.WorkflowRuntime._worker_ready`
        # so the FastAPI lifespan (WF-IMPL-043) can swap the fake in
        # for the real runtime without touching readiness wiring.
        self._worker_ready = False
        self.now: datetime = now if now is not None else _DEFAULT_EPOCH
        # WF-IMPL-074: optional resolver for the activity-task
        # yield-protocol tokens (BindForStepCallToken /
        # ScheduleActivityCallToken) the Run Controller orchestrator
        # may yield via ``yield from activity_handler.iter_calls(...)``.
        # Tests that exercise that path inject a dispatcher wired to
        # in-process FakeActivityRuntimeClient / FakeConnectorClient
        # so the yielded tokens resolve against the same fakes the
        # test fixtures construct for direct handler exercise. When
        # ``None`` (the default), encountering one of these tokens
        # fails the instance loudly via the unknown-task-type arm in
        # :meth:`_drive`.
        self._activity_dispatcher: FakeDaprActivityDispatcher | None = activity_dispatcher
        # WF-IMPL-079: outbound clients the production
        # :class:`~custos_workflow.runtime.WorkflowRuntime` injects
        # the two bridge activities against. The fake mirrors the
        # registration so a test driving the Run Controller
        # orchestrator through ``ctx.call_activity(name, ...)``
        # exercises the same activity surface against in-process
        # fakes — keeping orchestrator code production-shape
        # without spinning a Dapr sidecar.
        self._activity_runtime_client = activity_runtime_client
        self._connector_client = connector_client

    # --- registration ----------------------------------------------------

    def register_workflow(self, fn: FakeWorkflowFn, *, name: str | None = None) -> None:
        """Register a workflow function under ``name`` (defaults to ``fn.__name__``)."""

        key = name or fn.__name__
        self._workflows[key] = fn

    def register_activity(self, fn: FakeActivityFn, *, name: str | None = None) -> None:
        """Register an activity callable under ``name`` (defaults to ``fn.__name__``)."""

        key = name or fn.__name__
        if key not in self._activities:
            self._registered_activity_order.append(key)
        self._activities[key] = fn

    @property
    def registered_activities(self) -> tuple[str, ...]:
        """Names of activities registered on this runtime, in registration order.

        Mirrors
        :attr:`custos_workflow.runtime.WorkflowRuntime.registered_activities`
        so tests asserting against either surface look identical.
        """

        return tuple(self._registered_activity_order)

    # --- lifecycle -------------------------------------------------------

    async def start(self) -> None:
        """Mark the runtime started. No background threads are spawned.

        On the first call, each WF-IMPL-079 bridge activity is
        registered independently when its corresponding client
        was supplied to the constructor (ARM
        ``schedule_activity`` bridge when
        ``activity_runtime_client`` is non-``None``, Connector
        ``bind_for_step`` bridge when ``connector_client`` is
        non-``None``), so test code driving the orchestrator via
        ``ctx.call_activity(name, ...)`` resolves through the
        same surface as production.
        """

        if not self._started:
            self._register_outbound_bridge_activities()
        self._started = True

    def _register_outbound_bridge_activities(self) -> None:
        already_registered = set(self._registered_activity_order)
        if (
            self._activity_runtime_client is not None
            and SCHEDULE_ACTIVITY_ACTIVITY_NAME not in already_registered
        ):
            self.register_activity(
                build_arm_schedule_activity(self._activity_runtime_client),
                name=SCHEDULE_ACTIVITY_ACTIVITY_NAME,
            )
        if (
            self._connector_client is not None
            and BIND_FOR_STEP_ACTIVITY_NAME not in already_registered
        ):
            self.register_activity(
                build_connector_bind_for_step_activity(self._connector_client),
                name=BIND_FOR_STEP_ACTIVITY_NAME,
            )

    async def shutdown(self) -> None:
        """Mark the runtime stopped. Existing instance state is retained."""

        self._started = False
        self._worker_ready = False

    async def wait_for_worker_ready(self, *, timeout: float = 30.0) -> bool:
        """Always returns ``True`` once :meth:`start` has been called."""

        del timeout  # explicitly unused — fake is always-ready
        if self._started:
            self._worker_ready = True
        return self._started

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def is_ready(self) -> bool:
        """``True`` once :meth:`wait_for_worker_ready` has been called post-start.

        Mirrors :attr:`custos_workflow.runtime.WorkflowRuntime.is_ready`
        so the FastAPI lifespan (WF-IMPL-043) gates ``/readyz``
        identically against the fake.
        """

        return self._started and self._worker_ready

    # --- client ----------------------------------------------------------

    def client(self) -> FakeWorkflowClient:
        """Return a :class:`FakeWorkflowClient` wired to this runtime."""

        return FakeWorkflowClient(runtime=self)

    # --- introspection ---------------------------------------------------

    def instance(self, instance_id: str) -> _InstanceState:
        """Return the live :class:`_InstanceState` for ``instance_id``.

        Raises :class:`UnknownInstanceError` if no such instance exists.
        Intended for test assertions only — production callers use
        :meth:`FakeWorkflowClient.get_workflow_state`.
        """

        try:
            return self._instances[instance_id]
        except KeyError as exc:
            raise UnknownInstanceError(instance_id) from exc

    def instance_ids(self) -> list[str]:
        """All known instance ids, in insertion order."""

        return list(self._instances)

    # --- driver ----------------------------------------------------------

    def _schedule(self, request: ScheduleWorkflowRequest) -> str:
        """Create an instance, kick the generator, and return its id."""

        ref = request.workflow
        name = ref if isinstance(ref, str) else getattr(ref, "__name__", "")
        if name not in self._workflows:
            raise UnknownInstanceError(f"workflow {name!r} is not registered on the fake runtime")
        instance_id = request.instance_id or str(uuid4())
        if instance_id in self._instances:
            # Mirror Dapr's idempotent-id contract: scheduling the same id twice
            # returns the existing id and does not reset the instance.
            return instance_id

        state = _InstanceState(
            instance_id=instance_id,
            name=name,
            input=request.input,
            created_at=self.now,
            last_updated_at=self.now,
        )
        fn = self._workflows[name]
        ctx = FakeWorkflowContext(instance_id=instance_id, now=self.now)
        state.ctx = ctx
        result = fn(ctx, request.input)
        state.history.append(HistoryEvent(kind="started", detail={"input": request.input}))
        # Capture any custom status the workflow set before its first yield.
        state.custom_status = ctx.custom_status
        if hasattr(result, "send") and hasattr(result, "throw"):
            state.generator = result
            state.status = RunStatus.RUNNING
            self._instances[instance_id] = state
            self._drive(state)
        else:
            # Non-generator workflow — immediately complete with the return value.
            state.status = RunStatus.COMPLETED
            state.output = result
            state.last_updated_at = self.now
            state.history.append(HistoryEvent(kind="completed", detail={"output": result}))
            self._instances[instance_id] = state
        return instance_id

    def _drive(self, state: _InstanceState) -> None:
        """Run the generator forward until completion, failure, or a block."""

        if state.generator is None or state.status in _TERMINAL_STATUSES:
            return
        if state.status == RunStatus.SUSPENDED:
            return

        generator = state.generator
        while True:
            try:
                if state.pending_exception is not None:
                    exc = state.pending_exception
                    state.pending_exception = None
                    task = generator.throw(exc)
                else:
                    task = generator.send(state.next_result)
            except StopIteration as stop:
                if state.ctx is not None:
                    state.custom_status = state.ctx.custom_status
                state.status = RunStatus.COMPLETED
                state.output = stop.value
                state.last_updated_at = self.now
                state.history.append(HistoryEvent(kind="completed", detail={"output": stop.value}))
                return
            except Exception as exc:
                if state.ctx is not None:
                    state.custom_status = state.ctx.custom_status
                state.status = RunStatus.FAILED
                state.failure_message = str(exc)
                state.failure_type = type(exc).__name__
                state.last_updated_at = self.now
                state.history.append(
                    HistoryEvent(
                        kind="failed",
                        detail={
                            "message": state.failure_message,
                            "error_type": state.failure_type,
                        },
                    )
                )
                return

            state.next_result = None

            # After every advance through the generator, propagate any
            # custom_status the workflow set on its context onto the
            # observable instance state so ``runtime.instance(id)`` reflects
            # what the workflow last published.
            if state.ctx is not None:
                state.custom_status = state.ctx.custom_status

            if isinstance(task, _ActivityTask):
                self._dispatch_activity(state, task)
                continue

            if isinstance(task, _ExternalEventTask):
                queue = state.pending_events.get(task.event_name)
                if queue:
                    payload = queue.popleft()
                    if not queue:
                        del state.pending_events[task.event_name]
                    state.history.append(
                        HistoryEvent(
                            kind="external_event",
                            detail={"name": task.event_name, "data": payload},
                        )
                    )
                    state.next_result = payload
                    continue
                state.waiting_for_event = task.event_name
                state.last_updated_at = self.now
                return

            if isinstance(task, _TimerTask):
                state.history.append(
                    HistoryEvent(kind="timer_fired", detail={"fire_at": task.fire_at})
                )
                continue

            if isinstance(task, BindForStepCallToken | ScheduleActivityCallToken):
                # WF-IMPL-074 yield protocol. The Run Controller
                # orchestrator yielded a deferred bind / schedule
                # call from
                # :meth:`ActivityStepHandler.iter_calls`; resolve it
                # against the injected
                # :class:`FakeDaprActivityDispatcher` and feed the
                # response back via ``next_result`` so the handler's
                # generator advances to the next yield (or returns
                # its :class:`StepResult`). Each resolution appends a
                # ``activity_call_resolved`` history event so tests
                # can assert the ordered token sequence the
                # orchestrator yielded.
                if self._activity_dispatcher is None:
                    state.status = RunStatus.FAILED
                    state.failure_message = (
                        "orchestrator yielded an activity-call token "
                        f"({type(task).__name__}) but no "
                        "FakeDaprActivityDispatcher is wired on this runtime; "
                        "pass activity_dispatcher=... when constructing "
                        "FakeWorkflowRuntime to enable the WF-IMPL-074 yield "
                        "protocol"
                    )
                    state.failure_type = "MissingActivityDispatcherError"
                    state.last_updated_at = self.now
                    state.history.append(
                        HistoryEvent(
                            kind="failed",
                            detail={
                                "message": state.failure_message,
                                "error_type": state.failure_type,
                            },
                        )
                    )
                    return
                token_kind = (
                    "bind_for_step"
                    if isinstance(task, BindForStepCallToken)
                    else "schedule_activity"
                )
                try:
                    response = self._activity_dispatcher.resolve(task)
                except Exception as exc:
                    state.history.append(
                        HistoryEvent(
                            kind="activity_call_failed",
                            detail={
                                "token": token_kind,
                                "message": str(exc),
                                "error_type": type(exc).__name__,
                            },
                        )
                    )
                    state.pending_exception = exc
                    continue
                state.history.append(
                    HistoryEvent(
                        kind="activity_call_resolved",
                        detail={"token": token_kind},
                    )
                )
                state.next_result = response
                continue

            # Unknown task type — fail the instance loudly.
            state.status = RunStatus.FAILED
            state.failure_message = (
                f"orchestrator yielded unsupported task type: {type(task).__name__}"
            )
            state.failure_type = "UnsupportedTaskTypeError"
            state.last_updated_at = self.now
            state.history.append(
                HistoryEvent(
                    kind="failed",
                    detail={
                        "message": state.failure_message,
                        "error_type": state.failure_type,
                    },
                )
            )
            return

    def _dispatch_activity(self, state: _InstanceState, task: _ActivityTask) -> None:
        """Run an activity; on failure, prime the generator to receive the exception."""

        activity = self._activities.get(task.activity_name)
        if activity is None:
            state.history.append(
                HistoryEvent(
                    kind="activity_failed",
                    detail={
                        "activity": task.activity_name,
                        "message": (f"activity {task.activity_name!r} is not registered"),
                        "error_type": "UnknownActivityError",
                    },
                )
            )
            state.pending_exception = LookupError(
                f"activity {task.activity_name!r} is not registered"
            )
            return
        activity_ctx = FakeActivityContext(
            workflow_id=state.instance_id,
            task_id=str(uuid4()),
        )
        try:
            result = activity(activity_ctx, task.input)
        except Exception as exc:
            state.history.append(
                HistoryEvent(
                    kind="activity_failed",
                    detail={
                        "activity": task.activity_name,
                        "message": str(exc),
                        "error_type": type(exc).__name__,
                    },
                )
            )
            state.pending_exception = exc
            return

        state.history.append(
            HistoryEvent(
                kind="activity_completed",
                detail={
                    "activity": task.activity_name,
                    "input": task.input,
                    "output": result,
                },
            )
        )
        state.next_result = result

    # --- mutations driven by the client ----------------------------------

    def _raise_event(self, instance_id: str, event_name: str, data: Any) -> None:
        state = self.instance(instance_id)
        if state.status in _TERMINAL_STATUSES:
            return
        if state.waiting_for_event == event_name and state.status == RunStatus.RUNNING:
            state.waiting_for_event = None
            state.history.append(
                HistoryEvent(
                    kind="external_event",
                    detail={"name": event_name, "data": data},
                )
            )
            state.next_result = data
            self._drive(state)
            return
        # Either we're suspended or no one's waiting for this event yet:
        # buffer for FIFO delivery on the next wait.
        state.pending_events.setdefault(event_name, deque()).append(data)

    def _terminate(self, instance_id: str, output: Any) -> None:
        state = self.instance(instance_id)
        if state.status in _TERMINAL_STATUSES:
            return
        state.status = RunStatus.TERMINATED
        state.output = output
        state.last_updated_at = self.now
        state.history.append(HistoryEvent(kind="terminated", detail={"output": output}))

    def _pause(self, instance_id: str) -> None:
        state = self.instance(instance_id)
        if state.status == RunStatus.RUNNING:
            state.status = RunStatus.SUSPENDED
            state.last_updated_at = self.now
            state.history.append(HistoryEvent(kind="suspended"))

    def _resume(self, instance_id: str) -> None:
        state = self.instance(instance_id)
        if state.status != RunStatus.SUSPENDED:
            return
        state.status = RunStatus.RUNNING
        state.last_updated_at = self.now
        state.history.append(HistoryEvent(kind="resumed"))
        # If the instance was paused while waiting on an external event
        # and a matching event was buffered while suspended, deliver it
        # now. Otherwise the generator is logically still waiting; leave
        # ``_drive`` to a future ``_raise_event`` call.
        if state.waiting_for_event is not None:
            queue = state.pending_events.get(state.waiting_for_event)
            if queue:
                event_name = state.waiting_for_event
                payload = queue.popleft()
                if not queue:
                    del state.pending_events[event_name]
                state.waiting_for_event = None
                state.history.append(
                    HistoryEvent(
                        kind="external_event",
                        detail={"name": event_name, "data": payload},
                    )
                )
                state.next_result = payload
                self._drive(state)
            return
        self._drive(state)

    def _snapshot(self, instance_id: str) -> RunState | None:
        state = self._instances.get(instance_id)
        if state is None:
            return None
        return RunState(
            instance_id=state.instance_id,
            name=state.name,
            status=state.status,
            created_at=state.created_at,
            last_updated_at=state.last_updated_at,
            serialized_input=_to_serialized(state.input),
            serialized_output=_to_serialized(state.output),
            failure_message=state.failure_message,
            failure_type=state.failure_type,
        )


def _to_serialized(value: Any) -> str | None:
    """Render a payload to the string form a real Dapr ``WorkflowState`` would carry."""

    if value is None:
        return None
    return json.dumps(value, default=str, sort_keys=True)


# ---------------------------------------------------------------------------
# Fake client
# ---------------------------------------------------------------------------


class FakeWorkflowClient:
    """Async surface mirroring :class:`WorkflowClient` against an in-memory runtime."""

    def __init__(self, *, runtime: FakeWorkflowRuntime) -> None:
        self._runtime = runtime

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        # Tiny ``await asyncio.sleep(0)`` keeps the coroutine schedulable so
        # tests that depend on cooperative-yield ordering behave the same
        # as against the real :func:`asyncio.to_thread` wrapper.
        await asyncio.sleep(0)
        return self._runtime._schedule(request)

    async def get_workflow_state(self, request: GetRunStateRequest) -> RunState | None:
        # ``fetch_payloads`` is honoured by the real adapter; the fake
        # always returns full payloads because no extra round-trip cost
        # exists in-process.
        await asyncio.sleep(0)
        return self._runtime._snapshot(request.instance_id)

    async def terminate_workflow(self, request: TerminateRunRequest) -> None:
        # ``recursive`` has no effect in the fake (there are no child
        # instances yet); preserved on the request for API parity.
        await asyncio.sleep(0)
        self._runtime._terminate(request.instance_id, request.output)

    async def pause_workflow(self, request: PauseRunRequest) -> None:
        await asyncio.sleep(0)
        self._runtime._pause(request.instance_id)

    async def resume_workflow(self, request: ResumeRunRequest) -> None:
        await asyncio.sleep(0)
        self._runtime._resume(request.instance_id)

    async def raise_workflow_event(self, request: RaiseRunEventRequest) -> None:
        await asyncio.sleep(0)
        self._runtime._raise_event(request.instance_id, request.event_name, request.data)

    async def aclose(self) -> None:
        # No resources to release in the fake; provided for API symmetry.
        await asyncio.sleep(0)
