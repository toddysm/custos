"""Async adapter around the synchronous ``dapr-ext-workflow`` SDK.

The Dapr Workflow Python SDK exposes a synchronous client and runtime.
Both make blocking gRPC calls to the Dapr sidecar, which is unacceptable
on the FastAPI event loop. This module wraps every public surface the
Run Controller uses in :func:`asyncio.to_thread` so the loop never
blocks.

Two invariants matter for downstream callers:

1. **Import-safe.** Importing this module — and constructing
   :class:`WorkflowRuntime` or :class:`WorkflowClient` — must not
   attempt a sidecar connection. The underlying
   :class:`dapr.ext.workflow.DaprWorkflowClient` and
   :class:`dapr.ext.workflow.WorkflowRuntime` are constructed lazily on
   first use, and even their construction does not open a connection;
   the gRPC channel comes up on the first RPC call. Tests in
   :mod:`tests.runtime.test_dapr_adapter_shape` assert the import-safety.
2. **Frozen request payloads.** Every :class:`WorkflowClient` method
   takes a single ``@dataclass(frozen=True)`` request from
   :mod:`custos_workflow.runtime._common`. Immutability matters because
   the request crosses the async boundary into a worker thread.

The translation layer between Dapr's :class:`WorkflowStatus` enum and
the Run-Controller-public :class:`RunStatus` enum lives here too — the
two enums have identical members but differ on the underlying values
(``WorkflowStatus`` uses sequential integers; :class:`RunStatus` uses
lowercase string names that surface in audit payloads).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final

# The Dapr SDK is import-safe (no sidecar connection at import time);
# we import the symbols we need at module load to keep type annotations
# in this file accurate.
from dapr.ext.workflow import (
    DaprWorkflowClient,
    DaprWorkflowContext,
    WorkflowActivityContext,
    WorkflowStatus,
)
from dapr.ext.workflow import (
    WorkflowRuntime as _DaprWorkflowRuntime,
)
from dapr.ext.workflow import (
    when_all as _dapr_when_all,
)
from dapr.ext.workflow import (
    when_any as _dapr_when_any,
)

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
    build_arm_schedule_activity,
    build_connector_bind_for_step_activity,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime, timedelta

    from dapr.ext.workflow.workflow_state import WorkflowState

    from custos_workflow.clients.activity_runtime import ActivityRuntimeClient
    from custos_workflow.clients.connector import ConnectorClient

__all__ = [
    "ActivityFn",
    "DaprWorkflowContextAdapter",
    "WorkflowClient",
    "WorkflowFn",
    "WorkflowRuntime",
    "dapr_status_to_run_status",
]


#: A workflow function as expected by ``dapr-ext-workflow``: a generator
#: function that yields ``durabletask.task.Task`` objects from
#: :meth:`DaprWorkflowContext.call_activity` and friends. The first
#: positional arg is the :class:`DaprWorkflowContext`; the second is
#: the input payload.
WorkflowFn = Callable[[DaprWorkflowContext, Any], Any]

#: An activity function: a sync callable taking
#: :class:`WorkflowActivityContext` plus an input payload and returning
#: a JSON-serialisable result.
ActivityFn = Callable[[WorkflowActivityContext, Any], Any]


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------


_DAPR_TO_RUN: Final[dict[WorkflowStatus, RunStatus]] = {
    WorkflowStatus.UNKNOWN: RunStatus.UNKNOWN,
    WorkflowStatus.PENDING: RunStatus.PENDING,
    WorkflowStatus.RUNNING: RunStatus.RUNNING,
    WorkflowStatus.SUSPENDED: RunStatus.SUSPENDED,
    WorkflowStatus.COMPLETED: RunStatus.COMPLETED,
    WorkflowStatus.FAILED: RunStatus.FAILED,
    WorkflowStatus.TERMINATED: RunStatus.TERMINATED,
    WorkflowStatus.STALLED: RunStatus.STALLED,
}


def dapr_status_to_run_status(status: WorkflowStatus) -> RunStatus:
    """Translate Dapr's :class:`WorkflowStatus` to the Run Controller's :class:`RunStatus`.

    Falls back to :attr:`RunStatus.UNKNOWN` for any future Dapr enum
    member we have not mapped explicitly.
    """

    return _DAPR_TO_RUN.get(status, RunStatus.UNKNOWN)


# ---------------------------------------------------------------------------
# Workflow-context adapter
# ---------------------------------------------------------------------------


class DaprWorkflowContextAdapter:
    """Thin wrapper that adds the Sub-Orchestration primitives to a real ctx.

    The real :class:`dapr.ext.workflow.DaprWorkflowContext` natively
    exposes :meth:`call_activity`, :meth:`wait_for_external_event`,
    :meth:`create_timer`, and :meth:`call_child_workflow`, but the
    ``when_all`` / ``when_any`` fan-in combinators are *module-level*
    functions in ``dapr.ext.workflow`` rather than context methods.
    Run Controller code targets a single
    :class:`~custos_workflow.runs.step_handler.WorkflowContext`
    Protocol so the in-memory
    :class:`~custos_workflow.runtime.FakeWorkflowContext` can be
    dropped in interchangeably; this adapter closes the gap by
    re-exposing the two combinators as methods (WF-IMPL-084) while
    delegating every other call straight through to the wrapped
    context with **zero behaviour change**.

    Construction is cheap and side-effect-free; wrap the context the
    Dapr runtime hands the orchestrator entrypoint and pass the
    adapter down the step walk.
    """

    __slots__ = ("_ctx",)

    def __init__(self, ctx: DaprWorkflowContext) -> None:
        self._ctx = ctx

    @property
    def unwrap(self) -> DaprWorkflowContext:
        """Return the wrapped :class:`DaprWorkflowContext`."""

        return self._ctx

    @property
    def instance_id(self) -> str:
        return self._ctx.instance_id

    @property
    def current_utc_datetime(self) -> datetime:
        return self._ctx.current_utc_datetime

    @property
    def is_replaying(self) -> bool:
        return self._ctx.is_replaying

    def set_custom_status(self, custom_status: str) -> None:
        self._ctx.set_custom_status(custom_status)

    def call_activity(
        self,
        activity: Callable[..., Any] | str,
        *,
        input: Any = None,
    ) -> Any:
        return self._ctx.call_activity(activity, input=input)

    def wait_for_external_event(self, name: str) -> Any:
        return self._ctx.wait_for_external_event(name)

    def create_timer(self, fire_at: datetime | timedelta) -> Any:
        return self._ctx.create_timer(fire_at)

    def call_child_workflow(
        self,
        workflow: Callable[..., Any] | str,
        *,
        input: Any = None,
        instance_id: str | None = None,
    ) -> Any:
        return self._ctx.call_child_workflow(workflow, input=input, instance_id=instance_id)

    def when_all(self, tasks: Sequence[Any]) -> Any:
        """Fan-in: resolve once every task in ``tasks`` has completed."""

        return _dapr_when_all(list(tasks))

    def when_any(self, tasks: Sequence[Any]) -> Any:
        """Fan-in: resolve as soon as any task in ``tasks`` completes."""

        return _dapr_when_any(list(tasks))


def _run_state_from_dapr(state: WorkflowState) -> RunState:
    """Build a :class:`RunState` snapshot from a Dapr :class:`WorkflowState`.

    :class:`WorkflowState` proxies the underlying ``OrchestrationState``
    via ``__getattr__``, so the field reads below are safe even though
    the proxy declares no explicit attributes.
    """

    failure_message: str | None = None
    failure_type: str | None = None
    failure_details = getattr(state, "failure_details", None)
    if failure_details is not None:
        failure_message = getattr(failure_details, "message", None)
        failure_type = getattr(failure_details, "error_type", None)

    return RunState(
        instance_id=state.instance_id,
        name=state.name,
        status=dapr_status_to_run_status(state.runtime_status),
        created_at=getattr(state, "created_at", None),
        last_updated_at=getattr(state, "last_updated_at", None),
        serialized_input=getattr(state, "serialized_input", None),
        serialized_output=getattr(state, "serialized_output", None),
        failure_message=failure_message,
        failure_type=failure_type,
    )


# ---------------------------------------------------------------------------
# Workflow runtime
# ---------------------------------------------------------------------------


class WorkflowRuntime:
    """Async-friendly wrapper around :class:`dapr.ext.workflow.WorkflowRuntime`.

    Owns the worker that registers workflow + activity functions and
    pulls work from the Dapr sidecar. Lifecycle:

    1. Construct (no side effects).
    2. :meth:`register_workflow` / :meth:`register_activity` (any order
       and any number of times — must happen before :meth:`start`).
    3. :meth:`start` once at service boot (idempotent — repeated calls
       are no-ops).
    4. :meth:`shutdown` once at service stop.

    All four lifecycle methods are coroutines so the FastAPI
    ``lifespan`` can ``await`` them without blocking the event loop.
    The underlying SDK calls are sync.
    """

    def __init__(
        self,
        *,
        runtime: _DaprWorkflowRuntime | None = None,
        activity_runtime_client: ActivityRuntimeClient | None = None,
        connector_client: ConnectorClient | None = None,
    ) -> None:
        # ``runtime`` is constructor-injectable for tests; production
        # callers should pass nothing and let the lazy constructor
        # build the real runtime on first registration.
        self._runtime = runtime
        self._started = False
        # ``_worker_ready`` flips to True the first time
        # :meth:`wait_for_worker_ready` resolves truthy. It is the
        # signal :attr:`is_ready` gates ``/readyz`` on (WF-IMPL-043).
        # Kept distinct from ``_started`` so callers can observe the
        # "worker started but not yet pulling work" window without a
        # race on the SDK's internal threads.
        self._worker_ready = False
        # WF-IMPL-079: injected outbound clients the Run Controller
        # orchestrator (WF-IMPL-080) yields against via
        # ``ctx.call_activity(SCHEDULE_ACTIVITY_ACTIVITY_NAME, ...)``
        # / ``ctx.call_activity(BIND_FOR_STEP_ACTIVITY_NAME, ...)``.
        # :meth:`start` registers each bridge activity
        # independently when its corresponding client is non-
        # ``None`` (so a worker may expose only the ARM bridge,
        # only the Connector bridge, both, or neither). Early-boot
        # tests that don't exercise either RPC path may leave both
        # ``None``; callers that need a bridge whose client they
        # don't have can register the activity themselves via
        # :meth:`register_activity`.
        self._activity_runtime_client = activity_runtime_client
        self._connector_client = connector_client
        # Track registered activity names so callers (and tests)
        # can introspect which activity bridges the worker exposes
        # without round-tripping through the Dapr SDK's internals.
        self._registered_activities: list[str] = []

    def _ensure_runtime(self) -> _DaprWorkflowRuntime:
        if self._runtime is None:
            self._runtime = _DaprWorkflowRuntime()
        return self._runtime

    def register_workflow(self, fn: WorkflowFn, *, name: str | None = None) -> None:
        """Register a workflow generator function with the Dapr runtime."""

        self._ensure_runtime().register_workflow(fn, name=name)

    def register_activity(self, fn: ActivityFn, *, name: str | None = None) -> None:
        """Register an activity callable with the Dapr runtime."""

        self._ensure_runtime().register_activity(fn, name=name)
        # Track the effective registered name so
        # :attr:`registered_activities` reflects the worker surface
        # without re-querying the SDK.
        effective_name = name or getattr(fn, "__name__", "")
        if effective_name:
            self._registered_activities.append(effective_name)

    @property
    def registered_activities(self) -> tuple[str, ...]:
        """Names of activities registered on this runtime, in registration order.

        Used by the FastAPI lifespan (WF-IMPL-043) and tests to
        verify the WF-IMPL-079 ARM + Connector bridge activities
        appear on the worker once :meth:`start` has run.
        """

        return tuple(self._registered_activities)

    async def start(self) -> None:
        """Start the worker. Idempotent — subsequent calls are no-ops.

        On the first call, each WF-IMPL-079 bridge activity is
        registered independently when its corresponding client
        was supplied to the constructor:
        :data:`SCHEDULE_ACTIVITY_ACTIVITY_NAME` when
        ``activity_runtime_client`` is non-``None``, and
        :data:`BIND_FOR_STEP_ACTIVITY_NAME` when
        ``connector_client`` is non-``None``. Registration is
        skipped (per bridge) when its client is ``None`` or when
        an activity with the same name was already registered
        via :meth:`register_activity`.
        """

        if self._started:
            return
        self._register_outbound_bridge_activities()
        runtime = self._ensure_runtime()
        await asyncio.to_thread(runtime.start)
        self._started = True

    def _register_outbound_bridge_activities(self) -> None:
        """Register the WF-IMPL-079 ARM + Connector bridge activities.

        Idempotent on the runtime side (registering the same
        activity name twice would raise from the Dapr SDK), so we
        gate on whether the names are already present in
        :attr:`registered_activities`. This lets callers register
        the activities manually for tests without :meth:`start`
        double-registering them.
        """

        already_registered = set(self._registered_activities)
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
        """Stop the worker. Idempotent — safe to call before :meth:`start`."""

        if not self._started or self._runtime is None:
            return
        runtime = self._runtime
        await asyncio.to_thread(runtime.shutdown)
        self._started = False
        self._worker_ready = False

    async def wait_for_worker_ready(self, *, timeout: float = 30.0) -> bool:
        """Block (in a worker thread) until the worker is ready or ``timeout`` elapses.

        Latches :attr:`is_ready` to ``True`` on the first truthy
        return so subsequent ``/readyz`` polls do not re-cross the
        thread boundary. Subsequent timed-out calls do **not** flip
        the flag back to ``False`` — once ready, always ready until
        :meth:`shutdown`.
        """

        runtime = self._ensure_runtime()
        result = await asyncio.to_thread(runtime.wait_for_worker_ready, timeout)
        if result:
            self._worker_ready = True
        return bool(result)

    @property
    def is_started(self) -> bool:
        """``True`` between :meth:`start` and :meth:`shutdown`."""

        return self._started

    @property
    def is_ready(self) -> bool:
        """``True`` once :meth:`wait_for_worker_ready` has returned truthy.

        This is the signal the FastAPI lifespan in
        :mod:`custos_workflow.app` gates ``/readyz`` on per
        WF-IMPL-043's design.md § Configuration entry.
        """

        return self._started and self._worker_ready


# ---------------------------------------------------------------------------
# Workflow client
# ---------------------------------------------------------------------------


class WorkflowClient:
    """Async-friendly wrapper around :class:`dapr.ext.workflow.DaprWorkflowClient`.

    Each public method takes a single frozen request from
    :mod:`custos_workflow.runtime._common` and delegates to the sync
    SDK via :func:`asyncio.to_thread`.
    """

    def __init__(self, *, client: DaprWorkflowClient | None = None) -> None:
        # ``client`` is constructor-injectable for tests; production
        # callers should pass nothing and let the lazy constructor
        # build the real client on first use.
        self._client = client

    def _ensure_client(self) -> DaprWorkflowClient:
        if self._client is None:
            self._client = DaprWorkflowClient()
        return self._client

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        """Schedule a new workflow run and return the resulting ``instance_id``."""

        client = self._ensure_client()

        def _call() -> str:
            return client.schedule_new_workflow(
                request.workflow,  # type: ignore[arg-type, unused-ignore]
                input=request.input,
                instance_id=request.instance_id,
                start_at=request.start_at,
            )

        return await asyncio.to_thread(_call)

    async def get_workflow_state(self, request: GetRunStateRequest) -> RunState | None:
        """Fetch the current snapshot for ``request.instance_id``.

        Returns ``None`` if Dapr has no record of the instance.
        """

        client = self._ensure_client()
        state = await asyncio.to_thread(
            client.get_workflow_state,
            request.instance_id,
            fetch_payloads=request.fetch_payloads,
        )
        if state is None:
            return None
        return _run_state_from_dapr(state)

    async def terminate_workflow(self, request: TerminateRunRequest) -> None:
        """Terminate a running workflow instance."""

        client = self._ensure_client()
        await asyncio.to_thread(
            client.terminate_workflow,
            request.instance_id,
            output=request.output,
            recursive=request.recursive,
        )

    async def pause_workflow(self, request: PauseRunRequest) -> None:
        """Suspend a running workflow instance."""

        client = self._ensure_client()
        await asyncio.to_thread(client.pause_workflow, request.instance_id)

    async def resume_workflow(self, request: ResumeRunRequest) -> None:
        """Resume a suspended workflow instance."""

        client = self._ensure_client()
        await asyncio.to_thread(client.resume_workflow, request.instance_id)

    async def raise_workflow_event(self, request: RaiseRunEventRequest) -> None:
        """Deliver an external event to a workflow instance."""

        client = self._ensure_client()
        await asyncio.to_thread(
            client.raise_workflow_event,
            request.instance_id,
            request.event_name,
            data=request.data,
        )

    async def aclose(self) -> None:
        """Close the underlying gRPC channel if one was opened.

        Safe to call without ever having issued an RPC.
        """

        client = self._client
        if client is None:
            return
        await asyncio.to_thread(client.close)
        self._client = None
