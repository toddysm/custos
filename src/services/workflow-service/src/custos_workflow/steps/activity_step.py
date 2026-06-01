"""``activity:`` step handler for the Step Coordinator (WF-IMPL-054).

An ``activity:`` step is the workflow author's way to invoke a
containerized activity through the Activity Runtime Manager (ARM)
and stitch its result back into the per-run output bag:

.. code-block:: yaml

    - id: scan
      activity: scanners/trivy@1
      connector: registry
      with:
        image: ${{ inputs.image }}
      onError:
        - when: ${{ envelope.codePrefix == "registry." }}
          do: retry
      retry:
        maxAttempts: 3
        backoff: { strategy: exponentialJitter, base: 1s, max: 30s, factor: 2.0 }

The Step Coordinator dispatcher (WF-IMPL-055) routes every
:class:`~custos_workflow.graph.model.StepKind.ACTIVITY` node through
the shared :class:`~custos_workflow.runs.StepHandler` Protocol; this
module is the dedicated handler that drives the full
*resolve → bind → schedule → dispatch → retry* loop pinned by
``design.md`` § *Operation: Step Execution*.

Sequence (mirrored from ``design.md``):

1. Resolve the step's ``with:`` block via
   :class:`~custos_workflow.steps.WithInputResolver` — once, before
   the retry loop, so every attempt sees the same inputs.
2. For each ``attempt`` starting at ``1``:

   a. Derive the canonical idempotency triple
      ``(run_id, step_id, attempt)`` via
      :func:`~custos_workflow.steps.derive_triple`.
   b. Bind a *fresh* connector lease for the attempt's slot specs
      via :meth:`ConnectorClient.bind_for_step` — design pins
      bind-per-attempt so a previously-leaked context cannot leak
      into the retry path.
   c. Schedule the activity via
      :meth:`ActivityRuntimeClient.schedule_activity`, passing the
      triple + ``with:`` inputs + bound connector contexts +
      synthesized deadline.
   d. Dispatch on the returned
      :class:`~custos_workflow.clients.ActivityResultEnvelope`:

      * ``"success"`` → return
        :class:`~custos_workflow.runs.StepSucceeded` carrying the
        envelope outputs (frozen as a :class:`MappingProxyType`).
      * ``"retryable"`` / ``"permanent"`` / ``"cancelled"`` → hand
        the envelope to
        :func:`~custos_workflow.steps.retry_driver.decide` (seeded
        with a replay-deterministic RNG) and dispatch on the
        :class:`~custos_workflow.steps.RetryDecision`:

        * :class:`~custos_workflow.steps.RetryNow` → open a durable
          timer for ``delay_seconds`` and loop with
          ``attempt = next_attempt``.
        * :class:`~custos_workflow.steps.Skip` → return
          :class:`~custos_workflow.runs.StepSkipped`.
        * :class:`~custos_workflow.steps.FailNow` → return
          :class:`~custos_workflow.runs.StepFailed`.

Failures structurally upstream of the envelope dispatch (the
:meth:`WithInputResolver.resolve` raise, a
:meth:`ConnectorClient.bind_for_step` exception, an
:meth:`ActivityRuntimeClient.schedule_activity` exception) are
wrapped in the matching
:class:`~custos_workflow.steps.errors.StepCoordinatorError`
subclass (``WithInputResolutionError``, ``ConnectorBindError``,
``ActivityScheduleError``) and surfaced as a
:class:`~custos_workflow.runs.StepFailed` envelope.

Step lifecycle events (``step.started`` / ``step.completed`` /
``step.failed`` / ``step.skipped``) are *not* emitted from this
handler. They land in WF-IMPL-056, which wraps the dispatcher at
the orchestrator boundary so the publisher's ``async def publish``
contract can be honoured without forcing the synchronous
:meth:`StepHandler.execute` contract to grow an event loop. The
single lifecycle event the retry driver owns directly,
``step.retry_scheduled``, is similarly deferred to that task; this
handler returns the retry decision intact so the wrapper can emit
it without losing fidelity.

Durable retry timers
--------------------

:meth:`execute` calls
:meth:`~custos_workflow.runs.WorkflowContext.create_timer` on every
:class:`~custos_workflow.steps.RetryNow` and *discards* the returned
opaque task token. Under the test
:class:`~custos_workflow.runtime.FakeWorkflowContext` that is a no-op
(the fake returns a token without suspending anything). Under the
real Dapr Workflow runtime the orchestrator must yield the token to
actually suspend execution — that yield wiring is the subject of
WF-IMPL-055 (``StepCoordinator`` integration) and WF-IMPL-057
(FastAPI lifespan registration), which together choose between
keeping :meth:`execute` synchronous and hoisting the retry loop up
into the orchestrator's generator. Until then the in-process retry
loop drives the fakes correctly and tests assert byte-equal results
under :class:`~custos_cel.FixedClock`.

Acceptance criteria (mirrored from #425):

* Every code-path in ``design.md`` § *Operation: Step Execution*
  has at least one test in
  :mod:`tests.steps.test_activity_step`.
* Replay-determinism: two :meth:`execute` calls under the same
  fakes + :class:`~custos_cel.FixedClock` produce byte-equal
  results.
* Coverage on this module ≥ 95 %.
"""

from __future__ import annotations

import hashlib
import random
import time
from collections.abc import Generator, Mapping
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from custos_cel.scope import BindingScope, RunInfo, StepBinding, WorkflowInfo

from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    ActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.clients.connector import (
    BindForStepRequest,
    BindForStepResponse,
    ConnectorClient,
    SlotSpec,
)
from custos_workflow.document.models import ActivityStep
from custos_workflow.graph.model import ExecutionGraph, ExecutionNode, StepKind
from custos_workflow.runs.step_handler import (
    StepExecutionContext,
    StepFailed,
    StepResult,
    StepSkipped,
    StepSucceeded,
)
from custos_workflow.runtime.dapr_activities import (
    ActivityCallToken,
    BindForStepCallToken,
    ScheduleActivityCallToken,
    drive_activity_generator,
)
from custos_workflow.steps import retry_driver
from custos_workflow.steps.errors import (
    ActivityScheduleError,
    ConnectorBindError,
    WithInputResolutionError,
)
from custos_workflow.steps.idempotency import derive_triple
from custos_workflow.steps.with_inputs import WithInputResolver

# NOTE: ``custos_workflow._telemetry`` is imported lazily inside
# :meth:`ActivityStepHandler.execute` rather than at module level.
# ``_telemetry`` pulls in ``custos_workflow.runs.errors`` which
# transitively triggers ``custos_workflow.runs.__init__`` — and
# this module is loaded during ``custos_workflow.steps`` package
# initialisation (via ``clients.activity_runtime →
# steps.idempotency``). Eager loading would close the cycle.

if TYPE_CHECKING:
    pass

__all__ = ["DEFAULT_ACTIVITY_DEADLINE", "ActivityStepHandler"]


#: Default per-attempt deadline applied when the step does not pin
#: an explicit timeout. ``ActivityStep`` carries no timeout field
#: today; the Activity Runtime Manager enforces its own cancellation
#: ladder above this floor. 24 hours is long enough that no
#: reasonable activity will ever hit it as a structural deadline,
#: which keeps :class:`ScheduleActivityRequest` constructable
#: without forcing every test to invent one.
DEFAULT_ACTIVITY_DEADLINE: Final[timedelta] = timedelta(hours=24)


#: Slot name used when the step declares the singular
#: ``connector:`` binding form (vs the ``connectors:`` map form).
#: Pinned here so adapters that key off the slot name agree with
#: the handler.
_DEFAULT_SLOT_NAME: Final[str] = "default"


class ActivityStepHandler:
    """:class:`~custos_workflow.runs.StepHandler` for ``StepKind.ACTIVITY``.

    Stateless handler whose only state is the injected client
    Protocols and the
    :class:`~custos_workflow.steps.WithInputResolver`. A single
    instance is safe to share across runs.

    :param activity_client: The
        :class:`~custos_workflow.clients.ActivityRuntimeClient`
        used to schedule + cancel attempts.
    :param connector_client: The
        :class:`~custos_workflow.clients.ConnectorClient` used to
        lease a fresh connector context for each attempt.
    :param with_resolver: Optional explicit
        :class:`~custos_workflow.steps.WithInputResolver`. Defaults
        to a freshly-constructed instance — the resolver is itself
        stateless so a per-handler instance is fine.
    :param default_activity_deadline: Optional override for the
        per-attempt deadline. Defaults to
        :data:`DEFAULT_ACTIVITY_DEADLINE`.
    """

    __slots__ = (
        "_activity_client",
        "_connector_client",
        "_default_activity_deadline",
        "_with_resolver",
    )

    def __init__(
        self,
        activity_client: ActivityRuntimeClient,
        connector_client: ConnectorClient,
        *,
        with_resolver: WithInputResolver | None = None,
        default_activity_deadline: timedelta = DEFAULT_ACTIVITY_DEADLINE,
    ) -> None:
        self._activity_client = activity_client
        self._connector_client = connector_client
        self._with_resolver = with_resolver if with_resolver is not None else WithInputResolver()
        self._default_activity_deadline = default_activity_deadline

    # ------------------------------------------------------------------
    # StepHandler.execute
    # ------------------------------------------------------------------

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        """Drive the activity step's full lifecycle to a :class:`StepResult`.

        Synchronous adapter over :meth:`iter_calls` — drives the
        generator against ``self._connector_client`` and
        ``self._activity_client`` via
        :func:`~custos_workflow.runtime.dapr_activities.drive_activity_generator`,
        so :class:`~custos_workflow.runs.StepHandler` consumers
        (notably :class:`~custos_workflow.steps.coordinator.StepCoordinator`)
        keep observing the original ``StepResult``-returning
        contract. The Run Controller orchestrator (WF-IMPL-035)
        bypasses this adapter for ``StepKind.ACTIVITY`` nodes when
        an ``activity_handler`` is wired into
        :func:`~custos_workflow.runs.orchestrator.make_run_orchestrator`,
        delegating to :meth:`iter_calls` via ``yield from`` so each
        bind / schedule call becomes a durable Dapr activity (the
        production resolver lands in WF-IMPL-079).

        :param ctx: Per-dispatch context bundle. The handler reads
            :attr:`ctx.outputs` to seed ``steps.<id>.outputs``
            references in the ``with:`` scope,
            :attr:`ctx.run_id` / :attr:`ctx.workspace_id` for
            ``run.*`` references, :attr:`ctx.workflow_context` for
            durable timers, and :attr:`ctx.clock` for replay-safe
            ``now()``.
        :param graph: The compiled execution graph. The handler
            reads :attr:`graph.metadata` for ``workflow.*`` scope
            and looks up the target node by ``step_id``.
        :param step_id: The id of the node to execute.

        :returns: A :class:`StepResult` variant per the dispatch
            table in the module docstring.

        :raises KeyError: If ``step_id`` is not in ``graph.nodes``.
        :raises NotImplementedError: If the resolved node is not a
            :class:`~custos_workflow.graph.model.StepKind.ACTIVITY`
            step.
        """
        return drive_activity_generator(
            self.iter_calls(ctx, graph, step_id),
            self._activity_client,
            self._connector_client,
        )

    # ------------------------------------------------------------------
    # Generator-based yield protocol (WF-IMPL-074)
    # ------------------------------------------------------------------

    def iter_calls(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> Generator[ActivityCallToken, object, StepResult]:
        """Yield-protocol equivalent of :meth:`execute`.

        Produces the same dispatch sequence ``execute`` would, but
        replaces every inline outbound RPC with a yielded
        :class:`~custos_workflow.runtime.dapr_activities.ActivityCallToken`
        value object. The driver (a production Dapr worker,
        :class:`~custos_workflow.runtime.FakeWorkflowRuntime`, or
        the in-process
        :class:`~custos_workflow.runtime.dapr_activities.FakeDaprActivityDispatcher`)
        resolves each yielded token and feeds the response back
        in via ``gen.send(response)`` so the handler's retry loop,
        envelope dispatch, and observability instrumentation
        observe the same values they would have observed when the
        calls were inline.

        Yield contract (per attempt of the retry loop, in order):

        1. :class:`BindForStepCallToken` carrying the per-attempt
           :class:`BindForStepRequest`. Driver MUST send back the
           resolved :class:`BindForStepResponse`.
        2. :class:`ScheduleActivityCallToken` carrying the
           per-attempt :class:`ScheduleActivityRequest` (built
           from the inputs resolved once before the loop + the
           contexts the driver just resolved). Driver MUST send
           back the resolved :class:`ActivityResultEnvelope`.

        Exceptions raised by the driver during token resolution
        are propagated back into this generator via
        :meth:`Generator.throw` so the handler's existing
        ``try`` / ``except`` blocks observe the same exception
        types they observed under the inline call path
        (notably :class:`ConnectorBindError` and
        :class:`ActivityScheduleError`). Replay-determinism is
        preserved: the inputs are still resolved exactly once
        before the loop, and the per-attempt RNG is still seeded
        off ``(run_id, step_id, attempt)``.

        :returns: The same :class:`StepResult` variant
            :meth:`execute` would return.

        :raises KeyError: See :meth:`execute`.
        :raises NotImplementedError: See :meth:`execute`.
        """
        node = _resolve_node(graph, step_id)
        if node.kind is not StepKind.ACTIVITY:
            raise NotImplementedError(
                f"ActivityStepHandler.iter_calls received {node.kind.value!r} step "
                f"{step_id!r}; only StepKind.ACTIVITY is supported",
            )
        activity_step = node.step_source
        if not isinstance(activity_step, ActivityStep):  # pragma: no cover - defensive
            raise NotImplementedError(
                f"ActivityStepHandler.iter_calls: node {step_id!r} has step_source "
                f"of type {type(activity_step).__name__}, expected ActivityStep",
            )

        # ``with:`` inputs are resolved once, before the retry
        # loop. Per design.md, retries replay the same inputs —
        # the only thing that changes per attempt is the connector
        # lease and the attempt counter on the scheduling triple.
        scope = _build_scope(ctx, graph)
        try:
            inputs = self._with_resolver.resolve(
                node,
                scope,
                ctx.clock,
                run_id=str(ctx.run_id),
                attempt=1,
            )
        except WithInputResolutionError as exc:
            return StepFailed(envelope=MappingProxyType(exc.to_dict()))

        slot_specs = _build_slot_specs(activity_step)
        run_id_str = str(ctx.run_id)
        # Lazy import — see module-level NOTE on the
        # ``_telemetry`` ⇄ ``runs`` ⇄ ``steps`` circular import.
        from custos_workflow._telemetry import (
            observe_step_bind_connectors,
            observe_step_retry_decision,
            observe_step_schedule_activity,
            record_activity_schedule_sample,
            record_step_attempt,
        )

        # WF-IMPL-058: cache the StepKind value once — it labels
        # every span / histogram / counter the handler emits for
        # this dispatch.
        step_kind_label = node.kind.value

        attempt = 1
        prev_delay_seconds: float | None = None
        while True:
            triple = derive_triple(run_id_str, step_id, attempt)

            # Fresh connector lease per attempt — design pins
            # bind-before-schedule so the retry path cannot reuse
            # a stale slot handle. Surface bind failures as a
            # ``step.connector_bind_error`` envelope and stop the
            # loop (no automatic retry on infrastructure errors).
            bind_request = BindForStepRequest(
                step_key=triple.to_str(),
                slots=slot_specs,
            )
            try:
                with observe_step_bind_connectors(step_kind_label):
                    bind_response = yield BindForStepCallToken(request=bind_request)
            except ConnectorBindError as exc:
                # Bind failure short-circuits the attempt loop
                # without scheduling, so the attempt counter
                # records ``internal_error`` (no envelope class
                # was produced).
                record_step_attempt(step_kind_label, "internal_error")
                return StepFailed(envelope=MappingProxyType(exc.to_dict()))
            except Exception as exc:
                wrapped = ConnectorBindError(
                    f"connector bind failed for step {step_id!r} attempt={attempt}: {exc!r}",
                    run_id=run_id_str,
                    step_id=step_id,
                    attempt=attempt,
                    cause=repr(exc),
                )
                record_step_attempt(step_kind_label, "internal_error")
                return StepFailed(envelope=MappingProxyType(wrapped.to_dict()))

            # The driver MUST send back a BindForStepResponse for
            # the yielded BindForStepCallToken. A typed guard here
            # catches a driver that ships back the wrong shape
            # (e.g. forwards an ``ActivityResultEnvelope`` by
            # mistake) instead of mis-feeding the schedule
            # request and producing a confusing downstream
            # failure.
            if not isinstance(bind_response, BindForStepResponse):
                raise TypeError(
                    "ActivityStepHandler.iter_calls received a non-"
                    "BindForStepResponse value back from the driver for the "
                    f"yielded BindForStepCallToken: got {type(bind_response).__name__}",
                )

            deadline = ctx.workflow_context.current_utc_datetime + self._default_activity_deadline
            schedule_start = time.perf_counter()
            schedule_request = ScheduleActivityRequest(
                run_id=triple.run_id,
                step_id=triple.step_id,
                attempt=triple.attempt,
                activity_ref=activity_step.activity,
                inputs=inputs,
                connector_contexts=bind_response.contexts,
                deadline=deadline,
            )
            try:
                with observe_step_schedule_activity(step_kind_label):
                    envelope = yield ScheduleActivityCallToken(request=schedule_request)
            except ActivityScheduleError as exc:
                record_step_attempt(step_kind_label, "internal_error")
                return StepFailed(envelope=MappingProxyType(exc.to_dict()))
            except Exception as exc:
                wrapped_schedule = ActivityScheduleError(
                    f"activity schedule failed for step {step_id!r} attempt={attempt}: {exc!r}",
                    run_id=run_id_str,
                    step_id=step_id,
                    attempt=attempt,
                    activity_ref=activity_step.activity,
                    cause=repr(exc),
                )
                record_step_attempt(step_kind_label, "internal_error")
                return StepFailed(envelope=MappingProxyType(wrapped_schedule.to_dict()))

            if not isinstance(envelope, ActivityResultEnvelope):
                raise TypeError(
                    "ActivityStepHandler.iter_calls received a non-"
                    "ActivityResultEnvelope value back from the driver for "
                    "the yielded ScheduleActivityCallToken: got "
                    f"{type(envelope).__name__}",
                )

            # WF-IMPL-058: schedule succeeded — record the
            # envelope class on the duration histogram and bump
            # the per-attempt counter with the envelope's class
            # label. ``record_activity_schedule_sample`` validates
            # that ``envelope.class_`` is in ACTIVITY_RESULT_CLASSES
            # so a malformed envelope fails the recorder rather
            # than silently polluting the dashboard.
            schedule_elapsed_ms = (time.perf_counter() - schedule_start) * 1000.0
            record_activity_schedule_sample(schedule_elapsed_ms, step_kind_label, envelope.class_)
            record_step_attempt(step_kind_label, envelope.class_)

            if envelope.class_ == "success":
                outputs = envelope.outputs or {}
                return StepSucceeded(outputs=MappingProxyType(dict(outputs)))

            with observe_step_retry_decision(step_kind_label):
                decision = retry_driver.decide(
                    node,
                    _error_envelope_for_decide(envelope),
                    attempt,
                    prev_delay_seconds,
                    _make_rng(run_id_str, step_id, attempt),
                )

            if isinstance(decision, retry_driver.Skip):
                return StepSkipped(reason=decision.reason)
            if isinstance(decision, retry_driver.FailNow):
                return StepFailed(envelope=MappingProxyType(dict(decision.envelope)))

            # RetryNow — open a durable timer and loop with the
            # driver-supplied next attempt counter. See the module
            # docstring's *Durable retry timers* section for the
            # production-suspension caveat.
            fire_at = ctx.workflow_context.current_utc_datetime + timedelta(
                seconds=decision.delay_seconds
            )
            ctx.workflow_context.create_timer(fire_at)
            prev_delay_seconds = decision.delay_seconds
            attempt = decision.next_attempt


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _resolve_node(graph: ExecutionGraph, step_id: str) -> ExecutionNode:
    """Return the node for ``step_id`` or raise :class:`KeyError`.

    Mirrors the lookup pattern :class:`LetStepHandler` and
    :class:`~custos_workflow.runs.NoopStepHandler` use so dispatch
    behaviour stays uniform across handlers.
    """
    for node in graph.nodes:
        if node.step_id == step_id:
            return node
    raise KeyError(step_id)


def _build_scope(
    ctx: StepExecutionContext,
    graph: ExecutionGraph,
) -> BindingScope:
    """Build the per-step :class:`BindingScope` used by ``with:``.

    Mirrors the layout :class:`LetStepHandler` uses so a
    ``${{ inputs.x }}`` reference under ``with:`` resolves to
    the same value it would under ``let:`` (and under the
    orchestrator's gate evaluator). ``workflow.version`` resolves
    to :attr:`StepExecutionContext.workflow_version_id` to honour
    the WF-IMPL-052 consistency fix.
    """
    run_info = RunInfo(id=str(ctx.run_id), workspace=ctx.workspace_id)
    workflow_info = WorkflowInfo(
        name=graph.metadata.workflow_name,
        version=ctx.workflow_version_id,
    )
    steps_view: Mapping[str, StepBinding] = MappingProxyType(
        {sid: StepBinding(out, sealed=True) for sid, out in ctx.outputs.items()}
    )
    return BindingScope(
        run=run_info,
        workflow=workflow_info,
        now=ctx.clock.now,
        inputs=ctx.inputs,
        steps=steps_view,
        let=MappingProxyType({}),
    )


def _build_slot_specs(activity_step: ActivityStep) -> tuple[SlotSpec, ...]:
    """Translate the step's connector binding into a tuple of slot specs.

    Three shapes are accepted (mirroring
    :class:`ActivityStep`'s document model):

    * ``connector: <ref>`` — singular form. Emits one
      :class:`SlotSpec` keyed under :data:`_DEFAULT_SLOT_NAME`.
    * ``connectors: {alias: <ref>, ...}`` — map form. Emits one
      :class:`SlotSpec` per alias, preserving the document order
      so test assertions can pin it.
    * Neither set — connectorless activity. Emits an empty tuple.
    """
    if activity_step.connector is not None:
        return (SlotSpec(name=_DEFAULT_SLOT_NAME, connector_ref=activity_step.connector),)
    if activity_step.connectors is not None:
        return tuple(
            SlotSpec(name=alias, connector_ref=ref)
            for alias, ref in activity_step.connectors.items()
        )
    return ()


def _error_envelope_for_decide(envelope: ActivityResultEnvelope) -> Mapping[str, Any]:
    """Return the error envelope to hand to :func:`retry_driver.decide`.

    The Activity Runtime Manager guarantees ``envelope.error`` is
    populated whenever ``envelope.class_`` is not ``"success"``
    (validated by :class:`ActivityResultEnvelope.__post_init__`),
    so this helper just inflates the mapping into a fresh dict and
    **forces** the ``"class"`` field to match :attr:`class_`.
    Forcing (not :meth:`dict.setdefault`-ing) is intentional: the
    retry driver routes on ``envelope["class"]``, so any drift
    between the envelope-level ``class_`` and a stale / mismatched
    ``error["class"]`` value (should one ever leak through) would
    silently mis-route. Defence in depth.
    """
    payload: dict[str, Any] = dict(envelope.error or {})
    payload["class"] = envelope.class_
    return payload


def _make_rng(run_id: str, step_id: str, attempt: int) -> random.Random:
    """Build a replay-deterministic :class:`random.Random`.

    The seed is the leading 64 bits of
    ``sha256(f"{run_id}|{step_id}|{attempt}")``. The
    Dapr Workflow replay model requires the same retry-decision
    output on every replay — seeding off the per-attempt triple
    pins that without leaking real entropy into the run.
    """
    digest = hashlib.sha256(
        f"{run_id}|{step_id}|{attempt}".encode(),
    ).hexdigest()
    return random.Random(int(digest[:16], 16))
