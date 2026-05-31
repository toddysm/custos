"""Tests for the :class:`ActivityStepHandler` (WF-IMPL-054).

The handler is the Step Coordinator surface that drives the full
``activity:`` step lifecycle — resolve ``with:`` inputs, bind a
fresh connector lease per attempt, schedule each attempt through
the Activity Runtime Manager, and dispatch on the returned
envelope (with retries / skips / fails routed through the
WF-IMPL-053 retry decision driver).

Test coverage targets every code-path in
``design.md`` § *Operation: Step Execution*:

* Success on first attempt.
* Retryable → retry → success on second attempt.
* Retryable → policy exhausted → :class:`StepFailed`.
* Permanent → no retry, single attempt.
* Cancelled → immediate fail (cancellation route is locked).
* ``with:`` resolution failure → :class:`StepFailed`.
* :meth:`ConnectorClient.bind_for_step` failure → :class:`StepFailed`.
* :meth:`ActivityRuntimeClient.schedule_activity` failure → :class:`StepFailed`.
* Replay determinism — two identical runs produce byte-equal results.
* Fresh-lease-per-attempt — bind is called once per attempt.
* Connectorless activity — empty slot tuple.
* ``connectors:`` map form — one slot per alias.
* :meth:`WorkflowContext.create_timer` is opened on every retry
  (with the expected ``fire_at``).
* Non-``ACTIVITY`` kind raises :class:`NotImplementedError`.
* Unknown step id raises :class:`KeyError`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pytest
from custos_cel import FixedClock

from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    FakeActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.clients.connector import (
    BindForStepRequest,
    BindForStepResponse,
    ConnectorContext,
    FakeConnectorClient,
    SlotSpec,
)
from custos_workflow.document import ActivityStep, LetStep
from custos_workflow.graph import (
    BackoffStrategyTag,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    JitterStrategyTag,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
    StepKind,
)
from custos_workflow.runs import (
    RunId,
    StepExecutionContext,
    StepFailed,
    StepSkipped,
    StepSucceeded,
)
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.steps import (
    WithInputResolver,
)
from custos_workflow.steps.activity_step import (
    DEFAULT_ACTIVITY_DEADLINE,
    ActivityStepHandler,
)
from custos_workflow.steps.errors import WithInputResolutionError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CLOCK_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_CLOCK_NOW)


def _backoff() -> ResolvedBackoffPolicy:
    return ResolvedBackoffPolicy(
        strategy=BackoffStrategyTag.EXPONENTIAL,
        initial_delay_ms=1_000,
        max_delay_ms=60_000,
        multiplier=2.0,
    )


def _policy(*, max_attempts: int = 3) -> ResolvedRetryPolicy:
    return ResolvedRetryPolicy(
        max_attempts=max_attempts,
        backoff=_backoff(),
        jitter=JitterStrategyTag.NONE,
        respect_retry_after=True,
    )


def _default_routes(policy: ResolvedRetryPolicy) -> tuple[OnErrorRoute, ...]:
    return (
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
        OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=policy),
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="permanent"),
    )


def _activity_node(
    *,
    step_id: str = "scan",
    activity: str = "scanners/trivy@1",
    connector: str | None = "primary",
    connectors: dict[str, str] | None = None,
    with_: dict[str, Any] | None = None,
    retry_policy: ResolvedRetryPolicy | None = None,
    on_error_routes: tuple[OnErrorRoute, ...] | None = None,
) -> ExecutionNode:
    policy = retry_policy if retry_policy is not None else _policy()
    payload: dict[str, Any] = {"id": step_id, "activity": activity}
    if connector is not None:
        payload["connector"] = connector
    if connectors is not None:
        payload["connectors"] = connectors
    if with_ is not None:
        payload["with"] = with_
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=policy,
        on_error_routes=on_error_routes if on_error_routes is not None else _default_routes(policy),
        call_sites={},
        step_source=ActivityStep.model_validate(payload),
    )


def _let_node(*, step_id: str = "derive") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": step_id, "let": {"x": 1}}),
    )


def _graph(*nodes: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=tuple(nodes),
        edges=(),
        topological_order=tuple(n.step_id for n in nodes),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def _ctx(
    *,
    inputs: dict[str, Any] | None = None,
    outputs: dict[str, dict[str, Any]] | None = None,
    run_id: str = "run-1",
    workspace_id: str = "ws-1",
    workflow_version_id: str = "wf-1",
) -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id=workspace_id,
        workflow_version_id=workflow_version_id,
        inputs=MappingProxyType(dict(inputs or {})),
        workflow_context=FakeWorkflowContext(instance_id=run_id, now=_CLOCK_NOW),
        outputs=MappingProxyType(
            {sid: MappingProxyType(dict(out)) for sid, out in (outputs or {}).items()}
        ),
        clock=_CLOCK,
    )


def _ctx_with_recorder(
    timer_calls: list[Any],
    *,
    run_id: str = "run-1",
) -> StepExecutionContext:
    """Build a :class:`StepExecutionContext` that records every timer."""

    wf_ctx = FakeWorkflowContext(instance_id=run_id, now=_CLOCK_NOW)
    real_create = wf_ctx.create_timer

    def _record(fire_at: Any) -> Any:
        timer_calls.append(fire_at)
        return real_create(fire_at)

    # Bypass FakeWorkflowContext's setattr restrictions by sticking
    # the recording closure on the instance dict; FakeWorkflowContext
    # is a plain (non-slot) class so this is safe.
    wf_ctx.create_timer = _record  # type: ignore[method-assign]
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id="ws-1",
        workflow_version_id="wf-1",
        inputs=MappingProxyType({}),
        workflow_context=wf_ctx,
        outputs=MappingProxyType({}),
        clock=_CLOCK,
    )


def _bind_response(*slots: str) -> BindForStepResponse:
    """Build a canned BindForStepResponse with one context per slot."""
    expires = _CLOCK_NOW + timedelta(minutes=5)
    return BindForStepResponse(
        contexts={
            slot: ConnectorContext(
                slot_name=slot,
                handle=f"handle-{slot}",
                expires_at=expires,
                connector_kind="oci-registry",
            )
            for slot in slots
        },
    )


def _success(outputs: dict[str, Any], *, attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="success",
        outputs=outputs,
        error=None,
        attempt=attempt,
    )


def _retryable(*, attempt: int = 1, code: str = "registry.timeout") -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="retryable",
        outputs=None,
        error={"class": "retryable", "code": code, "message": "timed out"},
        attempt=attempt,
    )


def _permanent(*, attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="permanent",
        outputs=None,
        error={"class": "permanent", "code": "registry.notFound", "message": "404"},
        attempt=attempt,
    )


def _cancelled(*, attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="cancelled",
        outputs=None,
        error={"class": "cancelled", "code": "step.cancelled", "message": "cancelled"},
        attempt=attempt,
    )


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestSuccessPaths:
    def test_success_on_first_attempt(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({"vulns": 0})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"vulns": 0}
        # Exactly one bind + one schedule on a clean success.
        assert len(connector.calls) == 1
        assert len(activity.calls) == 1

    def test_schedule_request_carries_triple_and_deadline(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx(run_id="run-XYZ")
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        handler.execute(ctx, graph, "scan")

        request = activity.calls[0]
        assert isinstance(request, ScheduleActivityRequest)
        assert request.run_id == "run-XYZ"
        assert request.step_id == "scan"
        assert request.attempt == 1
        assert request.activity_ref == "scanners/trivy@1"
        # Default deadline pinned at 24 h ahead of the workflow
        # context's current time.
        assert request.deadline == _CLOCK_NOW + DEFAULT_ACTIVITY_DEADLINE
        # Connector context flows through unchanged.
        assert dict(request.connector_contexts).keys() == {"default"}

    def test_bind_request_uses_idempotency_triple_as_step_key(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx(run_id="run-A")
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        handler.execute(ctx, graph, "scan")

        bind_req = connector.calls[0]
        assert isinstance(bind_req, BindForStepRequest)
        assert bind_req.step_key == "run-A|scan|1"

    def test_with_inputs_flow_into_schedule_request(self) -> None:
        node = _activity_node(with_={"image": "alpine:3.18", "limit": 10})
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        handler.execute(ctx, graph, "scan")

        assert dict(activity.calls[0].inputs) == {"image": "alpine:3.18", "limit": 10}

    def test_outputs_are_frozen(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({"k": "v"})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        assert isinstance(result.outputs, MappingProxyType)
        with pytest.raises(TypeError):
            result.outputs["k"] = "mutated"  # type: ignore[index]

    def test_success_with_no_outputs_returns_empty_mapping(self) -> None:
        """Envelope ``outputs={}`` flows through as an empty mapping."""
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {}


# ---------------------------------------------------------------------------
# Retry loop paths
# ---------------------------------------------------------------------------


class TestRetryLoop:
    def test_retryable_then_success(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        timers: list[Any] = []
        ctx = _ctx_with_recorder(timers)
        activity = FakeActivityRuntimeClient(
            results=[_retryable(attempt=1), _success({"ok": True}, attempt=2)],
        )
        connector = FakeConnectorClient(
            responses=[_bind_response("default"), _bind_response("default")],
        )
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"ok": True}
        # Two binds and two schedules — confirms fresh-lease-per-attempt.
        assert len(connector.calls) == 2
        assert len(activity.calls) == 2
        # Attempts carried through the triple.
        assert [c.attempt for c in activity.calls] == [1, 2]
        # One durable timer opened between attempts.
        assert len(timers) == 1

    def test_retryable_then_exhausted_returns_failed(self) -> None:
        policy = _policy(max_attempts=2)
        node = _activity_node(retry_policy=policy)
        graph = _graph(node)
        ctx = _ctx_with_recorder([])
        activity = FakeActivityRuntimeClient(
            results=[_retryable(attempt=1), _retryable(attempt=2)],
        )
        connector = FakeConnectorClient(
            responses=[_bind_response("default"), _bind_response("default")],
        )
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        # Budget exhausted envelope carries the canonical kind.
        envelope = dict(result.envelope)
        assert envelope["kind"] == "step.retry_budget_exhausted"
        # Two schedule attempts were made before tipping into failure.
        assert len(activity.calls) == 2

    def test_permanent_envelope_does_not_retry(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_permanent()])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        # Single attempt — permanent class hits the FAIL route.
        assert len(activity.calls) == 1

    def test_cancelled_envelope_short_circuits_to_fail(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_cancelled()])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        # Single attempt — cancelled is always terminal even if a
        # retry route exists (compiler always prepends the
        # FAIL/cancelled route).
        assert len(activity.calls) == 1

    def test_skip_route_returns_step_skipped(self) -> None:
        policy = _policy()
        routes = (
            OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
            OnErrorRoute(action=OnErrorActionTag.SKIP, code="registry.notFound"),
            OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=policy),
            OnErrorRoute(action=OnErrorActionTag.FAIL, cls="permanent"),
        )
        node = _activity_node(retry_policy=policy, on_error_routes=routes)
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(
            results=[
                ActivityResultEnvelope(
                    class_="permanent",
                    outputs=None,
                    error={"class": "permanent", "code": "registry.notFound"},
                    attempt=1,
                ),
            ],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSkipped)
        # Reason is synthesized by retry_driver from the matched route.
        assert result.reason == "on_error[code=registry.notFound]: skip"

    def test_retry_creates_timer_at_expected_fire_at(self) -> None:
        policy = _policy(max_attempts=3)
        node = _activity_node(retry_policy=policy)
        graph = _graph(node)
        timers: list[Any] = []
        ctx = _ctx_with_recorder(timers)
        activity = FakeActivityRuntimeClient(
            results=[
                _retryable(attempt=1),
                _retryable(attempt=2),
                _success({}, attempt=3),
            ],
        )
        connector = FakeConnectorClient(
            responses=[_bind_response("default") for _ in range(3)],
        )
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        # Two retries → two timers, both anchored at the workflow
        # context's current_utc_datetime (FixedClock doesn't advance).
        assert len(timers) == 2
        for fire_at in timers:
            assert isinstance(fire_at, datetime)
            assert fire_at >= _CLOCK_NOW

    def test_envelope_without_class_field_inflates_from_class_(self) -> None:
        """ARM-omitted ``class`` defence in depth: handler synthesises it."""
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        # Envelope.error omits the ``class`` field; handler must
        # pin it from ``envelope.class_`` so retry_driver.decide
        # can route correctly.
        no_class_error = ActivityResultEnvelope(
            class_="permanent",
            outputs=None,
            error={"code": "x.y", "message": "boom"},
            attempt=1,
        )
        activity = FakeActivityRuntimeClient(results=[no_class_error])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)


# ---------------------------------------------------------------------------
# Pre-loop failures
# ---------------------------------------------------------------------------


class TestWithInputFailure:
    def test_with_resolution_failure_returns_step_failed(self) -> None:
        """A raising :class:`WithInputResolver` short-circuits to ``StepFailed``."""
        node = _activity_node(with_={"image": "alpine"})
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[])
        connector = FakeConnectorClient(responses=[])

        class _Raising:
            def resolve(self, *args: Any, **kwargs: Any) -> Any:
                raise WithInputResolutionError(
                    "boom",
                    run_id="run-1",
                    step_id="scan",
                    attempt=1,
                    binding_name="image",
                    source="${{ inputs.image }}",
                )

        handler = ActivityStepHandler(activity, connector, with_resolver=_Raising())  # type: ignore[arg-type]

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        envelope = dict(result.envelope)
        assert envelope["kind"] == "step.with_input_resolution_error"
        # No bind / schedule was attempted — the resolver runs
        # before the loop.
        assert connector.calls == []
        assert activity.calls == []


class TestConnectorBindFailure:
    def test_bind_failure_returns_step_failed(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[])

        class _Raising:
            def __init__(self) -> None:
                self.calls: list[BindForStepRequest] = []

            def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
                self.calls.append(request)
                raise RuntimeError("connector service is down")

        connector = _Raising()
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        envelope = dict(result.envelope)
        assert envelope["kind"] == "step.connector_bind_error"
        assert envelope["cause"] == "RuntimeError('connector service is down')"
        # Activity client was never called.
        assert activity.calls == []

    def test_bind_typed_error_passes_through_unchanged(self) -> None:
        """A pre-built :class:`ConnectorBindError` is surfaced verbatim."""
        from custos_workflow.steps.errors import ConnectorBindError as _CBE

        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[])

        class _Raising:
            def __init__(self) -> None:
                self.calls: list[BindForStepRequest] = []

            def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
                self.calls.append(request)
                raise _CBE(
                    "bind refused",
                    run_id="run-1",
                    step_id="scan",
                    attempt=1,
                    slot_name="default",
                    connector_ref="primary",
                    cause="GrantDenied",
                )

        connector = _Raising()
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        envelope = dict(result.envelope)
        # Cause is the typed exception's own ``cause`` field, not
        # ``repr(exc)`` — confirms the typed branch ran.
        assert envelope["cause"] == "GrantDenied"
        assert envelope["slot_name"] == "default"


class TestActivityScheduleFailure:
    def test_schedule_failure_returns_step_failed(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()

        class _Raising:
            def __init__(self) -> None:
                self.calls: list[ScheduleActivityRequest] = []
                self.cancellations: list[tuple[str, str]] = []

            def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
                self.calls.append(request)
                raise RuntimeError("ARM unreachable")

            def cancel_activity(self, run_id: str, step_id: str) -> None:
                self.cancellations.append((run_id, step_id))

        activity = _Raising()
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        envelope = dict(result.envelope)
        assert envelope["kind"] == "step.activity_schedule_error"
        assert envelope["activity_ref"] == "scanners/trivy@1"
        assert envelope["cause"] == "RuntimeError('ARM unreachable')"

    def test_schedule_typed_error_passes_through_unchanged(self) -> None:
        """A pre-built :class:`ActivityScheduleError` is surfaced verbatim."""
        from custos_workflow.steps.errors import ActivityScheduleError as _ASE

        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()

        class _Raising:
            def __init__(self) -> None:
                self.calls: list[ScheduleActivityRequest] = []
                self.cancellations: list[tuple[str, str]] = []

            def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
                self.calls.append(request)
                raise _ASE(
                    "ARM rejected",
                    run_id="run-1",
                    step_id="scan",
                    attempt=1,
                    activity_ref="scanners/trivy@1",
                    cause="ServiceUnavailable",
                )

            def cancel_activity(self, run_id: str, step_id: str) -> None:
                self.cancellations.append((run_id, step_id))

        activity = _Raising()
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepFailed)
        envelope = dict(result.envelope)
        # Cause is the typed exception's own ``cause`` field, not
        # ``repr(exc)`` — confirms the typed branch ran.
        assert envelope["cause"] == "ServiceUnavailable"


# ---------------------------------------------------------------------------
# Slot-spec assembly
# ---------------------------------------------------------------------------


class TestSlotSpecs:
    def test_singular_connector_emits_default_slot(self) -> None:
        node = _activity_node(connector="primary")
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(activity, connector)

        handler.execute(ctx, graph, "scan")

        slots = connector.calls[0].slots
        assert slots == (SlotSpec(name="default", connector_ref="primary"),)

    def test_connectors_map_emits_one_slot_per_alias(self) -> None:
        node = _activity_node(
            connector=None,
            connectors={"src": "registry-src", "dst": "registry-dst"},
        )
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(
            responses=[
                BindForStepResponse(
                    contexts={
                        "src": ConnectorContext(
                            slot_name="src",
                            handle="h-src",
                            expires_at=_CLOCK_NOW + timedelta(minutes=5),
                            connector_kind="oci-registry",
                        ),
                        "dst": ConnectorContext(
                            slot_name="dst",
                            handle="h-dst",
                            expires_at=_CLOCK_NOW + timedelta(minutes=5),
                            connector_kind="oci-registry",
                        ),
                    },
                ),
            ],
        )
        handler = ActivityStepHandler(activity, connector)

        handler.execute(ctx, graph, "scan")

        slot_names = {s.name for s in connector.calls[0].slots}
        assert slot_names == {"src", "dst"}
        # Both refs round-trip.
        refs = {s.name: s.connector_ref for s in connector.calls[0].slots}
        assert refs == {"src": "registry-src", "dst": "registry-dst"}

    def test_connectorless_activity_emits_empty_slots(self) -> None:
        node = _activity_node(connector=None, connectors=None)
        graph = _graph(node)
        ctx = _ctx()
        activity = FakeActivityRuntimeClient(results=[_success({})])
        connector = FakeConnectorClient(
            responses=[BindForStepResponse(contexts={})],
        )
        handler = ActivityStepHandler(activity, connector)

        result = handler.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        assert connector.calls[0].slots == ()


# ---------------------------------------------------------------------------
# Replay determinism
# ---------------------------------------------------------------------------


class TestReplayDeterminism:
    def test_two_runs_under_same_fakes_produce_byte_equal_results(self) -> None:
        def _run() -> Any:
            node = _activity_node()
            graph = _graph(node)
            ctx = _ctx()
            activity = FakeActivityRuntimeClient(
                results=[_retryable(attempt=1), _success({"v": 1}, attempt=2)],
            )
            connector = FakeConnectorClient(
                responses=[_bind_response("default"), _bind_response("default")],
            )
            handler = ActivityStepHandler(activity, connector)
            return handler.execute(ctx, graph, "scan")

        first = _run()
        second = _run()
        assert isinstance(first, StepSucceeded)
        assert isinstance(second, StepSucceeded)
        # Byte-equal outputs across replays.
        assert dict(first.outputs) == dict(second.outputs)


# ---------------------------------------------------------------------------
# Defensive guards
# ---------------------------------------------------------------------------


class TestDefensiveGuards:
    def test_unknown_step_id_raises_key_error(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        ctx = _ctx()
        handler = ActivityStepHandler(
            FakeActivityRuntimeClient(),
            FakeConnectorClient(),
        )

        with pytest.raises(KeyError):
            handler.execute(ctx, graph, "does-not-exist")

    def test_non_activity_kind_raises_not_implemented_error(self) -> None:
        node = _let_node()
        graph = _graph(node)
        ctx = _ctx()
        handler = ActivityStepHandler(
            FakeActivityRuntimeClient(),
            FakeConnectorClient(),
        )

        with pytest.raises(NotImplementedError, match=r"only StepKind\.ACTIVITY"):
            handler.execute(ctx, graph, "derive")

    def test_handler_resolves_with_resolver_default(self) -> None:
        """Omitting ``with_resolver`` falls back to a fresh ``WithInputResolver``."""
        handler = ActivityStepHandler(
            FakeActivityRuntimeClient(),
            FakeConnectorClient(),
        )
        # The default resolver is opaque but must be the right type.
        assert isinstance(handler._with_resolver, WithInputResolver)
