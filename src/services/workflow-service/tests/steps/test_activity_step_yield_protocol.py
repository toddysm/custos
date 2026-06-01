"""Tests for the WF-IMPL-074 activity-task yield protocol.

The yield protocol decouples
:class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
from its outbound bind / schedule RPCs by replacing every inline
client call with a yielded
:class:`~custos_workflow.runtime.dapr_activities.ActivityCallToken`
value object. Production wiring (WF-IMPL-079) will resolve each
yielded token as a separately-suspendable Dapr activity. This
test module pins three contracts the lifespan wiring depends on:

* :meth:`ActivityStepHandler.iter_calls` yields a
  :class:`BindForStepCallToken` then a
  :class:`ScheduleActivityCallToken` for every attempt, in order,
  carrying exactly the same request payloads the inline path
  would have built.
* :func:`drive_activity_generator` round-trips the generator
  against the canonical in-process clients and produces the same
  :class:`StepResult` :meth:`execute` would return. This is the
  contract that lets :meth:`execute` stay a thin synchronous
  wrapper without behavioural drift from the generator path.
* The handler still observes :class:`ConnectorBindError` /
  :class:`ActivityScheduleError` raised by the driver via
  :meth:`Generator.throw`, so the legacy ``try`` / ``except``
  arms in :meth:`iter_calls` continue to map driver-side failures
  to ``step.connector_bind_error`` / ``step.activity_schedule_error``
  envelopes.

The non-protocol behaviours of the handler (retry policy
exhaustion, ``with:`` resolution, replay determinism, ...) are
already covered by :mod:`test_activity_step` against
:meth:`execute`; this module focuses strictly on the new
generator surface so a regression there fails its own test
file rather than getting buried in a 900-line suite.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
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
)
from custos_workflow.document import ActivityStep
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
    StepSucceeded,
)
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.runtime.dapr_activities import (
    BIND_FOR_STEP_ACTIVITY_NAME,
    SCHEDULE_ACTIVITY_ACTIVITY_NAME,
    BindForStepCallToken,
    FakeDaprActivityDispatcher,
    ScheduleActivityCallToken,
    drive_activity_generator,
)
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.errors import ActivityScheduleError, ConnectorBindError

# ---------------------------------------------------------------------------
# Fixtures (lean — most are already covered by test_activity_step)
# ---------------------------------------------------------------------------

_CLOCK_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_CLOCK_NOW)


def _policy(*, max_attempts: int = 3) -> ResolvedRetryPolicy:
    return ResolvedRetryPolicy(
        max_attempts=max_attempts,
        backoff=ResolvedBackoffPolicy(
            strategy=BackoffStrategyTag.EXPONENTIAL,
            initial_delay_ms=1_000,
            max_delay_ms=60_000,
            multiplier=2.0,
        ),
        jitter=JitterStrategyTag.NONE,
        respect_retry_after=True,
    )


def _routes(policy: ResolvedRetryPolicy) -> tuple[OnErrorRoute, ...]:
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
    max_attempts: int = 3,
) -> ExecutionNode:
    policy = _policy(max_attempts=max_attempts)
    payload: dict[str, Any] = {"id": step_id, "activity": activity}
    if connector is not None:
        payload["connector"] = connector
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=policy,
        on_error_routes=_routes(policy),
        call_sites={},
        step_source=ActivityStep.model_validate(payload),
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


def _ctx(*, run_id: str = "run-1") -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id="ws-1",
        workflow_version_id="wf-1",
        inputs=MappingProxyType({}),
        workflow_context=FakeWorkflowContext(instance_id=run_id, now=_CLOCK_NOW),
        outputs=MappingProxyType({}),
        clock=_CLOCK,
    )


def _bind_response(*slots: str) -> BindForStepResponse:
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


def _retryable(*, attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="retryable",
        outputs=None,
        error={"class": "retryable", "code": "registry.timeout", "message": "timed out"},
        attempt=attempt,
    )


# ---------------------------------------------------------------------------
# Activity-name constants (production registration contract for WF-IMPL-079)
# ---------------------------------------------------------------------------


class TestActivityNameConstants:
    """The constants pin the names production Dapr will register
    each token-resolver activity under. Changing them is a wire
    contract break — failing here forces the design discussion."""

    def test_bind_activity_name_is_stable(self) -> None:
        assert BIND_FOR_STEP_ACTIVITY_NAME == "custos.workflow.connector.bind_for_step"

    def test_schedule_activity_name_is_stable(self) -> None:
        assert SCHEDULE_ACTIVITY_ACTIVITY_NAME == "custos.workflow.arm.schedule_activity"


# ---------------------------------------------------------------------------
# Token value-object semantics
# ---------------------------------------------------------------------------


class TestTokens:
    def test_bind_token_is_frozen_and_hashable(self) -> None:
        request = BindForStepRequest(step_key="run-1::scan::1", slots=())
        token = BindForStepCallToken(request=request)

        with pytest.raises(FrozenInstanceError):
            token.request = request  # type: ignore[misc]

        # frozen + slots dataclasses are hashable iff every field
        # is hashable — BindForStepRequest is frozen + slots itself
        # so the token must be hashable for use in test sets.
        assert hash(token) == hash(BindForStepCallToken(request=request))

    def test_schedule_token_is_frozen(self) -> None:
        deadline = _CLOCK_NOW + timedelta(seconds=30)
        request = ScheduleActivityRequest(
            run_id="run-1",
            step_id="scan",
            attempt=1,
            activity_ref="scanners/trivy@1",
            inputs=MappingProxyType({}),
            connector_contexts=MappingProxyType({}),
            deadline=deadline,
        )
        token = ScheduleActivityCallToken(request=request)

        with pytest.raises(FrozenInstanceError):
            token.request = request  # type: ignore[misc]


# ---------------------------------------------------------------------------
# iter_calls yield contract
# ---------------------------------------------------------------------------


class TestIterCallsYieldContract:
    """Drive ``iter_calls`` manually so each yield is observable.

    These tests are the *contract* tests for the yield protocol;
    they bypass :func:`drive_activity_generator` so a regression
    in the generator (wrong token order, missing typed guard,
    re-yielding, etc.) fails here and not in a downstream test
    file."""

    def test_first_yield_is_bind_token_with_step_key(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        first = next(gen)

        assert isinstance(first, BindForStepCallToken)
        assert first.request.step_key == "run-1|scan|1"
        assert len(first.request.slots) == 1
        # ``connector: primary`` on the document compiles to a
        # single SlotSpec keyed under the ``_DEFAULT_SLOT_NAME``
        # sentinel ("default"); the document-level alias
        # "primary" becomes the ``connector_ref``.
        assert first.request.slots[0].name == "default"
        assert first.request.slots[0].connector_ref == "primary"
        gen.close()

    def test_second_yield_is_schedule_token_carrying_bind_contexts(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)  # BindForStepCallToken
        second = gen.send(_bind_response("default"))

        assert isinstance(second, ScheduleActivityCallToken)
        assert second.request.run_id == "run-1"
        assert second.request.step_id == "scan"
        assert second.request.attempt == 1
        assert second.request.activity_ref == "scanners/trivy@1"
        # The bind response's contexts mapping is threaded through
        # to the schedule request verbatim — the handler does no
        # filtering or re-binding between yields.
        assert dict(second.request.connector_contexts) == {
            "default": ConnectorContext(
                slot_name="default",
                handle="handle-default",
                expires_at=_CLOCK_NOW + timedelta(minutes=5),
                connector_kind="oci-registry",
            ),
        }
        gen.close()

    def test_generator_returns_step_succeeded_for_success_envelope(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        gen.send(_bind_response("default"))
        with pytest.raises(StopIteration) as stop:
            gen.send(_success({"sbom": "s3://bucket/sbom.json"}))

        result = stop.value.value
        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"sbom": "s3://bucket/sbom.json"}

    def test_retry_yields_a_second_bind_schedule_pair_with_incremented_attempt(
        self,
    ) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        # Attempt 1
        first_bind = next(gen)
        first_schedule = gen.send(_bind_response("default"))
        assert isinstance(first_bind, BindForStepCallToken)
        assert isinstance(first_schedule, ScheduleActivityCallToken)
        assert first_schedule.request.attempt == 1
        # Attempt 2 — the handler creates a durable timer between
        # the schedule that returns retryable and the bind for the
        # next attempt; the generator path does NOT yield the
        # timer (the orchestrator handles WF-IMPL-061 timers
        # separately), so the next yield is the bind for attempt 2.
        second_bind = gen.send(_retryable(attempt=1))
        assert isinstance(second_bind, BindForStepCallToken)
        assert second_bind.request.step_key == "run-1|scan|2"
        second_schedule = gen.send(_bind_response("default"))
        assert isinstance(second_schedule, ScheduleActivityCallToken)
        assert second_schedule.request.attempt == 2
        with pytest.raises(StopIteration) as stop:
            gen.send(_success({"ok": True}, attempt=2))
        assert isinstance(stop.value.value, StepSucceeded)

    def test_wrong_type_sent_back_for_bind_raises_type_error(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        with pytest.raises(TypeError, match="BindForStepResponse"):
            gen.send("not-a-bind-response")

    def test_wrong_type_sent_back_for_schedule_raises_type_error(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        gen.send(_bind_response("default"))
        with pytest.raises(TypeError, match="ActivityResultEnvelope"):
            gen.send({"not": "an envelope"})

    def test_bind_exception_thrown_in_maps_to_connector_bind_error_envelope(
        self,
    ) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        with pytest.raises(StopIteration) as stop:
            gen.throw(RuntimeError("dapr sidecar down"))

        result = stop.value.value
        assert isinstance(result, StepFailed)
        assert result.envelope["kind"] == "step.connector_bind_error"

    def test_schedule_exception_thrown_in_maps_to_schedule_error_envelope(
        self,
    ) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        gen.send(_bind_response("default"))
        with pytest.raises(StopIteration) as stop:
            gen.throw(RuntimeError("arm timeout"))

        result = stop.value.value
        assert isinstance(result, StepFailed)
        assert result.envelope["kind"] == "step.activity_schedule_error"

    def test_bind_connector_bind_error_thrown_in_preserves_original_envelope(
        self,
    ) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        bind_error = ConnectorBindError(
            "bind refused",
            run_id="run-1",
            step_id="scan",
            attempt=1,
            cause="HTTP 503",
        )
        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        with pytest.raises(StopIteration) as stop:
            gen.throw(bind_error)

        result = stop.value.value
        assert isinstance(result, StepFailed)
        assert result.envelope == MappingProxyType(bind_error.to_dict())

    def test_schedule_activity_schedule_error_thrown_in_preserves_envelope(
        self,
    ) -> None:
        node = _activity_node()
        graph = _graph(node)
        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        schedule_error = ActivityScheduleError(
            "schedule rejected",
            run_id="run-1",
            step_id="scan",
            attempt=1,
            activity_ref="scanners/trivy@1",
            cause="HTTP 503",
        )
        gen = handler.iter_calls(_ctx(), graph, "scan")
        next(gen)
        gen.send(_bind_response("default"))
        with pytest.raises(StopIteration) as stop:
            gen.throw(schedule_error)

        result = stop.value.value
        assert isinstance(result, StepFailed)
        assert result.envelope == MappingProxyType(schedule_error.to_dict())


# ---------------------------------------------------------------------------
# drive_activity_generator equivalence
# ---------------------------------------------------------------------------


class TestDriverEquivalence:
    """``execute`` is a thin sync wrapper:
    ``drive_activity_generator(iter_calls(...), activity_client, connector_client)``.
    Lock that down — if it ever drifts, every test in
    :mod:`test_activity_step` would still pass (they all go
    through ``execute``), and downstream callers relying on
    direct ``iter_calls`` use would silently observe different
    behaviour."""

    def test_driver_path_matches_execute_path_on_success(self) -> None:
        node = _activity_node()
        graph = _graph(node)

        # Path 1: handler.execute (the thin wrapper).
        wrapper_handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(results=[_success({"ok": True})]),
            connector_client=FakeConnectorClient(responses=[_bind_response("default")]),
        )
        wrapper_result = wrapper_handler.execute(_ctx(), graph, "scan")

        # Path 2: build a separate handler, then invoke
        # ``drive_activity_generator`` directly against
        # ``iter_calls`` with fresh canned clients.
        gen_activity = FakeActivityRuntimeClient(results=[_success({"ok": True})])
        gen_connector = FakeConnectorClient(responses=[_bind_response("default")])
        gen_handler = ActivityStepHandler(
            activity_client=gen_activity,
            connector_client=gen_connector,
        )
        driver_result = drive_activity_generator(
            gen_handler.iter_calls(_ctx(), graph, "scan"),
            gen_activity,
            gen_connector,
        )

        assert isinstance(wrapper_result, StepSucceeded)
        assert isinstance(driver_result, StepSucceeded)
        assert dict(wrapper_result.outputs) == dict(driver_result.outputs)

    def test_driver_re_raises_client_exceptions_into_generator(self) -> None:
        node = _activity_node()
        graph = _graph(node)

        class _BoomConnector:
            def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
                raise RuntimeError("dapr down")

        handler = ActivityStepHandler(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=_BoomConnector(),
        )

        # The driver MUST catch the client exception and throw it
        # back into the generator so the handler's try/except
        # arms convert it to a ``step.connector_bind_error``
        # envelope (rather than letting it escape as an unwrapped
        # exception that would crash the workflow instance).
        result = drive_activity_generator(
            handler.iter_calls(_ctx(), graph, "scan"),
            handler._activity_client,
            handler._connector_client,
        )
        assert isinstance(result, StepFailed)
        assert result.envelope["kind"] == "step.connector_bind_error"

    def test_driver_raises_type_error_for_unknown_token(self) -> None:
        # Hand-roll a generator that yields a non-ActivityCallToken
        # value to prove the driver fails loudly (the production
        # Dapr resolver / FakeDaprActivityDispatcher both rely on
        # this guard to surface contract bugs early).
        def _bogus() -> Any:
            yield "not-a-token"

        with pytest.raises(TypeError, match="ActivityCallToken"):
            drive_activity_generator(
                _bogus(),
                FakeActivityRuntimeClient(),
                FakeConnectorClient(),
            )


# ---------------------------------------------------------------------------
# FakeDaprActivityDispatcher
# ---------------------------------------------------------------------------


class TestFakeDaprActivityDispatcher:
    def test_drive_round_trips_iter_calls_to_step_succeeded(self) -> None:
        node = _activity_node()
        graph = _graph(node)
        activity_client = FakeActivityRuntimeClient(results=[_success({"v": 1})])
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        handler = ActivityStepHandler(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        dispatcher = FakeDaprActivityDispatcher(
            activity_client=activity_client,
            connector_client=connector_client,
        )

        result = dispatcher.drive(handler.iter_calls(_ctx(), graph, "scan"))
        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"v": 1}

    def test_resolve_dispatches_bind_token_to_connector_client(self) -> None:
        activity_client = FakeActivityRuntimeClient()
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        dispatcher = FakeDaprActivityDispatcher(
            activity_client=activity_client,
            connector_client=connector_client,
        )

        token = BindForStepCallToken(
            request=BindForStepRequest(step_key="run-1::scan::1", slots=()),
        )
        response = dispatcher.resolve(token)

        assert isinstance(response, BindForStepResponse)
        assert len(connector_client.calls) == 1

    def test_resolve_dispatches_schedule_token_to_activity_client(self) -> None:
        activity_client = FakeActivityRuntimeClient(results=[_success({"ok": True})])
        connector_client = FakeConnectorClient()
        dispatcher = FakeDaprActivityDispatcher(
            activity_client=activity_client,
            connector_client=connector_client,
        )

        token = ScheduleActivityCallToken(
            request=ScheduleActivityRequest(
                run_id="run-1",
                step_id="scan",
                attempt=1,
                activity_ref="scanners/trivy@1",
                inputs=MappingProxyType({}),
                connector_contexts=MappingProxyType({}),
                deadline=_CLOCK_NOW + timedelta(seconds=30),
            ),
        )
        response = dispatcher.resolve(token)

        assert isinstance(response, ActivityResultEnvelope)
        assert response.class_ == "success"
        assert len(activity_client.calls) == 1

    def test_resolve_raises_type_error_for_unknown_token(self) -> None:
        dispatcher = FakeDaprActivityDispatcher(
            activity_client=FakeActivityRuntimeClient(),
            connector_client=FakeConnectorClient(),
        )

        with pytest.raises(TypeError):
            dispatcher.resolve("not-a-token")  # type: ignore[arg-type]
