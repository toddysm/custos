"""WF-IMPL-059 — Step Coordinator end-to-end integration suite.

Drives the **real** :class:`~custos_workflow.runs.RunController`,
the **real** :func:`~custos_workflow.runs.make_run_orchestrator`,
and the **real**
:class:`~custos_workflow.steps.StepCoordinator` (composed with a
real :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
+ default :class:`~custos_workflow.steps.LetStepHandler`) under
the synchronous :class:`~custos_workflow.runtime.FakeWorkflowRuntime`,
backed by :class:`~custos_workflow.clients.FakeActivityRuntimeClient`
+ :class:`~custos_workflow.clients.FakeConnectorClient`.

Six scenarios pinned by ``implementation-plan.md`` § WF-IMPL-059:

1. **Single activity step success** — happy-path bind +
   schedule, ``RunOutput.status == "succeeded"`` carries the
   envelope outputs, exactly one bind + one schedule observed.
2. **Multi-step `let → activity → let` with cross-step refs** —
   the second ``let:`` reads ``${{ steps.scan.outputs.* }}`` and
   sees the envelope outputs the activity returned.
3. **Activity retry loop** — envelope ``retryable`` on attempts
   1 + 2, ``success`` on attempt 3. The handler opens a
   :class:`FakeWorkflowContext.create_timer` per retry and
   eventually returns :class:`StepSucceeded`; the orchestrator
   surfaces ``status="succeeded"``.
4. **Retry budget exhaustion** — three ``retryable`` envelopes
   with ``maxAttempts=3`` exhaust the budget, the handler
   returns :class:`StepFailed` carrying a
   ``step.retry_budget_exhausted`` envelope, and the
   orchestrator surfaces ``status="failed"`` with that envelope.
5. **Cancel mid-flight** — ``start_run`` lands the run on the
   runtime, then ``cancel_run`` drives the store row through
   ``cancelling → cancelled`` and emits ``workflow.cancelled``
   on the publisher tape (under the synchronous fake the
   orchestrator has already produced a terminal ``RunOutput`` on
   the runtime instance by the time ``cancel_run`` fires; the
   AC surface for the Run Controller cancel path is the store
   row + event tape, mirroring the existing
   :mod:`tests.integration.test_run_lifecycle` discipline).
6. **Replay determinism** — two identical runs (fresh harness,
   identical inputs, identical fake responses) produce
   byte-equal :class:`RunOutput` payloads + byte-equal
   lifecycle event kinds in the same order.

A few cross-cutting notes:

* The Run Controller currently only emits ``workflow.started`` +
  ``workflow.cancelled`` / ``workflow.paused`` / ``workflow.resumed``
  through the :class:`LifecycleEventPublisher`. The forthcoming
  ``workflow.completed`` event + the orchestrator-side ``step.*``
  emitter wiring (built on top of the WF-IMPL-056
  ``StepLifecyclePublisher`` adapter) are tracked separately and
  intentionally **not** asserted here so this suite locks the
  *currently-shipped* observable surface and stays stable across
  the deferred sub-modules.
* The :class:`FakeWorkflowRuntime` drives the orchestrator inline
  during :meth:`schedule_new_workflow`, so the moment
  ``start_run`` returns the runtime instance carries the terminal
  :class:`RunOutput`. Tests assert on
  ``runtime.instance(run_id).output`` for the orchestrator surface
  and on the in-memory ``RunStore`` row for the controller
  surface. Both surfaces are independent — the Run Controller
  drives the store transitions regardless of what the runtime
  instance reports (see :mod:`tests.integration.test_run_lifecycle`
  for the same discipline).
"""

from __future__ import annotations

from datetime import timedelta
from types import MappingProxyType
from typing import Any

import pytest

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    FakeActivityRuntimeClient,
)
from custos_workflow.clients.connector import (
    BindForStepResponse,
    ConnectorContext,
    FakeConnectorClient,
)
from custos_workflow.runs import (
    LIFECYCLE_KIND_WORKFLOW_CANCELLED,
    LIFECYCLE_KIND_WORKFLOW_STARTED,
    RunOutput,
    RunStatus,
)
from custos_workflow.steps import StepCoordinator
from custos_workflow.steps.activity_step import ActivityStepHandler
from tests.integration._harness import (
    FIXED_NOW,
    IDEMPOTENCY_KEY,
    WORKFLOW_VERSION_ID,
    WORKSPACE,
    make_harness,
)

# ---------------------------------------------------------------------------
# Workflow documents
# ---------------------------------------------------------------------------


_SINGLE_ACTIVITY_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs:
        image: {type: string, default: 'alpine:3.19'}
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
          with:
            image: ${{ inputs.image }}
"""


_LET_ACTIVITY_LET_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs:
        image: {type: string, default: 'alpine:3.19'}
        threshold: {type: integer, default: 10}
      steps:
        - id: derive
          let:
            target: ${{ inputs.image }}
        - id: scan
          needs: [derive]
          activity: security/scan@1
          connector: primary
          with:
            image: ${{ steps.derive.outputs.target }}
        - id: verdict
          needs: [scan]
          let:
            critical: ${{ steps.scan.outputs.critical }}
            ok: ${{ steps.scan.outputs.critical <= inputs.threshold }}
"""


_RETRY_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs: {}
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
          retry:
            maxAttempts: 3
            backoff:
              strategy: exponential
              initialDelay: PT1S
              maxDelay: PT30S
              multiplier: 2.0
"""


# ---------------------------------------------------------------------------
# Activity-type registry shared across the scenarios
# ---------------------------------------------------------------------------


def _registry() -> InMemoryActivityTypeRegistry:
    """Registry with the output schemas the docs above reference.

    ``security/scan@1`` exposes ``critical: integer`` so the
    ``let:`` step in :data:`_LET_ACTIVITY_LET_DOC` type-checks
    its ``steps.scan.outputs.critical`` reference at compile
    time.
    """
    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                    "findings": {"type": "array", "items": {"type": "string"}},
                },
            },
        }
    )


# ---------------------------------------------------------------------------
# Fake-client + envelope helpers
# ---------------------------------------------------------------------------


def _bind_response(*slots: str) -> BindForStepResponse:
    """One :class:`ConnectorContext` per slot, all valid until
    five minutes past :data:`FIXED_NOW`.

    Callers pass slot **names** (e.g. ``"default"`` for the
    singular ``connector: primary`` shorthand which collapses
    to a slot named ``default``); the values are connector
    handles arbitrarily derived from the slot name so the
    bind tape is human-readable.
    """
    expires = FIXED_NOW + timedelta(minutes=5)
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
        outputs=MappingProxyType(outputs),
        error=None,
        attempt=attempt,
    )


def _retryable(*, attempt: int, code: str = "registry.timeout") -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="retryable",
        outputs=None,
        error=MappingProxyType(
            {
                "class": "retryable",
                "code": code,
                "message": "transient registry timeout",
            },
        ),
        attempt=attempt,
    )


def _make_coordinator(
    *,
    activity: FakeActivityRuntimeClient,
    connector: FakeConnectorClient,
) -> StepCoordinator:
    """Wire the real :class:`StepCoordinator` over the fakes."""
    return StepCoordinator(ActivityStepHandler(activity, connector))


# ---------------------------------------------------------------------------
# Scenario 1: single activity step success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSingleActivityStepSuccess:
    async def test_run_succeeds_with_envelope_outputs_on_runtime_instance(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[_success({"critical": 0, "findings": []})],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        h = make_harness(
            doc_yaml=_SINGLE_ACTIVITY_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19"},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value
        assert state.output.failed_step is None
        assert dict(state.output.outputs["scan"]) == {
            "critical": 0,
            "findings": [],
        }

    async def test_run_records_one_schedule_and_one_bind(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[_success({"critical": 1, "findings": ["CVE-2026-0001"]})],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        h = make_harness(
            doc_yaml=_SINGLE_ACTIVITY_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19"},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        # Exactly one schedule call, exactly one bind call. The
        # schedule call carries the resolved ``with:`` input
        # (``${{ inputs.image }}``) and the first attempt.
        assert len(activity.calls) == 1
        request = activity.calls[0]
        assert request.activity_ref == "security/scan@1"
        assert request.attempt == 1
        assert request.step_id == "scan"
        assert dict(request.inputs) == {"image": "alpine:3.19"}

        assert len(connector.calls) == 1
        bind_request = connector.calls[0]
        # The bind ``step_key`` is the canonical idempotency
        # triple string, which embeds the attempt index so a
        # retry would bind under a fresh key.
        assert bind_request.step_key.endswith("|scan|1")
        # Singular ``connector: primary`` shorthand collapses
        # to one slot keyed under the default slot name with
        # the connector reference threaded through to the bind
        # request (see :func:`_build_slot_specs`).
        assert [spec.name for spec in bind_request.slots] == ["default"]
        assert [spec.connector_ref for spec in bind_request.slots] == ["primary"]

    async def test_lifecycle_event_tape_is_workflow_started_only(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[_success({"critical": 0, "findings": []})],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        h = make_harness(
            doc_yaml=_SINGLE_ACTIVITY_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19"},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        # The Run Controller currently emits only
        # ``workflow.started`` on the success path; ``step.*`` +
        # ``workflow.completed`` wiring lands with the deferred
        # sub-modules per the suite docstring.
        assert [e.kind for e in h.publisher.events] == [LIFECYCLE_KIND_WORKFLOW_STARTED]


# ---------------------------------------------------------------------------
# Scenario 2: multi-step let → activity → let with cross-step refs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestLetActivityLetCrossStepRefs:
    async def test_second_let_sees_envelope_outputs_from_activity(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[_success({"critical": 7, "findings": ["CVE-2026-0002"]})],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        h = make_harness(
            doc_yaml=_LET_ACTIVITY_LET_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19", "threshold": 10},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value
        # ``derive`` is a ``let:`` that pulls from ``inputs.image``.
        assert dict(state.output.outputs["derive"]) == {"target": "alpine:3.19"}
        # ``scan`` is the activity step — its envelope outputs
        # land on the output bag verbatim.
        assert dict(state.output.outputs["scan"]) == {
            "critical": 7,
            "findings": ["CVE-2026-0002"],
        }
        # ``verdict`` is a second ``let:`` that consumes
        # ``${{ steps.scan.outputs.critical }}`` and
        # ``${{ inputs.threshold }}``. The boolean comparison is
        # evaluated by ``custos_cel`` at orchestrator time.
        assert dict(state.output.outputs["verdict"]) == {"critical": 7, "ok": True}

    async def test_activity_with_input_resolves_through_first_let(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[_success({"critical": 0, "findings": []})],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        h = make_harness(
            doc_yaml=_LET_ACTIVITY_LET_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19", "threshold": 10},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        # The activity's ``with.image`` references
        # ``steps.derive.outputs.target`` which itself unwraps
        # ``inputs.image`` — so the resolved scheduling request
        # must observe the through-routed value.
        assert len(activity.calls) == 1
        assert dict(activity.calls[0].inputs) == {"image": "alpine:3.19"}


# ---------------------------------------------------------------------------
# Scenario 3: retry loop succeeds on attempt 3
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRetryLoopSucceedsOnThirdAttempt:
    async def test_three_schedules_and_a_succeeded_run(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[
                _retryable(attempt=1),
                _retryable(attempt=2),
                _success({"critical": 0, "findings": []}, attempt=3),
            ],
        )
        # One bind per attempt — design pins bind-per-attempt
        # (see :mod:`custos_workflow.steps.activity_step` module
        # docstring).
        connector = FakeConnectorClient(
            responses=[_bind_response("default") for _ in range(3)],
        )
        h = make_harness(
            doc_yaml=_RETRY_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value
        assert dict(state.output.outputs["scan"]) == {
            "critical": 0,
            "findings": [],
        }
        # Three schedule attempts; three fresh connector binds
        # (one per attempt).
        assert len(activity.calls) == 3
        assert [c.attempt for c in activity.calls] == [1, 2, 3]
        assert len(connector.calls) == 3
        assert [c.step_key.split("|")[-1] for c in connector.calls] == ["1", "2", "3"]


# ---------------------------------------------------------------------------
# Scenario 4: retry budget exhaustion → failed run with locked envelope kind
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRetryBudgetExhausted:
    async def test_run_fails_with_step_retry_budget_exhausted_envelope(self) -> None:
        activity = FakeActivityRuntimeClient(
            results=[
                _retryable(attempt=1),
                _retryable(attempt=2),
                _retryable(attempt=3),
            ],
        )
        connector = FakeConnectorClient(
            responses=[_bind_response("default") for _ in range(3)],
        )
        h = make_harness(
            doc_yaml=_RETRY_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.FAILED.value
        assert state.output.failed_step == "scan"
        assert state.output.failure_envelope is not None
        # The envelope ``kind`` is the locked
        # ``step.retry_budget_exhausted`` taxon — verified against
        # the LOCKED_STEP_KINDS set on the kind-grid suite
        # (:mod:`tests.steps.test_errors`).
        assert state.output.failure_envelope["kind"] == "step.retry_budget_exhausted"
        # All three attempts ran before the budget tipped.
        assert len(activity.calls) == 3
        assert [c.attempt for c in activity.calls] == [1, 2, 3]


# ---------------------------------------------------------------------------
# Scenario 5: cancel mid-flight (Run Controller cancel path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCancelMidFlight:
    async def test_cancel_run_drives_store_row_and_emits_workflow_cancelled(self) -> None:
        # Even though the fake drives the orchestrator inline (so
        # the runtime instance is already terminal when
        # ``cancel_run`` fires), the Run Controller's cancel path
        # MUST still drive the store row through the
        # ``cancelling → cancelled`` transition + emit the
        # ``workflow.cancelled`` event. That contract is the AC
        # surface for WF-IMPL-045 cancel and is preserved
        # verbatim by the WF-IMPL-055 StepCoordinator wiring.
        activity = FakeActivityRuntimeClient(
            results=[_success({"critical": 0, "findings": []})],
        )
        connector = FakeConnectorClient(responses=[_bind_response("default")])
        h = make_harness(
            doc_yaml=_SINGLE_ACTIVITY_DOC,
            handler=_make_coordinator(activity=activity, connector=connector),
            activity_registry=_registry(),
        )

        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19"},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        cancelled = await h.controller.cancel_run(
            workspace_id=WORKSPACE,
            run_id=ref.run_id,
            reason="operator stop",
        )

        assert cancelled.status is RunStatus.CANCELLED
        record = await h.store.get_run(WORKSPACE, ref.run_id)
        assert record is not None
        assert record.status is RunStatus.CANCELLED
        assert record.reason == "operator stop"

        assert [e.kind for e in h.publisher.events] == [
            LIFECYCLE_KIND_WORKFLOW_STARTED,
            LIFECYCLE_KIND_WORKFLOW_CANCELLED,
        ]
        # ``reason`` round-trips on the cancelled event's
        # ``extra`` bag.
        assert dict(h.publisher.events[1].extra) == {"reason": "operator stop"}


# ---------------------------------------------------------------------------
# Scenario 6: replay determinism
# ---------------------------------------------------------------------------


def _run_once() -> tuple[RunOutput, list[str]]:
    """Drive one fresh end-to-end run and return the captured
    :class:`RunOutput` + the published lifecycle event kinds.

    Fresh harness + fresh fakes on each call — the determinism
    AC is that two *independent* invocations with the same
    inputs + the same canned responses produce byte-equal
    artefacts.
    """
    activity = FakeActivityRuntimeClient(
        results=[
            _retryable(attempt=1),
            _success({"critical": 4, "findings": ["CVE-2026-0099"]}, attempt=2),
        ],
    )
    connector = FakeConnectorClient(
        responses=[_bind_response("default"), _bind_response("default")],
    )
    h = make_harness(
        doc_yaml=_RETRY_DOC,
        handler=_make_coordinator(activity=activity, connector=connector),
        activity_registry=_registry(),
    )

    import asyncio as _asyncio

    async def _go() -> str:
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        return str(ref.run_id)

    run_id_str = _asyncio.run(_go())
    state = h.runtime.instance(run_id_str)
    assert isinstance(state.output, RunOutput)
    return state.output, [e.kind for e in h.publisher.events]


class TestReplayDeterminism:
    def test_two_runs_produce_byte_equal_run_output_and_event_tape(self) -> None:
        first_output, first_events = _run_once()
        second_output, second_events = _run_once()

        # Compare the JSON-serialised RunOutput so the comparison
        # walks every nested mapping (``RunOutput.outputs`` is a
        # ``MappingProxyType`` snapshot which compares by
        # identity-of-keys not by deep equality of nested dicts).
        assert first_output.to_dict() == second_output.to_dict()
        assert first_events == second_events


# ---------------------------------------------------------------------------
# Coverage discipline
# ---------------------------------------------------------------------------


def test_registry_helper_exposes_security_scan_schema() -> None:
    """Belt-and-braces: the shared :func:`_registry` helper is
    the single source of truth for the activity output schema
    every scenario above type-checks against. A guard against
    accidental drift between the schema and the document
    references (``${{ steps.scan.outputs.critical }}``).
    """
    reg = _registry()
    schema = reg.get_outputs_schema("security/scan@1")
    assert schema is not None
    assert schema["properties"]["critical"] == {"type": "integer"}
