"""WF-IMPL-060 — docs/developers/workflow-step-coordinator.md examples test.

Pins the Step Coordinator developer documentation to the running code:

* Every fenced ```yaml``` block in the doc is parsed, compiled
  through :func:`custos_workflow.compiler.compile` against the
  populated activity registry, and pushed through the in-memory
  :func:`tests.integration._harness.make_harness` wired with a
  real :class:`StepCoordinator` over fake activity / connector
  clients.
* Every documented :class:`StepCoordinatorError` subclass `kind`
  string is asserted to be a member of
  :data:`custos_workflow.steps.LOCKED_STEP_KINDS`; the
  exhaustiveness guard fails the build if a sixth subclass
  appears without being added here.
* Every documented ``step.*`` lifecycle kind is asserted to be
  a member of :data:`custos_workflow.steps.LOCKED_STEP_EVENT_KINDS`;
  same exhaustiveness guarantee.
* Every documented :class:`PrimitiveHandler` member is asserted
  to be a live member of the live enum; same exhaustiveness
  guarantee.

The docs cannot drift away from the code; CI catches it here.

Sibling pin for the Run Controller doc lives at
``tests/test_docs_examples_run_controller.py`` (WF-IMPL-046).
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

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
from custos_workflow.graph.model import PrimitiveHandler
from custos_workflow.runs import RunOutput
from custos_workflow.steps import StepCoordinator
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.errors import LOCKED_STEP_KINDS, StepCoordinatorError
from custos_workflow.steps.events import LOCKED_STEP_EVENT_KINDS
from tests.integration._harness import (
    FIXED_NOW,
    IDEMPOTENCY_KEY,
    WORKFLOW_VERSION_ID,
    WORKSPACE,
    make_harness,
)

# ---------------------------------------------------------------------------
# Doc location and parsing
# ---------------------------------------------------------------------------


#: Repo-root-relative path to the doc; the workflow-service test
#: tree is four levels deep from the repo root.
_DOC_PATH: Final[Path] = (
    Path(__file__).resolve().parents[4] / "docs" / "developers" / "workflow-step-coordinator.md"
)


def _read_doc() -> str:
    assert _DOC_PATH.is_file(), f"developer doc missing at {_DOC_PATH}"
    return _DOC_PATH.read_text(encoding="utf-8")


_YAML_BLOCK_RE: Final[re.Pattern[str]] = re.compile(
    r"```yaml\n(.*?)\n```",
    re.DOTALL,
)


def _iter_yaml_blocks(doc_text: str) -> Iterator[tuple[int, str]]:
    """Yield ``(1-based-block-index, body)`` for every fenced ```yaml``` block."""

    for idx, match in enumerate(_YAML_BLOCK_RE.finditer(doc_text), start=1):
        yield idx, match.group(1)


# ---------------------------------------------------------------------------
# Activity-type registry shared across the doc examples
# ---------------------------------------------------------------------------


def _registry() -> InMemoryActivityTypeRegistry:
    """Mirror of the activity output schemas the doc examples reference.

    ``security/scan@1`` exposes ``critical: integer`` so the
    ``verdict`` step's CEL expression in worked example 2
    type-checks ``steps.scan.outputs.critical`` at compile time.
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
# Fake-client + envelope helpers (mirror tests/integration/_harness usage)
# ---------------------------------------------------------------------------


def _bind_response(*slots: str) -> BindForStepResponse:
    """One :class:`ConnectorContext` per slot, valid for five minutes."""
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


def _retryable(*, attempt: int) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="retryable",
        outputs=None,
        error=MappingProxyType(
            {
                "class": "retryable",
                "code": "registry.timeout",
                "message": "transient registry timeout",
            },
        ),
        attempt=attempt,
    )


def _make_coordinator(
    activity: FakeActivityRuntimeClient,
    connector: FakeConnectorClient,
) -> StepCoordinator:
    """Wire the real :class:`StepCoordinator` over the fakes."""
    return StepCoordinator(ActivityStepHandler(activity, connector))


# ---------------------------------------------------------------------------
# Per-example expected outcome
# ---------------------------------------------------------------------------


#: The doc's "## Worked examples" section pins three examples in
#: this exact order; the exhaustiveness assertion below pairs a
#: documented snippet with an asserted outcome 1:1, so adding a
#: fourth worked example without extending this table surfaces
#: here as a clear failure.
_EXAMPLE_OUTCOMES: Final[dict[int, str]] = {
    1: "succeeded-single-activity",
    2: "succeeded-cross-step",
    3: "failed-retry-exhausted",
}


def _drive_example_1() -> None:
    """Worked example 1 — single activity step, success on attempt 1."""

    activity = FakeActivityRuntimeClient(
        results=[_success({"critical": 0, "findings": []})],
    )
    connector = FakeConnectorClient(responses=[_bind_response("default")])
    h = make_harness(
        doc_yaml=_DOC_BLOCK_BODIES[1],
        handler=_make_coordinator(activity, connector),
        activity_registry=_registry(),
    )

    async def _go() -> RunOutput:
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19"},
            idempotency_key=f"{IDEMPOTENCY_KEY}-doc-1",
        )
        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        return state.output

    output = asyncio.run(_go())

    assert output.status == "succeeded"
    assert output.outputs["scan"] == {"critical": 0, "findings": []}
    # One bind, one schedule — doc pins exactly one attempt.
    assert len(connector.calls) == 1
    assert connector.calls[0].step_key.endswith("|scan|1")
    assert len(activity.calls) == 1
    assert activity.calls[0].activity_ref == "security/scan@1"
    assert activity.calls[0].attempt == 1


def _drive_example_2() -> None:
    """Worked example 2 — multi-step let → activity → let."""

    activity = FakeActivityRuntimeClient(
        results=[_success({"critical": 0, "findings": []})],
    )
    connector = FakeConnectorClient(responses=[_bind_response("default")])
    h = make_harness(
        doc_yaml=_DOC_BLOCK_BODIES[2],
        handler=_make_coordinator(activity, connector),
        activity_registry=_registry(),
    )

    async def _go() -> RunOutput:
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"image": "alpine:3.19", "threshold": 10},
            idempotency_key=f"{IDEMPOTENCY_KEY}-doc-2",
        )
        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        return state.output

    output = asyncio.run(_go())

    assert output.status == "succeeded"
    # All three step ids carry outputs.
    assert {"derive", "scan", "verdict"} <= set(output.outputs)
    # The verdict step's CEL expression must evaluate to true when
    # the fake returns critical=0 against the default threshold 10.
    assert output.outputs["verdict"]["ok"] is True
    assert output.outputs["verdict"]["critical"] == 0
    # The activity's `with.image` resolves through the first let:
    # step's outputs, so the recorded inputs carry the literal.
    assert dict(activity.calls[0].inputs) == {"image": "alpine:3.19"}


def _drive_example_3() -> None:
    """Worked example 3 — retry budget exhaustion."""

    activity = FakeActivityRuntimeClient(
        results=[
            _retryable(attempt=1),
            _retryable(attempt=2),
            _retryable(attempt=3),
        ],
    )
    connector = FakeConnectorClient(
        responses=[
            _bind_response("default"),
            _bind_response("default"),
            _bind_response("default"),
        ],
    )
    h = make_harness(
        doc_yaml=_DOC_BLOCK_BODIES[3],
        handler=_make_coordinator(activity, connector),
        activity_registry=_registry(),
    )

    async def _go() -> RunOutput:
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=f"{IDEMPOTENCY_KEY}-doc-3",
        )
        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        return state.output

    output = asyncio.run(_go())

    assert output.status == "failed"
    assert output.failed_step == "scan"
    assert output.failure_envelope is not None
    assert output.failure_envelope["kind"] == "step.retry_budget_exhausted"
    # All three attempts schedule, with a fresh bind on each.
    assert [c.attempt for c in activity.calls] == [1, 2, 3]
    assert len(connector.calls) == 3


# Module-level cache of the doc's yaml block bodies so the drivers
# can read them once.
_DOC_BLOCK_BODIES: Final[dict[int, str]] = dict(_iter_yaml_blocks(_read_doc()))


_EXAMPLE_DRIVERS: Final[dict[int, Any]] = {
    1: _drive_example_1,
    2: _drive_example_2,
    3: _drive_example_3,
}


@pytest.mark.parametrize(
    "block_idx,outcome_tag",
    sorted(_EXAMPLE_OUTCOMES.items()),
)
def test_doc_example_yaml_runs_to_documented_terminal_status(
    block_idx: int, outcome_tag: str
) -> None:
    assert block_idx in _DOC_BLOCK_BODIES, (
        f"doc block #{block_idx} missing from docs/developers/workflow-step-coordinator.md"
    )
    driver = _EXAMPLE_DRIVERS[block_idx]
    driver()
    # The outcome tag carries the asserted-on shape; the driver
    # body owns the assertions so failures surface a clear stack
    # trace pointing at the specific assertion.
    assert outcome_tag in {
        "succeeded-single-activity",
        "succeeded-cross-step",
        "failed-retry-exhausted",
    }


def test_every_doc_yaml_block_has_an_asserted_outcome() -> None:
    """Exhaustiveness guard — no doc snippet runs un-exercised."""

    assert set(_DOC_BLOCK_BODIES) == set(_EXAMPLE_OUTCOMES), (
        "doc yaml-block count drifted from the test's expected "
        f"outcome table: blocks={sorted(_DOC_BLOCK_BODIES)} "
        f"expected_keys={sorted(_EXAMPLE_OUTCOMES)}"
    )


# ---------------------------------------------------------------------------
# Reflection-based contract pins — locked error taxonomy
# ---------------------------------------------------------------------------


#: Mirror of the doc's "Locked error taxonomy" table. Every entry
#: MUST be a member of :data:`LOCKED_STEP_KINDS`; the
#: exhaustiveness guard below fails the build on any drift.
_DOCUMENTED_STEP_KINDS: Final[frozenset[str]] = frozenset(
    {
        "step.kind_not_implemented",
        "step.with_input_resolution_error",
        "step.connector_bind_error",
        "step.activity_schedule_error",
        "step.retry_budget_exhausted",
        "step.loop_expansion_error",
        "step.sub_orchestration_spawn_error",
        "step.sub_workflow_failed",
        "step.approval_timeout",
    }
)


@pytest.mark.parametrize("kind", sorted(_DOCUMENTED_STEP_KINDS))
def test_documented_step_kind_is_locked(kind: str) -> None:
    assert kind in LOCKED_STEP_KINDS, (
        f"documented error kind {kind!r} is not a member of LOCKED_STEP_KINDS"
    )


def test_documented_step_kind_set_is_exhaustive() -> None:
    """Doc's error taxonomy MUST list every locked step.* error kind."""

    subclass_kinds = {cls.KIND for cls in StepCoordinatorError.__subclasses__()}
    assert subclass_kinds == _DOCUMENTED_STEP_KINDS, (
        "StepCoordinatorError subclass set drifted from "
        "docs/developers/workflow-step-coordinator.md § Locked error taxonomy: "
        f"subclasses={sorted(subclass_kinds)} "
        f"documented={sorted(_DOCUMENTED_STEP_KINDS)}"
    )
    assert subclass_kinds == set(LOCKED_STEP_KINDS), (
        "StepCoordinatorError subclass set drifted from LOCKED_STEP_KINDS"
    )


# ---------------------------------------------------------------------------
# Reflection-based contract pins — locked step.* lifecycle taxonomy
# ---------------------------------------------------------------------------


#: Mirror of the doc's "step.* event taxonomy" table. Every entry
#: MUST be a member of :data:`LOCKED_STEP_EVENT_KINDS`; the
#: exhaustiveness guard below fails the build on any drift.
_DOCUMENTED_STEP_EVENT_KINDS: Final[frozenset[str]] = frozenset(
    {
        "step.started",
        "step.completed",
        "step.failed",
        "step.skipped",
        "step.waiting",
        "step.retry_scheduled",
    }
)


@pytest.mark.parametrize("kind", sorted(_DOCUMENTED_STEP_EVENT_KINDS))
def test_documented_step_event_kind_is_locked(kind: str) -> None:
    assert kind in LOCKED_STEP_EVENT_KINDS, (
        f"documented lifecycle kind {kind!r} is not a member of LOCKED_STEP_EVENT_KINDS"
    )


def test_documented_step_event_kind_set_is_exhaustive() -> None:
    """Doc's event taxonomy MUST list every locked step.* lifecycle kind."""

    assert set(LOCKED_STEP_EVENT_KINDS) == _DOCUMENTED_STEP_EVENT_KINDS, (
        "LOCKED_STEP_EVENT_KINDS drifted from "
        "docs/developers/workflow-step-coordinator.md § step.* event taxonomy: "
        f"locked={sorted(LOCKED_STEP_EVENT_KINDS)} "
        f"documented={sorted(_DOCUMENTED_STEP_EVENT_KINDS)}"
    )


# ---------------------------------------------------------------------------
# Reflection-based contract pins — dispatch table
# ---------------------------------------------------------------------------


#: Mirror of the doc's "Dispatch table". Every entry MUST be a
#: live :class:`PrimitiveHandler` member; the exhaustiveness
#: guard below fails the build on any drift.
_DOCUMENTED_PRIMITIVE_HANDLERS: Final[frozenset[str]] = frozenset(
    {
        "EXPRESSION_INLINE",
        "ACTIVITY_RUNTIME",
        "SUB_ORCHESTRATION",
        "RUN_CONTROLLER_TIMER",
        "RESUME_SUBSCRIPTION",
    }
)


@pytest.mark.parametrize("name", sorted(_DOCUMENTED_PRIMITIVE_HANDLERS))
def test_documented_primitive_handler_is_a_live_member(name: str) -> None:
    assert name in PrimitiveHandler.__members__, (
        f"documented PrimitiveHandler member {name!r} is not a live member of the enum"
    )


def test_documented_primitive_handler_set_is_exhaustive() -> None:
    """Doc's dispatch table MUST list every PrimitiveHandler member."""

    live_members = set(PrimitiveHandler.__members__)
    assert live_members == _DOCUMENTED_PRIMITIVE_HANDLERS, (
        "PrimitiveHandler members drifted from "
        "docs/developers/workflow-step-coordinator.md § Dispatch table: "
        f"live={sorted(live_members)} "
        f"documented={sorted(_DOCUMENTED_PRIMITIVE_HANDLERS)}"
    )
