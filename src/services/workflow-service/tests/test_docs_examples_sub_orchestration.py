"""WF-IMPL-098 — docs/developers/workflow-sub-orchestration.md examples test.

Pins the Sub-Orchestration Manager developer documentation to the
running code:

* Every fenced ```yaml``` block in the doc is parsed, compiled
  through :func:`custos_workflow.compiler.compile`, and driven
  through a real :class:`SubOrchestrationManager` via the matching
  primitive method (``run_loop`` / ``run_approval`` /
  ``run_sub_workflow``) so the documented surface cannot drift away
  from the code.
* Every documented ``step.*`` error kind is asserted to be a member
  of :data:`custos_workflow.steps.errors.LOCKED_STEP_KINDS`, and the
  four sub-orchestration kinds are pinned exhaustively.
* The documented child-id reserved keys and Configuration defaults
  are pinned against the live constants.

Sibling pin for the Step Coordinator doc lives at
``tests/test_docs_examples_step_coordinator.py`` (WF-IMPL-060).
"""

from __future__ import annotations

import re
from collections.abc import Generator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, cast

import pytest
from custos_cel import FixedClock

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import parse_document
from custos_workflow.graph import ExecutionGraph, PrimitiveHandler, StepKind
from custos_workflow.runs import RunId, RunOutput, StepExecutionContext
from custos_workflow.steps.errors import LOCKED_STEP_KINDS, StepCoordinatorError
from custos_workflow.steps.sub_orchestration import (
    APPROVAL_ITERATION_KEY,
    CHILD_STEP_WORKFLOW_NAME,
    DEFAULT_APPROVAL_TIMEOUT,
    DEFAULT_MAX_FANOUT_WIDTH,
    WORKFLOW_ITERATION_KEY,
    ChildStepResult,
    SubOrchestrationManager,
)

# ---------------------------------------------------------------------------
# Doc location and parsing
# ---------------------------------------------------------------------------


#: Repo-root-relative path to the doc; the workflow-service test
#: tree is four levels deep from the repo root.
_DOC_PATH: Final[Path] = (
    Path(__file__).resolve().parents[4] / "docs" / "developers" / "workflow-sub-orchestration.md"
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


# Module-level cache of the doc's yaml block bodies so the drivers
# can read them once.
_DOC_BLOCK_BODIES: Final[dict[int, str]] = dict(_iter_yaml_blocks(_read_doc()))


# ---------------------------------------------------------------------------
# Compile helpers
# ---------------------------------------------------------------------------


_NOW: Final[datetime] = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_RUN_ID: Final[str] = "parent-1"


def _meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-1",
        workflow_version_id="wfv-1",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=_NOW,
    )


def _registry() -> InMemoryActivityTypeRegistry:
    """The loop example references ``security/scan@1``."""
    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {"critical": {"type": "integer"}},
            },
        }
    )


def _compile(doc_yaml: str) -> ExecutionGraph:
    return compile_workflow(parse_document(doc_yaml), _meta(), _registry())


def _step_ctx(workflow_context: Any, inputs: dict[str, Any]) -> StepExecutionContext:
    return StepExecutionContext(
        run_id=cast(RunId, _RUN_ID),
        workspace_id="ws-1",
        workflow_version_id="wfv-1",
        inputs=inputs,
        workflow_context=workflow_context,
        outputs={},
        clock=FixedClock(_NOW),
    )


# ---------------------------------------------------------------------------
# Stub workflow contexts (one per primitive) that record spawns without
# executing any child, mirroring the manager unit tests.
# ---------------------------------------------------------------------------


class _FanOutContext:
    """Records ``call_child_workflow`` spawns for loop / sub-workflow."""

    instance_id = _RUN_ID

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def call_child_workflow(
        self, workflow: Any, *, input: Any = None, instance_id: str | None = None
    ) -> tuple[str, str | None]:
        self.calls.append({"workflow": workflow, "instance_id": instance_id})
        return ("task", instance_id)

    def when_all(self, tasks: Any) -> tuple[str, list[Any]]:
        return ("when_all", list(tasks))


class _EventToken:
    """Opaque external-event token carrying the approval payload."""

    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def get_result(self) -> Any:
        return self._payload


class _ApprovalContext:
    """Races an (already-resolved) approval event against the timer."""

    instance_id = _RUN_ID

    def __init__(self, payload: Any) -> None:
        self.event = _EventToken(payload)
        self.timer = object()
        self.event_name: str | None = None

    def wait_for_external_event(self, name: str) -> _EventToken:
        self.event_name = name
        return self.event

    def create_timer(self, fire_at: Any) -> object:
        return self.timer

    def when_any(self, tasks: Any) -> tuple[str, list[Any]]:
        return ("when_any", list(tasks))


#: A minimal child graph for the sub-workflow example. The manager
#: only needs *a* compiled :class:`ExecutionGraph` to spawn the child;
#: its body is irrelevant to the spawn/merge contract under test.
_CHILD_DOC: Final[str] = """
apiVersion: custos.dev/v1
kind: Workflow
metadata:
  name: promote
  workspace: security
spec:
  steps:
    - id: greet
      let:
        msg: hi
"""


# ---------------------------------------------------------------------------
# Per-example expected outcome
# ---------------------------------------------------------------------------


#: The doc's "## Worked examples" section pins three examples in this
#: exact order; the exhaustiveness guard below pairs a documented
#: snippet with an asserted outcome 1:1, so adding a fourth worked
#: example without extending this table surfaces here as a failure.
_EXAMPLE_OUTCOMES: Final[dict[int, str]] = {
    1: "loop-fans-out",
    2: "approval-decision-binds",
    3: "sub-workflow-merges",
}


def _drive_to_yield(gen: Generator[Any, Any, Any]) -> Any:
    return next(gen)


def _drive_example_1() -> None:
    """Worked example 1 — ``forEach:`` loop fans out one child per item."""

    graph = _compile(_DOC_BLOCK_BODIES[1])
    node = graph.nodes[0]
    assert node.kind is StepKind.ACTIVITY
    assert node.primitive_handler is PrimitiveHandler.SUB_ORCHESTRATION

    ctx = _FanOutContext()
    items = [{"id": "alpha"}, {"id": "beta"}]
    gen = SubOrchestrationManager().run_loop(_step_ctx(ctx, {"targets": items}), graph, "scan-all")
    yielded = _drive_to_yield(gen)
    assert yielded[0] == "when_all"

    # Documented child instance ids, keyed on each item's ``id`` field.
    assert [c["workflow"] for c in ctx.calls] == [CHILD_STEP_WORKFLOW_NAME] * 2
    assert [c["instance_id"] for c in ctx.calls] == [
        f"{_RUN_ID}/scan-all/alpha",
        f"{_RUN_ID}/scan-all/beta",
    ]

    # The merged per-child outputs come back as an ordered list.
    with pytest.raises(StopIteration) as si:
        gen.send(
            [
                ChildStepResult.succeeded({"n": 1}).to_dict(),
                ChildStepResult.succeeded({"n": 2}).to_dict(),
            ]
        )
    assert si.value.value == [{"n": 1}, {"n": 2}]


def _drive_example_2() -> None:
    """Worked example 2 — ``approval:`` gate binds the decision payload."""

    graph = _compile(_DOC_BLOCK_BODIES[2])
    node = graph.nodes[0]
    assert node.kind is StepKind.APPROVAL
    assert node.primitive_handler is PrimitiveHandler.SUB_ORCHESTRATION

    decision = {"by": "alice", "decision": "approved"}
    ctx = _ApprovalContext(decision)
    gen = SubOrchestrationManager().run_approval(_step_ctx(ctx, {}), graph, "gate")
    yielded = _drive_to_yield(gen)
    assert yielded[0] == "when_any"
    assert ctx.event_name == f"{_RUN_ID}/gate/{APPROVAL_ITERATION_KEY}"

    # The event wins the race → its payload binds to the gate outputs.
    with pytest.raises(StopIteration) as si:
        gen.send(ctx.event)
    assert si.value.value == decision


def _drive_example_3() -> None:
    """Worked example 3 — ``workflow:`` step merges the child outputs."""

    graph = _compile(_DOC_BLOCK_BODIES[3])
    node = graph.nodes[0]
    assert node.kind is StepKind.WORKFLOW
    assert node.primitive_handler is PrimitiveHandler.SUB_ORCHESTRATION

    child_graph = _compile(_CHILD_DOC)
    ctx = _FanOutContext()
    gen = SubOrchestrationManager().run_sub_workflow(
        _step_ctx(ctx, {"who": "ada"}), graph, "promote", child_graph
    )
    yielded = _drive_to_yield(gen)
    assert yielded[0] == "when_all"

    # Single child spawned under the reserved ``workflow`` key.
    assert len(ctx.calls) == 1
    assert ctx.calls[0]["instance_id"] == f"{_RUN_ID}/promote/{WORKFLOW_ITERATION_KEY}"

    child_output = RunOutput(
        status="succeeded",
        outputs={"greet": {"msg": "hi ada"}},
    ).to_dict()
    with pytest.raises(StopIteration) as si:
        gen.send([child_output])
    assert si.value.value == {"greet": {"msg": "hi ada"}}


_EXAMPLE_DRIVERS: Final[dict[int, Any]] = {
    1: _drive_example_1,
    2: _drive_example_2,
    3: _drive_example_3,
}


@pytest.mark.parametrize(
    "block_idx,outcome_tag",
    sorted(_EXAMPLE_OUTCOMES.items()),
)
def test_doc_example_yaml_runs_through_the_manager(block_idx: int, outcome_tag: str) -> None:
    assert block_idx in _DOC_BLOCK_BODIES, (
        f"doc block #{block_idx} missing from docs/developers/workflow-sub-orchestration.md"
    )
    _EXAMPLE_DRIVERS[block_idx]()
    assert outcome_tag in set(_EXAMPLE_OUTCOMES.values())


def test_every_doc_yaml_block_has_an_asserted_outcome() -> None:
    """Exhaustiveness guard — no doc snippet runs un-exercised."""

    assert set(_DOC_BLOCK_BODIES) == set(_EXAMPLE_OUTCOMES), (
        "doc yaml-block count drifted from the test's expected outcome "
        f"table: blocks={sorted(_DOC_BLOCK_BODIES)} "
        f"expected_keys={sorted(_EXAMPLE_OUTCOMES)}"
    )


# ---------------------------------------------------------------------------
# Reflection-based contract pins — locked error taxonomy
# ---------------------------------------------------------------------------


#: Mirror of the doc's "## Locked error taxonomy" table. Every entry
#: MUST be a member of :data:`LOCKED_STEP_KINDS`.
_DOCUMENTED_SUB_ORCHESTRATION_KINDS: Final[frozenset[str]] = frozenset(
    {
        "step.loop_expansion_error",
        "step.sub_orchestration_spawn_error",
        "step.sub_workflow_failed",
        "step.approval_timeout",
    }
)


@pytest.mark.parametrize("kind", sorted(_DOCUMENTED_SUB_ORCHESTRATION_KINDS))
def test_documented_step_kind_is_locked(kind: str) -> None:
    assert kind in LOCKED_STEP_KINDS, (
        f"documented error kind {kind!r} is not a member of LOCKED_STEP_KINDS"
    )


def test_documented_sub_orchestration_kinds_are_a_subset_of_locked() -> None:
    """The four documented sub-orchestration kinds are all locked."""

    assert set(LOCKED_STEP_KINDS) >= _DOCUMENTED_SUB_ORCHESTRATION_KINDS
    # And they are all live ``StepCoordinatorError`` subclass kinds.
    subclass_kinds = {cls.KIND for cls in StepCoordinatorError.__subclasses__()}
    assert subclass_kinds >= _DOCUMENTED_SUB_ORCHESTRATION_KINDS, (
        "documented sub-orchestration error kinds drifted from the live "
        f"StepCoordinatorError subclasses: documented="
        f"{sorted(_DOCUMENTED_SUB_ORCHESTRATION_KINDS)} subclasses={sorted(subclass_kinds)}"
    )


# ---------------------------------------------------------------------------
# Reflection-based contract pins — child-id keys, config, handler enum
# ---------------------------------------------------------------------------


def test_documented_reserved_iteration_keys_match_constants() -> None:
    """Child-instance-id table pins the approval / workflow reserved keys."""

    assert APPROVAL_ITERATION_KEY == "approval"
    assert WORKFLOW_ITERATION_KEY == "workflow"


def test_documented_configuration_defaults_match_constants() -> None:
    """The Configuration table pins the live default knobs."""

    assert DEFAULT_MAX_FANOUT_WIDTH == 1000
    assert timedelta(hours=24) == DEFAULT_APPROVAL_TIMEOUT


def test_documented_primitive_handler_is_a_live_member() -> None:
    assert "SUB_ORCHESTRATION" in PrimitiveHandler.__members__
