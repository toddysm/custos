"""Replay-determinism property tests for the Sub-Orchestration Manager (WF-IMPL-095).

Dapr Workflow recovers a crashed/evicted orchestrator by **replaying**
the orchestrator function from the start against the recorded history.
For that replay to be sound the Sub-Orchestration Manager must
re-derive the *exact* same child instance id set on every entry — a
divergent id would make Dapr spawn a duplicate child (re-running side
effects) or fail with a non-determinism error mid-run.

The child id is ``<parentRunId>/<stepId>/<iterationKey>`` (WF-IMPL-085),
and every input to that derivation is a pure function of the parent's
durable state (``run_id``, ``step_id``, and the ``forEach`` items, which
themselves come from the deterministic CEL evaluator). So re-entering
``run_loop`` / ``run_sub_workflow`` / ``run_approval`` with the same
context yields **byte-equal** spawn requests. This module locks that
invariant under property-based fuzzing:

* ``run_loop`` — the spawned ``(workflow, instance_id)`` sequence (and
  the encoded child input bytes) are identical across repeated entries
  for the same item list, including ``where:``-filtered and
  index-keyed (identity-less) item lists.
* ``run_sub_workflow`` — the single child instance id is stable.
* ``run_approval`` — the gate address (external-event name) is stable.

The acceptance criterion ("byte-equal child id sets across repeated
orchestrator entries") is asserted both as an exact-sequence equality
*and* as a no-duplicate-instances property: a replay must never grow
the spawned-child set.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any, cast

from custos_cel import (
    BoolType,
    FixedClock,
    MapType,
    SchemaBindings,
    StringType,
    parse,
    type_check,
)
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from custos_workflow.document import ApprovalStep, LetStep, WorkflowStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.runs import RunId, StepExecutionContext
from custos_workflow.steps.sub_orchestration import SubOrchestrationManager

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "scan"

#: How many times each example re-enters the manager when proving
#: byte-equality across repeats. Mirrors the WF-IMPL-026 ``_BYTE_REPEAT``
#: convention — small-n flakiness cannot hide a stray ``set()`` ordering.
_REPLAY_REPEAT = 5

_ARRAY_OF_OBJECTS = {
    "type": "object",
    "properties": {"items": {"type": "array", "items": {"type": "object"}}},
    "required": ["items"],
}


# ---------------------------------------------------------------------------
# Graph builders (self-contained mirror of the loop/approval test harnesses)
# ---------------------------------------------------------------------------


def _for_each_call_site(cel: str) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), SchemaBindings(inputs=_ARRAY_OF_OBJECTS)),
        kind=CallSiteKind.FOR_EACH,
        document_path="spec.steps[0].forEach",
    )


def _where_call_site(cel: str) -> TypedCallSite:
    bindings = SchemaBindings(
        inputs=_ARRAY_OF_OBJECTS,
        let={"item": MapType(key=StringType(), value=BoolType())},
    )
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), bindings),
        kind=CallSiteKind.WHERE,
        document_path="spec.steps[0].where",
    )


def _loop_graph(*, where_cel: str | None = None) -> ExecutionGraph:
    sites: dict[str, TypedCallSite] = {"forEach": _for_each_call_site("inputs.items")}
    if where_cel is not None:
        sites["where"] = _where_call_site(where_cel)
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites=sites,
        step_source=LetStep.model_validate({"id": _STEP_ID, "let": {"noop": 0}}),
    )
    return _wrap(node)


def _approval_graph() -> ExecutionGraph:
    node = ExecutionNode(
        step_id="gate",
        kind=StepKind.APPROVAL,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=ApprovalStep.model_validate(
            {"id": "gate", "approval": {"approvers": ["alice"], "timeout": "PT1H"}}
        ),
    )
    return _wrap(node)


def _workflow_graph() -> ExecutionGraph:
    node = ExecutionNode(
        step_id="call",
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WorkflowStep.model_validate(
            {"id": "call", "workflow": "11111111-1111-4111-8111-111111111111"}
        ),
    )
    return _wrap(node)


def _wrap(node: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=(node.step_id,),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


# ---------------------------------------------------------------------------
# Spawn-recording stub context
# ---------------------------------------------------------------------------


class _Token:
    def __init__(self, instance_id: str | None) -> None:
        self.instance_id = instance_id

    def get_result(self) -> Any:  # pragma: no cover - approval timer path only
        return None


class _RecordingContext:
    """Records every spawn / gate request without executing anything."""

    instance_id = "parent-1"

    def __init__(self) -> None:
        #: Ordered ``(workflow, instance_id, input-json)`` per spawn.
        self.spawns: list[tuple[Any, str | None, str]] = []
        self.event_name: str | None = None

    def call_child_workflow(
        self,
        workflow: Any,
        *,
        input: Any = None,
        instance_id: str | None = None,
    ) -> _Token:
        self.spawns.append((workflow, instance_id, json.dumps(input, sort_keys=True)))
        return _Token(instance_id)

    def when_all(self, tasks: Any) -> tuple[str, list[Any]]:
        return ("when_all", list(tasks))

    def wait_for_external_event(self, name: str) -> _Token:
        self.event_name = name
        return _Token(None)

    def create_timer(self, fire_at: Any) -> _Token:
        return _Token(None)

    def when_any(self, tasks: Any) -> tuple[str, list[Any]]:
        return ("when_any", list(tasks))


def _ctx(*, inputs: dict[str, Any], workflow_context: Any) -> StepExecutionContext:
    return StepExecutionContext(
        run_id=cast(RunId, "parent-1"),
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        inputs=inputs,
        workflow_context=workflow_context,
        outputs={},
        clock=FixedClock(_NOW),
    )


def _drive_to_yield(gen: Generator[Any, Any, Any]) -> None:
    """Advance a manager generator to its first ``when_*`` yield."""
    next(gen)


def _loop_spawn_signature(
    items: list[Any], *, where_cel: str | None = None
) -> list[tuple[Any, str | None, str]]:
    """Re-enter ``run_loop`` once and return its recorded spawn list."""
    ctx = _RecordingContext()
    graph = _loop_graph(where_cel=where_cel)
    step_ctx = _ctx(inputs={"items": items}, workflow_context=ctx)
    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    # Empty / fully-filtered loop: no ``when_all`` yield, no spawns.
    with contextlib.suppress(StopIteration):
        _drive_to_yield(gen)
    return ctx.spawns


# ---------------------------------------------------------------------------
# Property: object-keyed loops re-derive a byte-equal spawn set
# ---------------------------------------------------------------------------

# Each item carries a unique ``id`` so ``iteration_key`` uses the stable
# identity path (no duplicate-key rejection).
_unique_id_items = st.lists(
    st.integers(min_value=0, max_value=10_000),
    min_size=0,
    max_size=12,
    unique=True,
).map(lambda ids: [{"id": f"item-{n}", "n": n} for n in ids])


@settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow])
@given(items=_unique_id_items)
def test_loop_spawn_set_is_byte_equal_across_replays(items: list[Any]) -> None:
    baseline = _loop_spawn_signature(items)

    # The instance ids are the deterministic ``<run>/<step>/<key>`` set.
    instance_ids = [spawn[1] for spawn in baseline]
    assert instance_ids == [f"parent-1/scan/item-{item['n']}" for item in items]
    # No duplicate child instances within a single expansion.
    assert len(instance_ids) == len(set(instance_ids))

    # Every replay reproduces the identical spawn list — same workflow,
    # same instance ids, byte-identical encoded child inputs.
    for _ in range(_REPLAY_REPEAT):
        assert _loop_spawn_signature(items) == baseline


# ---------------------------------------------------------------------------
# Property: identity-less items fall back to a stable index key
# ---------------------------------------------------------------------------

_indexless_items = st.lists(
    st.integers(min_value=0, max_value=10_000),
    min_size=0,
    max_size=12,
).map(lambda ns: [{"n": n} for n in ns])


@settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow])
@given(items=_indexless_items)
def test_loop_index_fallback_keys_are_stable_across_replays(items: list[Any]) -> None:
    baseline = _loop_spawn_signature(items)

    # Items without ``id``/``key`` use their list index — unique within
    # one expansion, so no duplicate-key rejection and a stable set.
    instance_ids = [spawn[1] for spawn in baseline]
    assert instance_ids == [f"parent-1/scan/{i}" for i in range(len(items))]
    assert len(instance_ids) == len(set(instance_ids))

    for _ in range(_REPLAY_REPEAT):
        assert _loop_spawn_signature(items) == baseline


# ---------------------------------------------------------------------------
# Property: where:-filtered loops re-derive the same surviving set
# ---------------------------------------------------------------------------

_keep_flag_items = st.lists(
    st.tuples(st.integers(min_value=0, max_value=10_000), st.booleans()),
    min_size=0,
    max_size=12,
    unique_by=lambda t: t[0],
).map(lambda rows: [{"id": f"item-{n}", "keep": keep} for n, keep in rows])


@settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow])
@given(items=_keep_flag_items)
def test_where_filtered_loop_is_byte_equal_across_replays(items: list[Any]) -> None:
    baseline = _loop_spawn_signature(items, where_cel='let.item["keep"]')

    kept = [item for item in items if item["keep"]]
    instance_ids = [spawn[1] for spawn in baseline]
    assert instance_ids == [f"parent-1/scan/{item['id']}" for item in kept]
    assert len(instance_ids) == len(set(instance_ids))

    for _ in range(_REPLAY_REPEAT):
        assert _loop_spawn_signature(items, where_cel='let.item["keep"]') == baseline


# ---------------------------------------------------------------------------
# Sub-workflow + approval: single reserved-key child id is stable
# ---------------------------------------------------------------------------


def test_sub_workflow_child_id_is_stable_across_replays() -> None:
    graph = _workflow_graph()
    child_graph = _approval_graph()  # any byte-stable graph works as the child body

    def _signature() -> list[tuple[Any, str | None, str]]:
        ctx = _RecordingContext()
        step_ctx = _ctx(inputs={}, workflow_context=ctx)
        gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, "call", child_graph)
        _drive_to_yield(gen)
        return ctx.spawns

    baseline = _signature()
    assert [spawn[1] for spawn in baseline] == ["parent-1/call/workflow"]
    for _ in range(_REPLAY_REPEAT):
        assert _signature() == baseline


def test_approval_gate_address_is_stable_across_replays() -> None:
    graph = _approval_graph()

    def _gate_address() -> str | None:
        ctx = _RecordingContext()
        step_ctx = _ctx(inputs={}, workflow_context=ctx)
        gen = SubOrchestrationManager().run_approval(step_ctx, graph, "gate")
        _drive_to_yield(gen)
        return ctx.event_name

    baseline = _gate_address()
    assert baseline == "parent-1/gate/approval"
    for _ in range(_REPLAY_REPEAT):
        assert _gate_address() == baseline
