"""WF-IMPL-045 — replay-safety integration test.

Drives the :class:`FakeWorkflowRuntime` through **100** fresh
schedules of the same :class:`RunInput` and asserts byte-equal
dispatch history and byte-equal lifecycle event sequence under
``json.dumps(..., sort_keys=True)``.

The replay-determinism contract is the runtime side of the
WF-IMPL-042 reconciler hook: same compiled graph + same inputs =
same step dispatch order + same runtime history events. The
sibling per-orchestrator test
``tests/runs/test_orchestrator.py::TestReplayDeterminism`` covers
the same invariant at the unit layer; this integration test
exercises the wired :class:`RunController` path (catalog →
controller → bridge → fake runtime + orchestrator) end-to-end so
a regression in any one of those layers fails here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs import (
    RunOutput,
    StepExecutionContext,
    StepResult,
    StepSucceeded,
)
from tests.integration._harness import (
    WORKFLOW_VERSION_ID,
    WORKSPACE,
    make_harness,
)

# ---------------------------------------------------------------------------
# Document and recording handler
# ---------------------------------------------------------------------------


_FANOUT_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs:
        flag: {type: boolean, default: true}
      steps:
        - id: a
          let: {x: '${{ true }}'}
        - id: b
          let: {y: '${{ true }}'}
        - id: c
          needs: [a, b]
          let: {z: '${{ true }}'}
        - id: d
          needs: [c]
          let: {w: '${{ true }}'}
"""


@dataclass
class _RecordingHandler:
    """Records every dispatched step id."""

    dispatched: list[str] = field(default_factory=list)
    outputs_per_step: dict[str, dict[str, Any]] = field(default_factory=dict)

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        del ctx, graph
        self.dispatched.append(step_id)
        return StepSucceeded(outputs=self.outputs_per_step.get(step_id, {}))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _canonical(payload: Any) -> str:
    """Stable byte string for any JSON-serialisable payload."""

    return json.dumps(payload, sort_keys=True, default=str)


def _serialise_history(events: list[Any]) -> str:
    """``json.dumps(history, sort_keys=True)`` over the fake's history list.

    The fake's :class:`HistoryEvent` is a dataclass; we render it
    through a plain dict so the canonical form is the public
    ``kind`` / ``detail`` shape (no Python identity leaking in).
    """

    return _canonical([{"kind": e.kind, "detail": e.detail} for e in events])


def _serialise_lifecycle(events: list[Any]) -> str:
    """Canonical form of the in-memory lifecycle publisher's tape."""

    return _canonical([e.to_dict() for e in events])


# ---------------------------------------------------------------------------
# Replay-determinism integration test
# ---------------------------------------------------------------------------


_REPLAYS = 100


class TestReplayDeterminismThroughController:
    @pytest.mark.asyncio
    async def test_dispatch_history_and_lifecycle_byte_equal_across_100_replays(
        self,
    ) -> None:
        outputs_per_step = {
            "a": {"av": 1},
            "b": {"bv": 2},
            "c": {"cv": 3},
            "d": {"dv": 4},
        }

        baseline_dispatch: str | None = None
        baseline_runtime_history: str | None = None
        baseline_lifecycle: str | None = None
        baseline_output: dict[str, Any] | None = None

        for replay_idx in range(_REPLAYS):
            handler = _RecordingHandler(outputs_per_step=outputs_per_step)
            h = make_harness(doc_yaml=_FANOUT_DOC, handler=handler)
            # Each replay uses a fresh harness *and* a fixed
            # idempotency key so the derived ``run_id`` is byte-equal
            # across replays. Determinism here is the runtime/
            # orchestrator contract: same inputs + same compiled
            # graph + same ids = byte-equal dispatch order, history,
            # lifecycle, and output.
            idem = "client-replay"

            ref = await h.controller.start_run(
                workspace_id=WORKSPACE,
                workflow_version_id=WORKFLOW_VERSION_ID,
                inputs={"flag": True},
                idempotency_key=idem,
            )

            state = h.runtime.instance(str(ref.run_id))
            assert isinstance(state.output, RunOutput)

            dispatch_str = _canonical(handler.dispatched)
            history_str = _serialise_history(list(state.history))
            lifecycle_str = _serialise_lifecycle(list(h.publisher.events))
            output_dict = state.output.to_dict()

            if baseline_dispatch is None:
                baseline_dispatch = dispatch_str
                baseline_runtime_history = history_str
                baseline_lifecycle = lifecycle_str
                baseline_output = output_dict
                continue

            assert dispatch_str == baseline_dispatch, (
                f"replay {replay_idx} diverged on dispatch: "
                f"{dispatch_str!r} != {baseline_dispatch!r}"
            )
            assert history_str == baseline_runtime_history, (
                f"replay {replay_idx} diverged on runtime history"
            )
            assert lifecycle_str == baseline_lifecycle, (
                f"replay {replay_idx} diverged on lifecycle events"
            )
            assert output_dict == baseline_output, (
                f"replay {replay_idx} diverged on terminal RunOutput"
            )

    @pytest.mark.asyncio
    async def test_baseline_replay_dispatched_every_step_once(self) -> None:
        # Self-check: the recording handler picked up every step in
        # topological order (alphabetic frontier order on the two
        # zero-in-degree nodes). If this assertion ever drifts the
        # 100-replay test above will too; isolating it here makes
        # the failure message far more actionable.
        handler = _RecordingHandler(outputs_per_step={"a": {}, "b": {}, "c": {}, "d": {}})
        h = make_harness(doc_yaml=_FANOUT_DOC, handler=handler)

        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key="baseline",
        )
        assert handler.dispatched == ["a", "b", "c", "d"]
