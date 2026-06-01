"""Public surface of the ``custos_workflow.steps.sub_orchestration`` subpackage.

The Sub-Orchestration Manager sub-module (WF-IMPL-084 … WF-IMPL-098,
tracker ``WF-IMPL-000-SUB-ORCHESTRATION``) spawns and awaits child
Dapr Workflow instances for dynamic loops (``for:``), approval gates
(``approval:``), and sub-workflow invocation (``workflow:``) per
``design.md`` § *Sub-Orchestration Manager* (ADR-007).

* WF-IMPL-085 lands the deterministic child-instance-id contract —
  :func:`child_instance_id` and the :func:`iteration_key` derivation
  rule — that makes the child set reproducible under Dapr replay and
  child outputs addressable from ``steps.<stepId>.outputs``.
* WF-IMPL-088 lands the child orchestrator function —
  :func:`make_child_step_orchestrator` plus its
  :class:`ChildStepInput` / :class:`ChildStepResult` envelopes — that
  runs a single inner ``activity:`` / ``let:`` step body against an
  item-scoped context and returns its outputs to the parent.

Later tasks (WF-IMPL-089 …) extend this package with the per-kind
loop expansion + sub-workflow invocation paths that spawn and await
these children.
"""

from __future__ import annotations

from custos_workflow.steps.sub_orchestration.child import (
    CHILD_STEP_WORKFLOW_NAME,
    DEFAULT_LOOP_VAR,
    ChildStepInput,
    ChildStepResult,
    make_child_step_orchestrator,
)
from custos_workflow.steps.sub_orchestration.ids import (
    APPROVAL_ITERATION_KEY,
    CHILD_INSTANCE_ID_SEPARATOR,
    WORKFLOW_ITERATION_KEY,
    ChildInstanceIdError,
    child_instance_id,
    iteration_key,
)

__all__ = [
    "APPROVAL_ITERATION_KEY",
    "CHILD_INSTANCE_ID_SEPARATOR",
    "CHILD_STEP_WORKFLOW_NAME",
    "DEFAULT_LOOP_VAR",
    "WORKFLOW_ITERATION_KEY",
    "ChildInstanceIdError",
    "ChildStepInput",
    "ChildStepResult",
    "child_instance_id",
    "iteration_key",
    "make_child_step_orchestrator",
]
