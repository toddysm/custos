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

Later tasks (WF-IMPL-086 …) extend this package with the
sub-orchestration error taxonomy, the child orchestrator function,
and the per-kind expansion + invocation paths.
"""

from __future__ import annotations

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
    "WORKFLOW_ITERATION_KEY",
    "ChildInstanceIdError",
    "child_instance_id",
    "iteration_key",
]
