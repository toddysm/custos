"""Public surface of the ``custos_workflow.steps`` subpackage.

The Step Coordinator sub-module (WF-IMPL-047 … WF-IMPL-060, tracker
``WF-IMPL-000-STEP-COORDINATOR``) drives execution of one step at a
time within a workflow Run. WF-IMPL-047 lands the foundation that
the rest of the sub-module builds on: the deterministic
``(run_id, step_id, attempt)`` idempotency triple that becomes the
shared scheduling key for the Activity Runtime Manager
(``ScheduleActivity``), the Connector Service lease key, and the
audit-event correlation key per
``design.md`` § *Idempotency Model*.

Later tasks (WF-IMPL-048 …) extend this package with the public
error taxonomy, the per-kind ``StepHandler`` implementations, and
the top-level ``StepCoordinator`` dispatcher.
"""

from __future__ import annotations

from custos_workflow.steps.idempotency import (
    IdempotencyTriple,
    IdempotencyTripleError,
    derive_triple,
)

__all__ = [
    "IdempotencyTriple",
    "IdempotencyTripleError",
    "derive_triple",
]
