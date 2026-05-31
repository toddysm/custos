"""Public surface of the ``custos_workflow.steps`` subpackage.

The Step Coordinator sub-module (WF-IMPL-047 … WF-IMPL-060, tracker
``WF-IMPL-000-STEP-COORDINATOR``) drives execution of one step at a
time within a workflow Run.

* WF-IMPL-047 lands the deterministic
  ``(run_id, step_id, attempt)`` idempotency triple that becomes
  the shared scheduling key for the Activity Runtime Manager
  (``ScheduleActivity``), the Connector Service lease key, and the
  audit-event correlation key per ``design.md`` §
  *Idempotency Model*.
* WF-IMPL-048 lands the public Step Coordinator error taxonomy —
  a frozen ``StepCoordinatorError`` hierarchy with locked
  ``step.*`` ``kind`` strings, mirrored on the
  :data:`LOCKED_STEP_KINDS` frozenset for the WF-IMPL-058 OTel
  counter and downstream audit consumers.

Later tasks (WF-IMPL-049 …) extend this package with the outbound
client Protocols, the per-kind ``StepHandler`` implementations,
and the top-level ``StepCoordinator`` dispatcher.
"""

from __future__ import annotations

from custos_workflow.steps.errors import (
    LOCKED_STEP_KINDS,
    ActivityScheduleError,
    ConnectorBindError,
    RetryBudgetExhaustedError,
    StepCoordinatorError,
    StepKindNotImplementedError,
    WithInputResolutionError,
)
from custos_workflow.steps.idempotency import (
    IdempotencyTriple,
    IdempotencyTripleError,
    derive_triple,
)
from custos_workflow.steps.let_step import LetStepHandler
from custos_workflow.steps.retry_driver import (
    LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
    FailNow,
    RetryDecision,
    RetryNow,
    Skip,
    build_retry_scheduled_event,
    decide,
    emit_retry_scheduled,
)
from custos_workflow.steps.with_inputs import WithInputResolver

__all__ = [
    "LIFECYCLE_KIND_STEP_RETRY_SCHEDULED",
    "LOCKED_STEP_KINDS",
    "ActivityScheduleError",
    "ConnectorBindError",
    "FailNow",
    "IdempotencyTriple",
    "IdempotencyTripleError",
    "LetStepHandler",
    "RetryBudgetExhaustedError",
    "RetryDecision",
    "RetryNow",
    "Skip",
    "StepCoordinatorError",
    "StepKindNotImplementedError",
    "WithInputResolutionError",
    "WithInputResolver",
    "build_retry_scheduled_event",
    "decide",
    "derive_triple",
    "emit_retry_scheduled",
]
