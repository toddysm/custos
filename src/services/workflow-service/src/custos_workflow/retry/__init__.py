"""Effective retry-policy resolver (WF-IMPL-022).

This package owns the field-by-field overlay that turns the
document-level :class:`~custos_workflow.document.RetryPolicy`
optionals into a fully-resolved
:class:`~custos_workflow.graph.ResolvedRetryPolicy` ready for the
Step Coordinator's hot path. The Definition Compiler driver
(WF-IMPL-021) calls into this module so the compiled
:class:`~custos_workflow.graph.ExecutionGraph` carries flat,
millisecond-tagged policies — the runtime never re-walks the
overlay chain on each retry decision.

Public surface:

- :data:`PLATFORM_RETRY_DEFAULTS` — the platform-default
  :class:`~custos_workflow.document.RetryPolicy` (layer 4 in the
  precedence chain).
- :func:`resolve_step_retry` — overlay ``spec.defaults.retry``
  under ``step.retry`` under platform defaults to produce the
  step-level effective policy.
- :func:`resolve_arm_retry` — overlay the step-resolved policy
  under an ``on_error[].retry`` arm (with the inline
  ``maxAttempts:`` shorthand folded in) to produce the per-match
  effective policy.
- :exc:`RetryResolutionError` — the only failure mode (malformed
  ISO-8601 duration or conflicting shorthand vs structured
  ``maxAttempts:`` on the same arm).

Design.md § Retry Policy → § Precedence is the authoritative spec.
"""

from __future__ import annotations

from custos_workflow.retry.defaults import PLATFORM_RETRY_DEFAULTS
from custos_workflow.retry.resolve import (
    RetryResolutionError,
    resolve_arm_retry,
    resolve_step_retry,
)

__all__ = [
    "PLATFORM_RETRY_DEFAULTS",
    "RetryResolutionError",
    "resolve_arm_retry",
    "resolve_step_retry",
]
