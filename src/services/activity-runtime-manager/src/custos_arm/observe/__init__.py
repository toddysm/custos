"""Observability for the Activity Runtime Manager (ARM-IMPL-020).

Re-exports the OpenTelemetry instrumentation the Scheduler wires across
the attempt lifecycle: per-stage spans, the stage-duration histogram,
the terminal-result counter (labelled by ``class`` / ``code``), and the
activity-lifecycle audit span events.
"""

from __future__ import annotations

from .telemetry import (
    ATTEMPTS_TOTAL,
    EVENT_SCHEDULED,
    EVENT_TERMINAL,
    STAGE_DURATION_MS,
    STAGE_FINALIZE,
    STAGE_MATERIALIZE,
    STAGE_RESOLVE,
    STAGE_RUN,
    observe_attempt,
    observe_stage,
    record_result,
)

__all__ = [
    "ATTEMPTS_TOTAL",
    "EVENT_SCHEDULED",
    "EVENT_TERMINAL",
    "STAGE_DURATION_MS",
    "STAGE_FINALIZE",
    "STAGE_MATERIALIZE",
    "STAGE_RESOLVE",
    "STAGE_RUN",
    "observe_attempt",
    "observe_stage",
    "record_result",
]
