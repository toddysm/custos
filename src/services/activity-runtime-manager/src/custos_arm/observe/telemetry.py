"""OpenTelemetry instrumentation for the Activity Runtime Manager (ARM-IMPL-020).

Exposes a tracer, a meter, the per-stage attempt spans, the per-stage
duration histogram, and the terminal-result counter that surfaces the
failure-mode → error-code/class mapping in metrics. Activity-lifecycle
audit events ride as OTel span events on the attempt span.

Design notes
------------

The module imports ``opentelemetry-api`` only. The API ships default
no-op providers, so ``custos_arm`` imports cleanly without an SDK
configured. Production deployments wire their own SDK (the ARM Helm
subchart attaches the OTel Collector per design § Observability); the
in-memory SDK is dev-only and exists only to drive the assertions in
``tests/test_observe.py``.

The Scheduler is the single instrumentation site. It opens one
``custos_arm.attempt`` span per attempt, nests one
``custos_arm.attempt.<stage>`` span per lifecycle stage
(``resolve`` / ``materialize`` / ``run`` / ``finalize``), and records
the terminal :class:`~custos_arm.result.ActivityResultEnvelope` into
:data:`ATTEMPTS_TOTAL` (labelled by the ``class`` metric label — the
envelope's ``class_`` value — and the synthesized error ``code``) so
every failure mode in the design's terminal-state table maps to its
documented code/class on the metric.

Metric / span names
-------------------

* ``custos_arm_attempt_stage_duration_ms`` — histogram, labels
  ``stage`` (``resolve`` / ``materialize`` / ``run`` / ``finalize``)
  and ``outcome`` (``success`` or ``error``).
* ``custos_arm_attempts_total`` — counter, labels ``class`` (one of
  ``success`` / ``retryable`` / ``permanent`` / ``cancelled``) and
  ``code`` (the synthesized error code, or ``none`` on success).
* Spans: ``custos_arm.attempt`` (per attempt, carrying the
  ``run_id`` / ``step_id`` / ``attempt`` / ``activity_ref``
  attributes) with child spans ``custos_arm.attempt.<stage>``.
* Span events: ``activity.scheduled`` (attempt opened) and
  ``activity.terminal`` (terminal envelope produced) — the
  activity-lifecycle audit trail surfaced to Observability/Audit.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, Status, StatusCode, Tracer

if TYPE_CHECKING:
    from custos_arm.result import ActivityResultEnvelope
    from custos_arm.scheduler.request import ScheduleRequest

_INSTRUMENTATION_NAME: Final[str] = "custos_arm"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

#: Outcome label on the stage histogram when the stage returns normally.
_SUCCESS: Final[str] = "success"
#: Outcome label on the stage histogram when the stage raises.
_ERROR: Final[str] = "error"
#: ``code`` label used on :data:`ATTEMPTS_TOTAL` for a successful attempt
#: (which carries no error envelope).
_NO_CODE: Final[str] = "none"


_tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)


STAGE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_arm_attempt_stage_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time of each Activity Runtime Manager attempt-lifecycle "
        "stage (resolve, materialize, run, finalize), labelled by stage and "
        "outcome (success or error)."
    ),
)


ATTEMPTS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_arm_attempts_total",
    description=(
        "Count of activity attempts that reached a terminal result, labelled "
        "by the orchestrator-facing result class (success, retryable, "
        "permanent, cancelled) and the synthesized error code (or 'none' on "
        "success). Surfaces the design's failure-mode -> code/class mapping."
    ),
)


# Canonical attempt-lifecycle stage labels. Centralised so spans, the
# histogram, and the tests all reference the same strings.
STAGE_RESOLVE: Final[str] = "resolve"
STAGE_MATERIALIZE: Final[str] = "materialize"
STAGE_RUN: Final[str] = "run"
STAGE_FINALIZE: Final[str] = "finalize"

#: Span-event names for the activity-lifecycle audit trail.
EVENT_SCHEDULED: Final[str] = "activity.scheduled"
EVENT_TERMINAL: Final[str] = "activity.terminal"


@contextmanager
def observe_attempt(request: ScheduleRequest) -> Iterator[Span]:
    """Open the per-attempt span and emit the ``activity.scheduled`` event.

    Yields the active ``custos_arm.attempt`` span so the Scheduler can
    attach the terminal lifecycle event before the span closes. The
    span carries the attempt's coordinates as attributes; on a
    propagated exception it is marked ``ERROR`` and the exception is
    recorded, then re-raised so the wrapper stays transparent.
    """
    with _tracer.start_as_current_span("custos_arm.attempt") as span:
        span.set_attribute("custos.run_id", request.step.run_id)
        span.set_attribute("custos.step_id", request.step.step_id)
        span.set_attribute("custos.attempt", request.step.attempt)
        span.set_attribute("custos.activity_ref", request.activity_ref)
        span.add_event(
            EVENT_SCHEDULED,
            attributes={
                "custos.run_id": request.step.run_id,
                "custos.step_id": request.step.step_id,
                "custos.attempt": request.step.attempt,
                "custos.activity_ref": request.activity_ref,
            },
        )
        try:
            yield span
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise


@contextmanager
def observe_stage(stage: str) -> Iterator[Span]:
    """Wrap an attempt-lifecycle stage with a span + duration sample.

    Produces span ``custos_arm.attempt.<stage>`` and records into
    :data:`STAGE_DURATION_MS` labelled by ``stage`` + outcome. On a
    raised exception the sample is labelled ``outcome=error``, the span
    is marked ``ERROR``, and the exception is re-raised.

    Catches :class:`Exception` (not :class:`BaseException`) so process-
    control unwinds (``KeyboardInterrupt`` / ``SystemExit`` /
    ``GeneratorExit``) propagate untouched and never skew the stage
    histogram.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(f"custos_arm.attempt.{stage}") as span:
        try:
            yield span
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            STAGE_DURATION_MS.record(elapsed_ms, {"stage": stage, "outcome": _ERROR})
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            STAGE_DURATION_MS.record(elapsed_ms, {"stage": stage, "outcome": _SUCCESS})


def record_result(envelope: ActivityResultEnvelope) -> None:
    """Record a terminal attempt result into metrics + the lifecycle audit.

    Bumps :data:`ATTEMPTS_TOTAL` once, labelled by the ``class`` metric
    label (the envelope's ``class_`` value) and the synthesized error
    ``code`` (``none`` on success), so every failure mode in the
    design's terminal-state table is grouped by its documented
    code/class. Also emits the ``activity.terminal``
    audit event on the current attempt span; when no attempt span is
    active (a replay/reconcile path outside :func:`observe_attempt`) the
    event is a no-op on the API's non-recording span while the counter
    still records.
    """
    class_label = envelope.class_.value
    code_label = envelope.error.code if envelope.error is not None else _NO_CODE
    ATTEMPTS_TOTAL.add(1, {"class": class_label, "code": code_label})
    span = trace.get_current_span()
    span.add_event(
        EVENT_TERMINAL,
        attributes={"custos.result_class": class_label, "custos.error_code": code_label},
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
