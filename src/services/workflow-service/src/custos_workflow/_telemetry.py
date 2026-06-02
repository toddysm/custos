"""OpenTelemetry instrumentation for the Definition Compiler (WF-IMPL-027).

Mirrors :mod:`custos_cel._telemetry` (WF-IMPL-011, #186). Exposes a
single tracer + meter, one duration histogram per pipeline stage plus
a total-duration histogram, and one per-``kind`` error counter — all
keyed to the locked compile-time error taxonomy from WF-IMPL-024
(:mod:`custos_workflow.errors`).

Design notes
------------
This module imports ``opentelemetry-api`` only. The API ships default
no-op providers, so consumers without an SDK installed can import
:mod:`custos_workflow` safely without configuring telemetry first.
Production deployments configure their own SDK; the in-memory SDK is
dev-only and exists exclusively to drive the assertions in
``tests/test_observability.py``.

The instrumentation is intentionally narrow: only the public
:func:`custos_workflow.compiler.compile` entry point and the five
top-level pipeline stages emit spans and metrics. Internal helpers
(``_type_check_all``, ``_build_node``) deliberately stay
uninstrumented so a single user-visible compile maps one-to-one to
exactly one total-duration sample and one sample per stage that ran,
matching the observability conventions the Workflow Service Step
Coordinator expects.

Metric / span names follow the issue scope verbatim:

* ``custos_workflow_compile_parse_duration_ms`` — histogram, labels
  ``outcome=success|parse_error``.
* ``custos_workflow_compile_topology_duration_ms`` — histogram,
  labels ``outcome=success|topology_error``. The topology stage
  emits two samples per compile (one for the call-site / step-ref
  pre-flight, one for the explicit + implicit edge + cycle +
  sort pass); a regression in either trips the histogram.
* ``custos_workflow_compile_type_check_duration_ms`` — histogram,
  labels ``outcome=success|type_error``.
* ``custos_workflow_compile_retry_policy_duration_ms`` — histogram,
  labels ``outcome=success|retry_policy_error``.
* ``custos_workflow_compile_total_duration_ms`` — histogram,
  labels ``outcome=success|parse_error|type_error|topology_error|
  retry_policy_error|bindings_error``. One sample per compile.
* ``custos_workflow_compile_errors_total`` — counter, labels
  ``kind`` (one of the locked ``compile.*`` strings from
  :mod:`custos_workflow.errors` plus ``compile.bindings_error``
  from :class:`custos_workflow.compiler.BindingsCompileError`).
  Bumped exactly once per failed compile, by the outer total
  wrapper — per-stage wrappers do not double-count.
* Spans: ``custos_workflow.compile`` (outer),
  ``custos_workflow.compile.parse``,
  ``custos_workflow.compile.topology``,
  ``custos_workflow.compile.type_check``,
  ``custos_workflow.compile.retry_policy``.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, Status, StatusCode, Tracer

from custos_workflow.runs.errors import LOCKED_RUN_KINDS
from custos_workflow.steps.errors import LOCKED_STEP_KINDS

_INSTRUMENTATION_NAME: Final[str] = "custos_workflow"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

# Outcome label used on the duration histograms when the wrapped
# stage returns normally.
_SUCCESS: Final[str] = "success"

# Outcome label used for any exception outside the
# :class:`CompileError` taxonomy. Public APIs may still raise
# built-in exceptions (for example invalid input or
# configuration errors), and this catch-all labels them
# ``internal_error`` so histogram totals remain consistent with
# the call count.
_INTERNAL_ERROR: Final[str] = "internal_error"


_tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)


PARSE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_compile_parse_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in the call-site collection stage of "
        "custos_workflow.compile(), labelled by outcome (success or parse_error)."
    ),
)

TOPOLOGY_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_compile_topology_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in the topology stage of "
        "custos_workflow.compile(), labelled by outcome (success or "
        "topology_error). Two samples are recorded per compile: one "
        "for the step-ref pre-flight and one for the explicit + "
        "implicit edge + cycle + sort pass."
    ),
)

TYPE_CHECK_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_compile_type_check_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in the type-check stage of "
        "custos_workflow.compile(), labelled by outcome (success or type_error)."
    ),
)

RETRY_POLICY_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_compile_retry_policy_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in the retry-policy resolution stage of "
        "custos_workflow.compile(), labelled by outcome (success or retry_policy_error)."
    ),
)

TOTAL_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_compile_total_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in custos_workflow.compile() end-to-end, "
        "labelled by outcome (success or one of the locked compile.* error kinds)."
    ),
)

ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_compile_errors_total",
    description=(
        "Count of custos_workflow.compile() failures, labelled by the "
        "structured error 'kind' from custos_workflow.errors (WF-IMPL-024 taxonomy) "
        "plus 'compile.bindings_error' from BindingsCompileError."
    ),
)


# Per-stage outcome maps. Each maps the stage's expected
# :class:`CompileError` ``kind`` string to the histogram's
# ``outcome`` label. A stage histogram never sees a ``kind`` from
# outside its mapping because the surrounding compile pipeline
# only raises one taxonomy class per stage — anything else falls
# through to ``internal_error``.
_PARSE_OUTCOMES: Final[Mapping[str, str]] = {
    "compile.parse_error": "parse_error",
}
_TOPOLOGY_OUTCOMES: Final[Mapping[str, str]] = {
    "compile.topology_error": "topology_error",
}
_TYPE_CHECK_OUTCOMES: Final[Mapping[str, str]] = {
    "compile.type_error": "type_error",
}
_RETRY_POLICY_OUTCOMES: Final[Mapping[str, str]] = {
    "compile.retry_policy_error": "retry_policy_error",
}
_TOTAL_OUTCOMES: Final[Mapping[str, str]] = {
    "compile.parse_error": "parse_error",
    "compile.type_error": "type_error",
    "compile.topology_error": "topology_error",
    "compile.retry_policy_error": "retry_policy_error",
    "compile.bindings_error": "bindings_error",
}


def _outcome_for(exc: BaseException, outcomes: Mapping[str, str]) -> str:
    """Resolve the duration-histogram ``outcome`` label for ``exc``.

    Discovers the structured error ``kind`` reflectively (any
    exception exposing a ``kind: str`` attribute — both
    :class:`~custos_workflow.errors.CompileError` and
    :class:`~custos_workflow.runs.errors.RunControllerError`
    qualify) and looks it up in the per-call-site ``outcomes``
    mapping. Anything not in the mapping (or anything lacking a
    ``kind`` attribute entirely) falls back to ``"internal_error"``
    so histogram totals stay consistent with the call count even
    when an unexpected exception escapes.
    """
    kind = getattr(exc, "kind", None)
    if isinstance(kind, str):
        label = outcomes.get(kind)
        if label is not None:
            return label
    return _INTERNAL_ERROR


@contextmanager
def instrument(
    span_name: str,
    histogram: Histogram,
    outcomes: Mapping[str, str],
    *,
    count_errors: bool = True,
    error_counter: Counter | None = None,
) -> Iterator[Span]:
    """Wrap a compile stage (or the whole pipeline) with span + sample + counter.

    The context manager yields the active span so the caller can
    attach operation-specific attributes (step count, edge count,
    call-site count, etc.) before the wrapped work runs. On normal
    completion the duration histogram receives a sample with
    ``outcome=success``; on a raised :class:`CompileError` it
    receives a sample labelled by ``outcomes[exc.kind]`` and, when
    ``count_errors`` is true, the
    ``custos_workflow_compile_errors_total`` counter is bumped by
    one with the error's stable ``kind`` string. The exception is
    always re-raised so the wrapper is transparent to callers.

    Args:
        span_name: Dotted span name (``"custos_workflow.compile"``,
          ``"custos_workflow.compile.parse"``, etc.) — becomes the
          OTel span's display name.
        histogram: The duration histogram to record into. One of
          the module-level instruments (e.g.
          :data:`PARSE_DURATION_MS`, :data:`TOTAL_DURATION_MS`).
        outcomes: Per-call-site mapping from a
          :class:`CompileError.kind` string to the ``outcome``
          label that histogram understands. Anything not in the
          mapping falls back to ``"internal_error"``.
        count_errors: When true (default), bump
          ``error_counter`` (or :data:`ERRORS_TOTAL` when none is
          supplied, for backwards compatibility with the WF-IMPL-027
          compile wrappers) on any raised exception whose ``kind``
          attribute is a string. Per-stage compile wrappers pass
          ``False`` so a single error does not double-count when
          it propagates through both a stage wrapper and the outer
          total wrapper; only the total wrapper bumps the counter.
          The run-side wrappers (:func:`observe_run_start` and
          friends) also pass ``False`` — the WF-IMPL-044 instrument
          set deliberately omits a per-error counter and tracks the
          outcome label on the duration histogram instead.
        error_counter: When supplied and ``count_errors`` is true,
          the counter that receives the ``+1`` bump (labelled by
          the exception's ``kind``). Defaults to
          :data:`ERRORS_TOTAL` to preserve the existing
          compile-pipeline semantics.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(span_name) as span:
        try:
            yield span
        except Exception as exc:
            # Deliberately catch ``Exception`` (not ``BaseException``)
            # so process-control unwinds — ``KeyboardInterrupt``,
            # ``SystemExit``, ``GeneratorExit`` — propagate through
            # the wrapper untouched and are never recorded into the
            # duration histograms or the
            # ``custos_workflow_compile_errors_total`` counter.
            # Those events are not application errors and
            # mis-labelling them as such would skew SLO dashboards.
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            outcome = _outcome_for(exc, outcomes)
            histogram.record(elapsed_ms, {"outcome": outcome})
            if count_errors:
                kind = getattr(exc, "kind", None)
                if isinstance(kind, str):
                    (error_counter or ERRORS_TOTAL).add(1, {"kind": kind})
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            histogram.record(elapsed_ms, {"outcome": _SUCCESS})


def observe_compile_total() -> AbstractContextManager[Span]:
    """Context manager wrapping the whole :func:`compile` pipeline.

    Records into :data:`TOTAL_DURATION_MS` and is the *only* layer
    that bumps :data:`ERRORS_TOTAL`. The yielded span carries the
    three :func:`compile`-wide attributes (``step_count``,
    ``edge_count``, ``call_site_count``).
    """
    return instrument(
        "custos_workflow.compile",
        TOTAL_DURATION_MS,
        _TOTAL_OUTCOMES,
        count_errors=True,
    )


def observe_compile_parse() -> AbstractContextManager[Span]:
    """Context manager wrapping the call-site collection stage."""
    return instrument(
        "custos_workflow.compile.parse",
        PARSE_DURATION_MS,
        _PARSE_OUTCOMES,
        count_errors=False,
    )


def observe_compile_topology() -> AbstractContextManager[Span]:
    """Context manager wrapping a topology pass.

    The compile pipeline runs two topology passes (pre-flight of
    ``${{ steps.X.outputs.* }}`` references and the
    explicit+implicit edge + cycle + sort assembly); both are
    instrumented with this wrapper, so each compile emits two
    samples into :data:`TOPOLOGY_DURATION_MS`.
    """
    return instrument(
        "custos_workflow.compile.topology",
        TOPOLOGY_DURATION_MS,
        _TOPOLOGY_OUTCOMES,
        count_errors=False,
    )


def observe_compile_type_check() -> AbstractContextManager[Span]:
    """Context manager wrapping the type-check stage."""
    return instrument(
        "custos_workflow.compile.type_check",
        TYPE_CHECK_DURATION_MS,
        _TYPE_CHECK_OUTCOMES,
        count_errors=False,
    )


def observe_compile_retry_policy() -> AbstractContextManager[Span]:
    """Context manager wrapping the retry-policy resolution stage."""
    return instrument(
        "custos_workflow.compile.retry_policy",
        RETRY_POLICY_DURATION_MS,
        _RETRY_POLICY_OUTCOMES,
        count_errors=False,
    )


# ---------------------------------------------------------------------------
# WF-IMPL-044: Run Controller observability hooks
# ---------------------------------------------------------------------------
#
# Spans, histograms, and counters that make every Run Controller
# lifecycle operation observable end-to-end without scraping
# logs. The ``outcome`` label set is the
# :data:`~custos_workflow.runs.errors.LOCKED_RUN_KINDS` taxonomy
# plus the ``ok`` success sentinel — the build-time assertion
# below pins that contract so adding a new
# :class:`~custos_workflow.runs.errors.RunControllerError`
# subclass without updating ``_RUN_LIFECYCLE_OUTCOMES`` fails
# import time.

RUN_LIFECYCLE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_run_lifecycle_call_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in each public RunController lifecycle "
        "method, labelled by ``operation`` ∈ {start,cancel,pause,"
        "resume,get,list,raise_external_event} (replay is also "
        "recorded for the orchestrator replay path) and ``outcome`` "
        "∈ {ok,not_found,state_conflict,state_corrupt,"
        "runtime_unavailable,internal_error}."
    ),
)

RUN_STATUS_TRANSITIONS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_run_status_transitions_total",
    description=(
        "Count of successful Run.status transitions persisted by the "
        "Run Controller, labelled by ``from`` and ``to`` status "
        "values. One increment per successful "
        "``RunStore.update_run_status`` call; failed transitions "
        "(illegal source state, store unavailable, etc.) do not bump "
        "the counter."
    ),
)

WORKFLOW_EVENTS_EMITTED_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_workflow_events_emitted_total",
    description=(
        "Count of workflow lifecycle events emitted by the Run "
        "Controller through the LifecycleEventPublisher, labelled by "
        "the event ``kind`` ∈ {workflow.started,workflow.cancelled,"
        "workflow.paused,workflow.resumed}. Bumped exactly once per "
        "successful ``publisher.publish`` call; publisher failures "
        "(which the controller absorbs to preserve the "
        "persisted-state→event ordering invariant) do not increment "
        "the counter."
    ),
)

#: ``outcome`` label is the locked
#: :data:`~custos_workflow.runs.errors.LOCKED_RUN_KINDS` taxonomy
#: with the ``run.`` prefix stripped — keeps the histogram label
#: short while preserving the 1:1 mapping back to the error class.
_RUN_LIFECYCLE_OUTCOMES: Final[Mapping[str, str]] = {
    "run.not_found": "not_found",
    "run.state_conflict": "state_conflict",
    "run.state_corrupt": "state_corrupt",
    "run.runtime_unavailable": "runtime_unavailable",
}

#: Closed set of ``workflow.*`` event kinds the Run Controller
#: emits. Pinned so the WF-IMPL-041 publisher kinds stay
#: synchronised with the observability counter; adding a new
#: ``LIFECYCLE_KIND_WORKFLOW_*`` constant without extending this
#: set fails the assertion in :func:`record_workflow_event_emitted`.
_WORKFLOW_EVENT_KINDS: Final[frozenset[str]] = frozenset(
    {
        "workflow.started",
        "workflow.cancelled",
        "workflow.paused",
        "workflow.resumed",
    }
)

#: Success sentinel for the ``outcome`` label.
_RUN_OK: Final[str] = "ok"

# Build-time check (per acceptance criteria of WF-IMPL-044): the
# outcome map keys MUST equal the locked Run Controller kind set.
# Adding or removing a subclass of
# :class:`~custos_workflow.runs.errors.RunControllerError` without
# updating ``_RUN_LIFECYCLE_OUTCOMES`` fails import — a noisy
# fail-fast keeps the observability contract honest.
assert frozenset(_RUN_LIFECYCLE_OUTCOMES) == LOCKED_RUN_KINDS, (
    "_RUN_LIFECYCLE_OUTCOMES keys must equal LOCKED_RUN_KINDS; "
    f"map={sorted(_RUN_LIFECYCLE_OUTCOMES)} "
    f"kinds={sorted(LOCKED_RUN_KINDS)}"
)


@contextmanager
def _instrument_run(span_name: str, operation: str) -> Iterator[Span]:
    """Span + histogram wrapper for Run Controller lifecycle calls.

    Kept distinct from :func:`instrument` because the run-side
    duration histogram carries an ``operation`` label in addition
    to ``outcome``; threading that through the generic helper
    would muddy the compile-side wrappers. Errors are mapped via
    the shared :func:`_outcome_for` (duck-typed on ``.kind``);
    anything not in :data:`_RUN_LIFECYCLE_OUTCOMES` falls back to
    ``internal_error`` so histogram totals stay consistent with
    the call count.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(span_name) as span:
        try:
            yield span
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            outcome = _outcome_for(exc, _RUN_LIFECYCLE_OUTCOMES)
            RUN_LIFECYCLE_DURATION_MS.record(
                elapsed_ms,
                {"operation": operation, "outcome": outcome},
            )
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            RUN_LIFECYCLE_DURATION_MS.record(
                elapsed_ms,
                {"operation": operation, "outcome": _RUN_OK},
            )


def observe_run_start() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.start_run`."""
    return _instrument_run("custos_workflow.run.start", "start")


def observe_run_cancel() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.cancel_run`."""
    return _instrument_run("custos_workflow.run.cancel", "cancel")


def observe_run_pause() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.pause_run`."""
    return _instrument_run("custos_workflow.run.pause", "pause")


def observe_run_resume() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.resume_run`."""
    return _instrument_run("custos_workflow.run.resume", "resume")


def observe_run_get() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.get_run`."""
    return _instrument_run("custos_workflow.run.get", "get")


def observe_run_list() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.list_runs`.

    Span name is ``custos_workflow.run.list``; the
    implementation plan lists ``replay`` instead of ``list`` in
    the span set (because ``replay`` is the orchestrator's
    replay path), but every public RunController method gets a
    span here so the ``operation`` label is fully covered.
    """
    return _instrument_run("custos_workflow.run.list", "list")


def observe_run_raise_external_event() -> AbstractContextManager[Span]:
    """Context manager wrapping :meth:`RunController.raise_external_event`.

    Span name is ``custos_workflow.run.raise_external_event``.
    The bridge is a write-side lifecycle entry point (Trigger
    Service inbound RPC) so it joins the same WF-IMPL-044
    ``operation`` x ``outcome`` matrix as ``start_run`` /
    ``cancel_run`` — the histogram description above pins
    ``raise_external_event`` as a permitted ``operation`` value.
    """
    return _instrument_run("custos_workflow.run.raise_external_event", "raise_external_event")


def observe_run_replay() -> AbstractContextManager[Span]:
    """Context manager wrapping the workflow orchestrator's replay hook.

    Records into :data:`RUN_LIFECYCLE_DURATION_MS` with
    ``operation=replay`` so dashboards can compare replay
    latency against the user-facing lifecycle operations. The
    underlying ReplayReconciler hook is documented as "MUST NOT
    raise"; if a reconciler ever does, the outcome label is
    resolved via the shared :func:`_outcome_for` so unexpected
    failures still show up on the histogram.
    """
    return _instrument_run("custos_workflow.run.replay", "replay")


def record_run_status_transition(from_status: str, to_status: str) -> None:
    """Bump :data:`RUN_STATUS_TRANSITIONS_TOTAL` for a persisted transition.

    Called by the Run Controller immediately after a successful
    :meth:`RunStore.update_run_status` so failed transitions
    (illegal source state, store unavailable, etc.) do not
    increment the counter. The label values are the raw
    :class:`~custos_workflow.runs.model.RunStatus` strings.
    """
    RUN_STATUS_TRANSITIONS_TOTAL.add(
        1,
        {"from": from_status, "to": to_status},
    )


def record_workflow_event_emitted(kind: str) -> None:
    """Bump :data:`WORKFLOW_EVENTS_EMITTED_TOTAL` for a published event.

    Called by the Run Controller after the
    :class:`~custos_workflow.runs.controller.LifecycleEventPublisher`
    returns from a successful ``publish``; publisher failures
    (which the controller deliberately absorbs to preserve the
    persisted-state→event ordering invariant) do not bump the
    counter. Unknown kinds raise :class:`ValueError` so a
    typo or unregistered lifecycle constant fails loudly instead
    of silently dropping the sample.
    """
    if kind not in _WORKFLOW_EVENT_KINDS:
        raise ValueError(
            f"unknown workflow event kind {kind!r}; expected one of {sorted(_WORKFLOW_EVENT_KINDS)}"
        )
    WORKFLOW_EVENTS_EMITTED_TOTAL.add(1, {"kind": kind})


# ---------------------------------------------------------------------------
# WF-IMPL-058: Step Coordinator observability hooks
# ---------------------------------------------------------------------------
#
# Spans, histograms, and counters that make every Step Coordinator
# dispatch observable end-to-end. Mirrors the WF-IMPL-044 Run
# Controller pattern: per-call-site context-manager wrappers
# (:func:`observe_step_execute`, :func:`observe_step_bind_connectors`,
# :func:`observe_step_schedule_activity`,
# :func:`observe_step_retry_decision`) record into a duration
# histogram and emit one span each; explicit recorder functions
# (:func:`record_step_attempt`, :func:`record_step_error`) bump
# the per-attempt and per-error counters at the call sites that
# already know the right ``final_class`` / ``kind`` label.
#
# Label sets
# ----------
# * ``step_kind`` — the
#   :class:`~custos_workflow.graph.model.StepKind` enum value
#   (the ``"activity"`` / ``"let"`` / ``"workflow"`` / ``"wait"``
#   wire strings). ``wait`` is dispatched by the Run Controller
#   inline and never reaches the Step Coordinator, so practical
#   values are ``activity`` / ``let`` / ``workflow``.
# * ``outcome`` — ``ok`` on success, one of the
#   :data:`~custos_workflow.steps.errors.LOCKED_STEP_KINDS`
#   suffixes (the ``step.`` prefix stripped — keeps the histogram
#   label short, mirrors the WF-IMPL-044 convention) on a
#   :class:`StepCoordinatorError`, or ``internal_error`` for any
#   other exception that escapes the wrapper.
# * ``class`` — the
#   :data:`~custos_workflow.clients.activity_runtime.ACTIVITY_RESULT_CLASSES`
#   value carried on the schedule envelope. The
#   ``custos_workflow.step.schedule_activity`` span records this
#   alongside ``step_kind`` so dashboards can pivot retryable vs
#   permanent failures without re-parsing the envelope.
# * ``final_class`` — same closed set as ``class``; recorded by
#   :func:`record_step_attempt` once per attempt so the counter
#   surfaces both the attempt count *and* its outcome class with
#   a single instrument.
# * ``kind`` — one of
#   :data:`~custos_workflow.steps.errors.LOCKED_STEP_KINDS`. The
#   build-time assertion below pins the
#   :data:`STEP_ERRORS_TOTAL` label set to that frozenset so
#   adding a :class:`StepCoordinatorError` subclass without
#   updating the locked set fails loudly.

STEP_EXECUTE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_step_execute_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in StepCoordinator.execute() per dispatch, "
        "labelled by ``step_kind`` (StepKind value) and ``outcome`` "
        "(``ok``, one of the LOCKED_STEP_KINDS suffixes, or "
        "``internal_error``). One sample per dispatch."
    ),
)

ACTIVITY_SCHEDULE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_activity_schedule_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in a single "
        "ActivityRuntimeClient.schedule_activity() call, labelled by "
        "``step_kind`` (always ``activity`` today) and ``class`` "
        "(one of the ACTIVITY_RESULT_CLASSES values plus "
        "``internal_error`` for client-side exceptions). One sample "
        "per attempt."
    ),
)

STEP_ATTEMPTS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_step_attempts_total",
    description=(
        "Count of step attempts executed by the Step Coordinator, "
        "labelled by ``step_kind`` and ``final_class`` (the "
        "ActivityResultClass value the attempt resolved to, or "
        "``internal_error`` when the attempt raised). Bumped exactly "
        "once per attempt — both retried and terminal attempts "
        "increment by one."
    ),
)

STEP_ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_step_errors_total",
    description=(
        "Count of Step Coordinator dispatch failures, labelled by "
        "the structured error ``kind`` from the locked "
        "LOCKED_STEP_KINDS taxonomy (WF-IMPL-048). Bumped exactly "
        "once per failed StepCoordinator.execute() that surfaces a "
        "StepFailed envelope or raises a StepCoordinatorError."
    ),
)

#: ``outcome`` label is the locked LOCKED_STEP_KINDS taxonomy
#: with the ``step.`` prefix stripped (mirrors the WF-IMPL-044
#: Run Controller convention so dashboards can lift the same
#: post-processing across both surfaces).
_STEP_EXECUTE_OUTCOMES: Final[Mapping[str, str]] = {
    "step.kind_not_implemented": "kind_not_implemented",
    "step.with_input_resolution_error": "with_input_resolution_error",
    "step.connector_bind_error": "connector_bind_error",
    "step.activity_schedule_error": "activity_schedule_error",
    "step.retry_budget_exhausted": "retry_budget_exhausted",
    "step.loop_expansion_error": "loop_expansion_error",
    "step.sub_orchestration_spawn_error": "sub_orchestration_spawn_error",
    "step.sub_workflow_failed": "sub_workflow_failed",
    "step.approval_timeout": "approval_timeout",
    "step.resume_registration_failed": "resume_registration_failed",
    "step.resume_subscription_divergent": "resume_subscription_divergent",
    "step.resume_mirror_persist_error": "resume_mirror_persist_error",
}

# Build-time check: the outcome map keys MUST equal the locked
# Step Coordinator kind set. Adding or removing a
# :class:`StepCoordinatorError` subclass without updating
# ``_STEP_EXECUTE_OUTCOMES`` fails import — a noisy fail-fast
# keeps the observability contract honest (parallel to the
# WF-IMPL-044 ``_RUN_LIFECYCLE_OUTCOMES`` assertion).
assert frozenset(_STEP_EXECUTE_OUTCOMES) == LOCKED_STEP_KINDS, (
    "_STEP_EXECUTE_OUTCOMES keys must equal LOCKED_STEP_KINDS; "
    f"map={sorted(_STEP_EXECUTE_OUTCOMES)} "
    f"kinds={sorted(LOCKED_STEP_KINDS)}"
)


@contextmanager
def _instrument_step(
    span_name: str,
    histogram: Histogram | None,
    step_kind: str,
) -> Iterator[Span]:
    """Span + (optional) duration-histogram wrapper for Step Coordinator call sites.

    Kept distinct from :func:`instrument` for the same reason
    :func:`_instrument_run` is: the Step Coordinator histograms
    carry a ``step_kind`` label that the generic helper does not
    thread. When ``histogram`` is :data:`None` the wrapper still
    emits the span (so spans without a paired histogram — like
    ``custos_workflow.step.bind_connectors`` and
    ``custos_workflow.step.retry_decision`` — get the same span
    shape) but records no sample. Errors mark the span ``ERROR``
    and are re-raised; the duration histogram (when present)
    receives a sample labelled with the locked outcome string or
    ``internal_error``.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(span_name) as span:
        span.set_attribute("step_kind", step_kind)
        try:
            yield span
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if histogram is not None:
                outcome = _outcome_for(exc, _STEP_EXECUTE_OUTCOMES)
                histogram.record(
                    elapsed_ms,
                    {"step_kind": step_kind, "outcome": outcome},
                )
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if histogram is not None:
                histogram.record(
                    elapsed_ms,
                    {"step_kind": step_kind, "outcome": _RUN_OK},
                )


def observe_step_execute(step_kind: str) -> AbstractContextManager[Span]:
    """Context manager wrapping one :meth:`StepCoordinator.execute` dispatch.

    Records into :data:`STEP_EXECUTE_DURATION_MS` and emits the
    ``custos_workflow.step.execute`` span. The caller is
    responsible for bumping :data:`STEP_ERRORS_TOTAL` via
    :func:`record_step_error` when the dispatch returns a
    :class:`StepFailed` envelope (the wrapper cannot see the
    envelope — it only sees raised exceptions — so the
    StepFailed-return path goes through the explicit recorder).
    """
    return _instrument_step(
        "custos_workflow.step.execute",
        STEP_EXECUTE_DURATION_MS,
        step_kind,
    )


def observe_step_bind_connectors(step_kind: str) -> AbstractContextManager[Span]:
    """Context manager wrapping a :meth:`ConnectorClient.bind_for_step` call.

    Emits the ``custos_workflow.step.bind_connectors`` span. No
    duration histogram is paired — bind latency is captured by
    upstream Connector Service instrumentation in the deferred
    *Real Connector Client* sub-module, and double-counting it
    here would skew the per-step total.
    """
    return _instrument_step(
        "custos_workflow.step.bind_connectors",
        None,
        step_kind,
    )


def observe_step_schedule_activity(step_kind: str) -> AbstractContextManager[Span]:
    """Context manager wrapping an :meth:`ActivityRuntimeClient.schedule_activity` call.

    Emits the ``custos_workflow.step.schedule_activity`` span and
    records a sample into :data:`ACTIVITY_SCHEDULE_DURATION_MS`.
    The ``class`` label on the histogram sample is *not* set by
    this wrapper — the caller records it explicitly via
    :func:`record_activity_schedule_sample` after the envelope
    lands, because the class is carried on the returned envelope
    rather than raised. The wrapper still records an
    ``internal_error`` sample (with ``class=internal_error``) on
    a raised exception so histogram totals stay consistent with
    the call count.
    """
    return _instrument_schedule_activity(step_kind)


@contextmanager
def _instrument_schedule_activity(step_kind: str) -> Iterator[Span]:
    """Internal context-manager implementation for :func:`observe_step_schedule_activity`."""
    start = time.perf_counter()
    with _tracer.start_as_current_span("custos_workflow.step.schedule_activity") as span:
        span.set_attribute("step_kind", step_kind)
        try:
            yield span
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            ACTIVITY_SCHEDULE_DURATION_MS.record(
                elapsed_ms,
                {"step_kind": step_kind, "class": _INTERNAL_ERROR},
            )
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise


def record_activity_schedule_sample(elapsed_ms: float, step_kind: str, class_: str) -> None:
    """Record a successful :func:`observe_step_schedule_activity` sample.

    Called by :class:`ActivityStepHandler` after the
    ``schedule_activity`` envelope lands so the histogram sample
    carries the ``class`` label from the envelope (one of
    :data:`ACTIVITY_RESULT_CLASSES`). The separate-recorder
    pattern (rather than a context-manager that owns the sample)
    is needed because the class value is *returned*, not raised,
    so the wrapper cannot see it.

    Unknown ``class_`` values raise :class:`ValueError` so a typo
    fails loudly. Negative ``elapsed_ms`` is rejected for the
    same reason.

    Args:
        elapsed_ms: The wall-clock time spent in the schedule
            call, in milliseconds (matches the histogram unit).
        step_kind: The :class:`StepKind` value of the dispatching
            node (always ``activity`` today, but threaded so the
            label set stays uniform if other kinds ever schedule
            activities).
        class_: The :data:`ActivityResultClass` value from the
            envelope.
    """
    # Lazy import — see module-top NOTE on the
    # ``_telemetry`` ⇄ ``clients.activity_runtime`` cycle. This
    # function is only ever called from the request-handling
    # path, by which time the ``clients`` package is fully
    # initialised.
    from custos_workflow.clients.activity_runtime import ACTIVITY_RESULT_CLASSES

    if class_ not in ACTIVITY_RESULT_CLASSES:
        raise ValueError(
            f"unknown activity result class {class_!r}; "
            f"expected one of {sorted(ACTIVITY_RESULT_CLASSES)}"
        )
    if elapsed_ms < 0:
        raise ValueError(f"elapsed_ms must be non-negative; got {elapsed_ms!r}")
    ACTIVITY_SCHEDULE_DURATION_MS.record(
        elapsed_ms,
        {"step_kind": step_kind, "class": class_},
    )


def observe_step_retry_decision(step_kind: str) -> AbstractContextManager[Span]:
    """Context manager wrapping a :func:`retry_driver.decide` call.

    Emits the ``custos_workflow.step.retry_decision`` span. No
    duration histogram is paired — the decide function is pure
    arithmetic over the route table and its latency is well below
    the resolution of the surrounding step-execute histogram.
    """
    return _instrument_step(
        "custos_workflow.step.retry_decision",
        None,
        step_kind,
    )


def record_step_attempt(step_kind: str, final_class: str) -> None:
    """Bump :data:`STEP_ATTEMPTS_TOTAL` for one completed attempt.

    Called by :class:`ActivityStepHandler` exactly once per
    attempt with the activity envelope's class (one of
    :data:`ACTIVITY_RESULT_CLASSES`) or ``internal_error`` when
    the attempt raised before producing an envelope. Unknown
    ``final_class`` values raise :class:`ValueError` so a typo
    fails loudly.
    """
    # Lazy import — see module-top NOTE on the
    # ``_telemetry`` ⇄ ``clients.activity_runtime`` cycle.
    from custos_workflow.clients.activity_runtime import ACTIVITY_RESULT_CLASSES

    if final_class not in ACTIVITY_RESULT_CLASSES and final_class != _INTERNAL_ERROR:
        raise ValueError(
            f"unknown final_class {final_class!r}; expected one of "
            f"{sorted(ACTIVITY_RESULT_CLASSES)} plus {_INTERNAL_ERROR!r}"
        )
    STEP_ATTEMPTS_TOTAL.add(
        1,
        {"step_kind": step_kind, "final_class": final_class},
    )


def record_step_error(kind: str) -> None:
    """Bump :data:`STEP_ERRORS_TOTAL` for a Step Coordinator failure.

    Called by :class:`StepCoordinator` (and the activity-step
    sub-handler) immediately after a :class:`StepFailed` is
    returned or a :class:`StepCoordinatorError` is raised. The
    ``kind`` MUST be one of :data:`LOCKED_STEP_KINDS`; unknown
    kinds raise :class:`ValueError` so a typo or unregistered
    error subclass fails loudly instead of silently dropping the
    sample (mirrors :func:`record_workflow_event_emitted`).
    """
    if kind not in LOCKED_STEP_KINDS:
        raise ValueError(
            f"unknown step error kind {kind!r}; expected one of {sorted(LOCKED_STEP_KINDS)}"
        )
    STEP_ERRORS_TOTAL.add(1, {"kind": kind})


# ---------------------------------------------------------------------------
# WF-IMPL-096: Sub-Orchestration Manager observability hooks
# ---------------------------------------------------------------------------
#
# Spans + counters that make every Sub-Orchestration Manager primitive
# (``forEach`` loop, ``workflow:`` sub-workflow, ``approval:`` gate)
# observable. The manager runs inline in the run orchestrator
# (WF-IMPL-093), so — like the ``wait:`` path — each primitive is a
# generator that yields Dapr child-workflow / external-event / timer
# tokens. The instrumentation context managers wrap the
# ``yield from`` drive so the span stays open across the durable
# suspends and closes when the primitive resolves.
#
# Label sets
# ----------
# * ``primitive`` — ``loop`` / ``sub_workflow`` / ``approval``, the
#   three shapes that share the
#   :attr:`PrimitiveHandler.SUB_ORCHESTRATION` tag.
# * ``outcome`` — ``ok`` on success, or one of the four
#   sub-orchestration suffixes of the locked
#   :data:`~custos_workflow.steps.errors.LOCKED_STEP_KINDS` taxonomy
#   (``step.`` prefix stripped, mirroring the WF-IMPL-058 convention)
#   when the primitive raises a :class:`StepCoordinatorError`, or
#   ``internal_error`` for any other escaping exception.
#
# Two counters:
#
# * :data:`SUB_ORCHESTRATION_CHILDREN_SPAWNED_TOTAL` — number of child
#   workflow instances spawned by the ``loop`` / ``sub_workflow``
#   primitives, labelled by ``primitive`` and ``outcome``. A loop
#   that expands to N items contributes N on success; a sub-workflow
#   contributes 1. A primitive that fails before (or without)
#   spawning contributes a 0-valued sample under its failure outcome
#   so the (``primitive``, ``outcome``) series still appears.
# * :data:`SUB_ORCHESTRATION_APPROVALS_TIMED_OUT_TOTAL` — number of
#   ``approval`` gates that resolved by timing out, labelled by
#   ``outcome``. A gate that is approved (or that fails to arm)
#   contributes a 0-valued sample so the series appears for every
#   outcome the gate reaches.

#: ``outcome`` label map for the sub-orchestration primitives — the
#: sub-orchestration subset of the locked
#: :data:`~custos_workflow.steps.errors.LOCKED_STEP_KINDS` taxonomy
#: with the ``step.`` prefix stripped.
_SUB_ORCHESTRATION_OUTCOMES: Final[Mapping[str, str]] = {
    "step.loop_expansion_error": "loop_expansion_error",
    "step.sub_orchestration_spawn_error": "sub_orchestration_spawn_error",
    "step.sub_workflow_failed": "sub_workflow_failed",
    "step.approval_timeout": "approval_timeout",
}

#: Closed set of ``primitive`` label values.
_SUB_ORCHESTRATION_PRIMITIVES: Final[frozenset[str]] = frozenset(
    {"loop", "sub_workflow", "approval"}
)

#: ``outcome`` label for a gate that timed out (drives the
#: approvals-timed-out counter increment).
_APPROVAL_TIMEOUT_OUTCOME: Final[str] = "approval_timeout"

SUB_ORCHESTRATION_CHILDREN_SPAWNED_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_sub_orchestration_children_spawned_total",
    description=(
        "Count of child workflow instances spawned by the "
        "Sub-Orchestration Manager loop / sub-workflow primitives, "
        "labelled by ``primitive`` (``loop`` or ``sub_workflow``) and "
        "``outcome``. A loop contributes one per expanded item, a "
        "sub-workflow contributes one; a primitive that fails before "
        "spawning contributes a 0-valued sample under its failure "
        "outcome."
    ),
)

SUB_ORCHESTRATION_APPROVALS_TIMED_OUT_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_sub_orchestration_approvals_timed_out_total",
    description=(
        "Count of Sub-Orchestration Manager approval gates that "
        "resolved by timing out, labelled by ``outcome``. An approved "
        "gate (or one that fails to arm) contributes a 0-valued "
        "sample so every reached outcome appears."
    ),
)


class _SubOrchestrationObservation:
    """Mutable handle yielded by :func:`observe_sub_orchestration`.

    The caller sets :attr:`children` to the number of child instances
    the primitive spawned (``loop``: the expanded item count;
    ``sub_workflow``: 1) on the success path. The context manager
    reads it when it records the children-spawned counter. On the
    failure path the count stays 0 — the failure outcome series is
    still emitted with a 0-valued sample.
    """

    __slots__ = ("children",)

    def __init__(self) -> None:
        self.children: int = 0


def _record_sub_orchestration_counters(primitive: str, outcome: str, children: int) -> None:
    """Bump the sub-orchestration counters for one resolved primitive."""
    if primitive in ("loop", "sub_workflow"):
        SUB_ORCHESTRATION_CHILDREN_SPAWNED_TOTAL.add(
            children,
            {"primitive": primitive, "outcome": outcome},
        )
    else:  # approval
        timed_out = 1 if outcome == _APPROVAL_TIMEOUT_OUTCOME else 0
        SUB_ORCHESTRATION_APPROVALS_TIMED_OUT_TOTAL.add(timed_out, {"outcome": outcome})


@contextmanager
def observe_sub_orchestration(primitive: str) -> Iterator[_SubOrchestrationObservation]:
    """Span + counter wrapper for one Sub-Orchestration Manager primitive.

    Emits the ``custos_workflow.sub_orchestration.{primitive}`` span
    (with ``primitive`` and ``outcome`` attributes) and records the
    matching counter sample exactly once per dispatch: the
    children-spawned counter for ``loop`` / ``sub_workflow``, the
    approvals-timed-out counter for ``approval``. The yielded
    observation handle lets the caller report the spawned-child count
    on the success path; failures record a 0-valued sample under the
    locked failure outcome and re-raise.
    """
    if primitive not in _SUB_ORCHESTRATION_PRIMITIVES:
        raise ValueError(
            f"unknown sub-orchestration primitive {primitive!r}; "
            f"expected one of {sorted(_SUB_ORCHESTRATION_PRIMITIVES)}"
        )
    with _tracer.start_as_current_span(f"custos_workflow.sub_orchestration.{primitive}") as span:
        span.set_attribute("primitive", primitive)
        observation = _SubOrchestrationObservation()
        try:
            yield observation
        except Exception as exc:
            outcome = _outcome_for(exc, _SUB_ORCHESTRATION_OUTCOMES)
            span.set_attribute("outcome", outcome)
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            _record_sub_orchestration_counters(primitive, outcome, 0)
            raise
        else:
            span.set_attribute("outcome", _RUN_OK)
            _record_sub_orchestration_counters(primitive, _RUN_OK, observation.children)


# ---------------------------------------------------------------------------
# WF-IMPL-070: API Adapter HTTP-server observability hooks
# ---------------------------------------------------------------------------
#
# Spans, histograms, and counters that make every inbound REST /
# Internal RPC request observable end-to-end. Mirrors the
# WF-IMPL-044 / WF-IMPL-058 patterns above.
#
# Three instruments:
#
# * :data:`HTTP_SERVER_DURATION_MS` — per-request latency
#   histogram, labelled by ``http.route`` (the FastAPI template
#   path, **not** the live URL — keeps cardinality bounded),
#   ``http.method``, and ``http.status_code``. One sample per
#   request.
# * :data:`API_ERRORS_TOTAL` — counter, labelled by the locked
#   :data:`~custos_workflow.api.errors.LOCKED_API_KINDS` taxonomy
#   value (``wf.error.kind``). Bumped exactly once per failed
#   request, by the matching exception handler in
#   :mod:`custos_workflow.api.errors`. The build-time assertion
#   below pins the contract — adding a kind to ``LOCKED_API_KINDS``
#   without extending the recogniser here fails at import time.
# * :data:`IDEMPOTENCY_OUTCOMES_TOTAL` — counter, labelled by
#   ``wf.idempotency.outcome ∈ {fresh, replay, conflict}``. Bumped
#   exactly once per StartRun that supplied an idempotency key:
#   ``fresh`` and ``replay`` from the Validator's
#   ``record_or_replay`` happy path, ``conflict`` from the same
#   call's :class:`IdempotencyConflictError` branch. Requests
#   without a key produce no sample (otherwise the
#   ``fresh``/``replay``/``conflict`` split is meaningless).
#
# Spans
# -----
# A single ``custos_workflow.http.request`` span wraps every
# inbound request (mounted by the
# :class:`OTelHttpServerMiddleware` ASGI middleware in
# :mod:`custos_workflow.api.observability`). Span attributes:
#
# * ``http.method``, ``http.route``, ``http.status_code`` —
#   standard OTel HTTP-server semconv values.
# * ``wf.workspace.id`` — populated from the ``ws`` path parameter
#   when present (every public ``/v1/workspaces/{ws}/...`` route).
# * ``wf.run.id`` — populated from the ``run_id`` path parameter
#   when present (``GET ../runs/{runId}`` and ``...:cancel``)
#   **or** from ``request.state.wf_run_id`` (set by the StartRun
#   route after the controller mints a fresh run id).
# * ``wf.workflow_version.id`` — populated from
#   ``request.state.wf_workflow_version_id`` (set by the StartRun
#   route after the Validator confirms the workflow version
#   exists; the Validator's normalised id is what the controller
#   sees, so the span carries the same value).
# * ``wf.idempotency.outcome`` — populated from
#   ``request.state.wf_idempotency_outcome`` (set by the StartRun
#   route on a successful validation, by the Validator's
#   exception handler on conflict).
# * ``wf.error.kind`` — populated from
#   ``request.state.wf_error_kind`` (set by the API exception
#   handlers when they emit a Problem+JSON envelope). Set as an
#   attribute only; the span's :class:`StatusCode.ERROR` flag is
#   driven exclusively by :func:`observe_http_request` on
#   unhandled exceptions — Problem+JSON responses are HTTP-level
#   errors, not span-level errors, so the span status stays at
#   the SDK default (UNSET) on those paths.

HTTP_SERVER_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_http_server_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in inbound HTTP request handling by "
        "the workflow-service API Adapter, labelled by http.route "
        "(the FastAPI template path, NOT the live URL), http.method, "
        "and http.status_code. One sample per request."
    ),
)

API_ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_api_errors_total",
    description=(
        "Count of inbound HTTP requests that emitted an "
        "application/problem+json envelope, labelled by the locked "
        "WF-IMPL-061 API kind (custos_workflow.api.errors.LOCKED_API_KINDS). "
        "Bumped exactly once per failed request, by the matching "
        "exception handler."
    ),
)

IDEMPOTENCY_OUTCOMES_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_idempotency_outcomes_total",
    description=(
        "Count of StartRun requests that consulted the Validator's "
        "(workspaceId, idempotencyKey) ledger, labelled by "
        "wf.idempotency.outcome ∈ {fresh, replay, conflict}. Requests "
        "that omit the idempotency key produce no sample."
    ),
)

#: Closed set of valid ``wf.idempotency.outcome`` label values.
#: Pinned so a typo at a call site fails loudly via
#: :func:`record_idempotency_outcome` instead of leaking a bad
#: label into the meter.
_IDEMPOTENCY_OUTCOMES: Final[frozenset[str]] = frozenset({"fresh", "replay", "conflict"})


@contextmanager
def observe_http_request(method: str, route: str) -> Iterator[Span]:
    """Wrap one inbound HTTP request in a span.

    Used by :class:`~custos_workflow.api.observability.OTelHttpServerMiddleware`
    around every request that traverses the FastAPI router stack.
    The yielded span carries the standard HTTP-server semconv
    attributes (``http.method``, ``http.route``) plus any
    workflow-service-specific ``wf.*`` attributes the middleware
    chooses to set after ``call_next`` returns (``wf.workspace.id``,
    ``wf.run.id``, ``wf.workflow_version.id``,
    ``wf.idempotency.outcome``, ``wf.error.kind``).

    Duration sampling is owned by the middleware
    (:func:`record_http_server_duration`) because only the
    middleware has the final ``http.status_code`` value — exception
    handlers may convert raised errors into 4xx / 5xx responses
    with arbitrary status codes, and the histogram label needs to
    reflect what actually went out on the wire.

    On a raised exception the span is marked
    :attr:`StatusCode.ERROR` and the exception is recorded, then
    re-raised — the middleware never swallows route exceptions
    (the WF-IMPL-061 handler chain is responsible for translating
    them into Problem+JSON responses).
    """
    with _tracer.start_as_current_span("custos_workflow.http.request") as span:
        span.set_attribute("http.method", method)
        span.set_attribute("http.route", route)
        try:
            yield span
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise


def record_http_server_duration(
    *, method: str, route: str, status_code: int, duration_ms: float
) -> None:
    """Record one sample into :data:`HTTP_SERVER_DURATION_MS`.

    Called by the
    :class:`~custos_workflow.api.observability.OTelHttpServerMiddleware`
    on every exit (success or exception) so the histogram total
    stays consistent with the request count. The
    ``http.status_code`` label is stringified to keep the label
    cardinality consistent across OTel SDK versions (some
    aggregators stringify on emit, others don't; pinning the
    label type here removes the discrepancy).
    """
    HTTP_SERVER_DURATION_MS.record(
        duration_ms,
        {
            "http.route": route,
            "http.method": method,
            "http.status_code": str(status_code),
        },
    )


def record_api_error(kind: str) -> None:
    """Bump :data:`API_ERRORS_TOTAL` for a Problem+JSON-emitting request.

    Called by each exception handler in
    :mod:`custos_workflow.api.errors` immediately before returning
    the envelope, so the counter ticks exactly once per failed
    request regardless of which class in the locked taxonomy was
    raised. The ``kind`` MUST be one of
    :data:`~custos_workflow.api.errors.LOCKED_API_KINDS`; unknown
    kinds raise :class:`ValueError` so a typo or unregistered
    handler fails loudly instead of silently dropping the sample
    (mirrors :func:`record_workflow_event_emitted` /
    :func:`record_step_error`).
    """
    # Imported lazily to dodge the import cycle
    # custos_workflow.api.errors -> _telemetry -> custos_workflow.api.errors.
    from custos_workflow.api.errors import LOCKED_API_KINDS

    if kind not in LOCKED_API_KINDS:
        raise ValueError(
            f"unknown API error kind {kind!r}; expected one of {sorted(LOCKED_API_KINDS)}"
        )
    API_ERRORS_TOTAL.add(1, {"wf.error.kind": kind})


def record_idempotency_outcome(outcome: str) -> None:
    """Bump :data:`IDEMPOTENCY_OUTCOMES_TOTAL` for one StartRun.

    Called by :class:`~custos_workflow.validator.StartRunValidator`
    immediately after :meth:`IdempotencyLedger.record_or_replay`
    returns (``fresh`` / ``replay``) or raises
    :class:`IdempotencyConflictError` (``conflict``). Bumped at
    most once per request: StartRun calls that omit the
    idempotency key skip the ledger entirely and therefore never
    reach this function. Unknown outcomes raise :class:`ValueError`
    so a typo at a call site fails loudly instead of leaking a
    bad label into the meter.
    """
    if outcome not in _IDEMPOTENCY_OUTCOMES:
        raise ValueError(
            f"unknown idempotency outcome {outcome!r}; "
            f"expected one of {sorted(_IDEMPOTENCY_OUTCOMES)}"
        )
    IDEMPOTENCY_OUTCOMES_TOTAL.add(1, {"wf.idempotency.outcome": outcome})


# ---------------------------------------------------------------------------
# WF-IMPL-081: Outbound RPC observability hooks
# ---------------------------------------------------------------------------
#
# Spans, histograms, and counters that make every outbound Dapr
# Service-Invocation call from :mod:`custos_workflow.clients`
# observable end-to-end. Wired through both
# :class:`~custos_workflow.clients.activity_runtime.DaprActivityRuntimeClient`
# (``ScheduleActivity`` / ``CancelActivity``) and
# :class:`~custos_workflow.clients.connector.DaprConnectorClient`
# (``BindForStep``) via the single
# :func:`observe_outbound_rpc` async context manager so the three
# call sites share one instrument set and one span name.
#
# Three instruments:
#
# * :data:`OUTBOUND_RPC_DURATION_MS` — histogram, labels
#   ``wf.client`` (``"arm"`` | ``"connector"``), ``wf.method``,
#   and ``http.status_code`` (string; ``"0"`` when no response
#   was observed — transport failure). One sample per outbound
#   call, recorded on every exit (success or exception) so the
#   histogram total stays consistent with the call count.
# * :data:`OUTBOUND_RPC_TOTAL` — counter, labels ``wf.client``,
#   ``wf.method``, and ``wf.outcome`` ∈
#   :data:`LOCKED_OUTBOUND_RPC_OUTCOMES`. Bumped exactly once
#   per outbound call.
# * :data:`OUTBOUND_RPC_ERRORS_TOTAL` — counter, label
#   ``wf.error.kind`` ∈
#   :data:`~custos_workflow.clients._errors.LOCKED_OUTBOUND_RPC_KINDS`.
#   Bumped exactly once per failed outbound call, by the
#   :class:`OutboundRpcError` recogniser inside
#   :func:`observe_outbound_rpc` — Cancel's HTTP-404 / HTTP-409
#   idempotent no-op branches return normally and therefore never
#   reach this counter (asserted in the unit-test suite).
#
# Span
# ----
# ``custos_workflow.outbound_rpc.call`` — one span per call,
# attributes pinned to
# :data:`LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES` so any drift fails
# the exhaustiveness guard in
# ``tests/test_outbound_rpc_telemetry.py``.

OUTBOUND_RPC_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_workflow_outbound_rpc_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in one outbound Dapr Service-Invocation "
        "call from the workflow-service (ARM ScheduleActivity / "
        "CancelActivity, Connector BindForStep), labelled by wf.client, "
        "wf.method, and http.status_code (string; '0' when no response "
        "was observed). One sample per call."
    ),
)

OUTBOUND_RPC_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_outbound_rpc_total",
    description=(
        "Count of outbound Dapr Service-Invocation calls from the "
        "workflow-service, labelled by wf.client, wf.method, and "
        "wf.outcome ∈ LOCKED_OUTBOUND_RPC_OUTCOMES. Bumped exactly "
        "once per call."
    ),
)

OUTBOUND_RPC_ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_workflow_outbound_rpc_errors_total",
    description=(
        "Count of outbound Dapr Service-Invocation calls that raised "
        "an OutboundRpcError, labelled by wf.error.kind ∈ "
        "LOCKED_OUTBOUND_RPC_KINDS (workflow.client.transport / "
        "workflow.client.status / workflow.client.decode / "
        "workflow.client.cancelled). Bumped exactly once per failed "
        "call. Cancel's HTTP-404 / HTTP-409 idempotent no-op branches "
        "return normally and never reach this counter."
    ),
)


#: Locked outcome label set. Pinned so a new outcome cannot ship
#: without an explicit edit here (and a matching test update).
#: ``success`` → 2xx with no observed envelope-level error.
#: ``transport`` → no HTTP response observed (DNS / connect / TLS
#: / read / write / timeout — i.e. :class:`OutboundRpcTransportError`).
#: ``retryable`` → :class:`OutboundRpcStatusError` with status in
#: 408 / 429 / 5xx (matches the WF-IMPL-075 envelope mapper).
#: ``permanent`` → :class:`OutboundRpcStatusError` with status in
#: 4xx \ {408, 429} OR :class:`OutboundRpcDecodeError`. Also the
#: catch-all bucket for any unexpected non-``OutboundRpc`` exception
#: that escapes the wrapped block (the error counter is *not* bumped
#: for these — they carry no locked ``wf.error.kind``).
#: ``cancelled`` → :class:`OutboundRpcCancelledError` (HTTP 499 or
#: explicit upstream cancel) OR an :class:`asyncio.CancelledError`
#: propagating through the wrapped call.
LOCKED_OUTBOUND_RPC_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"success", "transport", "retryable", "permanent", "cancelled"}
)


#: Locked span-attribute key set for the
#: ``custos_workflow.outbound_rpc.call`` span. Asserted exhaustively
#: in ``tests/test_outbound_rpc_telemetry.py`` so a new attribute
#: cannot be added without landing here.
#:
#: ``wf.run.id``, ``wf.step.id``, and ``wf.attempt`` are emitted
#: only when supplied (Cancel has no ``wf.attempt``).
LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {
        "wf.client",
        "wf.method",
        "wf.run.id",
        "wf.step.id",
        "wf.attempt",
        "http.method",
        "http.url",
        "http.status_code",
        "wf.outcome",
        "wf.error.kind",
    }
)


#: HTTP status codes that the WF-IMPL-075 envelope mapper classifies
#: as ``retryable`` even though they fall in the 4xx range. Kept
#: in lockstep with ``custos_workflow.clients._errors._RETRYABLE_4XX``
#: so the histogram outcome label tracks the envelope class.
_OUTBOUND_RETRYABLE_4XX: Final[frozenset[int]] = frozenset({408, 429})


def _classify_status_outcome(status_code: int) -> str:
    """Map an HTTP status code to its :data:`LOCKED_OUTBOUND_RPC_OUTCOMES` bucket.

    Mirrors :func:`custos_workflow.clients._errors._classify_status`
    one-for-one so the histogram outcome and the envelope class
    can never disagree.
    """
    if status_code in _OUTBOUND_RETRYABLE_4XX:
        return "retryable"
    if 400 <= status_code < 500:
        return "permanent"
    if 500 <= status_code < 600:
        return "retryable"
    return "permanent"


class _OutboundRpcCallContext:
    """Per-call mutable scratchpad shared between caller and ctx manager.

    The async context manager :func:`observe_outbound_rpc` yields
    one of these; the caller is expected to set
    :attr:`status_code` as soon as it receives an HTTP response so
    the ctx manager can label the duration histogram and the span
    consistently on the exception path too. Left unset (``None``)
    on transport-layer failure (no response observed) — the ctx
    manager labels these as ``http.status_code="0"``.
    """

    __slots__ = ("status_code",)

    def __init__(self) -> None:
        self.status_code: int | None = None

    def set_status_code(self, status_code: int) -> None:
        """Record the HTTP status code observed on the wire."""
        self.status_code = status_code


def _set_optional_attr(span: Span, key: str, value: object) -> None:
    """Set ``key`` on ``span`` only when ``value`` is not ``None``.

    Keeps the locked-attribute set honest: optional attributes
    (``wf.run.id``, ``wf.step.id``, ``wf.attempt``) are emitted
    only when the caller supplied them, so the exhaustiveness
    guard can compare against the full set.
    """
    if value is None:
        return
    if isinstance(value, (str, bool, int, float)):
        span.set_attribute(key, value)
    else:
        span.set_attribute(key, str(value))


@asynccontextmanager
async def observe_outbound_rpc(
    *,
    client: str,
    method: str,
    run_id: str | None = None,
    step_id: str | None = None,
    attempt: int | None = None,
) -> AsyncIterator[_OutboundRpcCallContext]:
    """Wrap one outbound Dapr Service-Invocation call.

    Emits a single ``custos_workflow.outbound_rpc.call`` span and
    records one sample into each of
    :data:`OUTBOUND_RPC_DURATION_MS` and :data:`OUTBOUND_RPC_TOTAL`
    per call. On any :class:`OutboundRpcError` subclass the
    matching ``wf.error.kind`` is bumped on
    :data:`OUTBOUND_RPC_ERRORS_TOTAL`; non-``OutboundRpc`` exits
    (Cancel's HTTP-404 / HTTP-409 idempotent no-ops, normal
    returns) never touch the error counter, matching the
    acceptance criteria pinned in WF-IMPL-081.

    The yielded :class:`_OutboundRpcCallContext` lets the caller
    record the HTTP status code as soon as the response is
    received — used both to label the duration histogram and to
    classify :class:`OutboundRpcStatusError` outcomes into
    ``retryable`` / ``permanent`` consistently with the WF-IMPL-075
    envelope mapper.

    :param client: Either ``"arm"`` or ``"connector"``. Pinned to
        the two adapters wired in WF-IMPL-079 / WF-IMPL-080;
        anything else raises :class:`ValueError` so a typo at a
        call site fails loudly instead of leaking a bad label.
    :param method: Dapr method name. ``"ScheduleActivity"`` /
        ``"CancelActivity"`` for ARM, ``"BindForStep"`` for the
        Connector adapter.
    :param run_id: Workflow run id (when known). Emitted as the
        ``wf.run.id`` span attribute when supplied.
    :param step_id: Workflow step id (when known). Emitted as the
        ``wf.step.id`` span attribute when supplied. Cancel passes
        the step id even though it omits ``attempt``.
    :param attempt: Per-step attempt counter (when applicable).
        Emitted as the ``wf.attempt`` span attribute when supplied.
        ``cancel_activity`` deliberately omits this — cancellation
        is not attempt-scoped.
    """
    if client not in {"arm", "connector"}:
        raise ValueError(
            f"observe_outbound_rpc.client must be one of {{'arm', 'connector'}}; got {client!r}"
        )

    # Import lazily to break the ``clients/_errors -> _telemetry ->
    # clients/_errors`` cycle (the LOCKED set is already importable
    # at module load — only the subclass tree needs lazy access so
    # ``clients._errors`` can finish initialising first).
    from custos_workflow.clients._errors import (
        OutboundRpcCancelledError,
        OutboundRpcDecodeError,
        OutboundRpcError,
        OutboundRpcStatusError,
        OutboundRpcTransportError,
    )

    ctx = _OutboundRpcCallContext()
    sanitized_url = f"…/method/{method}"
    start = time.perf_counter()
    with _tracer.start_as_current_span("custos_workflow.outbound_rpc.call") as span:
        span.set_attribute("wf.client", client)
        span.set_attribute("wf.method", method)
        span.set_attribute("http.method", "POST")
        span.set_attribute("http.url", sanitized_url)
        _set_optional_attr(span, "wf.run.id", run_id)
        _set_optional_attr(span, "wf.step.id", step_id)
        _set_optional_attr(span, "wf.attempt", attempt)

        # Default outcome assumes a clean exit; the except branches
        # below downgrade it. ``error_kind`` stays ``None`` for the
        # success path *and* for unexpected non-``OutboundRpc``
        # failures (the error counter is locked to
        # ``LOCKED_OUTBOUND_RPC_KINDS``, which only covers the
        # outbound-RPC taxonomy). The shared ``finally`` records the
        # duration histogram + total counter exactly once on *every*
        # exit path — success, ``OutboundRpcError``, asyncio
        # cancellation, or any other escaping exception — so the
        # "one sample per call" invariant holds even when an
        # unexpected exception propagates through the wrapped block.
        outcome = "success"
        error_kind: str | None = None
        try:
            yield ctx
        except OutboundRpcError as exc:
            if isinstance(exc, OutboundRpcTransportError):
                outcome = "transport"
            elif isinstance(exc, OutboundRpcCancelledError):
                outcome = "cancelled"
            elif isinstance(exc, OutboundRpcStatusError):
                outcome = _classify_status_outcome(exc.status_code)
                # ``OutboundRpcStatusError`` always carries the real
                # status; surface it on the context so the
                # histogram label below picks it up even when the
                # call site never reached ``ctx.set_status_code``.
                ctx.status_code = exc.status_code
            elif isinstance(exc, OutboundRpcDecodeError):
                outcome = "permanent"
            else:  # pragma: no cover - defensive; closed taxonomy
                outcome = "permanent"
            error_kind = exc.kind
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        except BaseException as exc:
            # Unexpected non-``OutboundRpc`` failure escaping the
            # wrapped block (asyncio cancellation, an unforeseen
            # parsing/validation error, etc.). Classify cancellation
            # as ``cancelled`` and everything else as ``permanent``
            # so the total counter still records one sample, then
            # re-raise untouched. The error counter is *not* bumped:
            # these exceptions carry no ``wf.error.kind`` in the
            # locked outbound-RPC taxonomy.
            outcome = "cancelled" if isinstance(exc, asyncio.CancelledError) else "permanent"
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            status_code_label = str(ctx.status_code) if ctx.status_code is not None else "0"
            OUTBOUND_RPC_DURATION_MS.record(
                elapsed_ms,
                {
                    "wf.client": client,
                    "wf.method": method,
                    "http.status_code": status_code_label,
                },
            )
            OUTBOUND_RPC_TOTAL.add(
                1,
                {"wf.client": client, "wf.method": method, "wf.outcome": outcome},
            )
            if error_kind is not None:
                OUTBOUND_RPC_ERRORS_TOTAL.add(1, {"wf.error.kind": error_kind})
            span.set_attribute("wf.outcome", outcome)
            if error_kind is not None:
                span.set_attribute("wf.error.kind", error_kind)
            if ctx.status_code is not None:
                span.set_attribute("http.status_code", str(ctx.status_code))


# Build-time exhaustiveness guard: every :class:`OutboundRpcError`
# subclass must map to an outcome in :data:`LOCKED_OUTBOUND_RPC_OUTCOMES`.
# We assert the relationship in tests rather than at import time
# to keep the production import side-effect-free.


__all__ = [
    "ACTIVITY_SCHEDULE_DURATION_MS",
    "API_ERRORS_TOTAL",
    "ERRORS_TOTAL",
    "HTTP_SERVER_DURATION_MS",
    "IDEMPOTENCY_OUTCOMES_TOTAL",
    "LOCKED_OUTBOUND_RPC_OUTCOMES",
    "LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES",
    "OUTBOUND_RPC_DURATION_MS",
    "OUTBOUND_RPC_ERRORS_TOTAL",
    "OUTBOUND_RPC_TOTAL",
    "PARSE_DURATION_MS",
    "RETRY_POLICY_DURATION_MS",
    "RUN_LIFECYCLE_DURATION_MS",
    "RUN_STATUS_TRANSITIONS_TOTAL",
    "STEP_ATTEMPTS_TOTAL",
    "STEP_ERRORS_TOTAL",
    "STEP_EXECUTE_DURATION_MS",
    "TOPOLOGY_DURATION_MS",
    "TOTAL_DURATION_MS",
    "TYPE_CHECK_DURATION_MS",
    "WORKFLOW_EVENTS_EMITTED_TOTAL",
    "instrument",
    "observe_compile_parse",
    "observe_compile_retry_policy",
    "observe_compile_topology",
    "observe_compile_total",
    "observe_compile_type_check",
    "observe_http_request",
    "observe_outbound_rpc",
    "observe_run_cancel",
    "observe_run_get",
    "observe_run_list",
    "observe_run_pause",
    "observe_run_raise_external_event",
    "observe_run_replay",
    "observe_run_resume",
    "observe_run_start",
    "observe_step_bind_connectors",
    "observe_step_execute",
    "observe_step_retry_decision",
    "observe_step_schedule_activity",
    "record_activity_schedule_sample",
    "record_api_error",
    "record_http_server_duration",
    "record_idempotency_outcome",
    "record_run_status_transition",
    "record_step_attempt",
    "record_step_error",
    "record_workflow_event_emitted",
]
