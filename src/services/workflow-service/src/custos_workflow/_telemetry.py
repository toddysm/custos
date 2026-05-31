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

import time
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
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
        "resume,get,list} (replay is also recorded for the "
        "orchestrator replay path) and ``outcome`` ∈ {ok,not_found,"
        "state_conflict,state_corrupt,runtime_unavailable,"
        "internal_error}."
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


__all__ = [
    "ACTIVITY_SCHEDULE_DURATION_MS",
    "ERRORS_TOTAL",
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
    "observe_run_cancel",
    "observe_run_get",
    "observe_run_list",
    "observe_run_pause",
    "observe_run_replay",
    "observe_run_resume",
    "observe_run_start",
    "observe_step_bind_connectors",
    "observe_step_execute",
    "observe_step_retry_decision",
    "observe_step_schedule_activity",
    "record_activity_schedule_sample",
    "record_run_status_transition",
    "record_step_attempt",
    "record_step_error",
    "record_workflow_event_emitted",
]
