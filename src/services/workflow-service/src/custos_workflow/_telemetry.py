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

from custos_workflow.errors import CompileError

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

    Looks up ``CompileError.kind`` in the per-stage ``outcomes``
    mapping. Anything not in the mapping (including non-
    :class:`CompileError` exceptions) falls back to
    ``"internal_error"`` so histogram totals stay consistent with
    the call count even when an unexpected exception escapes.
    """
    if isinstance(exc, CompileError):
        label = outcomes.get(exc.kind)
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
        count_errors: When true (default), bump the
          ``custos_workflow_compile_errors_total`` counter on any
          raised :class:`CompileError`. Per-stage wrappers pass
          ``False`` so a single error does not double-count when
          it propagates through both a stage wrapper and the outer
          total wrapper; only the total wrapper bumps the counter.
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
            if count_errors and isinstance(exc, CompileError):
                ERRORS_TOTAL.add(1, {"kind": exc.kind})
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


__all__ = [
    "ERRORS_TOTAL",
    "PARSE_DURATION_MS",
    "RETRY_POLICY_DURATION_MS",
    "TOPOLOGY_DURATION_MS",
    "TOTAL_DURATION_MS",
    "TYPE_CHECK_DURATION_MS",
    "instrument",
    "observe_compile_parse",
    "observe_compile_retry_policy",
    "observe_compile_topology",
    "observe_compile_total",
    "observe_compile_type_check",
]
