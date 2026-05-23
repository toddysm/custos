"""OpenTelemetry instrumentation for the ``custos_cel`` public surface.

Implements WF-IMPL-011 (#186). Exposes a tracer, a meter, three duration
histograms (one per public entry point), and one per-``kind`` error
counter — all keyed to the locked error taxonomy from WF-IMPL-008
(``custos_cel.errors``).

Design notes
------------
The module imports ``opentelemetry-api`` only. The API ships default
no-op providers, so consumers without an SDK installed can import
``custos_cel`` safely without configuring telemetry first. The issue's
performance budget applies to import-time behavior; this module does not
make a zero-overhead guarantee for per-call ``parse`` / ``type_check`` /
``evaluate`` latency. Production deployments configure their own SDK; the
in-memory SDK is dev-only and exists exclusively to drive the assertions
in ``tests/test_observability.py``.

The instrumentation is intentionally narrow: only the three public
top-level functions (``parse``, ``type_check``, ``evaluate``) emit
spans and metrics. Internal recursion (``_eval``, ``_typecheck``)
deliberately stays uninstrumented so a single user-visible call
maps one-to-one to a single span and one duration sample, matching
the observability conventions the Workflow Service Step Coordinator
expects.

Metric / span names follow the issue scope verbatim:

* ``custos_cel_parse_duration_ms`` — histogram, labels
  ``outcome=success|parse_error``.
* ``custos_cel_type_check_duration_ms`` — histogram, labels
  ``outcome=success|type_error|unbound_name``.
* ``custos_cel_evaluate_duration_ms`` — histogram, labels
  ``outcome=success|timeout|evaluation_error|unbound_name``.
* ``custos_cel_errors_total`` — counter, labels ``kind`` (one of the
  ``CelError.KIND`` constants).
* Spans: ``custos_cel.parse``, ``custos_cel.type_check``,
  ``custos_cel.evaluate``.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, Status, StatusCode, Tracer

from custos_cel.ast import Node
from custos_cel.errors import (
    CelError,
    EvaluationError,
    ParseError,
)
from custos_cel.errors import (
    TimeoutError as _TimeoutError,
)
from custos_cel.errors import (
    TypeError as _TypeError,
)
from custos_cel.errors import (
    UnboundNameError as _UnboundNameError,
)

_INSTRUMENTATION_NAME: Final[str] = "custos_cel"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

# Outcome label used on the duration histograms when the wrapped
# operation returns normally.
_SUCCESS: Final[str] = "success"

# Outcome label used as a defence-in-depth fallback for an unexpected
# (non-``CelError``) exception. The library's public API doesn't
# raise these in practice, but the catch-all keeps the histogram
# total consistent with the call count even when something goes
# wrong outside the taxonomy.
_INTERNAL_ERROR: Final[str] = "internal_error"


_tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)


PARSE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_cel_parse_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in custos_cel.parse(), labelled by outcome (success or parse_error)."
    ),
)

TYPE_CHECK_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_cel_type_check_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in custos_cel.type_check(), labelled by "
        "outcome (success, type_error, or unbound_name)."
    ),
)

EVALUATE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_cel_evaluate_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in custos_cel.evaluate(), labelled by "
        "outcome (success, timeout, evaluation_error, or unbound_name)."
    ),
)

ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_cel_errors_total",
    description=(
        "Count of custos_cel errors raised through the public API, "
        "labelled by the structured error 'kind' from "
        "custos_cel.errors (WF-IMPL-008 taxonomy)."
    ),
)


# Mapping ``CelError`` subtype → outcome label, per the issue's scope
# bullet for each histogram. The mapping is per-operation because the
# valid label set differs across the three entry points (e.g. only
# ``evaluate`` produces ``timeout``).
_PARSE_OUTCOMES: Final[Mapping[type[BaseException], str]] = {
    ParseError: "parse_error",
}
_TYPE_CHECK_OUTCOMES: Final[Mapping[type[BaseException], str]] = {
    _TypeError: "type_error",
    _UnboundNameError: "unbound_name",
}
_EVALUATE_OUTCOMES: Final[Mapping[type[BaseException], str]] = {
    _TimeoutError: "timeout",
    EvaluationError: "evaluation_error",
    _UnboundNameError: "unbound_name",
}


def _outcome_for(
    exc: BaseException,
    mapping: Mapping[type[BaseException], str],
) -> str:
    """Resolve the duration-histogram ``outcome`` label for ``exc``.

    Walks the mapping in declaration order so more specific exception
    types can be matched before broader base classes. For example, in
    the evaluate mapping ``_TimeoutError`` should resolve to
    ``"timeout"`` before any broader ``EvaluationError`` match.
    """
    for exc_type, label in mapping.items():
        if isinstance(exc, exc_type):
            return label
    return _INTERNAL_ERROR


@contextmanager
def instrument(
    span_name: str,
    histogram: Histogram,
    outcomes: Mapping[type[BaseException], str],
) -> Iterator[Span]:
    """Wrap a public-API call with a span + duration sample + error counter.

    The context manager yields the active span so the caller can
    attach operation-specific attributes (source length, node count,
    timeout budget, etc.) before the wrapped work runs. On normal
    completion the duration histogram receives a sample with
    ``outcome=success``; on a raised ``CelError`` it receives a
    sample labelled by ``outcomes[type(exc)]`` and the
    ``custos_cel_errors_total`` counter is bumped by one with the
    error's stable ``kind`` string. The exception is always
    re-raised so the wrapper is transparent to callers.

    Args:
        span_name: Dotted span name (``"custos_cel.parse"`` etc.) —
          becomes the OTel span's display name.
        histogram: The duration histogram to record into. One of
          :data:`PARSE_DURATION_MS`, :data:`TYPE_CHECK_DURATION_MS`,
          :data:`EVALUATE_DURATION_MS`.
        outcomes: Per-call-site mapping from exception type to the
          ``outcome`` label that histogram understands. Anything not
          present in the mapping falls back to ``"internal_error"``.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(span_name) as span:
        try:
            yield span
        except BaseException as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            outcome = _outcome_for(exc, outcomes)
            histogram.record(elapsed_ms, {"outcome": outcome})
            if isinstance(exc, CelError):
                ERRORS_TOTAL.add(1, {"kind": exc.KIND})
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            histogram.record(elapsed_ms, {"outcome": _SUCCESS})


def observe_parse() -> AbstractContextManager[Span]:
    """Context manager wrapping a :func:`custos_cel.parse` call."""
    return instrument("custos_cel.parse", PARSE_DURATION_MS, _PARSE_OUTCOMES)


def observe_type_check() -> AbstractContextManager[Span]:
    """Context manager wrapping a :func:`custos_cel.type_check` call."""
    return instrument("custos_cel.type_check", TYPE_CHECK_DURATION_MS, _TYPE_CHECK_OUTCOMES)


def observe_evaluate() -> AbstractContextManager[Span]:
    """Context manager wrapping a :func:`custos_cel.evaluate` call."""
    return instrument("custos_cel.evaluate", EVALUATE_DURATION_MS, _EVALUATE_OUTCOMES)


__all__ = [
    "ERRORS_TOTAL",
    "EVALUATE_DURATION_MS",
    "PARSE_DURATION_MS",
    "TYPE_CHECK_DURATION_MS",
    "count_nodes",
    "instrument",
    "observe_evaluate",
    "observe_parse",
    "observe_type_check",
]


def count_nodes(node: Node) -> int:
    """Return the total :class:`~custos_cel.ast.Node` count under ``node``.

    Used as a span attribute (``custos_cel.node_count``) on
    ``custos_cel.parse``, ``custos_cel.type_check``, and
    ``custos_cel.evaluate`` so downstream dashboards can correlate
    duration with AST size.

    The walk visits every field of every node via
    :mod:`dataclasses.fields`; :class:`Node`-typed children are
    counted recursively, and tuple-of-nodes / tuple-of-tuple-of-nodes
    field shapes (``Call.args``, ``ListLit.elements``,
    ``MapLit.entries``) are handled in-line. The implementation is
    intentionally tiny — node counting is on the hot path of every
    successful parse, so it must not allocate auxiliary data
    structures.
    """
    count = 1
    for f in dataclasses.fields(node):
        value = getattr(node, f.name)
        if isinstance(value, Node):
            count += count_nodes(value)
        elif isinstance(value, tuple):
            for item in value:
                if isinstance(item, Node):
                    count += count_nodes(item)
                elif isinstance(item, tuple):
                    for inner in item:
                        if isinstance(inner, Node):
                            count += count_nodes(inner)
    return count
