"""OpenTelemetry instrumentation for the ``custos_catalog`` public surface.

Implements CS-IMPL-019 (#220). Exposes a tracer, a meter, two duration
histograms (one for top-level public operations, one for the per-stage
publish pipeline), and one per-``kind`` error counter — all keyed to
the error taxonomy raised by the manager + API layers.

Design notes
------------

The module imports ``opentelemetry-api`` only. The API ships default
no-op providers, so consumers without an SDK installed can import
``custos_catalog`` safely without configuring telemetry first.
Production deployments configure their own SDK (the catalog-service
Helm subchart wires the OTel Collector sidecar per design § Telemetry);
the in-memory SDK is dev-only and exists exclusively to drive the
assertions in ``tests/test_telemetry.py``.

The instrumentation is intentionally narrow: spans + samples are
emitted at the **manager entry points** (one per user-visible
operation) and at the **publish-pipeline stages** that have a known
cost profile. Internal recursion stays uninstrumented so a single
user-visible call maps one-to-one to a single span and one duration
sample on the operation histogram. Matches the convention established
by ``custos-cel`` WF-IMPL-011 (#186, PR #200).

Metric / span names
-------------------

* ``custos_catalog_operation_duration_ms`` — histogram, labels
  ``operation`` (one of the canonical operation strings exported
  below) and ``outcome`` (``success`` or a stable error-kind slug).
* ``custos_catalog_publish_stage_duration_ms`` — histogram for the
  per-stage cost inside ``DefinitionManager.publish_workflow`` and
  ``TemplateManager.publish_template``. Labels ``stage`` (``parse``,
  ``schema``, ``normalize``, ``resolve``, ``cel``, ``idempotency``,
  ``mint_put``) and ``outcome``.
* ``custos_catalog_errors_total`` — counter, label ``kind``. The
  ``kind`` string is the ``code:`` attribute on the manager's
  structured errors (``catalog.workflow_not_found`` etc.) or the
  HTTP error envelope ``code`` for boundary failures.
* Spans: ``custos_catalog.<operation>`` (e.g.
  ``custos_catalog.workflow.publish``) at the manager entry point;
  child spans ``custos_catalog.publish.<stage>`` from inside the
  publish pipeline.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, Status, StatusCode, Tracer

_INSTRUMENTATION_NAME: Final[str] = "custos_catalog"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

# Outcome label used on the duration histograms when the wrapped
# operation returns normally.
_SUCCESS: Final[str] = "success"

# Outcome label used for any exception not in the per-call outcomes
# map. Public APIs may raise built-in exceptions (validation guards,
# programmer-error ``RuntimeError`` etc.); this catch-all labels them
# ``internal_error`` so histogram totals match call counts even when
# something unexpected slips through.
_INTERNAL_ERROR: Final[str] = "internal_error"


_tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)


OPERATION_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_catalog_operation_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time of catalog-service public operations, labelled by "
        "operation (e.g. workflow.publish, template.materialize) and outcome "
        "(success or a stable error-kind slug)."
    ),
)


PUBLISH_STAGE_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_catalog_publish_stage_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time of each publish-pipeline stage inside "
        "DefinitionManager.publish_workflow / TemplateManager.publish_template, "
        "labelled by stage (parse, schema, normalize, resolve, cel, "
        "idempotency, mint_put) and outcome."
    ),
)


ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_catalog_errors_total",
    description=(
        "Count of catalog-service errors raised through the public manager "
        "and API surfaces, labelled by the structured error 'kind' (the "
        "``code`` attribute on the manager error or the HTTP envelope code)."
    ),
)


# Canonical operation labels. Centralised so spans, histograms, and
# tests all reference the same set of strings.
OP_WORKFLOW_PUBLISH: Final[str] = "workflow.publish"
OP_WORKFLOW_DEPRECATE: Final[str] = "workflow.deprecate"
OP_WORKFLOW_GET: Final[str] = "workflow.get"
OP_WORKFLOW_LIST: Final[str] = "workflow.list"

OP_TEMPLATE_PUBLISH: Final[str] = "template.publish"
OP_TEMPLATE_MATERIALIZE: Final[str] = "template.materialize"
OP_TEMPLATE_EXTRACT: Final[str] = "template.extract"
OP_TEMPLATE_DEPRECATE: Final[str] = "template.deprecate"
OP_TEMPLATE_GET: Final[str] = "template.get"
OP_TEMPLATE_LIST: Final[str] = "template.list"

OP_ACTIVITY_REGISTER: Final[str] = "activity.register"
OP_ACTIVITY_DEPRECATE: Final[str] = "activity.deprecate"
OP_ACTIVITY_GET: Final[str] = "activity.get"
OP_ACTIVITY_LIST: Final[str] = "activity.list"

OP_CONNECTOR_REGISTER: Final[str] = "connector.register"
OP_CONNECTOR_DEPRECATE: Final[str] = "connector.deprecate"
OP_CONNECTOR_GET: Final[str] = "connector.get"
OP_CONNECTOR_LIST: Final[str] = "connector.list"

OP_RPC_GET_WORKFLOW_VERSION: Final[str] = "rpc.get_workflow_version"
OP_RPC_RESOLVE_CONNECTOR_TYPE: Final[str] = "rpc.resolve_connector_type"


# Canonical publish-pipeline stage labels.
STAGE_PARSE: Final[str] = "parse"
STAGE_SCHEMA: Final[str] = "schema"
STAGE_NORMALIZE: Final[str] = "normalize"
STAGE_RESOLVE: Final[str] = "resolve"
STAGE_CEL: Final[str] = "cel"
STAGE_IDEMPOTENCY: Final[str] = "idempotency"
STAGE_MINT_PUT: Final[str] = "mint_put"


def _outcome_for(
    exc: BaseException,
    mapping: Mapping[type[BaseException], str],
) -> str:
    """Resolve the duration-histogram ``outcome`` label for ``exc``.

    Walks the mapping in declaration order so more specific exception
    types match before broader base classes. Falls back to
    :data:`_INTERNAL_ERROR` when nothing matches.
    """
    for exc_type, label in mapping.items():
        if isinstance(exc, exc_type):
            return label
    return _INTERNAL_ERROR


def _error_kind(exc: BaseException) -> str | None:
    """Return the structured ``code`` slug for an exception if it carries one.

    Catalog manager errors expose their HTTP/error-envelope code as a
    class attribute named ``code``. Anything else is treated as an
    unstructured failure and is not counted into
    :data:`ERRORS_TOTAL`.
    """
    code = getattr(exc, "code", None)
    if isinstance(code, str) and code:
        return code
    return None


@contextmanager
def instrument(
    span_name: str,
    histogram: Histogram,
    labels: Mapping[str, str],
    outcomes: Mapping[type[BaseException], str],
) -> Iterator[Span]:
    """Wrap a public-API call with a span + duration sample + error counter.

    Yields the active span so the caller can attach
    operation-specific attributes (workflow name, principal id, etc.)
    before the wrapped work runs. On normal completion the duration
    histogram receives a sample with the supplied ``labels`` plus
    ``outcome=success``; on a raised exception it receives a sample
    labelled by ``outcomes[type(exc)]`` (falling back to
    ``internal_error``) and the :data:`ERRORS_TOTAL` counter is bumped
    by one with the error's stable ``code`` slug when present. The
    exception is always re-raised so the wrapper is transparent.

    Args:
        span_name: Dotted span name (``custos_catalog.workflow.publish``
          etc.) — becomes the OTel span's display name.
        histogram: Duration histogram to record into. One of
          :data:`OPERATION_DURATION_MS` or
          :data:`PUBLISH_STAGE_DURATION_MS`.
        labels: Constant labels for both the success and error sample
          paths (e.g. ``{"operation": "workflow.publish"}`` or
          ``{"stage": "schema"}``).
        outcomes: Per-call-site mapping from exception type to the
          ``outcome`` label that histogram understands. Anything not
          present in the mapping falls back to ``internal_error``.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(span_name) as span:
        try:
            yield span
        except Exception as exc:
            # Catch ``Exception`` (not ``BaseException``) so process-
            # control unwinds — ``KeyboardInterrupt``, ``SystemExit``,
            # ``GeneratorExit`` — propagate untouched and are never
            # recorded into histograms or the error counter. Those
            # events are not application errors; mislabelling them as
            # such would skew SLO dashboards.
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            outcome = _outcome_for(exc, outcomes)
            histogram.record(elapsed_ms, {**labels, "outcome": outcome})
            kind = _error_kind(exc)
            if kind is not None:
                ERRORS_TOTAL.add(1, {"kind": kind})
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            histogram.record(elapsed_ms, {**labels, "outcome": _SUCCESS})


def observe_operation(
    operation: str,
    outcomes: Mapping[type[BaseException], str] | None = None,
) -> AbstractContextManager[Span]:
    """Context manager wrapping a public manager / RPC operation.

    Produces span ``custos_catalog.<operation>`` and records into
    :data:`OPERATION_DURATION_MS` labelled by ``operation`` + outcome.
    """
    return instrument(
        f"custos_catalog.{operation}",
        OPERATION_DURATION_MS,
        {"operation": operation},
        outcomes or {},
    )


def observe_stage(
    stage: str,
    outcomes: Mapping[type[BaseException], str] | None = None,
) -> AbstractContextManager[Span]:
    """Context manager wrapping a publish-pipeline stage.

    Produces span ``custos_catalog.publish.<stage>`` and records into
    :data:`PUBLISH_STAGE_DURATION_MS` labelled by ``stage`` + outcome.
    """
    return instrument(
        f"custos_catalog.publish.{stage}",
        PUBLISH_STAGE_DURATION_MS,
        {"stage": stage},
        outcomes or {},
    )


def record_error_kind(kind: str) -> None:
    """Bump :data:`ERRORS_TOTAL` for an out-of-band error path.

    Used by the API exception handlers which catch and translate
    manager errors *after* the manager's own ``instrument`` block has
    already exited — at that point the manager has already counted
    its own kind, so this helper is reserved for failures originating
    inside the API layer itself (input validation, malformed paths,
    etc.).
    """
    ERRORS_TOTAL.add(1, {"kind": kind})


__all__ = [
    "ERRORS_TOTAL",
    "OPERATION_DURATION_MS",
    "OP_ACTIVITY_DEPRECATE",
    "OP_ACTIVITY_GET",
    "OP_ACTIVITY_LIST",
    "OP_ACTIVITY_REGISTER",
    "OP_CONNECTOR_DEPRECATE",
    "OP_CONNECTOR_GET",
    "OP_CONNECTOR_LIST",
    "OP_CONNECTOR_REGISTER",
    "OP_RPC_GET_WORKFLOW_VERSION",
    "OP_RPC_RESOLVE_CONNECTOR_TYPE",
    "OP_TEMPLATE_DEPRECATE",
    "OP_TEMPLATE_EXTRACT",
    "OP_TEMPLATE_GET",
    "OP_TEMPLATE_LIST",
    "OP_TEMPLATE_MATERIALIZE",
    "OP_TEMPLATE_PUBLISH",
    "OP_WORKFLOW_DEPRECATE",
    "OP_WORKFLOW_GET",
    "OP_WORKFLOW_LIST",
    "OP_WORKFLOW_PUBLISH",
    "PUBLISH_STAGE_DURATION_MS",
    "STAGE_CEL",
    "STAGE_IDEMPOTENCY",
    "STAGE_MINT_PUT",
    "STAGE_NORMALIZE",
    "STAGE_PARSE",
    "STAGE_RESOLVE",
    "STAGE_SCHEMA",
    "instrument",
    "observe_operation",
    "observe_stage",
    "record_error_kind",
]
