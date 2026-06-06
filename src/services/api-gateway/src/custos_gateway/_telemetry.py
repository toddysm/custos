"""OpenTelemetry instrumentation for the gateway request pipeline (AGW-IMPL-018).

The gateway proxies every external request to its owning component; this module
gives that pass-through the per-request span, counters, and histograms the design
pins (``design/components/api-gateway/design.md`` § Observability):

* **Span.** Every proxied request opens a :data:`SPAN_NAME` span carrying
  ``{http.method, http.route, workspaceId, principalId, correlationId,
  decisionAuditEventId}`` — the last three populated once the correlation
  middleware and the authorize dependency have run.
* **Counters.** :data:`REQUESTS_TOTAL` (``gateway_requests_total`` tagged
  ``{route, method, status}``), :data:`RATE_LIMIT_DENIALS_TOTAL`
  (``gateway_rate_limit_denials_total``), and :data:`IDEMPOTENCY_REPLAYS_TOTAL`
  (``gateway_idempotency_replays_total``).
* **Histograms.** :data:`REQUEST_DURATION_SECONDS`
  (``gateway_request_duration_seconds``) and :data:`DOWNSTREAM_DURATION_SECONDS`
  (``gateway_downstream_duration_seconds``).

The module depends only on ``opentelemetry-api``. The API ships no-op tracer and
meter providers by default, so instrumentation is inert until an SDK + exporter
is configured at the process boundary (the Helm-managed OTel Collector in
production; the in-memory exporters in :mod:`tests.test_telemetry`).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram
from opentelemetry.trace import Span

__all__ = [
    "ATTR_CORRELATION_ID",
    "ATTR_DECISION_AUDIT_EVENT_ID",
    "ATTR_HTTP_METHOD",
    "ATTR_HTTP_ROUTE",
    "ATTR_PRINCIPAL_ID",
    "ATTR_WORKSPACE_ID",
    "DOWNSTREAM_DURATION_SECONDS",
    "IDEMPOTENCY_REPLAYS_TOTAL",
    "RATE_LIMIT_DENIALS_TOTAL",
    "REQUESTS_TOTAL",
    "REQUEST_DURATION_SECONDS",
    "SPAN_NAME",
    "RequestTelemetry",
    "instrument_downstream",
    "record_idempotency_replay",
    "record_rate_limit_denial",
    "request_telemetry",
]

_INSTRUMENTATION_NAME: Final = "custos_gateway"
_INSTRUMENTATION_VERSION: Final = "0.1.0"

_tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)

#: The span name every proxied request opens.
SPAN_NAME: Final = "custos_gateway.request"

#: Span attribute keys (design § Observability). ``http.method`` / ``http.route``
#: follow the OTel HTTP semantic conventions; the Custos-specific ids use the
#: gateway's camelCase wire vocabulary so a trace lines up with the audit trail.
ATTR_HTTP_METHOD: Final = "http.method"
ATTR_HTTP_ROUTE: Final = "http.route"
ATTR_WORKSPACE_ID: Final = "workspaceId"
ATTR_PRINCIPAL_ID: Final = "principalId"
ATTR_CORRELATION_ID: Final = "correlationId"
ATTR_DECISION_AUDIT_EVENT_ID: Final = "decisionAuditEventId"

#: ``gateway_requests_total{route,method,status}`` — one count per completed
#: request, success or error (the locked taxonomy status code is the label).
REQUESTS_TOTAL: Final[Counter] = _meter.create_counter(
    name="gateway_requests_total",
    description="Count of proxied gateway requests by route, method, and status.",
    unit="1",
)

#: ``gateway_rate_limit_denials_total`` — incremented on every ``429`` the
#: per-principal/workspace token buckets raise.
RATE_LIMIT_DENIALS_TOTAL: Final[Counter] = _meter.create_counter(
    name="gateway_rate_limit_denials_total",
    description="Count of requests rejected by the gateway rate limiter.",
    unit="1",
)

#: ``gateway_idempotency_replays_total`` — incremented when a write replays a
#: previously-completed response from the idempotency store.
IDEMPOTENCY_REPLAYS_TOTAL: Final[Counter] = _meter.create_counter(
    name="gateway_idempotency_replays_total",
    description="Count of write requests served from the idempotency replay cache.",
    unit="1",
)

#: ``gateway_request_duration_seconds`` — end-to-end gateway handling latency.
REQUEST_DURATION_SECONDS: Final[Histogram] = _meter.create_histogram(
    name="gateway_request_duration_seconds",
    description="End-to-end gateway request handling duration in seconds.",
    unit="s",
)

#: ``gateway_downstream_duration_seconds`` — time spent in the Dapr forward to
#: the owning component, isolating downstream latency from gateway overhead.
DOWNSTREAM_DURATION_SECONDS: Final[Histogram] = _meter.create_histogram(
    name="gateway_downstream_duration_seconds",
    description="Downstream Dapr service-invocation duration in seconds.",
    unit="s",
)


class RequestTelemetry:
    """Per-request telemetry handle bound to the active gateway span.

    Yielded by :func:`request_telemetry`; the forwarder stamps the
    authorize-time ids and the final status on it as the pipeline learns them.
    The span attributes are set eagerly; :data:`REQUESTS_TOTAL` and
    :data:`REQUEST_DURATION_SECONDS` are recorded when the context closes.
    """

    __slots__ = ("_span", "_status")

    def __init__(self, span: Span, *, method: str, route: str) -> None:
        self._span = span
        # Default to 500 so an unexpected (non-:class:`GatewayError`) failure
        # that escapes the forwarder is still counted as a server error.
        self._status = 500
        span.set_attribute(ATTR_HTTP_METHOD, method)
        span.set_attribute(ATTR_HTTP_ROUTE, route)

    @property
    def status(self) -> int:
        return self._status

    def set_status(self, status_code: int) -> None:
        """Record the HTTP status the request resolved to (also on the span)."""
        self._status = status_code
        self._span.set_attribute("http.status_code", status_code)

    def set_caller(
        self, *, workspace_id: str, principal_id: str, decision_audit_event_id: str
    ) -> None:
        """Stamp the authorized caller's ids onto the span."""
        self._span.set_attribute(ATTR_WORKSPACE_ID, workspace_id)
        self._span.set_attribute(ATTR_PRINCIPAL_ID, principal_id)
        self._span.set_attribute(ATTR_DECISION_AUDIT_EVENT_ID, decision_audit_event_id)

    def set_correlation_id(self, correlation_id: str | None) -> None:
        """Stamp the request correlation id onto the span when one is bound."""
        if correlation_id is not None:
            self._span.set_attribute(ATTR_CORRELATION_ID, correlation_id)


@contextmanager
def request_telemetry(*, method: str, route: str) -> Iterator[RequestTelemetry]:
    """Open the per-request span and record the request counter + duration.

    On exit (success or error) it increments :data:`REQUESTS_TOTAL` tagged with
    ``{route, method, status}`` and observes :data:`REQUEST_DURATION_SECONDS`.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(SPAN_NAME) as span:
        telemetry = RequestTelemetry(span, method=method, route=route)
        try:
            yield telemetry
        finally:
            duration = time.perf_counter() - start
            REQUESTS_TOTAL.add(
                1, {"route": route, "method": method, "status": str(telemetry.status)}
            )
            REQUEST_DURATION_SECONDS.record(duration, {"route": route, "method": method})


@contextmanager
def instrument_downstream(*, app_id: str) -> Iterator[None]:
    """Time the Dapr forward and observe :data:`DOWNSTREAM_DURATION_SECONDS`."""
    start = time.perf_counter()
    try:
        yield
    finally:
        DOWNSTREAM_DURATION_SECONDS.record(time.perf_counter() - start, {"app": app_id})


def record_rate_limit_denial(*, route: str, method: str) -> None:
    """Increment :data:`RATE_LIMIT_DENIALS_TOTAL` for a rate-limited request."""
    RATE_LIMIT_DENIALS_TOTAL.add(1, {"route": route, "method": method})


def record_idempotency_replay(*, route: str, method: str) -> None:
    """Increment :data:`IDEMPOTENCY_REPLAYS_TOTAL` for a replayed write."""
    IDEMPOTENCY_REPLAYS_TOTAL.add(1, {"route": route, "method": method})
