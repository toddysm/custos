"""OpenTelemetry instrumentation for the trigger pipeline (TS-IMPL-019).

The trigger pipeline (``Classify -> Match -> Dedup -> Dispatch``) emits a small
set of audit events through the dispatcher's
:class:`~custos_trigger.pipeline.dispatch.AuditSink` Protocol. This module wraps
that surface with :class:`TelemetryAuditSink`, which records an OpenTelemetry
counter (and a short pipeline-stage span) for every event before forwarding it
to an inner sink.

The library depends only on ``opentelemetry-api``. The API ships no-op tracer /
meter providers by default, so instrumentation is inert until an SDK + exporter
is configured at the process boundary (the Helm-managed OTel Collector in
production; the in-memory exporters in :mod:`tests.test_telemetry`). The inner
sink defaults to :class:`~custos_trigger.pipeline.dispatch.NoopAuditSink`; a
durable Observability/Audit forwarder can be injected when one is wired.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter

from custos_trigger.pipeline.dispatch import (
    AUDIT_DEDUPED,
    AUDIT_DISPATCH_FAILED,
    AUDIT_DISPATCHED,
    AUDIT_LOOP_DETECTED,
    AUDIT_MATCHED,
    AUDIT_RESUME_DELIVERED,
    AuditSink,
    NoopAuditSink,
)

__all__ = [
    "PIPELINE_EVENTS_TOTAL",
    "STAGE_DEDUP",
    "STAGE_DISPATCH",
    "STAGE_MATCH",
    "STAGE_OTHER",
    "TelemetryAuditSink",
]

_INSTRUMENTATION_NAME: Final = "custos_trigger"
_INSTRUMENTATION_VERSION: Final = "0.1.0"

_tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)

#: Pipeline stage labels recorded on the counter / span.
STAGE_MATCH: Final = "match"
STAGE_DEDUP: Final = "dedup"
STAGE_DISPATCH: Final = "dispatch"
STAGE_OTHER: Final = "other"

#: One counter for the whole pipeline; the ``stage`` / ``outcome`` attributes
#: distinguish each terminal event so a single time series fans out per pair.
PIPELINE_EVENTS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_trigger_pipeline_events_total",
    description="Count of trigger pipeline audit events by stage and outcome.",
    unit="1",
)

#: Map each dispatcher audit event onto its ``(stage, outcome)`` label pair.
_EVENT_STAGE_OUTCOME: Final[dict[str, tuple[str, str]]] = {
    AUDIT_MATCHED: (STAGE_MATCH, "matched"),
    AUDIT_DEDUPED: (STAGE_DEDUP, "duplicate"),
    AUDIT_DISPATCHED: (STAGE_DISPATCH, "dispatched"),
    AUDIT_RESUME_DELIVERED: (STAGE_DISPATCH, "resume_delivered"),
    AUDIT_DISPATCH_FAILED: (STAGE_DISPATCH, "failed"),
    AUDIT_LOOP_DETECTED: (STAGE_DISPATCH, "loop_rejected"),
}


class TelemetryAuditSink:
    """An :class:`AuditSink` that records OTel telemetry then forwards events.

    For every audit event it increments :data:`PIPELINE_EVENTS_TOTAL` (tagged
    with the event's ``stage`` / ``outcome``) and opens a short pipeline-stage
    span, then delegates to ``inner`` so a durable audit forwarder still sees
    the event. Unmapped event names are recorded under :data:`STAGE_OTHER` with
    the raw event name as the outcome, so nothing is silently dropped.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AuditSink | None = None) -> None:
        self._inner: AuditSink = inner if inner is not None else NoopAuditSink()

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
    ) -> None:
        stage, outcome = _EVENT_STAGE_OUTCOME.get(event_name, (STAGE_OTHER, event_name))
        PIPELINE_EVENTS_TOTAL.add(1, {"stage": stage, "outcome": outcome})
        with _tracer.start_as_current_span(f"custos_trigger.pipeline.{stage}") as span:
            span.set_attribute("trigger.event", event_name)
            span.set_attribute("trigger.stage", stage)
            span.set_attribute("trigger.outcome", outcome)
            span.set_attribute("trigger.workspace_id", workspace_id)
            # Forward inside the span so the inner sink's work (and any child
            # spans it emits) is parented to this stage span and the span
            # duration captures the audit-forwarding latency.
            await self._inner.emit(event_name, workspace_id=workspace_id, attributes=attributes)
