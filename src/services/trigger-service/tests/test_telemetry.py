"""Telemetry tests (TS-IMPL-019).

Bootstraps the OpenTelemetry SDK with in-memory exporters *before* importing the
:mod:`custos_trigger._telemetry` module so its module-level tracer/meter bind to
the SDK providers, then drives the dispatcher through every pipeline outcome and
asserts a counter data point (and a span) is recorded for each ``(stage,
outcome)`` pair.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# --- SDK bootstrap (must run before importing the telemetry module) ----------
_SPAN_EXPORTER = InMemorySpanExporter()
_TRACE_PROVIDER = TracerProvider()
_TRACE_PROVIDER.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
trace.set_tracer_provider(_TRACE_PROVIDER)

_METRIC_READER = InMemoryMetricReader()
_METER_PROVIDER = MeterProvider(metric_readers=[_METRIC_READER])
metrics.set_meter_provider(_METER_PROVIDER)

from custos_trigger import _telemetry as telemetry  # noqa: E402
from custos_trigger.clients.workflow import (  # noqa: E402
    RaiseExternalEventRequest,
    RunRef,
    StartRunRequest,
    WorkflowClientStatusError,
)
from custos_trigger.dedup import Deduplicator  # noqa: E402
from custos_trigger.events import EventSource, NormalizedEvent  # noqa: E402
from custos_trigger.models import (  # noqa: E402
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
)
from custos_trigger.pipeline.dispatch import Dispatcher, DispatchStatus  # noqa: E402
from custos_trigger.pipeline.match_resume import ResumeMatch  # noqa: E402
from custos_trigger.pipeline.match_start import StartMatch  # noqa: E402
from custos_trigger.providers import InMemoryTriggerMetadataStore  # noqa: E402

pytestmark = pytest.mark.asyncio

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
_METRIC_NAME = "custos_trigger_pipeline_events_total"


def _event(*, event_id: str = "evt-1", data: dict[str, Any] | None = None) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=event_id,
        source=EventSource(type=SourceType.INTERNAL, occurred_at="2026-06-04T12:00:00Z"),
        kind="workflow.completed",
        data=data or {},
    )


def _start_match(*, target_version: str | None = "wfv-1") -> StartMatch:
    sub = Subscription(
        workspace_id="ws-1",
        subscription_id="sub-1",
        kind=SubscriptionKind.START,
        source_type=SourceType.INTERNAL,
        workflow_id="wf-1",
        target_workflow_version_id=target_version,
        input_mapping={},
        state=SubscriptionState.ACTIVE,
        created_at=_NOW,
        updated_at=_NOW,
    )
    return StartMatch(subscription=sub)


def _resume_match() -> ResumeMatch:
    reg = ResumeRegistration(run_id="run-9", step_id="step-3", event_key="workflow.completed")
    return ResumeMatch(resume_id="res-1", registration=reg)


@dataclass(slots=True)
class _FakeClient:
    fail: bool = False
    run_ref: RunRef = field(
        default_factory=lambda: RunRef(
            run_id="run-1", status="queued", workspace_id="ws-1", workflow_version_id="wfv-1"
        )
    )

    async def start_run(self, request: StartRunRequest) -> RunRef:
        if self.fail:
            raise WorkflowClientStatusError("boom", status_code=400)
        return self.run_ref

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        if self.fail:
            raise WorkflowClientStatusError("boom", status_code=400)
        return None


def _dispatcher(client: object, dedup: Deduplicator) -> Dispatcher:
    return Dispatcher(
        client,  # type: ignore[arg-type]
        dedup,
        max_fanout_depth=2,
        audit=telemetry.TelemetryAuditSink(),
    )


def _counter_points() -> dict[tuple[str, str], float]:
    """Collect ``(stage, outcome) -> value`` from the in-memory metric reader."""
    data = _METRIC_READER.get_metrics_data()
    points: dict[tuple[str, str], float] = {}
    if data is None:
        return points
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name != _METRIC_NAME:
                    continue
                for point in metric.data.data_points:
                    attributes = {str(k): v for k, v in (point.attributes or {}).items()}
                    key = (str(attributes["stage"]), str(attributes["outcome"]))
                    points[key] = float(getattr(point, "value", 0))
    return points


def _span_names() -> set[str]:
    return {span.name for span in _SPAN_EXPORTER.get_finished_spans()}


async def test_every_pipeline_outcome_records_counter_and_span() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)

    # match + dispatched
    healthy = _dispatcher(_FakeClient(), dedup)
    first = await healthy.dispatch_start(_event(), _start_match())
    assert first.status is DispatchStatus.DISPATCHED

    # match + deduped (replay of the same subscription/event)
    second = await healthy.dispatch_start(_event(), _start_match())
    assert second.status is DispatchStatus.DUPLICATE

    # match + failed (start subscription with no resolved target version)
    missing = await healthy.dispatch_start(_event(), _start_match(target_version=None))
    assert missing.status is DispatchStatus.DEAD_LETTERED

    # match + loop_rejected (fan-out depth above the limit)
    looped = await healthy.dispatch_start(_event(event_id="evt-loop"), _start_match(), depth=5)
    assert looped.status is DispatchStatus.LOOP_REJECTED

    # match + resume_delivered
    resume = await healthy.dispatch_resume(
        _event(event_id="evt-2", data={"x": "y"}), _resume_match(), workspace_id="ws-1"
    )
    assert resume.status is DispatchStatus.DISPATCHED

    points = _counter_points()
    expected = {
        (telemetry.STAGE_MATCH, "matched"),
        (telemetry.STAGE_DEDUP, "duplicate"),
        (telemetry.STAGE_DISPATCH, "dispatched"),
        (telemetry.STAGE_DISPATCH, "resume_delivered"),
        (telemetry.STAGE_DISPATCH, "failed"),
        (telemetry.STAGE_DISPATCH, "loop_rejected"),
    }
    assert expected <= set(points)
    # Five matches were processed across the scenarios above.
    assert points[(telemetry.STAGE_MATCH, "matched")] >= 5

    span_names = _span_names()
    assert f"custos_trigger.pipeline.{telemetry.STAGE_MATCH}" in span_names
    assert f"custos_trigger.pipeline.{telemetry.STAGE_DISPATCH}" in span_names
    assert f"custos_trigger.pipeline.{telemetry.STAGE_DEDUP}" in span_names


async def test_telemetry_sink_forwards_to_inner_sink() -> None:
    @dataclass(slots=True)
    class _Recording:
        events: list[str] = field(default_factory=list)

        async def emit(
            self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
        ) -> None:
            self.events.append(event_name)

    inner = _Recording()
    sink = telemetry.TelemetryAuditSink(inner)

    await sink.emit("trigger.matched", workspace_id="ws-1", attributes={})
    await sink.emit("custom.unmapped", workspace_id="ws-1", attributes={})

    assert inner.events == ["trigger.matched", "custom.unmapped"]
    points = _counter_points()
    # Unmapped events are recorded under the "other" stage rather than dropped.
    assert (telemetry.STAGE_OTHER, "custom.unmapped") in points


async def test_default_inner_sink_is_noop() -> None:
    from custos_trigger.pipeline.dispatch import NoopAuditSink

    sink = telemetry.TelemetryAuditSink()
    assert isinstance(sink._inner, NoopAuditSink)
