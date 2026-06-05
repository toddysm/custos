"""Tests for OTel self-instrumentation (OBS-IMPL-015).

Cover the operational ``/metrics`` exposition, the ``obs.*`` event → metric
mapping, the ``instrument_emit`` seam, span no-op safety, and the OTel-SDK
mirror (using an in-memory reader so no global provider is mutated).
"""

from __future__ import annotations

from datetime import UTC, datetime

from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from custos_obs._telemetry import (
    ALERT_DISPATCHED,
    ALERT_FAILED,
    PROMETHEUS_CONTENT_TYPE,
    ServiceMetrics,
    build_metrics_router,
    get_meter,
    get_tracer,
    instrument_emit,
    span,
)
from custos_obs.events import (
    AlertDispatched,
    AlertFailed,
    ExporterConfigApplied,
    ExporterConfigRejected,
    OutboxLagging,
    RetentionApplied,
)

_AT = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def _render(metrics: ServiceMetrics) -> str:
    return metrics.render()


# ----- tracer / meter / span -----


def test_get_tracer_and_meter_return_objects() -> None:
    assert get_tracer() is not None
    assert get_meter() is not None


def test_span_is_noop_safe_and_yields_a_span() -> None:
    with span("custos_obs.test", run_id="run-1", attempt=1) as current:
        assert current is not None


def test_span_records_and_reraises_exceptions() -> None:
    sentinel = ValueError("boom")
    try:
        with span("custos_obs.test"):
            raise sentinel
    except ValueError as exc:
        assert exc is sentinel
    else:  # pragma: no cover - the span must re-raise
        raise AssertionError("span swallowed the exception")


# ----- render shape -----


def test_render_emits_all_metric_families_even_when_empty() -> None:
    text = _render(ServiceMetrics())
    for name in (
        "custos_obs_audit_outbox_lag_rows",
        "custos_obs_audit_retention_last_run_timestamp_seconds",
        "custos_obs_audit_retention_rows_deleted_total",
        "custos_obs_exporter_config_status",
        "custos_obs_exporter_config_changes_total",
        "custos_obs_alert_dispatch_total",
    ):
        assert f"# TYPE {name} " in text
    # Counters seed both retention kinds at zero.
    assert 'custos_obs_audit_retention_rows_deleted_total{kind="audit"} 0' in text
    assert 'custos_obs_audit_retention_rows_deleted_total{kind="outbox"} 0' in text
    assert text.endswith("\n")


# ----- observe_event mapping -----


def test_observe_outbox_lagging_sets_lag_gauge() -> None:
    metrics = ServiceMetrics()
    metrics.observe_event(
        OutboxLagging(pipeline_id="audit-1", lag_rows=42, threshold_rows=10).to_audit_event(
            occurred_at=_AT
        )
    )
    assert 'custos_obs_audit_outbox_lag_rows{pipeline_id="audit-1"} 42' in _render(metrics)


def test_observe_retention_applied_sets_last_run_and_rows() -> None:
    metrics = ServiceMetrics()
    metrics.observe_event(
        RetentionApplied(audit_rows_deleted=3, outbox_rows_deleted=5).to_audit_event(
            occurred_at=_AT
        )
    )
    text = _render(metrics)
    assert f"custos_obs_audit_retention_last_run_timestamp_seconds {_AT.timestamp()}" in text
    assert 'custos_obs_audit_retention_rows_deleted_total{kind="audit"} 3' in text
    assert 'custos_obs_audit_retention_rows_deleted_total{kind="outbox"} 5' in text


def test_observe_exporter_applied_then_rejected_tracks_status_and_counts() -> None:
    metrics = ServiceMetrics()
    metrics.observe_event(
        ExporterConfigApplied(configmap="tenant-a", exporter_names=("otlp",)).to_audit_event(
            occurred_at=_AT
        )
    )
    assert 'custos_obs_exporter_config_status{configmap="tenant-a"} 1' in _render(metrics)

    metrics.observe_event(
        ExporterConfigRejected(configmap="tenant-a", reason="bad yaml").to_audit_event(
            occurred_at=_AT
        )
    )
    text = _render(metrics)
    assert 'custos_obs_exporter_config_status{configmap="tenant-a"} 0' in text
    assert (
        'custos_obs_exporter_config_changes_total{configmap="tenant-a",outcome="applied"} 1' in text
    )
    assert (
        'custos_obs_exporter_config_changes_total{configmap="tenant-a",outcome="rejected"} 1'
        in text
    )


def test_observe_alert_outcomes_increment_counter() -> None:
    metrics = ServiceMetrics()
    metrics.observe_event(
        AlertDispatched(rule_name="r1", sink="webhook", audit_event_id="e1").to_audit_event(
            occurred_at=_AT
        )
    )
    metrics.observe_event(
        AlertDispatched(rule_name="r1", sink="webhook", audit_event_id="e2").to_audit_event(
            occurred_at=_AT
        )
    )
    metrics.observe_event(
        AlertFailed(
            rule_name="r1", sink="webhook", audit_event_id="e3", reason="503"
        ).to_audit_event(occurred_at=_AT)
    )
    text = _render(metrics)
    assert (
        f'custos_obs_alert_dispatch_total{{outcome="{ALERT_DISPATCHED}",'
        'rule="r1",sink="webhook"} 2' in text
    )
    assert (
        f'custos_obs_alert_dispatch_total{{outcome="{ALERT_FAILED}",'
        'rule="r1",sink="webhook"} 1' in text
    )


def test_observe_ignores_non_obs_events() -> None:
    metrics = ServiceMetrics()
    foreign = AuditEvent(
        workspace_id=WorkspaceId("ws"),
        event_id="x",
        event_type="workflow.run.started",
        actor="user:alice",
        subject={"run_id": "run-1"},
        payload={"foo": "bar"},
        occurred_at=_AT,
    )
    before = _render(metrics)
    metrics.observe_event(foreign)
    assert _render(metrics) == before


def test_label_values_are_escaped() -> None:
    metrics = ServiceMetrics()
    metrics.set_drainer_lag('weird"\\name', 7)
    assert 'pipeline_id="weird\\"\\\\name"' in _render(metrics)


# ----- instrument_emit seam -----


async def test_instrument_emit_observes_then_forwards() -> None:
    metrics = ServiceMetrics()
    seen: list[AuditEvent] = []

    async def inner(event: AuditEvent) -> None:
        seen.append(event)

    wrapped = instrument_emit(inner, metrics)
    event = OutboxLagging(pipeline_id="audit-1", lag_rows=9, threshold_rows=1).to_audit_event(
        occurred_at=_AT
    )
    await wrapped(event)

    assert seen == [event]
    assert 'custos_obs_audit_outbox_lag_rows{pipeline_id="audit-1"} 9' in _render(metrics)


# ----- /metrics router -----


def test_metrics_router_serves_prometheus_text() -> None:
    metrics = ServiceMetrics()
    metrics.set_drainer_lag("audit-1", 5)
    app = FastAPI()
    app.include_router(build_metrics_router(metrics))
    client = TestClient(app)

    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == PROMETHEUS_CONTENT_TYPE
    assert 'custos_obs_audit_outbox_lag_rows{pipeline_id="audit-1"} 5' in resp.text


def test_create_app_exposes_metrics_without_callctx() -> None:
    from custos_obs import create_app

    app = create_app()
    client = TestClient(app)
    # No call-context header: /metrics bypasses the middleware.
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "custos_obs_alert_dispatch_total" in resp.text


# ----- OTel SDK mirror (in-memory, no global provider mutation) -----


def _metric_names(reader: InMemoryMetricReader) -> set[str]:
    data = reader.get_metrics_data()
    names: set[str] = set()
    if data is None:
        return names
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                names.add(metric.name)
    return names


def test_metrics_mirror_to_an_otel_sdk_meter() -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("custos_obs")
    metrics = ServiceMetrics(meter=meter)

    metrics.set_drainer_lag("audit-1", 11)
    metrics.mark_retention_run(
        occurred_at_epoch=_AT.timestamp(), audit_rows_deleted=2, outbox_rows_deleted=4
    )
    metrics.set_exporter_status("tenant-a", applied=True)
    metrics.record_alert(outcome=ALERT_DISPATCHED, sink="webhook", rule="r1")

    names = _metric_names(reader)
    assert "custos_obs.audit.outbox.lag" in names
    assert "custos_obs.audit.retention.last_run" in names
    assert "custos_obs.exporter.config.status" in names
    assert "custos_obs.alert.dispatch" in names
    assert "custos_obs.audit.retention.rows_deleted" in names
    provider.shutdown()


def test_render_labels_is_blank_without_labels() -> None:
    from custos_obs import _telemetry

    assert _telemetry._render_labels({}) == ""


def test_observable_callbacks_reflect_current_state() -> None:
    from opentelemetry.metrics import CallbackOptions

    opts = CallbackOptions()
    metrics = ServiceMetrics()

    # Empty state: gauges observe nothing (retention has never run).
    assert list(metrics._observe_lag(opts)) == []
    assert list(metrics._observe_retention_last_run(opts)) == []
    assert list(metrics._observe_exporter_status(opts)) == []

    metrics.set_drainer_lag("audit-1", 4)
    metrics.mark_retention_run(occurred_at_epoch=_AT.timestamp())
    metrics.set_exporter_status("tenant-a", applied=True)

    assert [(o.value, o.attributes) for o in metrics._observe_lag(opts)] == [
        (4, {"pipeline_id": "audit-1"})
    ]
    assert [o.value for o in metrics._observe_retention_last_run(opts)] == [_AT.timestamp()]
    assert [(o.value, o.attributes) for o in metrics._observe_exporter_status(opts)] == [
        (1, {"configmap": "tenant-a"})
    ]
