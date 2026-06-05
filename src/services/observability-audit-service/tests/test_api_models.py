"""Tests for the Query API wire models (OBS-IMPL-012).

Each ``from_domain`` projection is checked against a hand-built SPL value object
so the public JSON shape (camelCase aliases, opaque cursor token, plain dict
attributes/labels) stays pinned independently of the SPL internals.
"""

from __future__ import annotations

from datetime import UTC, datetime

from custos_spl import (
    AuditEvent,
    Cursor,
    LogPage,
    LogRecord,
    MetricSample,
    MetricSeries,
    Page,
    RunId,
    StepId,
    WorkspaceId,
)

from custos_obs.api.models import (
    AuditEventModel,
    AuditEventPageModel,
    LogPageModel,
    LogRecordModel,
    MetricSampleModel,
    MetricSeriesModel,
)

_TS = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_log_record_from_domain_with_step() -> None:
    record = LogRecord(
        timestamp=_TS,
        severity="error",
        message="boom",
        run_id=RunId("run-1"),
        step_id=StepId("step-1"),
        attributes={"k": "v"},
    )
    model = LogRecordModel.from_domain(record)
    assert model.run_id == "run-1"
    assert model.step_id == "step-1"
    assert model.severity == "error"
    assert model.attributes == {"k": "v"}
    assert model.model_dump(by_alias=True)["runId"] == "run-1"
    assert model.model_dump(by_alias=True)["stepId"] == "step-1"


def test_log_record_from_domain_without_step() -> None:
    record = LogRecord(
        timestamp=_TS,
        severity="info",
        message="hi",
        run_id=RunId("run-2"),
        step_id=None,
        attributes={},
    )
    model = LogRecordModel.from_domain(record)
    assert model.step_id is None
    assert model.attributes == {}


def test_log_page_from_domain_with_cursor() -> None:
    record = LogRecord(
        timestamp=_TS,
        severity="info",
        message="a",
        run_id=RunId("run-1"),
        step_id=None,
        attributes={},
    )
    page: LogPage = Page(items=(record,), next_cursor=Cursor(token="next-token"))
    model = LogPageModel.from_domain(page)
    assert len(model.items) == 1
    assert model.items[0].message == "a"
    assert model.next_cursor == "next-token"
    assert model.model_dump(by_alias=True)["nextCursor"] == "next-token"


def test_log_page_from_domain_without_cursor() -> None:
    page: LogPage = Page(items=(), next_cursor=None)
    model = LogPageModel.from_domain(page)
    assert model.items == []
    assert model.next_cursor is None


def test_metric_sample_from_domain() -> None:
    sample = MetricSample(timestamp=_TS, value=1.5, labels={"job": "runner"})
    model = MetricSampleModel.from_domain(sample)
    assert model.value == 1.5
    assert model.labels == {"job": "runner"}


def test_metric_series_from_domain() -> None:
    series = MetricSeries(
        name="run_latency_seconds",
        labels={"workspace": "ws-1"},
        samples=(
            MetricSample(timestamp=_TS, value=1.0, labels={}),
            MetricSample(timestamp=_TS, value=2.0, labels={}),
        ),
    )
    model = MetricSeriesModel.from_domain(series)
    assert model.name == "run_latency_seconds"
    assert model.labels == {"workspace": "ws-1"}
    assert [s.value for s in model.samples] == [1.0, 2.0]


def test_audit_event_from_domain() -> None:
    event = AuditEvent(
        workspace_id=WorkspaceId("ws-1"),
        event_id="evt-1",
        event_type="workflow.run.completed",
        actor="user-1",
        subject={"runId": "run-1"},
        payload={"status": "succeeded", "attempts": 3},
        occurred_at=_TS,
    )
    model = AuditEventModel.from_domain(event)
    dumped = model.model_dump(by_alias=True)
    assert dumped["workspaceId"] == "ws-1"
    assert dumped["eventId"] == "evt-1"
    assert dumped["eventType"] == "workflow.run.completed"
    assert dumped["subject"] == {"runId": "run-1"}
    assert dumped["payload"] == {"status": "succeeded", "attempts": 3}
    assert dumped["occurredAt"] == _TS


def test_audit_event_page_from_domain() -> None:
    event = AuditEvent(
        workspace_id=WorkspaceId("ws-1"),
        event_id="evt-1",
        event_type="t",
        actor="a",
        subject={},
        payload={},
        occurred_at=_TS,
    )
    page: Page[AuditEvent] = Page(items=(event,), next_cursor=Cursor(token="c1"))
    model = AuditEventPageModel.from_domain(page)
    assert len(model.items) == 1
    assert model.items[0].event_id == "evt-1"
    assert model.next_cursor == "c1"


def test_audit_event_page_from_domain_empty() -> None:
    page: Page[AuditEvent] = Page(items=(), next_cursor=None)
    model = AuditEventPageModel.from_domain(page)
    assert model.items == []
    assert model.next_cursor is None
