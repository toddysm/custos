"""Pydantic wire models for the read-back Query API (OBS-IMPL-012).

These models are the JSON contract for the five read-back routes the later
phases mount:

* ``GET /v1/workspaces/{ws}/runs/{runId}/logs/tail`` — SSE frames of
  :class:`LogRecordModel` (OBS-IMPL-013).
* ``GET /v1/workspaces/{ws}/runs/{runId}/logs`` — paged :class:`LogPageModel`
  (OBS-IMPL-013).
* ``GET /v1/workspaces/{ws}/runs/{runId}/metrics`` — :class:`MetricSeriesModel`
  (OBS-IMPL-014).
* ``GET /v1/workspaces/{ws}/audit`` — paged :class:`AuditEventPageModel`
  (OBS-IMPL-014).
* ``GET /v1/workspaces/{ws}/audit/{eventId}`` — :class:`AuditEventModel`
  (OBS-IMPL-014).

Each model owns a ``from_domain`` classmethod that projects the corresponding
SPL value object (``custos_spl.LogRecord`` / ``LogPage`` / ``MetricSeries`` /
``AuditEvent`` / ``Page``) onto the wire. Keeping the projection here — rather
than in the routes — means the SPL contract and the public JSON shape evolve in
one place. The cursor is surfaced as its opaque ``token`` string; clients echo
it back verbatim on the next request.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from custos_spl import AuditEvent, Page
    from custos_spl.interfaces.log_query import LogPage, LogRecord
    from custos_spl.interfaces.metrics_query import MetricSample, MetricSeries


class LogRecordModel(BaseModel):
    """A single structured log line for a run (or step)."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    severity: str
    message: str
    run_id: str = Field(alias="runId")
    step_id: str | None = Field(default=None, alias="stepId")
    attributes: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_domain(cls, record: LogRecord) -> LogRecordModel:
        return cls(
            timestamp=record.timestamp,
            severity=record.severity,
            message=record.message,
            runId=str(record.run_id),
            stepId=None if record.step_id is None else str(record.step_id),
            attributes=dict(record.attributes),
        )


class LogPageModel(BaseModel):
    """A page of historical log records plus an optional continuation cursor."""

    model_config = ConfigDict(frozen=True)

    items: list[LogRecordModel]
    next_cursor: str | None = Field(default=None, alias="nextCursor")

    @classmethod
    def from_domain(cls, page: LogPage) -> LogPageModel:
        return cls(
            items=[LogRecordModel.from_domain(record) for record in page.items],
            nextCursor=None if page.next_cursor is None else page.next_cursor.token,
        )


class MetricSampleModel(BaseModel):
    """A single timestamped metric sample."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    value: float
    labels: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_domain(cls, sample: MetricSample) -> MetricSampleModel:
        return cls(
            timestamp=sample.timestamp,
            value=sample.value,
            labels=dict(sample.labels),
        )


class MetricSeriesModel(BaseModel):
    """A named metric series — its label set and ordered samples."""

    model_config = ConfigDict(frozen=True)

    name: str
    labels: dict[str, str] = Field(default_factory=dict)
    samples: list[MetricSampleModel]

    @classmethod
    def from_domain(cls, series: MetricSeries) -> MetricSeriesModel:
        return cls(
            name=series.name,
            labels=dict(series.labels),
            samples=[MetricSampleModel.from_domain(sample) for sample in series.samples],
        )


class AuditEventModel(BaseModel):
    """A single audit event recorded against a workspace."""

    model_config = ConfigDict(frozen=True)

    workspace_id: str = Field(alias="workspaceId")
    event_id: str = Field(alias="eventId")
    event_type: str = Field(alias="eventType")
    actor: str
    subject: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, object] = Field(default_factory=dict)
    occurred_at: datetime = Field(alias="occurredAt")

    @classmethod
    def from_domain(cls, event: AuditEvent) -> AuditEventModel:
        return cls(
            workspaceId=str(event.workspace_id),
            eventId=event.event_id,
            eventType=event.event_type,
            actor=event.actor,
            subject=dict(event.subject),
            payload=dict(event.payload),
            occurredAt=event.occurred_at,
        )


class AuditEventPageModel(BaseModel):
    """A page of audit events plus an optional continuation cursor."""

    model_config = ConfigDict(frozen=True)

    items: list[AuditEventModel]
    next_cursor: str | None = Field(default=None, alias="nextCursor")

    @classmethod
    def from_domain(cls, page: Page[AuditEvent]) -> AuditEventPageModel:
        return cls(
            items=[AuditEventModel.from_domain(event) for event in page.items],
            nextCursor=None if page.next_cursor is None else page.next_cursor.token,
        )


__all__ = [
    "AuditEventModel",
    "AuditEventPageModel",
    "LogPageModel",
    "LogRecordModel",
    "MetricSampleModel",
    "MetricSeriesModel",
]
