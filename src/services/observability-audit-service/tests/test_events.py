"""Tests for the locked ``obs.*`` audit-event taxonomy + models (OBS-IMPL-003)."""

from __future__ import annotations

from datetime import UTC, datetime

from custos_spl import AuditEvent

from custos_obs.events import (
    COMPONENT,
    LOCKED_OBS_EVENT_NAMES,
    PLATFORM_WORKSPACE_ID,
    SYSTEM_ACTOR,
    AlertDispatched,
    AlertFailed,
    ExporterConfigApplied,
    ExporterConfigRejected,
    ObsEvent,
    ObsEventName,
    OutboxLagging,
    RetentionApplied,
)

_FIXED_TS = datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC)

#: One representative instance of every concrete event model, paired with its
#: locked event name.
_EVENT_INSTANCES: list[tuple[ObsEvent, ObsEventName]] = [
    (
        RetentionApplied(audit_rows_deleted=10, outbox_rows_deleted=3),
        ObsEventName.RETENTION_APPLIED,
    ),
    (
        OutboxLagging(pipeline_id="audit-store", lag_rows=12_000, threshold_rows=10_000),
        ObsEventName.OUTBOX_LAGGING,
    ),
    (
        ExporterConfigRejected(configmap="custos-otel-exporters", reason="unknown type"),
        ObsEventName.EXPORTER_CONFIG_REJECTED,
    ),
    (
        ExporterConfigApplied(
            configmap="custos-otel-exporters", exporter_names=("loki", "datadog")
        ),
        ObsEventName.EXPORTER_CONFIG_APPLIED,
    ),
    (
        AlertDispatched(rule_name="drain-lag", sink="webhook", audit_event_id="evt-1"),
        ObsEventName.ALERT_DISPATCHED,
    ),
    (
        AlertFailed(rule_name="drain-lag", sink="smtp", audit_event_id="evt-1", reason="timeout"),
        ObsEventName.ALERT_FAILED,
    ),
]


def test_locked_event_name_set_matches_enum() -> None:
    assert {member.value for member in ObsEventName} == LOCKED_OBS_EVENT_NAMES


def test_locked_event_name_set_is_exactly_the_design_events() -> None:
    assert {
        "obs.retention.applied",
        "obs.outbox.lagging",
        "obs.exporter.config.rejected",
        "obs.exporter.config.applied",
        "obs.alert.dispatched",
        "obs.alert.failed",
    } == LOCKED_OBS_EVENT_NAMES


def test_every_event_name_is_obs_namespaced() -> None:
    assert all(name.startswith("obs.") for name in LOCKED_OBS_EVENT_NAMES)


def test_each_model_pins_its_event_name() -> None:
    for event, name in _EVENT_INSTANCES:
        assert event.EVENT_NAME is name


def test_models_cover_every_locked_event_name() -> None:
    covered = {event.EVENT_NAME.value for event, _ in _EVENT_INSTANCES}
    assert covered == LOCKED_OBS_EVENT_NAMES


def test_to_audit_event_envelope_defaults() -> None:
    event = RetentionApplied(audit_rows_deleted=10, outbox_rows_deleted=3)
    audit = event.to_audit_event(event_id="fixed-id", occurred_at=_FIXED_TS)
    assert isinstance(audit, AuditEvent)
    assert audit.event_type == "obs.retention.applied"
    assert audit.actor == SYSTEM_ACTOR
    assert audit.workspace_id == PLATFORM_WORKSPACE_ID
    assert audit.event_id == "fixed-id"
    assert audit.occurred_at == _FIXED_TS
    assert audit.subject == {"component": COMPONENT}
    assert audit.payload == {"audit_rows_deleted": 10, "outbox_rows_deleted": 3}


def test_to_audit_event_generates_defaults_when_unset() -> None:
    before = datetime.now(UTC)
    audit = RetentionApplied(audit_rows_deleted=0, outbox_rows_deleted=0).to_audit_event()
    after = datetime.now(UTC)
    assert audit.event_id  # a non-empty generated UUID
    assert before <= audit.occurred_at <= after


def test_to_audit_event_honours_overrides() -> None:
    audit = AlertDispatched(
        rule_name="drain-lag", sink="webhook", audit_event_id="evt-1"
    ).to_audit_event(actor="system:test", workspace_id="ws-7")
    assert audit.actor == "system:test"
    assert audit.workspace_id == "ws-7"


def test_every_model_builds_a_valid_audit_event() -> None:
    for event, name in _EVENT_INSTANCES:
        audit = event.to_audit_event(event_id="e", occurred_at=_FIXED_TS)
        assert audit.event_type == name.value
        assert audit.subject["component"] == COMPONENT
        assert isinstance(audit.payload, dict)


def test_outbox_lagging_payload_and_subject() -> None:
    event = OutboxLagging(pipeline_id="audit-store", lag_rows=12_000, threshold_rows=10_000)
    assert event.subject() == {"component": COMPONENT, "pipeline_id": "audit-store"}
    assert event.payload() == {
        "pipeline_id": "audit-store",
        "lag_rows": 12_000,
        "threshold_rows": 10_000,
    }


def test_exporter_config_rejected_payload_and_subject() -> None:
    event = ExporterConfigRejected(configmap="custos-otel-exporters", reason="unknown type")
    assert event.subject() == {"component": COMPONENT, "configmap": "custos-otel-exporters"}
    assert event.payload() == {"configmap": "custos-otel-exporters", "reason": "unknown type"}


def test_exporter_config_applied_serializes_names_as_list() -> None:
    event = ExporterConfigApplied(
        configmap="custos-otel-exporters", exporter_names=("loki", "datadog")
    )
    assert event.payload() == {
        "configmap": "custos-otel-exporters",
        "exporter_names": ["loki", "datadog"],
    }


def test_alert_dispatched_payload_and_subject() -> None:
    event = AlertDispatched(rule_name="drain-lag", sink="webhook", audit_event_id="evt-1")
    assert event.subject() == {"component": COMPONENT, "rule_name": "drain-lag", "sink": "webhook"}
    assert event.payload() == {
        "rule_name": "drain-lag",
        "sink": "webhook",
        "audit_event_id": "evt-1",
    }


def test_alert_failed_payload_includes_reason() -> None:
    event = AlertFailed(
        rule_name="drain-lag", sink="smtp", audit_event_id="evt-1", reason="timeout"
    )
    assert event.payload() == {
        "rule_name": "drain-lag",
        "sink": "smtp",
        "audit_event_id": "evt-1",
        "reason": "timeout",
    }


def test_models_are_frozen() -> None:
    event = RetentionApplied(audit_rows_deleted=1, outbox_rows_deleted=1)
    try:
        event.audit_rows_deleted = 2  # type: ignore[misc]
    except AttributeError:
        return
    raise AssertionError("expected frozen dataclass to reject mutation")
