"""Tests for the ``NormalizedEvent`` envelope (TS-IMPL-007)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from custos_trigger.events import EventRaw, EventSource, NormalizedEvent
from custos_trigger.models import SourceType
from custos_trigger.taxonomy import InvalidKindError


def _event() -> NormalizedEvent:
    return NormalizedEvent(
        event_id="evt-1",
        source=EventSource(
            type=SourceType.VENDOR_PUSH,
            connector_instance_id="prod-registry",
            subscription_id="sub-1",
            vendor="ghcr",
            occurred_at="2026-05-16T12:00:00Z",
        ),
        kind="registry.push",
        subject="ghcr.io/acme/app@sha256:abc",
        data={"repository": "ghcr.io/acme/app"},
        raw=EventRaw(headers={"x-id": "1"}, body="{}"),
    )


def test_normalized_event_json_round_trips() -> None:
    event = _event()
    restored = NormalizedEvent.model_validate_json(event.model_dump_json(by_alias=True))
    assert restored == event


def test_normalized_event_serializes_camel_case_aliases() -> None:
    dumped = _event().model_dump(by_alias=True)
    assert "schemaVersion" in dumped
    assert "eventId" in dumped
    assert "connectorInstanceId" in dumped["source"]
    assert "occurredAt" in dumped["source"]


def test_normalized_event_defaults() -> None:
    event = NormalizedEvent(
        event_id="evt-2",
        source=EventSource(type=SourceType.MANUAL, occurred_at="2026-05-16T12:00:00Z"),
        kind="manual.fire",
    )
    assert event.schema_version == "1"
    assert event.subject == ""
    assert event.data == {}
    assert event.raw == EventRaw()


def test_event_source_optional_fields_default_none() -> None:
    source = EventSource(type=SourceType.MANUAL, occurred_at="2026-05-16T12:00:00Z")
    assert source.connector_instance_id is None
    assert source.subscription_id is None
    assert source.vendor is None


def test_vendor_kind_accepted() -> None:
    event = NormalizedEvent(
        event_id="evt-3",
        source=EventSource(type=SourceType.WEBHOOK, occurred_at="2026-05-16T12:00:00Z"),
        kind="ghcr.image.pushed",
    )
    assert event.kind == "ghcr.image.pushed"


def test_malformed_kind_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        NormalizedEvent(
            event_id="evt-4",
            source=EventSource(type=SourceType.MANUAL, occurred_at="2026-05-16T12:00:00Z"),
            kind="NotAKind",
        )
    assert "kind" in str(exc.value)


def test_platform_collision_kind_rejected() -> None:
    with pytest.raises(ValidationError):
        NormalizedEvent(
            event_id="evt-5",
            source=EventSource(type=SourceType.INTERNAL, occurred_at="2026-05-16T12:00:00Z"),
            kind="workflow.exploded",
        )


def test_invalid_kind_error_is_value_error() -> None:
    assert issubclass(InvalidKindError, ValueError)


def test_extra_field_forbidden() -> None:
    with pytest.raises(ValidationError):
        NormalizedEvent(
            event_id="evt-6",
            source=EventSource(type=SourceType.MANUAL, occurred_at="2026-05-16T12:00:00Z"),
            kind="manual.fire",
            unexpected="boom",  # type: ignore[call-arg]
        )


def test_invalid_source_type_rejected() -> None:
    with pytest.raises(ValidationError):
        EventSource(type="not-a-source", occurred_at="2026-05-16T12:00:00Z")  # type: ignore[arg-type]
