"""Unit tests for :mod:`custos_connector.listen.normalizer` (CONN-IMPL-025, #308).

Covers the public contract:

* happy path → :class:`NormalizedEvent` carries the expected envelope
  fields and ``source.receivedAt`` is a canonical ISO-8601 string;
* missing / empty / non-string ``eventId`` → :class:`MissingEventIdError`;
* missing / empty / non-string ``eventType`` → :class:`MissingEventTypeError`;
* ``eventType`` not in catalog → :class:`UnknownEventTypeError`;
* non-mapping plugin event → :class:`MalformedEventError`;
* invalid ``delivery_mode`` → :class:`ValueError` (caller bug);
* payload extraction strips ``eventId`` / ``eventType``;
* ``to_wire`` produces the canonical wire shape.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from custos_connector.listen import (
    DELIVERY_MODE_PULL,
    DELIVERY_MODE_PUSH,
    SCHEMA_VERSION,
    EventNormalizer,
    MalformedEventError,
    MissingEventIdError,
    MissingEventTypeError,
    UnknownEventTypeError,
)

_WORKSPACE = "ws-1"
_INSTANCE = "inst-1"
_CATALOG = ("oci.image.pushed", "oci.image.deleted")


def _normalizer() -> EventNormalizer:
    return EventNormalizer(produced_event_types=_CATALOG)


def test_normalize_happy_path_pull() -> None:
    fixed = datetime(2026, 5, 27, 12, 30, 45, tzinfo=UTC)
    normalized = _normalizer().normalize(
        {
            "eventId": "evt-123",
            "eventType": "oci.image.pushed",
            "repository": "library/nginx",
            "digest": "sha256:abc",
        },
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE,
        delivery_mode=DELIVERY_MODE_PULL,
        received_at=fixed,
    )
    assert normalized.schema_version == SCHEMA_VERSION
    assert normalized.event_id == "evt-123"
    assert normalized.event_type == "oci.image.pushed"
    assert normalized.source["workspaceId"] == _WORKSPACE
    assert normalized.source["instanceId"] == _INSTANCE
    assert normalized.source["deliveryMode"] == DELIVERY_MODE_PULL
    assert normalized.source["receivedAt"] == "2026-05-27T12:30:45+00:00"
    # Envelope keys stripped from payload, plugin fields preserved.
    assert "eventId" not in normalized.payload
    assert "eventType" not in normalized.payload
    assert normalized.payload["repository"] == "library/nginx"
    assert normalized.payload["digest"] == "sha256:abc"


def test_normalize_happy_path_push_to_wire() -> None:
    """Wire form matches the spec from issue #308 acceptance criteria."""
    fixed = datetime(2026, 1, 1, tzinfo=UTC)
    wire = (
        _normalizer()
        .normalize(
            {"eventId": "e1", "eventType": "oci.image.pushed", "foo": 1},
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode=DELIVERY_MODE_PUSH,
            received_at=fixed,
        )
        .to_wire()
    )
    assert wire == {
        "schemaVersion": "1",
        "eventId": "e1",
        "eventType": "oci.image.pushed",
        "source": {
            "workspaceId": _WORKSPACE,
            "instanceId": _INSTANCE,
            "deliveryMode": "push",
            "receivedAt": "2026-01-01T00:00:00+00:00",
        },
        "payload": {"foo": 1},
    }


def test_normalize_default_received_at_is_utc_now() -> None:
    normalized = _normalizer().normalize(
        {"eventId": "e1", "eventType": "oci.image.pushed"},
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE,
        delivery_mode=DELIVERY_MODE_PULL,
    )
    # The default clock returns timezone-aware UTC; the formatted
    # string ends with ``+00:00``.
    assert normalized.source["receivedAt"].endswith("+00:00")


def test_normalize_naive_received_at_treated_as_utc() -> None:
    naive = datetime(2026, 1, 1, 0, 0, 0)
    normalized = _normalizer().normalize(
        {"eventId": "e1", "eventType": "oci.image.pushed"},
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE,
        delivery_mode=DELIVERY_MODE_PULL,
        received_at=naive,
    )
    assert normalized.source["receivedAt"] == "2026-01-01T00:00:00+00:00"


@pytest.mark.parametrize("event_id", [None, "", 123, 1.5, []])
def test_normalize_missing_event_id_raises(event_id: object) -> None:
    raw: dict[str, object] = {"eventType": "oci.image.pushed"}
    if event_id is not None:
        raw["eventId"] = event_id
    with pytest.raises(MissingEventIdError) as excinfo:
        _normalizer().normalize(
            raw,
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode=DELIVERY_MODE_PULL,
        )
    assert excinfo.value.reason == "missing-event-id"


@pytest.mark.parametrize("event_type", [None, "", 0, []])
def test_normalize_missing_event_type_raises(event_type: object) -> None:
    raw: dict[str, object] = {"eventId": "e1"}
    if event_type is not None:
        raw["eventType"] = event_type
    with pytest.raises(MissingEventTypeError) as excinfo:
        _normalizer().normalize(
            raw,
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode=DELIVERY_MODE_PULL,
        )
    assert excinfo.value.reason == "missing-event-type"


def test_normalize_unknown_event_type_raises() -> None:
    with pytest.raises(UnknownEventTypeError) as excinfo:
        _normalizer().normalize(
            {"eventId": "e1", "eventType": "not.in.catalog"},
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode=DELIVERY_MODE_PULL,
        )
    assert excinfo.value.reason == "unknown-event-type"
    assert excinfo.value.event_type == "not.in.catalog"


def test_normalize_non_mapping_raises_malformed() -> None:
    with pytest.raises(MalformedEventError):
        _normalizer().normalize(
            "not-a-dict",
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode=DELIVERY_MODE_PULL,
        )
    with pytest.raises(MalformedEventError):
        _normalizer().normalize(
            ["list", "of", "stuff"],
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode=DELIVERY_MODE_PULL,
        )


def test_normalize_invalid_delivery_mode_raises_value_error() -> None:
    with pytest.raises(ValueError):
        _normalizer().normalize(
            {"eventId": "e1", "eventType": "oci.image.pushed"},
            workspace_id=_WORKSPACE,
            instance_id=_INSTANCE,
            delivery_mode="bogus",
        )


def test_normalize_payload_is_immutable() -> None:
    normalized = _normalizer().normalize(
        {"eventId": "e1", "eventType": "oci.image.pushed", "k": "v"},
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE,
        delivery_mode=DELIVERY_MODE_PULL,
    )
    with pytest.raises(TypeError):
        normalized.payload["k"] = "x"  # type: ignore[index]
    with pytest.raises(TypeError):
        normalized.source["workspaceId"] = "other"  # type: ignore[index]
