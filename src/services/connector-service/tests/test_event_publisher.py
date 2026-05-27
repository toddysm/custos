"""Unit tests for :mod:`custos_connector.listen.publisher` (CONN-IMPL-025, #308).

Covers the publisher Protocol implementations and the
:class:`PublisherBridge` adapter that both delivery paths share:

* :class:`NoOpEventPublisher` swallows; :class:`RecordingEventPublisher`
  records; :class:`LocalEventBus` fans out to subscribed handlers.
* Bridge happy path normalizes + publishes + audits ``event.normalized``.
* Bridge poison-pill path audits ``event.rejected`` with the stable
  reason code carried on the exception and continues with the next
  event. Cursor advance is NOT blocked.
* Bridge propagates publisher exceptions so :class:`CursorService`
  refuses to commit the cursor.
* Bridge supports both entry points
  (:meth:`__call__` for the pull path,
  :meth:`process_batch` for the push path)
  and produces identical normalized envelopes for the same input.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.audit import (
    EVENT_NORMALIZED,
    EVENT_REJECTED,
)
from custos_connector.listen import (
    DELIVERY_MODE_PULL,
    DELIVERY_MODE_PUSH,
    EventNormalizer,
    LocalEventBus,
    NoOpEventPublisher,
    NormalizedEvent,
    PublisherBridge,
    RecordingEventPublisher,
)
from tests._fakes import FakeMetadataAdapter

pytestmark = pytest.mark.asyncio


_WORKSPACE = "ws-1"
_INSTANCE_ID = "inst-1"
_CATALOG = ("oci.image.pushed",)


def _instance() -> ConnectorInstance:
    """Minimal :class:`ConnectorInstance` for bridge ``__call__`` tests."""
    return ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(_INSTANCE_ID),
        type="http",
        version="1.0.0",
        name="acme",
        lease_ttl_seconds=300,
        enabled=True,
        status="enabled",
        health_status="healthy",
        target_config={},
        credentials_authentication={},
        used_capabilities=(),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _bridge(
    *, publisher: NoOpEventPublisher | RecordingEventPublisher | LocalEventBus
) -> tuple[PublisherBridge, FakeMetadataAdapter]:
    metadata = FakeMetadataAdapter()
    bridge = PublisherBridge(
        normalizer=EventNormalizer(produced_event_types=_CATALOG),
        publisher=publisher,
        metadata_store=metadata,  # type: ignore[arg-type]
        actor="connector-service:listen",
    )
    return bridge, metadata


# ---------------------------------------------------------------------------
# Publisher implementations
# ---------------------------------------------------------------------------


async def test_noop_publisher_does_not_raise() -> None:
    event = NormalizedEvent(
        schema_version="1",
        event_id="e1",
        event_type="oci.image.pushed",
        source={"workspaceId": "ws", "instanceId": "i", "deliveryMode": "pull", "receivedAt": ""},
        payload={},
    )
    await NoOpEventPublisher().publish(event)


async def test_recording_publisher_captures_events() -> None:
    pub = RecordingEventPublisher()
    event = NormalizedEvent(
        schema_version="1",
        event_id="e1",
        event_type="oci.image.pushed",
        source={"workspaceId": "ws", "instanceId": "i", "deliveryMode": "pull", "receivedAt": ""},
        payload={},
    )
    await pub.publish(event)
    await pub.publish(event)
    assert pub.published == [event, event]


async def test_local_event_bus_fans_out_to_subscribed_handlers() -> None:
    bus = LocalEventBus()
    seen_a: list[NormalizedEvent] = []
    seen_b: list[NormalizedEvent] = []

    async def handler_a(event: NormalizedEvent) -> None:
        seen_a.append(event)

    async def handler_b(event: NormalizedEvent) -> None:
        seen_b.append(event)

    bus.subscribe(handler_a)
    bus.subscribe(handler_b)
    event = NormalizedEvent(
        schema_version="1",
        event_id="e1",
        event_type="oci.image.pushed",
        source={"workspaceId": "ws", "instanceId": "i", "deliveryMode": "pull", "receivedAt": ""},
        payload={},
    )
    await bus.publish(event)
    assert seen_a == [event]
    assert seen_b == [event]


async def test_local_event_bus_continues_on_handler_exception() -> None:
    bus = LocalEventBus()

    async def raises(event: NormalizedEvent) -> None:
        raise RuntimeError("boom")

    seen: list[NormalizedEvent] = []

    async def good(event: NormalizedEvent) -> None:
        seen.append(event)

    bus.subscribe(raises)
    bus.subscribe(good)
    event = NormalizedEvent(
        schema_version="1",
        event_id="e1",
        event_type="oci.image.pushed",
        source={"workspaceId": "ws", "instanceId": "i", "deliveryMode": "pull", "receivedAt": ""},
        payload={},
    )
    await bus.publish(event)
    assert seen == [event]


# ---------------------------------------------------------------------------
# PublisherBridge — pull path (__call__)
# ---------------------------------------------------------------------------


async def test_bridge_call_normalizes_and_publishes_and_audits() -> None:
    publisher = RecordingEventPublisher()
    bridge, metadata = _bridge(publisher=publisher)
    raw = [
        {"eventId": "e1", "eventType": "oci.image.pushed", "k": 1},
        {"eventId": "e2", "eventType": "oci.image.pushed", "k": 2},
    ]
    await bridge(WorkspaceId(_WORKSPACE), _instance(), raw)
    assert len(publisher.published) == 2
    assert [e.event_id for e in publisher.published] == ["e1", "e2"]
    normalized_audits = [
        e for _, e in metadata.append_audit_calls if e.event_type == EVENT_NORMALIZED
    ]
    assert len(normalized_audits) == 2
    assert normalized_audits[0].payload["deliveryMode"] == DELIVERY_MODE_PULL
    assert normalized_audits[0].payload["batchIndex"] == 0


async def test_bridge_missing_event_id_audits_rejected_and_continues() -> None:
    """Issue #308 acceptance criterion: missing eventId → event.rejected."""
    publisher = RecordingEventPublisher()
    bridge, metadata = _bridge(publisher=publisher)
    raw = [
        {"eventId": "e1", "eventType": "oci.image.pushed"},  # ok
        {"eventType": "oci.image.pushed"},  # bad: no eventId
        {"eventId": "e3", "eventType": "oci.image.pushed"},  # ok
    ]
    await bridge(WorkspaceId(_WORKSPACE), _instance(), raw)
    # Two events published, one rejected → cursor advance not blocked.
    assert [e.event_id for e in publisher.published] == ["e1", "e3"]
    rejected = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_REJECTED]
    assert len(rejected) == 1
    assert rejected[0].payload["reason"] == "missing-event-id"
    assert rejected[0].payload["batchIndex"] == 1


async def test_bridge_unknown_event_type_audits_rejected() -> None:
    publisher = RecordingEventPublisher()
    bridge, metadata = _bridge(publisher=publisher)
    raw = [{"eventId": "e1", "eventType": "not.in.catalog"}]
    await bridge(WorkspaceId(_WORKSPACE), _instance(), raw)
    assert publisher.published == []
    rejected = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_REJECTED]
    assert len(rejected) == 1
    assert rejected[0].payload["reason"] == "unknown-event-type"
    assert rejected[0].payload["eventType"] == "not.in.catalog"
    assert rejected[0].payload["eventId"] == "e1"


async def test_bridge_publisher_failure_propagates() -> None:
    """``CursorService`` must see the exception so the cursor does not advance."""

    class _Boom:
        async def publish(self, event: NormalizedEvent) -> None:
            raise RuntimeError("publish failed")

    bridge, metadata = _bridge(publisher=_Boom())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="publish failed"):
        await bridge(
            WorkspaceId(_WORKSPACE),
            _instance(),
            [{"eventId": "e1", "eventType": "oci.image.pushed"}],
        )
    # No event.normalized audit because the publish failed before it ran.
    normalized = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_NORMALIZED]
    assert normalized == []


# ---------------------------------------------------------------------------
# PublisherBridge — push path (process_batch)
# ---------------------------------------------------------------------------


async def test_bridge_process_batch_returns_counts() -> None:
    publisher = RecordingEventPublisher()
    bridge, _ = _bridge(publisher=publisher)
    raw = [
        {"eventId": "e1", "eventType": "oci.image.pushed"},
        {"eventType": "oci.image.pushed"},
        {"eventId": "e3", "eventType": "oci.image.pushed"},
    ]
    result = await bridge.process_batch(
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE_ID,
        events=raw,
        delivery_mode=DELIVERY_MODE_PUSH,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert result.accepted == 2
    assert result.rejected == 1


async def test_bridge_pull_and_push_produce_identical_envelopes() -> None:
    """Acceptance criterion for issue #308: both paths emit the same envelope."""
    pull_pub = RecordingEventPublisher()
    push_pub = RecordingEventPublisher()
    metadata = FakeMetadataAdapter()
    normalizer = EventNormalizer(produced_event_types=_CATALOG)
    pull_bridge = PublisherBridge(
        normalizer=normalizer,
        publisher=pull_pub,
        metadata_store=metadata,  # type: ignore[arg-type]
        actor="connector-service:listen",
    )
    push_bridge = PublisherBridge(
        normalizer=normalizer,
        publisher=push_pub,
        metadata_store=metadata,  # type: ignore[arg-type]
        actor="connector-service:webhook",
    )
    received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    raw = [{"eventId": "e1", "eventType": "oci.image.pushed", "repo": "library/nginx"}]

    await pull_bridge.process_batch(
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE_ID,
        events=raw,
        delivery_mode=DELIVERY_MODE_PULL,
        received_at=received_at,
    )
    await push_bridge.process_batch(
        workspace_id=_WORKSPACE,
        instance_id=_INSTANCE_ID,
        events=raw,
        delivery_mode=DELIVERY_MODE_PUSH,
        received_at=received_at,
    )

    assert len(pull_pub.published) == 1
    assert len(push_pub.published) == 1
    pull_event = pull_pub.published[0]
    push_event = push_pub.published[0]
    # Everything except deliveryMode is identical.
    assert pull_event.schema_version == push_event.schema_version
    assert pull_event.event_id == push_event.event_id
    assert pull_event.event_type == push_event.event_type
    assert pull_event.payload == push_event.payload
    assert pull_event.source["workspaceId"] == push_event.source["workspaceId"]
    assert pull_event.source["instanceId"] == push_event.source["instanceId"]
    assert pull_event.source["receivedAt"] == push_event.source["receivedAt"]
    # Only delivery mode differs.
    assert pull_event.source["deliveryMode"] == DELIVERY_MODE_PULL
    assert push_event.source["deliveryMode"] == DELIVERY_MODE_PUSH
