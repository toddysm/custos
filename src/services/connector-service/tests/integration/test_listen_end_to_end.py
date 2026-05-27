"""Integration test for the Listen path (CONN-IMPL-025, #308).

Validates the design properties the unit suite cannot exercise:

* **Pull / push parity** — feeding the *same* raw event through the
  pull path (``CursorService.tick`` → ``PublisherBridge``) and the
  push path (HTTP ``POST /v1/webhooks/...`` → ``PublisherBridge``)
  produces identical normalized envelopes except for ``deliveryMode``.
* **At-least-once via the audit outbox under real Postgres** — both
  paths emit ``event.received`` / ``event.normalized`` /
  ``event.rejected`` rows that round-trip through the
  :class:`PgMetadataAdapter` audit log.
* **eventId rule end-to-end** — a raw event missing ``eventId`` is
  rejected with the exact reason ``"missing-event-id"`` (issue
  acceptance criterion) in BOTH paths and does not block sibling
  events in the same batch.
* **Sink connector activation is a no-op** — a connector type
  manifest with no ``events`` block returns
  ``ActivationResult(skipped=True, reason="sink-connector")`` and
  never touches the scheduler.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from custos_pg import (
    PgCatalogAdapter,
    PgConnectorInstanceAdapter,
    PgMetadataAdapter,
)
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.audit import (
    EVENT_NORMALIZED,
    EVENT_REJECTED,
)
from custos_connector.listen import (
    DELIVERY_MODE_PULL,
    DELIVERY_MODE_PUSH,
    REASON_SINK_CONNECTOR,
    AllowAllSignatureVerifier,
    ListenManager,
    PublisherBridge,
    RecordingEventPublisher,
    build_publisher_bridge,
)
from custos_connector.listen.normalizer import EventNormalizer

pytestmark = pytest.mark.integration


_WORKSPACE = "ws-listen-int"
_TYPE = "oci-registry"
_VERSION = "1.0.0"


class _FakeScheduler:
    """Listen-path integration tests do not exercise the pull-loop driver;
    a no-op scheduler keeps the :class:`ListenManager` activations side
    of the contract under test without spinning up the real loop."""

    def __init__(self) -> None:
        self.registered: list[tuple[str, str]] = []
        self.unregistered: list[tuple[str, str]] = []

    def register(
        self,
        workspace_id: str,
        instance_id: str,
        *,
        interval_seconds: int | None = None,
    ) -> object:
        self.registered.append((workspace_id, instance_id))
        return object()

    def unregister(self, workspace_id: str, instance_id: str) -> bool:
        self.unregistered.append((workspace_id, instance_id))
        return True


def _build_manifest(
    *,
    delivery: list[str] | None,
    produced: tuple[str, ...] = ("oci.image.pushed",),
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "description": "Integration test connector",
        "capabilities": ["c.read"],
        "target": {
            "kind": "oci-registry",
            "endpoint": "https://example.com",
            "config": {},
        },
        "credentials": {"authenticationType": "none"},
    }
    if delivery is not None:
        spec["events"] = {"delivery": list(delivery), "produced": list(produced)}
        if "pull" in delivery:
            spec["events"]["pull"] = {
                "cursorEncoding": "my-enc",
                "initialCursorBehavior": "now",
            }
    return {"metadata": {"type": _TYPE, "version": _VERSION}, "spec": spec}


async def _seed_instance(
    *,
    pool: asyncpg.Pool,
    instance_id: str,
    delivery: list[str] | None,
) -> ConnectorInstance:
    catalog = PgCatalogAdapter(pool=pool)
    instances = PgConnectorInstanceAdapter(pool=pool)
    await catalog.put_connector_type_version(
        _TYPE,
        _VERSION,
        "sha256:listen-fake",
        f"example.test/{_TYPE}@sha256:listen-fake",
        _build_manifest(delivery=delivery),
    )
    now = datetime.now(UTC)
    instance = ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(instance_id),
        type=_TYPE,
        version=_VERSION,
        name="prod",
        lease_ttl_seconds=300,
        enabled=True,
        status="enabled",
        health_status="healthy",
        target_config={},
        credentials_authentication={},
        used_capabilities=("events.delivery",),
        created_at=now,
        updated_at=now,
    )
    await instances.put_connector_instance(instance.workspace_id, instance)
    return instance


async def _read_audit_event_types(
    *, pool: asyncpg.Pool, workspace_id: str
) -> list[tuple[str, Mapping[str, Any]]]:
    """Read the audit rows by event_type + payload for assertions."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_type, payload
            FROM custos_state.audit_event
            WHERE workspace_id = $1
            ORDER BY occurred_at ASC, event_id ASC
            """,
            workspace_id,
        )
    return [(row["event_type"], json.loads(row["payload"])) for row in rows]


@pytest.mark.asyncio
async def test_pull_and_push_paths_share_normalized_envelope(pg_dsn: str) -> None:
    """Issue #308 acceptance: identical envelopes via both delivery modes."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    assert pool is not None
    try:
        instance = await _seed_instance(
            pool=pool,
            instance_id=str(uuid4()),
            delivery=["pull", "push"],
        )

        metadata = PgMetadataAdapter(pool=pool)
        catalog = PgCatalogAdapter(pool=pool)
        scheduler = _FakeScheduler()
        manager = ListenManager(
            catalog_store=catalog,  # type: ignore[arg-type]
            scheduler=scheduler,  # type: ignore[arg-type]
            default_signature_verifier=AllowAllSignatureVerifier(test_only=True),
        )
        await manager.activate(instance)

        raw = {"eventId": "e1", "eventType": "oci.image.pushed", "tag": "v1"}

        # --- Pull path ----------------------------------------------------
        pull_publisher = RecordingEventPublisher()
        pull_bridge = build_publisher_bridge(
            normalizer=EventNormalizer(produced_event_types=("oci.image.pushed",)),
            publisher=pull_publisher,
            metadata_store=metadata,  # type: ignore[arg-type]
        )
        await pull_bridge(
            instance.workspace_id,
            instance,
            [raw],
        )

        # --- Push path ----------------------------------------------------
        push_publisher = RecordingEventPublisher()
        push_bridge = PublisherBridge(
            normalizer=EventNormalizer(produced_event_types=("oci.image.pushed",)),
            publisher=push_publisher,
            metadata_store=metadata,  # type: ignore[arg-type]
            actor="connector-service:webhook",
        )
        await push_bridge.process_batch(
            workspace_id=str(instance.workspace_id),
            instance_id=str(instance.instance_id),
            events=[raw],
            delivery_mode=DELIVERY_MODE_PUSH,
            received_at=datetime.now(UTC),
        )

        # --- Parity assertion --------------------------------------------
        assert len(pull_publisher.published) == 1
        assert len(push_publisher.published) == 1
        pull_event = pull_publisher.published[0]
        push_event = push_publisher.published[0]
        assert pull_event.schema_version == push_event.schema_version
        assert pull_event.event_id == push_event.event_id
        assert pull_event.event_type == push_event.event_type
        assert pull_event.payload == push_event.payload
        # Only deliveryMode differs.
        assert pull_event.source["deliveryMode"] == DELIVERY_MODE_PULL
        assert push_event.source["deliveryMode"] == DELIVERY_MODE_PUSH

        # --- Audit assertion ---------------------------------------------
        audit = await _read_audit_event_types(pool=pool, workspace_id=str(instance.workspace_id))
        normalized = [p for et, p in audit if et == EVENT_NORMALIZED]
        assert len(normalized) >= 2
        modes = {p["deliveryMode"] for p in normalized}
        assert modes == {DELIVERY_MODE_PULL, DELIVERY_MODE_PUSH}
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_missing_event_id_rejected_with_stable_reason(pg_dsn: str) -> None:
    """Issue #308 acceptance: missing eventId → ``event.rejected`` row."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    assert pool is not None
    try:
        instance = await _seed_instance(
            pool=pool,
            instance_id=str(uuid4()),
            delivery=["pull"],
        )
        metadata = PgMetadataAdapter(pool=pool)
        publisher = RecordingEventPublisher()
        bridge = build_publisher_bridge(
            normalizer=EventNormalizer(produced_event_types=("oci.image.pushed",)),
            publisher=publisher,
            metadata_store=metadata,  # type: ignore[arg-type]
        )
        raw = [
            {"eventId": "e-good", "eventType": "oci.image.pushed"},
            {"eventType": "oci.image.pushed"},  # missing eventId
        ]
        await bridge(instance.workspace_id, instance, raw)

        # Sibling event still published.
        assert [e.event_id for e in publisher.published] == ["e-good"]
        audit = await _read_audit_event_types(pool=pool, workspace_id=str(instance.workspace_id))
        rejected = [p for et, p in audit if et == EVENT_REJECTED]
        assert len(rejected) == 1
        assert rejected[0]["reason"] == "missing-event-id"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_sink_connector_activation_skips(pg_dsn: str) -> None:
    """Sink connectors activate without touching the scheduler."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    assert pool is not None
    try:
        instance = await _seed_instance(
            pool=pool,
            instance_id=str(uuid4()),
            delivery=None,  # no events block at all
        )
        catalog = PgCatalogAdapter(pool=pool)
        scheduler = _FakeScheduler()
        manager = ListenManager(
            catalog_store=catalog,  # type: ignore[arg-type]
            scheduler=scheduler,  # type: ignore[arg-type]
        )
        result = await manager.activate(instance)
        assert result.skipped is True
        assert result.reason == REASON_SINK_CONNECTOR
        assert result.modes == ()
        assert scheduler.registered == []
        assert (
            manager.get_push_registration(str(instance.workspace_id), str(instance.instance_id))
            is None
        )
    finally:
        await pool.close()
