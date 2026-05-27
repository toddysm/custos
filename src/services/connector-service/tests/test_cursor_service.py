"""Unit tests for :class:`custos_connector.cursor.CursorService`.

Drives the cursor lifecycle through in-memory fakes
(``FakeMetadataAdapter`` / ``FakeCatalogAdapter`` /
``FakeConnectorInstanceAdapter``) and a fake ``PluginInvoker`` that
records every ``listen`` call. No Docker, no Postgres.

The tests cover the design contract for CONN-IMPL-022 (#305):

* uninitialized envelope (``value=None``) on first tick;
* value round-trip through ``json.dumps`` for subsequent ticks;
* empty-tick semantics (no ``nextCursor`` → value unchanged,
  ``advanced_at`` bumped);
* halt paths (``CursorEncodingMismatch`` / ``CursorExpired``) including
  audit emission + instance status flip + lease release;
* publisher-raises-before-commit → cursor unchanged, lease released
  (at-least-once on the next tick);
* ineligible instance (missing / disabled / already halted);
* :meth:`CursorService.read_envelope` returning the combined record.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from custos_spl import LeaseBusy
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.audit import (
    EVENT_CURSOR_ADVANCED,
    EVENT_CURSOR_ENCODING_MISMATCH,
    EVENT_CURSOR_EXPIRED,
)
from custos_connector.cursor import (
    CursorEncodingMismatchHalt,
    CursorExpiredHalt,
    CursorInstanceUnavailable,
    CursorService,
)
from custos_connector.cursor.service import (
    HALT_STATUS_ENCODING_MISMATCH,
    HALT_STATUS_EXPIRED,
)
from custos_connector.runtime import (
    CursorEncodingMismatch,
    CursorEnvelope,
    CursorExpired,
    ListenMode,
    ListenResult,
)
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeMetadataAdapter,
)

pytestmark = pytest.mark.asyncio


_WORKSPACE = "ws-1"
_TYPE = "http"
_VERSION = "1.0.0"
_ENCODING = "my-encoding-v1"


# ---------------------------------------------------------------------------
# Fakes / fixtures
# ---------------------------------------------------------------------------


class _FakeListenInvoker:
    """Records every ``listen`` call and replays a scripted queue.

    Items pushed via :meth:`queue` are popped in FIFO order. An
    :class:`Exception` instance is raised in place of returning a
    :class:`ListenResult`, which lets tests script halt-error paths.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._results: list[ListenResult | Exception] = []

    def queue(self, item: ListenResult | Exception) -> None:
        self._results.append(item)

    async def listen(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        mode: ListenMode,
        cursor: CursorEnvelope | None,
    ) -> ListenResult:
        self.calls.append(
            {
                "connector_type": connector.type,
                "connector_version": connector.version,
                "instance_id": str(instance.instance_id),
                "mode": mode,
                "cursor": cursor,
            }
        )
        if not self._results:
            raise AssertionError("_FakeListenInvoker queue is empty")
        nxt = self._results.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


class _RecordingPublisher:
    """Captures every ``(workspace, instance, events)`` publish call."""

    def __init__(self, *, raise_on_call: BaseException | None = None) -> None:
        self.calls: list[tuple[str, str, tuple[Mapping[str, Any], ...]]] = []
        self._raise = raise_on_call

    async def __call__(
        self,
        workspace_id: WorkspaceId,
        instance: ConnectorInstance,
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        self.calls.append((str(workspace_id), str(instance.instance_id), tuple(events)))
        if self._raise is not None:
            raise self._raise


def _make_connector_type(
    *,
    cursor_encoding: str | None = _ENCODING,
    include_pull_block: bool = True,
) -> ConnectorTypeVersion:
    pull_block: dict[str, Any] = {}
    if cursor_encoding is not None:
        pull_block["cursorEncoding"] = cursor_encoding
    spec: dict[str, Any] = {"capabilities": ["events.delivery"]}
    if include_pull_block:
        spec["events"] = {"pull": pull_block}
    return ConnectorTypeVersion(
        type=_TYPE,
        version=_VERSION,
        digest="sha256:fake",
        image_ref=f"example.test/{_TYPE}@sha256:fake",
        normalized_manifest={
            "metadata": {"type": _TYPE, "version": _VERSION},
            "spec": spec,
        },
        parent_deprecated=False,
        published_at=datetime.now(UTC),
    )


def _make_instance(
    *,
    instance_id: str | None = None,
    enabled: bool = True,
    status: str = "active",
) -> ConnectorInstance:
    now = datetime.now(UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(instance_id or str(uuid4())),
        type=_TYPE,
        version=_VERSION,
        name="prod",
        lease_ttl_seconds=3600,
        enabled=enabled,
        status=status,
        health_status="healthy",
        target_config={},
        credentials_authentication={"type": "oidc"},
        used_capabilities=("events.delivery",),
        created_at=now,
        updated_at=now,
    )


async def _seed(
    *,
    catalog: FakeCatalogAdapter,
    instances: FakeConnectorInstanceAdapter,
    connector_type: ConnectorTypeVersion,
    instance: ConnectorInstance,
) -> None:
    await catalog.put_connector_type_version(
        connector_type.type,
        connector_type.version,
        connector_type.digest,
        connector_type.image_ref,
        connector_type.normalized_manifest,
    )
    await instances.put_connector_instance(instance.workspace_id, instance)


def _build_service(
    *,
    invoker: _FakeListenInvoker,
    publisher: _RecordingPublisher,
    clock: datetime | None = None,
) -> tuple[
    CursorService,
    FakeMetadataAdapter,
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
]:
    now = clock or datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    ticks = iter(now + timedelta(seconds=i) for i in range(0, 10_000))

    def _clock() -> datetime:
        return next(ticks)

    metadata = FakeMetadataAdapter(clock=_clock)
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    service = CursorService(
        metadata_store=metadata,  # type: ignore[arg-type]
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        plugin_invoker=invoker,  # type: ignore[arg-type]
        publisher=publisher,
        holder_id="test-holder",
        lease_ttl_seconds=60,
        clock=_clock,
    )
    return service, metadata, catalog, instances


def _fingerprint(persisted: str) -> str:
    return "sha256:" + hashlib.sha256(persisted.encode("utf-8")).hexdigest()


def _audit_events(metadata: FakeMetadataAdapter, event_type: str) -> list[Any]:
    return [evt for _ws, evt in metadata.append_audit_calls if evt.event_type == event_type]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_first_tick_uninitialized_envelope_then_commit() -> None:
    """First tick passes ``value=None`` to the plugin, commits new value,
    emits ``cursor.advanced`` with correct envelope fingerprints."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    invoker.queue(
        ListenResult(
            events=({"type": "e1"}, {"type": "e2"}),
            next_cursor=CursorEnvelope(encoding=_ENCODING, value={"offset": 42}, advanced_at=None),
            receiver_endpoint=None,
        )
    )

    result = await service.tick(_WORKSPACE, str(instance.instance_id))

    # Plugin saw the uninitialized envelope.
    assert len(invoker.calls) == 1
    listen_envelope = invoker.calls[0]["cursor"]
    assert isinstance(listen_envelope, CursorEnvelope)
    assert listen_envelope.encoding == _ENCODING
    assert listen_envelope.value is None
    assert invoker.calls[0]["mode"] is ListenMode.PULL

    # Publisher saw the batch BEFORE the commit (at-least-once).
    assert len(publisher.calls) == 1
    pub_ws, pub_inst, pub_events = publisher.calls[0]
    assert pub_ws == _WORKSPACE
    assert pub_inst == str(instance.instance_id)
    assert pub_events == ({"type": "e1"}, {"type": "e2"})

    # Cursor row now carries the json-encoded committed value.
    persisted = json.dumps({"offset": 42}, sort_keys=True, separators=(",", ":"))
    row = await metadata.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row is not None
    assert row.value == persisted
    assert row.lease_holder is None  # cleared on commit

    # TickResult reflects the transition.
    assert result.event_count == 2
    assert result.cursor_before.value is None
    assert result.cursor_after.value == {"offset": 42}
    assert result.receiver_endpoint is None

    # Audit event emitted with correct envelopes.
    advanced = _audit_events(metadata, EVENT_CURSOR_ADVANCED)
    assert len(advanced) == 1
    payload = advanced[0].payload
    assert payload["reason"] == "tick"
    assert payload["eventCount"] == 2
    assert payload["from"]["valueFingerprint"] is None  # uninitialized
    assert payload["from"]["valueLength"] is None
    assert payload["to"]["valueFingerprint"] == _fingerprint(persisted)
    assert payload["to"]["valueLength"] == len(persisted.encode("utf-8"))


async def test_second_tick_passes_round_tripped_value() -> None:
    """After a commit, the next tick's envelope carries the decoded value."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    invoker.queue(
        ListenResult(
            events=(),
            next_cursor=CursorEnvelope(
                encoding=_ENCODING, value="cursor-token-A", advanced_at=None
            ),
            receiver_endpoint=None,
        )
    )
    invoker.queue(ListenResult(events=(), next_cursor=None, receiver_endpoint=None))

    await service.tick(_WORKSPACE, str(instance.instance_id))
    await service.tick(_WORKSPACE, str(instance.instance_id))

    # Second listen got the previously-committed value decoded back.
    second_envelope = invoker.calls[1]["cursor"]
    assert isinstance(second_envelope, CursorEnvelope)
    assert second_envelope.value == "cursor-token-A"


async def test_empty_tick_keeps_value_but_bumps_advanced_at() -> None:
    """Plugin returning ``next_cursor=None`` keeps value, bumps advanced_at."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    # Seed a committed value via a first tick.
    invoker.queue(
        ListenResult(
            events=(),
            next_cursor=CursorEnvelope(encoding=_ENCODING, value=7, advanced_at=None),
            receiver_endpoint=None,
        )
    )
    await service.tick(_WORKSPACE, str(instance.instance_id))
    row_after_first = await metadata.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row_after_first is not None
    first_advanced_at = row_after_first.advanced_at
    first_value = row_after_first.value

    # Empty tick.
    invoker.queue(ListenResult(events=(), next_cursor=None, receiver_endpoint=None))
    await service.tick(_WORKSPACE, str(instance.instance_id))

    row_after_second = await metadata.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row_after_second is not None
    assert row_after_second.value == first_value
    assert row_after_second.advanced_at > first_advanced_at


# ---------------------------------------------------------------------------
# Halt paths
# ---------------------------------------------------------------------------


async def test_encoding_mismatch_halts_instance() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    invoker.queue(
        CursorEncodingMismatch(
            "manifest bumped encoding",
            persisted_encoding="old-encoding",
            plugin_encoding="my-encoding-v2",
        )
    )

    with pytest.raises(CursorEncodingMismatchHalt) as exc_info:
        await service.tick(_WORKSPACE, str(instance.instance_id))

    assert exc_info.value.plugin_encoding == "my-encoding-v2"

    # Status flipped on the row.
    refreshed = await instances.get_connector_instance(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert refreshed is not None
    assert refreshed.status == HALT_STATUS_ENCODING_MISMATCH

    # Audit emitted.
    mism = _audit_events(metadata, EVENT_CURSOR_ENCODING_MISMATCH)
    assert len(mism) == 1
    assert mism[0].payload["pluginEncoding"] == "my-encoding-v2"
    assert mism[0].payload["persistedEncoding"] == "old-encoding"

    # Publisher NEVER called and no advance audit emitted.
    assert publisher.calls == []
    assert _audit_events(metadata, EVENT_CURSOR_ADVANCED) == []

    # Lease was released.
    row = await metadata.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row is not None
    assert row.lease_holder is None


async def test_cursor_expired_halts_instance() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    invoker.queue(CursorExpired("upstream rejected cursor"))

    with pytest.raises(CursorExpiredHalt):
        await service.tick(_WORKSPACE, str(instance.instance_id))

    refreshed = await instances.get_connector_instance(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert refreshed is not None
    assert refreshed.status == HALT_STATUS_EXPIRED

    expired = _audit_events(metadata, EVENT_CURSOR_EXPIRED)
    assert len(expired) == 1
    assert expired[0].payload["errorDetail"] == "upstream rejected cursor"

    assert publisher.calls == []
    assert _audit_events(metadata, EVENT_CURSOR_ADVANCED) == []


# ---------------------------------------------------------------------------
# Publisher / failure semantics
# ---------------------------------------------------------------------------


async def test_publisher_failure_rolls_back_lease_and_cursor() -> None:
    """If publish raises, commit MUST NOT happen and lease MUST release."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher(raise_on_call=RuntimeError("kafka down"))
    service, metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    invoker.queue(
        ListenResult(
            events=({"type": "e1"},),
            next_cursor=CursorEnvelope(encoding=_ENCODING, value="A", advanced_at=None),
            receiver_endpoint=None,
        )
    )

    with pytest.raises(RuntimeError, match="kafka down"):
        await service.tick(_WORKSPACE, str(instance.instance_id))

    # Cursor stays uninitialized (value=='', lease released).
    row = await metadata.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row is not None
    assert row.value == ""
    assert row.lease_holder is None

    # No advance audit, status unchanged.
    assert _audit_events(metadata, EVENT_CURSOR_ADVANCED) == []
    refreshed = await instances.get_connector_instance(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert refreshed is not None
    assert refreshed.status == "active"


async def test_at_least_once_redelivery_after_publisher_failure() -> None:
    """After a publisher failure the next tick re-emits the same batch."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher(raise_on_call=RuntimeError("kafka down"))
    service, _metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    invoker.queue(
        ListenResult(
            events=({"type": "e1"},),
            next_cursor=CursorEnvelope(encoding=_ENCODING, value="A", advanced_at=None),
            receiver_endpoint=None,
        )
    )
    with pytest.raises(RuntimeError):
        await service.tick(_WORKSPACE, str(instance.instance_id))

    # Heal publisher, plugin (a real one would) return the same batch on
    # next listen because cursor did not advance.
    publisher._raise = None
    invoker.queue(
        ListenResult(
            events=({"type": "e1"},),
            next_cursor=CursorEnvelope(encoding=_ENCODING, value="A", advanced_at=None),
            receiver_endpoint=None,
        )
    )

    await service.tick(_WORKSPACE, str(instance.instance_id))
    # Plugin's second listen saw value=None (cursor never committed).
    assert invoker.calls[1]["cursor"].value is None
    # Same batch was emitted twice: once failed (recorded then raised),
    # once succeeded on the retry tick. That's the at-least-once
    # contract — downstream subscribers MUST dedup.
    assert len(publisher.calls) == 2
    assert publisher.calls[0][2] == ({"type": "e1"},)
    assert publisher.calls[1][2] == ({"type": "e1"},)


# ---------------------------------------------------------------------------
# Instance eligibility
# ---------------------------------------------------------------------------


async def test_unknown_instance_raises_unavailable() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, _catalog, _instances = _build_service(invoker=invoker, publisher=publisher)

    with pytest.raises(CursorInstanceUnavailable):
        await service.tick(_WORKSPACE, "nonexistent")


async def test_disabled_instance_raises_unavailable() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance(enabled=False)
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    with pytest.raises(CursorInstanceUnavailable):
        await service.tick(_WORKSPACE, str(instance.instance_id))


async def test_halted_instance_raises_unavailable() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance(status=HALT_STATUS_EXPIRED)
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    with pytest.raises(CursorInstanceUnavailable):
        await service.tick(_WORKSPACE, str(instance.instance_id))


async def test_missing_pull_encoding_raises_unavailable() -> None:
    """A connector-type without ``events.pull.cursorEncoding`` is unusable."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type(include_pull_block=False)
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    with pytest.raises(CursorInstanceUnavailable):
        await service.tick(_WORKSPACE, str(instance.instance_id))


# ---------------------------------------------------------------------------
# Lease contention
# ---------------------------------------------------------------------------


async def test_lease_busy_propagates() -> None:
    """If another holder has the lease, ``LeaseBusy`` propagates out."""
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    # Pre-acquire the lease as a different holder.
    await metadata.acquire_cursor_lease(
        WorkspaceId(_WORKSPACE),
        ConnectorInstanceId(str(instance.instance_id)),
        "other-holder",
        300,
    )

    with pytest.raises(LeaseBusy):
        await service.tick(_WORKSPACE, str(instance.instance_id))

    # Plugin was never invoked.
    assert invoker.calls == []
    assert publisher.calls == []


# ---------------------------------------------------------------------------
# Admin read
# ---------------------------------------------------------------------------


async def test_read_envelope_returns_combined_record() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, catalog, instances = _build_service(invoker=invoker, publisher=publisher)
    ctype = _make_connector_type()
    instance = _make_instance()
    await _seed(catalog=catalog, instances=instances, connector_type=ctype, instance=instance)

    # Before any tick, no cursor row exists yet.
    pre = await service.read_envelope(_WORKSPACE, str(instance.instance_id))
    assert pre is None

    invoker.queue(
        ListenResult(
            events=(),
            next_cursor=CursorEnvelope(encoding=_ENCODING, value={"k": "v"}, advanced_at=None),
            receiver_endpoint=None,
        )
    )
    await service.tick(_WORKSPACE, str(instance.instance_id))

    record = await service.read_envelope(_WORKSPACE, str(instance.instance_id))
    assert record is not None
    assert record.encoding == _ENCODING
    assert record.value == {"k": "v"}
    assert record.lease_holder is None


async def test_read_envelope_returns_none_for_unknown_instance() -> None:
    invoker = _FakeListenInvoker()
    publisher = _RecordingPublisher()
    service, _metadata, _catalog, _instances = _build_service(invoker=invoker, publisher=publisher)

    record = await service.read_envelope(_WORKSPACE, "nonexistent")
    assert record is None
