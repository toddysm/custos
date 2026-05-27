"""Integration test for :class:`CursorService` against real Postgres.

Drives the pull cursor lifecycle through ``PgMetadataAdapter`` +
``PgCatalogAdapter`` + ``PgConnectorInstanceAdapter`` to validate the
restart-resume property and the at-least-once contract end-to-end:

* fresh row, plugin emits batch → commit, value persisted;
* a *new* :class:`CursorService` (simulating a service restart) reads
  the cursor and the plugin's next ``listen`` carries the previously
  committed value;
* publisher failure rolls back: cursor stays at the prior value, the
  next tick re-emits the same batch.

The plugin invoker is a pure-Python stub (no Docker); the focus here
is the persistence boundary the unit suite cannot exercise.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.cursor import CursorService
from custos_connector.runtime import (
    CursorEnvelope,
    ListenMode,
    ListenResult,
)

pytestmark = pytest.mark.integration


_WORKSPACE = "ws-int"
_TYPE = "http"
_VERSION = "1.0.0"
_ENCODING = "my-encoding-v1"


class _FakeListenInvoker:
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
                "instance_id": str(instance.instance_id),
                "mode": mode,
                "cursor_value": None if cursor is None else cursor.value,
            }
        )
        if not self._results:
            raise AssertionError("listen queue empty")
        nxt = self._results.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


class _RecordingPublisher:
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


async def _seed(
    *,
    pool: asyncpg.Pool,
    instance_id: str,
) -> ConnectorInstance:
    catalog = PgCatalogAdapter(pool=pool)
    instances = PgConnectorInstanceAdapter(pool=pool)

    manifest: dict[str, Any] = {
        "metadata": {"type": _TYPE, "version": _VERSION},
        "spec": {
            "capabilities": ["events.delivery"],
            "events": {"pull": {"cursorEncoding": _ENCODING}},
        },
    }
    await catalog.put_connector_type_version(
        _TYPE,
        _VERSION,
        "sha256:fake",
        f"example.test/{_TYPE}@sha256:fake",
        manifest,
    )

    now = datetime.now(UTC)
    instance = ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(instance_id),
        type=_TYPE,
        version=_VERSION,
        name="prod",
        lease_ttl_seconds=3600,
        enabled=True,
        status="active",
        health_status="healthy",
        target_config={},
        credentials_authentication={"type": "oidc"},
        used_capabilities=("events.delivery",),
        created_at=now,
        updated_at=now,
    )
    await instances.put_connector_instance(instance.workspace_id, instance)
    return instance


def _build_service(
    pool: asyncpg.Pool,
    *,
    invoker: _FakeListenInvoker,
    publisher: _RecordingPublisher,
    holder_id: str = "test-holder-1",
) -> CursorService:
    return CursorService(
        metadata_store=PgMetadataAdapter(pool=pool),  # type: ignore[arg-type]
        catalog_store=PgCatalogAdapter(pool=pool),  # type: ignore[arg-type]
        instance_store=PgConnectorInstanceAdapter(pool=pool),
        plugin_invoker=invoker,  # type: ignore[arg-type]
        publisher=publisher,
        holder_id=holder_id,
        lease_ttl_seconds=60,
    )


@pytest.mark.asyncio
async def test_cursor_persists_and_resumes_after_restart(pg_dsn: str) -> None:
    """Commit a cursor value, tear down the service, rebuild, resume."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        instance = await _seed(pool=pool, instance_id=str(uuid4()))

        # --- service #1: commit "cursor-A"
        invoker_1 = _FakeListenInvoker()
        publisher_1 = _RecordingPublisher()
        svc_1 = _build_service(pool, invoker=invoker_1, publisher=publisher_1)

        invoker_1.queue(
            ListenResult(
                events=({"type": "e1"},),
                next_cursor=CursorEnvelope(encoding=_ENCODING, value="cursor-A", advanced_at=None),
                receiver_endpoint=None,
            )
        )
        await svc_1.tick(_WORKSPACE, str(instance.instance_id))

        assert invoker_1.calls[0]["cursor_value"] is None  # uninitialized
        assert publisher_1.calls[0][2] == ({"type": "e1"},)

        # --- service #2: simulate restart, read envelope + tick
        invoker_2 = _FakeListenInvoker()
        publisher_2 = _RecordingPublisher()
        svc_2 = _build_service(
            pool, invoker=invoker_2, publisher=publisher_2, holder_id="test-holder-2"
        )

        env = await svc_2.read_envelope(_WORKSPACE, str(instance.instance_id))
        assert env is not None
        assert env.encoding == _ENCODING
        assert env.value == "cursor-A"

        invoker_2.queue(
            ListenResult(
                events=({"type": "e2"},),
                next_cursor=CursorEnvelope(encoding=_ENCODING, value="cursor-B", advanced_at=None),
                receiver_endpoint=None,
            )
        )
        await svc_2.tick(_WORKSPACE, str(instance.instance_id))
        # The restart sees the committed value, not None.
        assert invoker_2.calls[0]["cursor_value"] == "cursor-A"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_publisher_failure_keeps_cursor_for_at_least_once(pg_dsn: str) -> None:
    """A publisher failure leaves the cursor untouched; next tick re-emits."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        instance = await _seed(pool=pool, instance_id=str(uuid4()))

        publisher = _RecordingPublisher(raise_on_call=RuntimeError("kafka down"))
        invoker = _FakeListenInvoker()
        svc = _build_service(pool, invoker=invoker, publisher=publisher)

        invoker.queue(
            ListenResult(
                events=({"type": "e1"},),
                next_cursor=CursorEnvelope(encoding=_ENCODING, value="cursor-A", advanced_at=None),
                receiver_endpoint=None,
            )
        )
        with pytest.raises(RuntimeError, match="kafka down"):
            await svc.tick(_WORKSPACE, str(instance.instance_id))

        # Cursor row exists but value is still the uninitialized sentinel.
        row = await PgMetadataAdapter(pool=pool).read_cursor(
            WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
        )
        assert row is not None
        assert row.value == ""
        assert row.lease_holder is None  # lease was released on the rollback

        # Heal publisher; next tick re-emits the same batch.
        publisher._raise = None
        invoker.queue(
            ListenResult(
                events=({"type": "e1"},),
                next_cursor=CursorEnvelope(encoding=_ENCODING, value="cursor-A", advanced_at=None),
                receiver_endpoint=None,
            )
        )
        await svc.tick(_WORKSPACE, str(instance.instance_id))

        assert publisher.calls[-1][2] == ({"type": "e1"},)
        row2 = await PgMetadataAdapter(pool=pool).read_cursor(
            WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
        )
        assert row2 is not None
        import json as _json

        assert row2.value == _json.dumps("cursor-A", separators=(",", ":"))
    finally:
        await pool.close()
