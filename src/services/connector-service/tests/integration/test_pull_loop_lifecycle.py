"""Integration test for :class:`PullLoopScheduler` against real Postgres.

Validates the two design properties the unit suite cannot exercise:

* **Single-writer enforcement under real concurrency** — two scheduler
  replicas (different holder ids) racing on the same instance: SPL's
  cursor-lease ``SELECT ... FOR UPDATE`` ensures exactly one wins
  per pass and the loser reports ``lease_busy`` without advancing
  the cursor.
* **Crash recovery via lease expiry** — holder A acquires a lease and
  "crashes" (we explicitly do not release it); after the TTL elapses
  holder B's next pass picks the instance up and ticks normally.

The plugin invoker is a pure-Python stub (no Docker); the focus here
is the persistence + concurrency boundary the unit suite cannot
exercise.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
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
from custos_connector.scheduler import PullLoopScheduler

pytestmark = pytest.mark.integration


_WORKSPACE = "ws-sched-int"
_TYPE = "http"
_VERSION = "1.0.0"
_ENCODING = "my-encoding-v1"


class _FakeListenInvoker:
    def __init__(self, *, hold_gate: asyncio.Event | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._results: list[ListenResult | Exception] = []
        # Optional barrier: when set, ``listen()`` waits on the event
        # before returning. Used by the single-writer race test to
        # guarantee the winner holds the lease long enough for the
        # loser's ``SELECT ... FOR UPDATE`` to observe the held row
        # (otherwise the winner's fast in-memory tick + lease release
        # can finish before the loser's acquire query even reaches
        # the DB, and the test flakes with two ``ticked`` outcomes).
        self._hold_gate: asyncio.Event | None = hold_gate

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
                "cursor_value": None if cursor is None else cursor.value,
            }
        )
        if self._hold_gate is not None:
            await self._hold_gate.wait()
        if not self._results:
            raise AssertionError("listen queue empty")
        nxt = self._results.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


class _RecordingPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[Mapping[str, Any], ...]]] = []

    async def __call__(
        self,
        workspace_id: WorkspaceId,
        instance: ConnectorInstance,
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        self.calls.append((str(workspace_id), str(instance.instance_id), tuple(events)))


async def _seed(*, pool: asyncpg.Pool, instance_id: str) -> ConnectorInstance:
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


def _build_cursor(
    pool: asyncpg.Pool,
    *,
    invoker: _FakeListenInvoker,
    publisher: _RecordingPublisher,
    holder_id: str,
    lease_ttl_seconds: int = 60,
) -> CursorService:
    return CursorService(
        metadata_store=PgMetadataAdapter(pool=pool),  # type: ignore[arg-type]
        catalog_store=PgCatalogAdapter(pool=pool),  # type: ignore[arg-type]
        instance_store=PgConnectorInstanceAdapter(pool=pool),
        plugin_invoker=invoker,  # type: ignore[arg-type]
        publisher=publisher,
        holder_id=holder_id,
        lease_ttl_seconds=lease_ttl_seconds,
    )


@pytest.mark.asyncio
async def test_two_scheduler_replicas_share_single_writer(pg_dsn: str) -> None:
    """Two scheduler instances race on the same cursor.

    Exactly one MUST advance per pass; the loser MUST report
    ``lease_busy`` and MUST NOT touch the persisted cursor.
    """
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    assert pool is not None
    try:
        instance = await _seed(pool=pool, instance_id=str(uuid4()))

        # Shared barrier so the winning replica's listen() blocks
        # until the loser has had a chance to attempt lease acquire.
        # Without this, the in-memory invoker finishes so fast that
        # the lease can be acquired + released before the second
        # replica's SELECT ... FOR UPDATE even runs, and both
        # replicas report ``ticked``.
        hold_gate = asyncio.Event()
        invoker_a = _FakeListenInvoker(hold_gate=hold_gate)
        invoker_b = _FakeListenInvoker(hold_gate=hold_gate)
        publisher_a = _RecordingPublisher()
        publisher_b = _RecordingPublisher()

        cursor_a = _build_cursor(
            pool, invoker=invoker_a, publisher=publisher_a, holder_id="replica-A"
        )
        cursor_b = _build_cursor(
            pool, invoker=invoker_b, publisher=publisher_b, holder_id="replica-B"
        )
        sched_a = PullLoopScheduler(cursor_service=cursor_a, min_interval_seconds=10)
        sched_b = PullLoopScheduler(cursor_service=cursor_b, min_interval_seconds=10)
        sched_a.register(_WORKSPACE, str(instance.instance_id))
        sched_b.register(_WORKSPACE, str(instance.instance_id))

        # Each scheduler script: one tick result with a distinct cursor
        # value so we can tell which one won.
        invoker_a.queue(
            ListenResult(
                events=({"type": "from-A"},),
                next_cursor=CursorEnvelope(
                    encoding=_ENCODING, value="cursor-from-A", advanced_at=None
                ),
                receiver_endpoint=None,
            )
        )
        invoker_b.queue(
            ListenResult(
                events=({"type": "from-B"},),
                next_cursor=CursorEnvelope(
                    encoding=_ENCODING, value="cursor-from-B", advanced_at=None
                ),
                receiver_endpoint=None,
            )
        )

        async def _release_after_delay() -> None:
            # 200ms is generous on GitHub Actions runners but still
            # finishes well under the 60s lease TTL.
            await asyncio.sleep(0.2)
            hold_gate.set()

        result_a, result_b, _ = await asyncio.gather(
            sched_a.tick_once(),
            sched_b.tick_once(),
            _release_after_delay(),
        )

        statuses = sorted([result_a.outcomes[0].status, result_b.outcomes[0].status])
        assert statuses == ["lease_busy", "ticked"], (
            f"expected one ticked + one lease_busy, got {statuses!r}"
        )

        # Exactly one publisher saw the batch; the loser saw nothing.
        publish_count = len(publisher_a.calls) + len(publisher_b.calls)
        assert publish_count == 1

        # Persisted cursor MUST match whoever won.
        row = await PgMetadataAdapter(pool=pool).read_cursor(
            WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
        )
        assert row is not None
        winner_value = (
            "cursor-from-A" if result_a.outcomes[0].status == "ticked" else "cursor-from-B"
        )
        import json as _json

        assert row.value == _json.dumps(winner_value, separators=(",", ":"))
        # Lease cleared after the winning commit.
        assert row.lease_holder is None
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_crashed_replica_lease_expires_and_other_picks_up(pg_dsn: str) -> None:
    """Replica A acquires the lease and 'crashes' — B picks up after TTL.

    We simulate the crash by directly poking a stale lease into the
    cursor row (holder=replica-A, expired in the past). Replica B's
    next scheduler pass MUST claim the lease and advance the cursor.
    """
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        instance = await _seed(pool=pool, instance_id=str(uuid4()))
        metadata = PgMetadataAdapter(pool=pool)

        # Touch the cursor row by acquiring + immediately releasing a
        # lease under holder-A — gives us a real row to mutate below.
        _, lease = await metadata.acquire_cursor_lease(
            WorkspaceId(_WORKSPACE),
            ConnectorInstanceId(str(instance.instance_id)),
            "replica-A",
            ttl_seconds=60,
        )
        await metadata.release_cursor_lease(WorkspaceId(_WORKSPACE), lease)

        # Force a stale, *expired* lease held by replica-A directly via
        # SQL — emulates the "A crashed mid-tick" condition.
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE custos_state.connector_cursor
                   SET lease_holder = $1,
                       lease_expires_at = $2
                 WHERE workspace_id = $3
                   AND instance_id = $4
                """,
                "replica-A",
                datetime.now(UTC) - timedelta(seconds=5),
                _WORKSPACE,
                str(instance.instance_id),
            )

        invoker_b = _FakeListenInvoker()
        publisher_b = _RecordingPublisher()
        cursor_b = _build_cursor(
            pool, invoker=invoker_b, publisher=publisher_b, holder_id="replica-B"
        )
        sched_b = PullLoopScheduler(cursor_service=cursor_b, min_interval_seconds=10)
        sched_b.register(_WORKSPACE, str(instance.instance_id))

        invoker_b.queue(
            ListenResult(
                events=({"type": "from-B"},),
                next_cursor=CursorEnvelope(
                    encoding=_ENCODING, value="cursor-from-B", advanced_at=None
                ),
                receiver_endpoint=None,
            )
        )

        result = await sched_b.tick_once()
        assert result.outcomes[0].status == "ticked"
        assert result.outcomes[0].event_count == 1

        row = await metadata.read_cursor(
            WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
        )
        assert row is not None
        import json as _json

        assert row.value == _json.dumps("cursor-from-B", separators=(",", ":"))
        # Replica-B's commit cleared the (now-stale, expired) lease.
        assert row.lease_holder is None
        assert publisher_b.calls[0][2] == ({"type": "from-B"},)
    finally:
        await pool.close()
