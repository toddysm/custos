"""Identical dead-letter privacy, claim and retention contracts against memory and PostgreSQL."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from asyncpg.pool import Pool
from custos_pg import PgMetadataAdapter
from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
)
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import (
    DeadLetterData,
    DeadLetterEntry,
    DeadLetterFilter,
    DeadLetterStoreProvider,
    DispatchAttempt,
    JsonValue,
    encode_dead_letter,
)
from custos_spl.pagination import Cursor

from custos_trigger.events import NormalizedEvent
from custos_trigger.middleware.callctx import CALLCTX_HEADER
from custos_trigger.providers import InMemoryTriggerMetadataStore

WS = WorkspaceId("dead-letter-test")


@dataclass
class Harness:
    """Exercise provider APIs with deterministic memory time and real database fault injection."""

    store: DeadLetterStoreProvider
    clock: list[datetime]
    pool: Pool | None

    async def now(self) -> datetime:
        """Read the actual provider clock, never assume the database follows the test clock."""
        if self.pool is None:
            return self.clock[0]
        async with self.pool.acquire() as conn:
            value = await conn.fetchval("SELECT clock_timestamp()")
            assert isinstance(value, datetime)
            return value

    async def expire(self, entry: DeadLetterEntry, *, retention: bool = False) -> None:
        """Reach an exact memory boundary or backdate both database projections consistently."""
        if self.pool is None:
            if retention:
                self.clock[0] = entry.expires_at
            else:
                assert entry.claim is not None
                self.clock[0] = entry.claim.expires_at
            return
        now = await self.now()
        if retention:
            updated = replace(entry, expires_at=now)
        else:
            assert entry.claim is not None
            updated = replace(entry, claim=replace(entry.claim, expires_at=now))
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE custos_state.dead_letter "
                "SET payload=$3::json, expires_at=$4, claim_expires_at=$5 "
                "WHERE workspace_id=$1 AND entry_id=$2",
                entry.data.workspace_id,
                entry.data.entry_id,
                encode_dead_letter(updated),
                updated.expires_at,
                updated.claim.expires_at if updated.claim else None,
            )


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def dead_letter_dsn(request: pytest.FixtureRequest) -> str | None:
    """Reset the real database before entering the per-test async loop."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def harness(dead_letter_dsn: str | None) -> AsyncIterator[Harness]:
    """Yield the production optional storage capability for both providers."""
    clock = [datetime(2026, 10, 29, tzinfo=UTC)]
    if dead_letter_dsn is None:
        yield Harness(InMemoryTriggerMetadataStore(now=lambda: clock[0]), clock, None)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dead_letter_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield Harness(PgMetadataAdapter(pool=pool), clock, pool)
    finally:
        await pool.close()


def capture(
    entry_id: str = "entry", workspace: WorkspaceId = WS, *, resume: bool = False
) -> DeadLetterData:
    """Project a real NormalizedEvent without transport data and retain its resolved target."""
    event = NormalizedEvent.model_validate(
        {
            "eventId": "event",
            "kind": "custom.test",
            "source": {"type": "manual", "occurredAt": "2026-10-29T00:00:00Z"},
            "data": {"value": "original"},
            "raw": {
                "headers": {"authorization": "private", "x-signature": "private"},
                "body": "private",
            },
        }
    )
    return DeadLetterData(
        workspace_id=workspace,
        entry_id=entry_id,
        subscription_id="sub",
        idempotency_key="original-key",
        event=event.model_dump(by_alias=True, exclude={"raw"}),
        target={
            "kind": "resume",
            "runId": "run",
            "stepId": "step",
            "eventName": "ready",
            "payload": {"v": 1},
        }
        if resume
        else {"kind": "start", "workflowVersionId": "version", "inputs": {"v": 1}},
        history=(DispatchAttempt(datetime(2026, 10, 29, tzinfo=UTC), "transient", 503),),
    )


async def test_capture_is_durable_scoped_and_private(harness: Harness) -> None:
    """Persist normalized start/resume inputs without raw transport; reject duplicate identity."""
    await harness.store.check_dead_letters_ready()
    before = await harness.now()
    data = capture()
    entry = await harness.store.put_dead_letter(data)
    after = await harness.now()
    assert before <= entry.created_at <= after
    assert entry.expires_at - entry.created_at == timedelta(days=30)
    assert await harness.store.get_dead_letter(WS, "entry") == entry
    assert await harness.store.get_dead_letter(WorkspaceId("other"), "entry") is None
    with pytest.raises(ImmutableViolation):
        await harness.store.put_dead_letter(data)
    assert await harness.store.get_dead_letter(WS, "entry") == entry
    other = await harness.store.put_dead_letter(
        capture(workspace=WorkspaceId("other"), resume=True)
    )
    assert other.data.target["kind"] == "resume"
    assert "private" not in encode_dead_letter(entry)
    if harness.pool is not None:
        fresh = PgMetadataAdapter(pool=harness.pool)
        assert await fresh.get_dead_letter(WS, "entry") == entry
        async with harness.pool.acquire() as conn:
            raw = await conn.fetchval(
                "SELECT payload::text FROM custos_state.dead_letter WHERE workspace_id=$1", WS
            )
            assert "private" not in raw and "authorization" not in raw and '"raw"' not in raw


async def test_filtered_pages_and_cursor_scope(harness: Harness) -> None:
    """Bound pages and reject changed tenant/filter scopes or malformed cursors."""
    before = await harness.now()
    for identifier in ("a", "z", "\u00e9"):
        await harness.store.put_dead_letter(capture(identifier))
    await harness.store.put_dead_letter(replace(capture("other"), subscription_id="other"))
    await harness.store.put_dead_letter(capture("foreign", WorkspaceId("foreign")))
    after = await harness.now()
    filters = DeadLetterFilter(subscription_id="sub", status="pending", since=before, until=after)
    first = await harness.store.query_dead_letters(WS, filters, limit=2)
    assert [row.data.entry_id for row in first.items] == ["a", "z"]
    assert first.next_cursor is not None
    second = await harness.store.query_dead_letters(WS, filters, cursor=first.next_cursor, limit=2)
    assert [row.data.entry_id for row in second.items] == ["\u00e9"]
    assert second.next_cursor is None
    for workspace, query in ((WorkspaceId("foreign"), filters), (WS, DeadLetterFilter())):
        with pytest.raises(ValueError, match="query"):
            await harness.store.query_dead_letters(workspace, query, cursor=first.next_cursor)
    with pytest.raises(ValueError):
        await harness.store.query_dead_letters(WS, cursor=Cursor("invalid"))
    with pytest.raises(ValueError):
        await harness.store.query_dead_letters(WS, limit=0)


@pytest.mark.parametrize(
    "secret",
    [
        {CALLCTX_HEADER: "signed-private-context"},
        {"username": "registry-user", "token": "private-registry-token"},
    ],
)
async def test_production_credential_shapes_rejected(
    harness: Harness, secret: dict[str, str]
) -> None:
    """Reject the actual signed-context header and Connector resolver material in mapped inputs."""
    data = capture()
    with pytest.raises(ValueError, match="private transport"):
        await harness.store.put_dead_letter(
            replace(data, target={**data.target, "inputs": {"connection": secret}})
        )
    assert not (await harness.store.query_dead_letters(WS)).items


async def test_exponent_numbers_survive_all_storage_transitions(harness: Harness) -> None:
    """Preserve accepted numeric representation through PostgreSQL, including retention cleanup."""
    data = capture()
    numbers: dict[str, JsonValue] = {
        "large": [1e308] * 900,
        "small": 5e-324,
        "whole": 1.0,
        "negativeZero": -0.0,
    }
    entry = await harness.store.put_dead_letter(
        replace(
            data,
            event={**data.event, "data": numbers},
            target={**data.target, "inputs": numbers},
            retention_seconds=1,
        )
    )
    loaded = await harness.store.get_dead_letter(WS, "entry")
    assert loaded is not None
    assert loaded == entry
    assert encode_dead_letter(loaded) == encode_dead_letter(entry)
    assert (await harness.store.query_dead_letters(WS)).items == (entry,)
    claimed = await harness.store.claim_dead_letter(WS, "entry", "worker")
    assert claimed.claim is not None
    finished = await harness.store.finish_dead_letter(claimed.claim, result_id="run")
    assert finished.data == entry.data
    await harness.expire(finished, retention=True)
    assert await harness.store.delete_expired_dead_letter(WS, "entry")


async def test_competing_claims_and_discard(harness: Harness) -> None:
    """Exactly one replica claims, and discard cannot take work from a live claim."""
    await harness.store.put_dead_letter(capture())
    results = await asyncio.gather(
        *(harness.store.claim_dead_letter(WS, "entry", worker) for worker in ("a", "b")),
        return_exceptions=True,
    )
    winners = [value for value in results if isinstance(value, DeadLetterEntry)]
    assert len(winners) == 1 and sum(isinstance(value, LeaseBusy) for value in results) == 1
    claimed = winners[0]
    assert claimed.claim is not None and claimed.claim.epoch == 1
    assert len(claimed.replay_history) == 1
    with pytest.raises(LeaseBusy):
        await harness.store.discard_dead_letter(WS, "entry")
    assert await harness.store.recover_dead_letter(WS, "entry") == claimed
    with pytest.raises(ImmutableViolation):
        await harness.store.claim_dead_letter(WorkspaceId("other"), "entry", "worker")
    for token in (
        replace(claimed.claim, token="wrong"),
        replace(claimed.claim, epoch=99),
        replace(claimed.claim, workspace_id=WorkspaceId("other")),
    ):
        with pytest.raises(LeaseExpired):
            await harness.store.finish_dead_letter(token)
    finished = await harness.store.finish_dead_letter(claimed.claim, result_id="run")
    assert finished.status == "replayed" and finished.result_id == "run"
    assert finished.data == claimed.data
    with pytest.raises(LeaseExpired):
        await harness.store.finish_dead_letter(claimed.claim)
    with pytest.raises(ImmutableViolation):
        await harness.store.discard_dead_letter(WS, "entry")


async def test_page_default_cap_and_terminal_failure(harness: Harness) -> None:
    """Check exact page limits and a failed final attempt, not just small-page proxies."""
    for index in range(201):
        await harness.store.put_dead_letter(capture(f"entry-{index:03d}"))
    assert len((await harness.store.query_dead_letters(WS)).items) == 50
    capped = await harness.store.query_dead_letters(WS, limit=999)
    assert len(capped.items) == 200 and capped.next_cursor is not None
    assert (
        len(
            (await harness.store.query_dead_letters(WS, cursor=capped.next_cursor, limit=999)).items
        )
        == 1
    )
    await harness.store.put_dead_letter(replace(capture("last"), max_replay_attempts=1))
    claimed = await harness.store.claim_dead_letter(WS, "last", "a")
    assert claimed.claim is not None
    failed = await harness.store.finish_dead_letter(
        claimed.claim, failure=DispatchAttempt(await harness.now(), "permanent", 400)
    )
    assert failed.status == "discarded"
    assert failed.replay_history[-1].failure is not None
    with pytest.raises(ImmutableViolation):
        await harness.store.claim_dead_letter(WS, "last", "a")


async def test_recreated_identity_cannot_accept_old_claim(harness: Harness) -> None:
    """Fresh random fencing tokens prevent ABA when retention permits reuse of an entry ID."""
    await harness.store.put_dead_letter(replace(capture(), retention_seconds=1))
    old = await harness.store.claim_dead_letter(WS, "entry", "worker", ttl_seconds=1)
    assert old.claim is not None
    await harness.expire(old, retention=True)
    current = await harness.store.get_dead_letter(WS, "entry")
    assert current is not None
    await harness.expire(current)
    assert await harness.store.delete_expired_dead_letter(WS, "entry")
    await harness.store.put_dead_letter(capture())
    new = await harness.store.claim_dead_letter(WS, "entry", "worker")
    assert new.claim is not None and new.claim.epoch == old.claim.epoch
    assert new.claim.token != old.claim.token
    with pytest.raises(LeaseExpired):
        await harness.store.finish_dead_letter(old.claim)
    assert await harness.store.get_dead_letter(WS, "entry") == new


async def test_expired_claim_handover_fences_old_result(harness: Harness) -> None:
    """A fresh token/epoch recovers abandoned work, preserving original RPC identity and history."""
    await harness.store.put_dead_letter(capture())
    first = await harness.store.claim_dead_letter(WS, "entry", "a")
    assert first.claim is not None
    await harness.expire(first)
    second = await harness.store.claim_dead_letter(WS, "entry", "b")
    assert second.claim is not None and second.claim.epoch == 2
    assert second.claim.token != first.claim.token
    assert second.replay_history[0].outcome == "interrupted"
    assert second.data == first.data
    with pytest.raises(LeaseExpired):
        await harness.store.finish_dead_letter(
            replace(first.claim, expires_at=second.claim.expires_at)
        )
    failed = await harness.store.finish_dead_letter(
        second.claim, failure=DispatchAttempt(await harness.now(), "transient", 503)
    )
    assert failed.status == "pending" and failed.replay_history[-1].outcome == "failed"
    last = await harness.store.claim_dead_letter(WS, "entry", "c")
    assert last.claim is not None and last.claim.epoch == 3
    await harness.expire(last)
    recovered = await harness.store.recover_dead_letter(WS, "entry")
    assert recovered.status == "discarded" and len(recovered.replay_history) == 3
    assert recovered.replay_history[-1].outcome == "interrupted"
    with pytest.raises(ImmutableViolation):
        await harness.store.claim_dead_letter(WS, "entry", "d")


async def test_retention_does_not_race_active_replay(harness: Harness) -> None:
    """No new replay starts at retention; existing live claims can finish and then be removed."""
    await harness.store.put_dead_letter(replace(capture(), retention_seconds=1))
    claimed = await harness.store.claim_dead_letter(WS, "entry", "a")
    assert claimed.claim is not None
    assert not await harness.store.delete_expired_dead_letter(WS, "entry")
    await harness.expire(claimed, retention=True)
    assert not await harness.store.delete_expired_dead_letter(WS, "entry")
    with pytest.raises(LeaseBusy):
        await harness.store.discard_dead_letter(WS, "entry")
    await harness.store.finish_dead_letter(claimed.claim)
    assert await harness.store.delete_expired_dead_letter(WS, "entry")
    assert not await harness.store.delete_expired_dead_letter(WS, "entry")
    assert await harness.store.get_dead_letter(WS, "entry") is None
    with pytest.raises(LeaseExpired):
        await harness.store.finish_dead_letter(claimed.claim)


async def test_expired_pending_and_discarded_retention(harness: Harness) -> None:
    """Retain terminal rows until expiry; neither expired nor discarded entries can be replayed."""
    entry = await harness.store.put_dead_letter(capture())
    discarded = await harness.store.discard_dead_letter(WS, "entry")
    assert discarded.status == "discarded"
    assert await harness.store.discard_dead_letter(WS, "entry") == discarded
    await harness.expire(discarded, retention=True)
    with pytest.raises(ImmutableViolation):
        await harness.store.claim_dead_letter(WS, "entry", "a")
    assert await harness.store.delete_expired_dead_letter(WS, "entry")
    recreated = await harness.store.put_dead_letter(entry.data)
    await harness.expire(recreated, retention=True)
    with pytest.raises(ImmutableViolation):
        await harness.store.claim_dead_letter(WS, "entry", "a")


async def test_retention_and_claim_compete_at_boundary(harness: Harness) -> None:
    """Expiry wins over both initial and abandoned claims, regardless of competing call order."""
    for identifier, abandoned in (("pending", False), ("abandoned", True)):
        entry = await harness.store.put_dead_letter(
            replace(capture(identifier), retention_seconds=1)
        )
        if abandoned:
            entry = await harness.store.claim_dead_letter(WS, identifier, "old", ttl_seconds=1)
            await harness.expire(entry)
            persisted = await harness.store.get_dead_letter(WS, identifier)
            assert persisted is not None
            entry = persisted
        await harness.expire(entry, retention=True)
        results = await asyncio.gather(
            harness.store.claim_dead_letter(WS, identifier, "new"),
            harness.store.delete_expired_dead_letter(WS, identifier),
            return_exceptions=True,
        )
        assert isinstance(results[0], ImmutableViolation)
        assert results[1] is True
        assert await harness.store.get_dead_letter(WS, identifier) is None


async def test_discard_and_claim_are_serialized(harness: Harness) -> None:
    """Either discard wins before claiming, or the live claim blocks discard; never both."""
    await harness.store.put_dead_letter(capture())
    results = await asyncio.gather(
        harness.store.discard_dead_letter(WS, "entry"),
        harness.store.claim_dead_letter(WS, "entry", "worker"),
        return_exceptions=True,
    )
    row = await harness.store.get_dead_letter(WS, "entry")
    assert row is not None
    if row.status == "discarded":
        assert isinstance(results[0], DeadLetterEntry)
        assert isinstance(results[1], ImmutableViolation)
    else:
        assert row.status == "replaying"
        assert isinstance(results[0], LeaseBusy)
        assert isinstance(results[1], DeadLetterEntry)


@pytest.mark.integration
async def test_postgres_migration_readiness_indexes_and_outage(pg_dsn: str) -> None:
    """Readiness fails closed; additive migration preserves the general metadata minimum."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    adapter = PgMetadataAdapter(pool=pool)
    try:
        async with pool.acquire() as conn:
            indexes = await conn.fetch(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname='custos_state' AND tablename='dead_letter'"
            )
            assert {
                "dead_letter_by_id",
                "dead_letter_by_status",
                "dead_letter_by_subscription",
                "dead_letter_by_created",
                "dead_letter_by_expiry",
                "dead_letter_by_claim_expiry",
            } <= {row["indexname"] for row in indexes}
            await conn.execute(
                "DELETE FROM custos_meta.adapter_revisions "
                "WHERE interface_name='MetadataStoreProvider' AND revision=7"
            )
        with pytest.raises(MigrationRequired):
            await adapter.check_dead_letters_ready()
        await adapter.apply_pending()
        assert adapter.SCHEMA_REVISION == 4
        assert await adapter.apply_pending() == []
        await adapter.check_dead_letters_ready()
    finally:
        await pool.close()
    with pytest.raises(BackendUnavailable):
        await adapter.get_dead_letter(WS, "entry")


@pytest.mark.integration
async def test_postgres_lock_wait_expires_claim_and_rolls_back(pg_dsn: str) -> None:
    """An operation begun live cannot commit after waiting beyond its claim's database expiry."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    assert pool is not None
    adapter = PgMetadataAdapter(pool=pool)
    try:
        await adapter.put_dead_letter(capture())
        entry = await adapter.claim_dead_letter(WS, "entry", "a", ttl_seconds=1)
        assert entry.claim is not None
        async with pool.acquire() as lock, lock.transaction():
            await lock.execute(
                "SELECT entry_id FROM custos_state.dead_letter WHERE workspace_id=$1 FOR UPDATE",
                WS,
            )
            task = asyncio.create_task(
                adapter.finish_dead_letter(entry.claim, result_id="must-not-win")
            )
            try:
                async with pool.acquire() as timer:
                    async with asyncio.timeout(5):
                        while not await timer.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                            "WHERE datname=current_database() AND wait_event_type='Lock' "
                            "AND query LIKE '%dead_letter%' AND pid <> pg_backend_pid())"
                        ):
                            await asyncio.sleep(0.01)
                    await timer.execute(
                        "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM "
                        "($1::timestamptz-clock_timestamp())))+0.02)",
                        entry.claim.expires_at,
                    )
                assert not task.done()
            except BaseException:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
                raise
        with pytest.raises(LeaseExpired):
            await task
        after = await adapter.get_dead_letter(WS, "entry")
        assert after == entry
    finally:
        await pool.close()
