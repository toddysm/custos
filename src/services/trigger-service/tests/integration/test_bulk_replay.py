"""Stable bulk-command selection, atomic progress and recovery on memory and PostgreSQL."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import httpx
import pytest
from asyncpg import Connection
from custos_pg import PgMetadataAdapter
from custos_spl.errors import BackendUnavailable, ImmutableViolation
from custos_spl.interfaces.bulk_replay import BulkReplayCommand, BulkReplayStoreProvider
from custos_spl.interfaces.dead_letters import DeadLetterFilter
from custos_spl.interfaces.delivery import delivery_identity

from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import RunRef, StartRunRequest
from custos_trigger.dead_letters import DeadLetterAdmin, DeadLetterAdminError
from tests.integration import test_dead_letter_state as state_tests
from tests.integration.test_dead_letter_admin import administration, audits
from tests.integration.test_dead_letter_state import WS, Harness, capture

harness = state_tests.harness
dead_letter_dsn = state_tests.dead_letter_dsn


def commands(harness: Harness) -> BulkReplayStoreProvider:
    """Require the real command capability rather than a test-only request cache."""
    assert isinstance(harness.store, BulkReplayStoreProvider)
    return harness.store


async def seed(harness: Harness, *identifiers: str) -> None:
    """Give each selected entry its own immutable downstream idempotency identity."""
    for identifier in identifiers:
        data = capture(identifier)
        await harness.store.put_dead_letter(
            replace(
                data,
                idempotency_key="key-" + identifier,
                target={**data.target, "deliveryProtocol": 2},
            )
        )


async def expire_coordinator(harness: Harness, command: BulkReplayCommand) -> None:
    """Expire actual coordinator and related entry authority without deleting selection/progress."""
    assert command.lease_until is not None
    if harness.pool is None:
        harness.clock[0] = command.lease_until
    else:
        async with harness.pool.acquire() as conn:
            await conn.execute(
                "UPDATE custos_state.bulk_replay_command SET lease_until=clock_timestamp() "
                "WHERE workspace_id=$1 AND command_id=$2",
                WS,
                command.command_id,
            )
    assert command.items is not None
    for item in command.items:
        entry = await harness.store.get_dead_letter(WS, item.entry_id)
        if entry is not None and entry.claim is not None:
            await harness.expire(entry)


async def test_lost_bulk_response_reuses_original_selection_and_counts(harness: Harness) -> None:
    """A pending-filter retry cannot select B after the original request completed A."""
    await seed(harness, "a", "b")
    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    filters = DeadLetterFilter(status="pending")
    first = await admin.bulk_replay(str(WS), filters, "operator", limit=1, request_key="one")
    assert first[:2] == (1, 0) and first[2] is not None
    repeated = await admin.bulk_replay(str(WS), filters, "operator", limit=1, request_key="one")
    assert repeated == first and len(client.start_run_calls) == 1
    pending = await harness.store.get_dead_letter(WS, "b")
    assert pending is not None and pending.status == "pending"
    with pytest.raises(DeadLetterAdminError) as changed:
        await admin.bulk_replay(str(WS), filters, "operator", limit=2, request_key="one")
    assert changed.value.code == "trigger.deadletter.conflict"
    assert await admin.bulk_replay(
        str(WS), filters, "operator", limit=1, cursor=first[2].token, request_key="two"
    ) == (1, 0, None)
    assert len(client.start_run_calls) == 2


async def test_bulk_pending_acknowledgement_can_resume_with_one_attempt(harness: Harness) -> None:
    """Pending acknowledgement leaves bulk progress unfinished, not permanently rejected."""
    from custos_trigger.clients.workflow import DaprEndpoint, DaprWorkflowServiceClient
    from custos_trigger.pipeline.dispatch import NoopAuditSink
    from custos_trigger.pipeline.reliable import ReliableDispatcher, ReliableStorage

    data = capture("resume", resume=True)
    await harness.store.put_dead_letter(
        replace(
            data,
            max_replay_attempts=1,
            target={**data.target, "deliveryProtocol": 2},
        )
    )
    responses = iter((202, 204))
    assert isinstance(harness.store, ReliableStorage)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(next(responses)))
    ) as http:
        client = DaprWorkflowServiceClient(
            http, DaprEndpoint(host="127.0.0.1", http_port=3500, app_id="workflow-service")
        )
        admin = DeadLetterAdmin(
            ReliableDispatcher(client, harness.store, NoopAuditSink(), max_retries=0)
        )
        with pytest.raises(DeadLetterAdminError):
            await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator", request_key="pending")
        command = await commands(harness).get_bulk_replay(
            WS, delivery_identity(str(WS), "operator", "pending")
        )
        assert command is not None and command.items is not None
        assert command.items[0].accepted is None
        await expire_coordinator(harness, command)
        assert await admin.bulk_replay(
            str(WS), DeadLetterFilter(), "operator", request_key="pending"
        ) == (1, 0, None)
    entry = await harness.store.get_dead_letter(WS, "resume")
    assert entry is not None and entry.status == "replayed" and len(entry.replay_history) == 1


async def test_partial_bulk_restart_preserves_progress_and_ignores_new_entries(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery skips committed A, resumes selected B and never adds new matching C."""
    await seed(harness, "a", "b")
    block_second = True

    class HeldClient(FakeWorkflowServiceClient):
        """Allow one effect while the other selected request remains uncertain."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Keep B in flight until the failed request's owned tasks are cancelled."""
            if request.idempotency_key == "key-b" and block_second:
                await asyncio.Future[None]()
            return await super().start_run(request)

    client = HeldClient()
    admin = administration(harness, client)
    store = commands(harness)
    record = store.record_bulk_replay_result

    async def lose_response(claim: BulkReplayCommand, entry_id: str, accepted: bool) -> None:
        """Fail after A's effect and progress have committed atomically in replay finish."""
        if entry_id == "a":
            raise BackendUnavailable("simulated response-path failure")
        await record(claim, entry_id, accepted)

    monkeypatch.setattr(store, "record_bulk_replay_result", lose_response)
    with pytest.raises(DeadLetterAdminError) as failed:
        await admin.bulk_replay(
            str(WS),
            DeadLetterFilter(status="pending"),
            "operator",
            request_key="restart",
        )
    assert failed.value.code == "trigger.deadletter.unavailable"
    identity = delivery_identity(str(WS), "operator", "restart")
    command = await store.get_bulk_replay(WS, identity)
    assert command is not None and command.items is not None
    assert [(item.entry_id, item.accepted) for item in command.items] == [("a", True), ("b", None)]
    with pytest.raises(DeadLetterAdminError) as busy:
        await admin.bulk_replay(
            str(WS),
            DeadLetterFilter(status="pending"),
            "operator",
            request_key="restart",
        )
    assert busy.value.code == "trigger.replay_in_progress"
    await seed(harness, "c")
    await expire_coordinator(harness, command)
    monkeypatch.setattr(store, "record_bulk_replay_result", record)
    block_second = False
    restarted = administration(harness, client)
    assert await restarted.bulk_replay(
        str(WS),
        DeadLetterFilter(status="pending"),
        "operator",
        request_key="restart",
    ) == (2, 0, None)
    assert [request.idempotency_key for request in client.start_run_calls] == ["key-a", "key-b"]
    untouched = await harness.store.get_dead_letter(WS, "c")
    assert untouched is not None and untouched.status == "pending"
    assert (await audits(harness)).count("trigger.deadletter.replayed") == 2


async def test_bulk_progress_failure_rolls_back_replay_finish(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A command-progress failure cannot leave a finished entry without its durable result."""
    await seed(harness, "a")
    admin = administration(harness, FakeWorkflowServiceClient())
    store = commands(harness)
    if harness.pool is None:
        from unittest.mock import Mock

        monkeypatch.setattr(
            harness.store,
            "_finished_bulk_item",
            Mock(side_effect=BackendUnavailable("progress unavailable")),
        )
    else:
        assert isinstance(harness.store, PgMetadataAdapter)
        original = harness.store._save_bulk_replay

        async def fail_progress(conn: Connection, command: BulkReplayCommand) -> None:
            """Allow selection creation, then fail the actual completion transaction."""
            if command.items and any(item.accepted is not None for item in command.items):
                raise BackendUnavailable("progress unavailable")
            await original(conn, command)

        monkeypatch.setattr(harness.store, "_save_bulk_replay", fail_progress)
    with pytest.raises(DeadLetterAdminError):
        await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator", request_key="rollback")
    command = await store.get_bulk_replay(WS, delivery_identity(str(WS), "operator", "rollback"))
    entry = await harness.store.get_dead_letter(WS, "a")
    assert command is not None and command.items is not None and command.items[0].accepted is None
    assert entry is not None and entry.status == "replaying"
    assert "trigger.deadletter.replayed" not in await audits(harness)


async def test_bulk_selection_retention_preserves_live_claims_and_identity(
    harness: Harness,
) -> None:
    """Expired selection payloads are erased only after related claims expire, never rebound."""
    await seed(harness, "a")
    store = commands(harness)
    command = await store.begin_bulk_replay(
        WS,
        "retained",
        "operator",
        DeadLetterFilter(),
        retention_seconds=60,
    )
    entry = await harness.store.claim_dead_letter(
        WS,
        "a",
        "operator",
        ttl_seconds=300,
        bulk_id=command.command_id,
        bulk_token=command.token,
    )
    if harness.pool is None:
        harness.clock[0] = command.expires_at
    else:
        async with harness.pool.acquire() as conn:
            await conn.execute(
                "UPDATE custos_state.bulk_replay_command SET expires_at=clock_timestamp(),"
                "lease_until=clock_timestamp() WHERE workspace_id=$1 AND command_id=$2",
                WS,
                command.command_id,
            )
    assert await store.purge_bulk_replay_selections(WS) == 0
    await harness.expire(entry)
    assert await store.purge_bulk_replay_selections(WS) == 1
    tombstone = await store.get_bulk_replay(WS, command.command_id)
    assert tombstone is not None and tombstone.items is None
    assert tombstone.request_hash == command.request_hash and tombstone.actor == command.actor
    with pytest.raises(ImmutableViolation):
        await store.begin_bulk_replay(WS, "retained", "operator", DeadLetterFilter())
