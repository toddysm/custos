"""Memory/PostgreSQL parity through protected administration routes and audited retention."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import (
    DeadLetterEntry,
    DeadLetterFilter,
    DispatchAttempt,
    ReplayClaim,
)
from custos_spl.interfaces.metadata_store import AuditEvent, AuditOutboxBatch, TransactionHandle
from custos_spl.pagination import encode_query_cursor
from fastapi import Depends, FastAPI, Request

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import (
    RunRef,
    StartRunRequest,
    WorkflowClientReplayUnsafeError,
    WorkflowClientTransportError,
)
from custos_trigger.dead_letters import DeadLetterAdmin, DeadLetterAdminError
from custos_trigger.dedup import Deduplicator, compute_dedup_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.pipeline.dispatch import Dispatcher, NoopAuditSink
from custos_trigger.pipeline.reliable import ReliableDispatcher, ReliableStorage
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.stores.base import TriggerMetadataStore
from tests.integration import test_dead_letter_state as state_tests
from tests.integration.test_dead_letter_state import (
    WS,
    Harness,
    capture,
)

harness = state_tests.harness
dead_letter_dsn = state_tests.dead_letter_dsn

BASE = f"/v1/workspaces/{WS}/triggers/deadletter"
HEADERS = {
    "x-custos-callctx": json.dumps(
        {
            "workspace_id": WS,
            "principal_id": "operator",
            "permissions": ["trigger:admin"],
        }
    )
}


def administration(harness: Harness, client: FakeWorkflowServiceClient) -> DeadLetterAdmin:
    """Assemble production dispatch/recovery against either authoritative storage backend."""
    assert isinstance(harness.store, ReliableStorage)
    return DeadLetterAdmin(
        ReliableDispatcher(client, harness.store, NoopAuditSink(), max_retries=0)
    )


async def audits(harness: Harness) -> list[str]:
    """Read actual durable audit rows or the memory backend's equivalent transition evidence."""
    if harness.pool is None:
        assert isinstance(harness.store, InMemoryTriggerMetadataStore)
        return [event.event_type for event in harness.store.dead_letter_audits]
    async with harness.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT event_id,event_type FROM custos_state.audit_event "
            "WHERE workspace_id=$1 ORDER BY event_id",
            WS,
        )
        outbox = await conn.fetch(
            "SELECT event_id,event_type FROM custos_state.audit_outbox "
            "WHERE workspace_id=$1 ORDER BY event_id",
            WS,
        )
        assert rows == outbox
        return [row["event_type"] for row in rows]


async def audit_payloads(harness: Harness, outcome: str) -> list[dict[str, Any]]:
    """Inspect precisely the payload delivered to outbox consumers after entry deletion."""
    event_type = "trigger.deadletter." + outcome
    if harness.pool is None:
        assert isinstance(harness.store, InMemoryTriggerMetadataStore)
        return [
            dict(event.payload)
            for event in harness.store.dead_letter_audits
            if event.event_type == event_type
        ]
    async with harness.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT payload::text FROM custos_state.audit_outbox "
            "WHERE workspace_id=$1 AND event_type=$2 ORDER BY id",
            WS,
            event_type,
        )
        return [json.loads(row["payload"]) for row in rows]


async def test_protected_routes_replay_original_target_and_idempotent_outcomes(
    harness: Harness,
) -> None:
    """Real handlers preserve targets, reject unsafe mutations and isolate tenant identities."""
    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    await harness.store.put_dead_letter(
        replace(capture("discard"), target={**data.target, "deliveryProtocol": 2})
    )
    store = cast(TriggerMetadataStore, harness.store)
    app = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(client, Deduplicator(store), reliable=admin.dispatcher),
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger",
        ) as http,
    ):
        assert (await http.get(BASE)).status_code == 401
        denied = {
            "x-custos-callctx": json.dumps(
                {
                    "workspace_id": WS,
                    "principal_id": "reader",
                    "permissions": ["trigger:subscriptions:read"],
                }
            )
        }
        for method, suffix in (
            ("GET", ""),
            ("GET", "/entry"),
            ("POST", "/entry:replay"),
            ("POST", "/entry:discard"),
            ("POST", ":replay"),
        ):
            assert (await http.request(method, BASE + suffix, json={})).status_code == 401
            assert (
                await http.request(method, BASE + suffix, headers=denied, json={})
            ).status_code == 403
        assert (await http.get(BASE, headers=denied)).status_code == 403
        assert (await http.get(BASE.replace(str(WS), "other"), headers=HEADERS)).status_code == 403
        response = await http.get(BASE + "/entry", headers=HEADERS)
        assert response.status_code == 200
        entry = response.json()
        assert entry["schemaVersion"] == 1 and entry["target"]["inputs"] == {"v": 1}
        assert "private" not in response.text and "token" not in response.text
        for _ in range(2):
            replay = await http.post(BASE + "/entry:replay", headers=HEADERS)
            assert replay.status_code == 200 and replay.json() == {"status": "replayed"}
        assert len(client.start_run_calls) == 1
        assert client.start_run_calls[0].inputs == {"v": 1}
        assert client.start_run_calls[0].idempotency_key == "original-key"
        assert (await http.post(BASE + "/entry:discard", headers=HEADERS)).status_code == 409
        invalid_reason = await http.post(
            BASE + "/discard:discard", headers=HEADERS, json={"reason": "invalid\x00reason"}
        )
        assert invalid_reason.status_code == 422
        unchanged = await harness.store.get_dead_letter(WS, "discard")
        assert unchanged is not None and unchanged.status == "pending"
        for _ in range(2):
            assert (
                await http.post(
                    BASE + "/discard:discard", headers=HEADERS, json={"reason": "operator cleanup"}
                )
            ).status_code == 204
        assert (await http.post(BASE + "/discard:replay", headers=HEADERS)).status_code == 409
        assert (await http.get(BASE + "/unknown", headers=HEADERS)).status_code == 404
        for query in ("limit=101", "status=expired", "since=2026-01-01", "cursor=invalid"):
            bad = await http.get(BASE + "?" + query, headers=HEADERS)
            assert bad.status_code == 422 and bad.json()["code"] == "trigger.deadletter.invalid"
        assert (
            await http.post(BASE + ":replay", headers=HEADERS, json={"limit": 101})
        ).status_code == 422
        assert (
            await http.post(BASE + ":replay", headers=HEADERS, json={"unexpected": True})
        ).status_code == 422
        bulk = await http.post(BASE + ":replay", headers=HEADERS, json={})
        assert bulk.status_code == 200 and bulk.json() == {
            "accepted": 1,
            "rejected": 1,
            "nextCursor": None,
        }
    events = await audits(harness)
    assert events.count("trigger.deadletter.stored") == 2
    assert events.count("trigger.deadletter.replayed") == 1
    assert events.count("trigger.deadletter.discarded") == 1
    discarded = await audit_payloads(harness, "discarded")
    assert discarded == [
        {
            "schemaVersion": 1,
            "sourceType": "manual",
            "entryId": "discard",
            "subscriptionId": "sub",
            "actor": "operator",
            "reason": "operator cleanup",
        }
    ]


async def test_pages_bind_filters_and_workspace(harness: Harness) -> None:
    """Pagination is stable across inserts and cannot be reused under different filters."""
    admin = administration(harness, FakeWorkflowServiceClient())
    for identifier in ("a", "c", "e"):
        await harness.store.put_dead_letter(capture(identifier))
    first = await admin.list(str(WS), DeadLetterFilter(), limit=2)
    assert [row.data.entry_id for row in first.items] == ["a", "c"]
    assert first.next_cursor is not None
    await harness.store.put_dead_letter(capture("b"))
    second = await admin.list(str(WS), DeadLetterFilter(), cursor=first.next_cursor.token, limit=2)
    assert [row.data.entry_id for row in second.items] == ["e"] and second.next_cursor is None
    for workspace, filters in (
        (str(WS), DeadLetterFilter(status="pending")),
        ("other", DeadLetterFilter()),
    ):
        with pytest.raises(DeadLetterAdminError, match="pagination"):
            await admin.list(workspace, filters, cursor=first.next_cursor.token)
    with pytest.raises(DeadLetterAdminError, match="not found"):
        await admin.inspect("other", "a")


@pytest.mark.parametrize("protocol", [1, 2])
@pytest.mark.parametrize("boundary", ["lookup", "claim"])
async def test_single_replay_never_sends_a_replacement_generation(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    protocol: int,
    boundary: str,
) -> None:
    """A recreated ID cannot retarget a replay or bypass its versioned-protocol check."""
    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    original = capture()
    await harness.store.put_dead_letter(
        replace(original, target={**original.target, "deliveryProtocol": 2})
    )
    get = harness.store.get_dead_letter
    claim = harness.store.claim_dead_letter
    reads = 0

    async def replace_entry() -> None:
        """Erase the actual inspected row and install a different immutable target."""
        entry = await get(WS, "entry")
        assert entry is not None
        if harness.pool is None:
            assert isinstance(harness.store, InMemoryTriggerMetadataStore)
            harness.store._dead_letters[(str(WS), "entry")] = replace(
                entry, expires_at=harness.clock[0]
            )
        else:
            await harness.expire(entry, retention=True)
        assert await harness.store.delete_expired_dead_letter(WS, "entry")
        await harness.store.put_dead_letter(
            replace(
                original,
                idempotency_key="replacement",
                target={
                    **original.target,
                    "workflowVersionId": "replacement",
                    "deliveryProtocol": protocol,
                },
            )
        )

    async def read_then_replace(workspace: WorkspaceId, identifier: str) -> DeadLetterEntry | None:
        """Replace between administration inspection and the dispatcher's inspection."""
        nonlocal reads
        reads += 1
        if reads == 2:
            await replace_entry()
        return await get(workspace, identifier)

    async def replace_then_claim(
        workspace: WorkspaceId,
        identifier: str,
        actor: str,
        *,
        ttl_seconds: int = 60,
        bulk_id: str | None = None,
        bulk_token: str | None = None,
        expected_identity: str | None = None,
    ) -> DeadLetterEntry:
        """Replace between protocol validation and the atomic claim check."""
        await replace_entry()
        return await claim(
            workspace,
            identifier,
            actor,
            ttl_seconds=ttl_seconds,
            bulk_id=bulk_id,
            bulk_token=bulk_token,
            expected_identity=expected_identity,
        )

    monkeypatch.setattr(
        harness.store,
        "get_dead_letter" if boundary == "lookup" else "claim_dead_letter",
        read_then_replace if boundary == "lookup" else replace_then_claim,
    )
    with pytest.raises(DeadLetterAdminError) as unsafe:
        await admin.replay(str(WS), "entry", "operator")
    assert unsafe.value.code == "trigger.replay_unsafe"
    assert not client.start_run_calls


async def test_bulk_does_not_count_an_already_replayed_replacement(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement's completed status is not proof that the frozen selection was delivered."""
    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    claim = harness.store.claim_dead_letter

    async def replace_then_claim(
        workspace: WorkspaceId,
        identifier: str,
        actor: str,
        *,
        ttl_seconds: int = 60,
        bulk_id: str | None = None,
        bulk_token: str | None = None,
        expected_identity: str | None = None,
    ) -> DeadLetterEntry:
        """Install and complete another generation after the original selection was validated."""
        entry = await harness.store.get_dead_letter(workspace, identifier)
        assert entry is not None
        if harness.pool is None:
            assert isinstance(harness.store, InMemoryTriggerMetadataStore)
            harness.store._dead_letters[(str(workspace), identifier)] = replace(
                entry, expires_at=harness.clock[0]
            )
        else:
            await harness.expire(entry, retention=True)
        assert await harness.store.delete_expired_dead_letter(workspace, identifier)
        await harness.store.put_dead_letter(
            replace(
                data,
                idempotency_key="new-key",
                target={**data.target, "workflowVersionId": "new-version", "deliveryProtocol": 2},
            )
        )
        replacement = await claim(workspace, identifier, "another")
        assert replacement.claim is not None
        await harness.store.finish_dead_letter(replacement.claim, result_id="replacement-run")
        return await claim(
            workspace,
            identifier,
            actor,
            ttl_seconds=ttl_seconds,
            bulk_id=bulk_id,
            bulk_token=bulk_token,
            expected_identity=expected_identity,
        )

    monkeypatch.setattr(harness.store, "claim_dead_letter", replace_then_claim)
    assert await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator") == (0, 1, None)
    assert not client.start_run_calls


async def test_failed_replay_returns_its_own_committed_status(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later operator's claim cannot change the prior failed attempt's response."""
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("unavailable"))
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    finish = harness.store.finish_dead_letter

    async def finish_then_reclaim(
        claim: ReplayClaim,
        *,
        failure: DispatchAttempt | None = None,
        result_id: str | None = None,
    ) -> DeadLetterEntry:
        """Return the immutable finished result after another real claim has acquired the entry."""
        finished = await finish(claim, failure=failure, result_id=result_id)
        await harness.store.claim_dead_letter(WS, "entry", "later-operator")
        return finished

    monkeypatch.setattr(harness.store, "finish_dead_letter", finish_then_reclaim)
    assert await admin.replay(str(WS), "entry", "operator") == "pending"
    current = await harness.store.get_dead_letter(WS, "entry")
    assert current is not None and current.status == "replaying"


async def test_discard_returns_not_found_after_concurrent_retention(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deletion between inspection and mutation is absence, not replay contention."""
    admin = administration(harness, FakeWorkflowServiceClient())
    entry = await harness.store.put_dead_letter(capture())
    discard = harness.store.discard_dead_letter

    async def delete_then_discard(
        workspace: WorkspaceId,
        identifier: str,
        *,
        actor: str = "trigger-service",
        reason: str | None = None,
    ) -> DeadLetterEntry:
        """Remove the persisted row before the actual provider tries to mutate it."""
        await harness.expire(entry, retention=True)
        assert await harness.store.delete_expired_dead_letter(workspace, identifier)
        return await discard(workspace, identifier, actor=actor, reason=reason)

    monkeypatch.setattr(harness.store, "discard_dead_letter", delete_then_discard)
    with pytest.raises(DeadLetterAdminError) as absent:
        await admin.discard(str(WS), "entry", "operator")
    assert absent.value.code == "trigger.deadletter.not_found"


@pytest.mark.integration
async def test_outbox_drain_waits_for_lower_uncommitted_ids(pg_dsn: str) -> None:
    """A higher committed sequence cannot move the drain cursor past an earlier pending writer."""
    import asyncpg
    from custos_pg import PgMetadataAdapter
    from custos_pg.migrations.metadata import AUDIT_OUTBOX_LOCK_KEY

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    inserted, release = asyncio.Event(), asyncio.Event()
    adapter = PgMetadataAdapter(pool=pool)
    async with pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT cache_size FROM pg_sequences "
                "WHERE schemaname='custos_state' AND sequencename='audit_outbox_id_seq'"
            )
            == 1
        )
    now = await adapter.delivery_now()
    first = AuditEvent(WS, "first", "test.order", "operator", {}, {}, now)
    second = replace(first, event_id="second")

    async def delayed(tx: TransactionHandle) -> None:
        """Allocate the first outbox ID but hold its transaction until the drain is waiting."""
        await adapter.append_audit(WS, first, tx=tx)
        inserted.set()
        await release.wait()

    async def immediate(tx: TransactionHandle) -> None:
        """Commit a higher allocated ID before the first transaction becomes visible."""
        await adapter.append_audit(WS, second, tx=tx)

    writer = asyncio.create_task(adapter.with_transaction(delayed))
    drain: asyncio.Task[AuditOutboxBatch] | None = None
    try:
        async with asyncio.timeout(10):
            await inserted.wait()
            await adapter.with_transaction(immediate)
            async with pool.acquire() as conn:
                visible = await conn.fetch(
                    "SELECT event_id FROM custos_state.audit_outbox ORDER BY id"
                )
                assert [row["event_id"] for row in visible] == ["second"]
            drain = asyncio.create_task(adapter.stream_audit_outbox(0, 100))
            async with pool.acquire() as conn:
                while not await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE "
                    "locktype='advisory' AND mode='ExclusiveLock' AND NOT granted "
                    "AND classid=$1::oid AND objid=$2::oid)",
                    AUDIT_OUTBOX_LOCK_KEY >> 32,
                    AUDIT_OUTBOX_LOCK_KEY & 0xFFFFFFFF,
                ):
                    assert not drain.done(), "drain skipped the uncommitted lower ID"
                    await asyncio.sleep(0.01)
            release.set()
            await writer
            batch = await drain
            assert [row.event_id for row in batch.rows] == ["first", "second"]
            assert not (await adapter.stream_audit_outbox(batch.next_cursor, 100)).rows

            async def read_only(connection: asyncpg.Connection) -> None:
                """Prove draining needs no UPDATE/DELETE/TRUNCATE permission on the outbox."""
                await connection.execute("SET ROLE pg_read_all_data")

            async with asyncpg.create_pool(
                dsn=pg_dsn,
                min_size=1,
                max_size=1,
                setup=read_only,
            ) as reader_pool:
                async with reader_pool.acquire() as conn:
                    assert await conn.fetchval(
                        "SELECT NOT has_table_privilege(current_user, "
                        "'custos_state.audit_outbox','UPDATE')"
                    )
                reader = PgMetadataAdapter(pool=reader_pool)
                assert len((await reader.stream_audit_outbox(0, 100)).rows) == 2
    finally:
        release.set()
        if drain is not None and not drain.done():
            drain.cancel()
        await asyncio.gather(
            writer, *([drain] if drain is not None else []), return_exceptions=True
        )
        await pool.close()


async def test_dispatcher_does_not_duplicate_provider_owned_lifecycle_audits(
    harness: Harness,
) -> None:
    """A forwarding audit sink receives pipeline events, never duplicate lifecycle writes."""
    from tests.integration.test_reliable_dispatch import event, match

    forwarded: list[str] = []

    class ForwardingSink:
        """Model a configured durable forwarding destination without silently deduplicating."""

        async def emit(
            self,
            event_name: str,
            *,
            workspace_id: str,
            attributes: Mapping[str, Any],
        ) -> None:
            """Record every event sent by the dispatcher, including unwanted duplicates."""
            forwarded.append(event_name)

    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("upstream unavailable"))
    assert isinstance(harness.store, ReliableStorage)
    reliable = ReliableDispatcher(client, harness.store, ForwardingSink(), max_retries=0)
    original = match()
    original.subscription.workspace_id = str(WS)
    assert (await reliable.dispatch_start(event(), original)).is_dead_lettered
    entry = (await harness.store.query_dead_letters(WS)).items[0]
    assert (await reliable.replay(str(WS), entry.data.entry_id, "operator")).is_dead_lettered
    client.error = None
    assert (await reliable.replay(str(WS), entry.data.entry_id, "operator")).is_dispatched
    assert forwarded
    assert not any(name.startswith("trigger.deadletter.") for name in forwarded)
    lifecycle = await audits(harness)
    for outcome in ("stored", "replay_failed", "replayed"):
        assert lifecycle.count("trigger.deadletter." + outcome) == 1


@pytest.mark.parametrize("audit_failure", [False, True])
async def test_failure_capture_audit_and_receipt_acknowledgement_are_atomic(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    audit_failure: bool,
) -> None:
    """Actual dispatch cannot acknowledge a failed batch unless capture and audit both commit."""
    from unittest.mock import Mock

    from tests.integration.test_reliable_dispatch import event, match

    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("upstream unavailable"))
    admin = administration(harness, client)
    original = match()
    original.subscription.workspace_id = str(WS)
    if audit_failure:
        if harness.pool is None:
            monkeypatch.setattr(
                "custos_spl.delivery_memory.dead_letter_transition_audits",
                Mock(side_effect=BackendUnavailable("audit unavailable")),
            )
        else:
            monkeypatch.setattr(
                harness.store,
                "append_audit",
                AsyncMock(side_effect=BackendUnavailable("audit unavailable")),
            )
        with pytest.raises(TriggerError) as failure:
            await admin.dispatcher.dispatch_start(event(), original)
        assert failure.value.kind == TriggerErrorKind.DISPATCH_UNAVAILABLE
    else:
        assert (await admin.dispatcher.dispatch_start(event(), original)).is_dead_lettered
    receipt = await admin.store.get_delivery(WS, "trigger", compute_dedup_key("sub", "event"))
    assert receipt is not None and receipt.completed is not audit_failure
    assert len((await harness.store.query_dead_letters(WS)).items) == int(not audit_failure)
    assert (await audits(harness)).count("trigger.deadletter.stored") == int(not audit_failure)


async def test_gateway_forwards_scoped_recovery_to_real_trigger_handlers(harness: Harness) -> None:
    """Exercise Gateway forwarding and Trigger recovery against both storage backends."""
    from custos_gateway.errors import register_exception_handlers as gateway_errors
    from custos_gateway.middleware.auth import AUTH_STATE_ATTR, AuthorizedCaller
    from custos_gateway.middleware.callctx_mint import OUTBOUND_METADATA_STATE_ATTR
    from custos_gateway.router import DownstreamRouter
    from custos_gateway.routes.registry import M1_ROUTE_REGISTRY, _make_forwarder
    from custos_gateway.settings import load_settings as gateway_settings
    from custos_workflow.providers import _InProcessMetadataStoreProvider

    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    held = await harness.store.claim_dead_letter(WS, "entry", "old-operator", ttl_seconds=300)
    store = cast(TriggerMetadataStore, harness.store)
    trigger = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(client, Deduplicator(store), reliable=admin.dispatcher),
    )
    gateway = FastAPI()
    gateway_errors(gateway)
    gateway.state.metadata_store = (
        harness.store if harness.pool is not None else _InProcessMetadataStoreProvider()
    )
    gateway.state.settings = gateway_settings(
        {
            "CUSTOS_GATEWAY_TLS_CERT_REF": "secretref://tls/cert",
            "CUSTOS_GATEWAY_TLS_KEY_REF": "secretref://tls/key",
            "CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": '["https://ui.custos.example"]',
        }
    )
    unavailable = False

    async def authorized(request: Request) -> None:
        """Bind the same already-authorized context used by Gateway's tested forwarding seam."""
        setattr(
            request.state,
            AUTH_STATE_ATTR,
            AuthorizedCaller(
                principal_id="operator",
                audit_event_id="test",
                permission="trigger:admin",
                workspace_id=str(WS),
            ),
        )
        setattr(request.state, OUTBOUND_METADATA_STATE_ATTR, HEADERS)

    for spec in M1_ROUTE_REGISTRY:
        if spec.required_permission == "trigger:admin":
            gateway.add_api_route(
                spec.path,
                _make_forwarder(spec),
                methods=[spec.method],
                dependencies=[Depends(authorized)],
            )
    async with (
        trigger.router.lifespan_context(trigger),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=trigger),
            base_url="http://trigger",
        ) as downstream,
    ):

        async def sidecar(request: httpx.Request) -> httpx.Response:
            """Pass actual serialized bytes and scope to the owning Trigger HTTP endpoint."""
            if unavailable:
                return httpx.Response(503)
            path = request.url.path.removeprefix("/v1.0/invoke/trigger-service/method")
            return await downstream.request(
                request.method,
                path,
                headers=request.headers,
                content=request.content,
                params=request.url.params,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(sidecar)) as transport:
            gateway.state.downstream_router = DownstreamRouter(transport)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=gateway),
                base_url="http://gateway",
            ) as http:
                listing = await http.get(BASE + "?limit=1")
                assert (
                    listing.status_code == 200 and listing.json()["items"][0]["entryId"] == "entry"
                )
                headers = {"content-type": "application/json", "idempotency-key": "same-key"}
                conflict = await http.post(BASE + "/entry:replay", headers=headers)
                assert conflict.status_code == 409
                assert conflict.json()["code"] == "trigger.replay_in_progress"
                unavailable = True
                assert (await http.post(BASE + "/entry:replay", headers=headers)).status_code == 503
                unavailable = False
                await harness.expire(held)
                for _ in range(2):
                    response = await http.post(BASE + "/entry:replay", headers=headers)
                    assert response.status_code == 200 and response.json() == {"status": "replayed"}
                changed = await http.post(BASE + "/other:replay", headers=headers)
                assert changed.status_code == 409
                assert changed.json()["code"] == "idempotency-key-reuse"
                for identifier in ("bulk-a", "bulk-b"):
                    await harness.store.put_dead_letter(
                        replace(
                            capture(identifier),
                            idempotency_key=identifier,
                            target={**data.target, "deliveryProtocol": 2},
                        )
                    )
                bulk_headers = {**headers, "idempotency-key": "bulk-request"}
                first = await http.post(
                    BASE + ":replay", headers=bulk_headers, json={"status": "pending", "limit": 1}
                )
                repeated = await http.post(
                    BASE + ":replay", headers=bulk_headers, json={"status": "pending", "limit": 1}
                )
                assert first.status_code == repeated.status_code == 200
                assert first.json() == repeated.json() and first.json()["accepted"] == 1
                assert repeated.headers["idempotency-key"] == "bulk-request"
                unselected = await harness.store.get_dead_letter(WS, "bulk-b")
                assert unselected is not None and unselected.status == "pending"
    assert len(client.start_run_calls) == 2
    assert (await audits(harness)).count("trigger.deadletter.replayed") == 2


def test_administration_openapi_documents_versioned_shapes_and_bounds() -> None:
    """Generated schemas use the shared wire aliases and exact response/filter shapes."""
    schema = create_app(authz_endpoint="").openapi()
    base = "/v1/workspaces/{workspace_id}/triggers/deadletter"
    assert set(schema["paths"][base]) == {"get"}
    entry = schema["components"]["schemas"]["EntryView"]["properties"]
    assert entry["schemaVersion"]["const"] == 1
    assert {"entryId", "workspaceId", "replayHistory", "claimExpiresAt"} <= set(entry)
    assert "claim" not in entry and "token" not in entry
    selector = schema["components"]["schemas"]["BulkSelector"]
    assert selector["additionalProperties"] is False
    assert selector["properties"]["limit"]["maximum"] == 100
    assert schema["components"]["schemas"]["PageView"]["properties"]["nextCursor"]


@pytest.mark.parametrize("enabled", [False, True])
async def test_disabled_dispatch_and_storage_outage_fail_explicitly(
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
) -> None:
    """Unavailable recovery never succeeds using an empty list or a volatile substitute."""
    store = InMemoryTriggerMetadataStore()
    workflow = FakeWorkflowServiceClient()
    reliable = ReliableDispatcher(workflow, store, NoopAuditSink()) if enabled else None
    app = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(workflow, Deduplicator(store), reliable=reliable),
    )
    query = AsyncMock(side_effect=BackendUnavailable("private database connection detail"))
    monkeypatch.setattr(store, "query_dead_letters", query)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger",
        ) as client,
    ):
        response = await client.get(BASE, headers=HEADERS)
    assert response.status_code == 503
    assert response.json()["code"] == "trigger.deadletter.unavailable"
    assert "private" not in response.text
    assert query.await_count == int(enabled)


async def test_concurrent_replay_and_discard_conflict(harness: Harness) -> None:
    """One real held downstream call permits neither another effect nor concurrent discard."""
    started, release = asyncio.Event(), asyncio.Event()

    class HeldClient(FakeWorkflowServiceClient):
        """Hold only the outbound start; use actual provider claim fencing."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Release the first call only after conflicting mutations are checked."""
            started.set()
            await release.wait()
            return await super().start_run(request)

    client = HeldClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    task = asyncio.create_task(admin.replay(str(WS), "entry", "operator"))
    try:
        async with asyncio.timeout(5):
            await started.wait()
            with pytest.raises(DeadLetterAdminError) as conflict:
                await admin.replay(str(WS), "entry", "another")
            assert conflict.value.code == "trigger.replay_in_progress"
            with pytest.raises(DeadLetterAdminError) as conflict:
                await admin.discard(str(WS), "entry", "another")
            assert conflict.value.code == "trigger.replay_in_progress"
            assert await admin.bulk_replay(str(WS), DeadLetterFilter(), "another") == (0, 1, None)
            release.set()
            assert await task == "replayed"
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(client.start_run_calls) == 1


async def test_replay_failure_and_terminal_attempt_are_durably_audited(harness: Harness) -> None:
    """Failed or unsafe upstream outcomes never masquerade as successful delivery."""
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("private vendor error"))
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, max_replay_attempts=1, target={**data.target, "deliveryProtocol": 2})
    )
    assert await admin.replay(str(WS), "entry", "operator") == "discarded"
    events = await audits(harness)
    assert events.count("trigger.deadletter.replay_failed") == 1
    assert events.count("trigger.deadletter.discarded") == 1
    client.error = WorkflowClientReplayUnsafeError()
    await harness.store.put_dead_letter(
        replace(capture("unsafe"), target={**data.target, "deliveryProtocol": 2})
    )
    with pytest.raises(DeadLetterAdminError) as conflict:
        await admin.replay(str(WS), "unsafe", "operator")
    assert conflict.value.code == "trigger.replay_unsafe"
    assert (await audits(harness)).count("trigger.deadletter.replay_failed") == 2


async def test_unacknowledged_resume_is_not_finalized_as_replayed(harness: Harness) -> None:
    """A submitted event followed by a terminal conflict never creates a false replay success."""
    from custos_trigger.clients.workflow import DaprEndpoint, DaprWorkflowServiceClient
    from custos_trigger.dead_letters import DeadLetterAdmin

    responses = iter((202, 409))
    data = capture(resume=True)
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
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
        with pytest.raises(DeadLetterAdminError) as waiting:
            await admin.replay(str(WS), "entry", "operator")
        assert waiting.value.code == "trigger.deadletter.unavailable"
        with pytest.raises(DeadLetterAdminError) as terminal:
            await admin.replay(str(WS), "entry", "operator")
        assert terminal.value.code == "trigger.replay_unsafe"
    retained = await harness.store.get_dead_letter(WS, "entry")
    assert retained is not None and retained.status == "pending"
    assert "trigger.deadletter.replayed" not in await audits(harness)


@pytest.mark.parametrize("statuses", [(202, 202), (202, 503), (503, 202), (202, 403)])
async def test_pending_acknowledgement_does_not_exhaust_one_attempt_budget(
    harness: Harness,
    statuses: tuple[int, int],
) -> None:
    """Unacknowledged submissions reconcile one logical attempt until consumption is proven."""
    from custos_trigger.clients.workflow import DaprEndpoint, DaprWorkflowServiceClient
    from custos_trigger.dead_letters import DeadLetterAdmin

    responses = iter((*statuses, 204))
    data = capture(resume=True)
    await harness.store.put_dead_letter(
        replace(
            data,
            max_replay_attempts=1,
            target={**data.target, "deliveryProtocol": 2},
        )
    )
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
        for actor in ("operator-a", "operator-b"):
            with pytest.raises(DeadLetterAdminError):
                await admin.replay(str(WS), "entry", actor)
            pending = await harness.store.get_dead_letter(WS, "entry")
            assert pending is not None and pending.status == "pending" and pending.claim is None
            assert len(pending.replay_history) == 1
            assert pending.replay_history[0].outcome == "awaiting_ack"
            assert pending.replay_history[0].worker_id == "operator-a"
        assert await admin.replay(str(WS), "entry", "operator-b") == "replayed"
    events = await audits(harness)
    assert events.count("trigger.deadletter.replay_pending") == 1
    assert "trigger.deadletter.replay_failed" not in events
    assert "trigger.deadletter.discarded" not in events
    final = (await audit_payloads(harness, "replayed"))[0]
    assert final["actor"] == "operator-b" and final["attemptOperator"] == "operator-a"


@pytest.mark.parametrize("key", ["nul\x00entry", "\ud800"])
async def test_decoded_cursor_text_is_rejected_consistently(harness: Harness, key: str) -> None:
    """Valid scope binding cannot make an invalid PostgreSQL identifier a valid pagination key."""
    workflow = FakeWorkflowServiceClient()
    admin = administration(harness, workflow)
    store = cast(TriggerMetadataStore, harness.store)
    app = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(workflow, Deduplicator(store), reliable=admin.dispatcher),
    )
    token = encode_query_cursor(DeadLetterFilter().scope(WS), key).token
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger",
        ) as client,
    ):
        for response in (
            await client.get(BASE, headers=HEADERS, params={"cursor": token}),
            await client.post(BASE + ":replay", headers=HEADERS, json={"cursor": token}),
        ):
            assert response.status_code == 422
            assert response.json()["code"] == "trigger.deadletter.invalid"


@pytest.mark.parametrize("status", ["pending", "replayed", "discarded", "replaying"])
async def test_retention_covers_all_statuses_without_deleting_live_replay(
    harness: Harness,
    status: str,
) -> None:
    """Expiry audit and deletion are atomic; live claims remain protected past retention."""
    admin = administration(harness, FakeWorkflowServiceClient())
    entry = await harness.store.put_dead_letter(capture())
    if status in ("replayed", "replaying"):
        entry = await harness.store.claim_dead_letter(WS, "entry", "operator", ttl_seconds=300)
        assert entry.claim is not None
        if status == "replayed":
            entry = await harness.store.finish_dead_letter(entry.claim, result_id="run")
    elif status == "discarded":
        entry = await harness.store.discard_dead_letter(WS, "entry")
    if status == "replaying" and harness.pool is None:
        harness.clock[0] = entry.created_at + timedelta(seconds=1)
        assert isinstance(harness.store, InMemoryTriggerMetadataStore)
        entry = replace(entry, expires_at=harness.clock[0])
        harness.store._dead_letters[(str(WS), "entry")] = entry
    else:
        await harness.expire(entry, retention=True)
    if status == "replaying":
        assert (await admin.sweep(str(WS))).expired == 0
        retained = await harness.store.get_dead_letter(WS, "entry")
        assert retained is not None
        await harness.expire(retained)
    assert (await admin.sweep(str(WS))).expired == 1
    assert await harness.store.get_dead_letter(WS, "entry") is None
    assert (await audits(harness)).count("trigger.deadletter.expired") == 1
    assert (await admin.sweep(str(WS))).expired == 0


@pytest.mark.parametrize("final_attempt", [False, True])
async def test_expiration_audits_interrupted_attempt_before_erasing_history(
    harness: Harness,
    final_attempt: bool,
) -> None:
    """The outbox keeps the original operator, interruption and terminal transition after purge."""
    await harness.store.put_dead_letter(
        replace(capture(), max_replay_attempts=1 if final_attempt else 3)
    )
    entry = await harness.store.claim_dead_letter(WS, "entry", "original-operator", ttl_seconds=1)
    await harness.expire(entry)
    expired = await harness.store.get_dead_letter(WS, "entry")
    assert expired is not None
    await harness.expire(expired, retention=True)
    assert await harness.store.delete_expired_dead_letter(WS, "entry")
    assert await harness.store.get_dead_letter(WS, "entry") is None
    failed = await audit_payloads(harness, "replay_failed")
    assert len(failed) == 1
    assert failed[0] == {
        "schemaVersion": 1,
        "sourceType": "manual",
        "entryId": "entry",
        "subscriptionId": "sub",
        "actor": "trigger-service",
        "attempt": 1,
        "outcome": "interrupted",
        "attemptOperator": "original-operator",
    }
    assert len(await audit_payloads(harness, "discarded")) == int(final_attempt)
    expiration = (await audit_payloads(harness, "expired"))[0]
    assert expiration["entryId"] == "entry" and expiration["actor"] == "trigger-service"
    assert expiration["previousStatus"] == ("discarded" if final_attempt else "pending")
    assert not await harness.store.delete_expired_dead_letter(WS, "entry")
    assert len(await audit_payloads(harness, "replay_failed")) == 1


async def test_reclaim_keeps_original_attempt_operator_separate_from_recoverer(
    harness: Harness,
) -> None:
    """Reclaim by another operator does not rewrite who initiated the interrupted attempt."""
    await harness.store.put_dead_letter(capture())
    entry = await harness.store.claim_dead_letter(WS, "entry", "operator-a", ttl_seconds=1)
    await harness.expire(entry)
    await harness.store.claim_dead_letter(WS, "entry", "operator-b")
    payload = (await audit_payloads(harness, "replay_failed"))[0]
    assert payload["actor"] == "operator-b" and payload["attemptOperator"] == "operator-a"


@pytest.mark.parametrize("operation", ["expire", "discard", "finish", "store"])
async def test_audit_failure_rolls_back_state(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """Audit failure cannot commit a lifecycle mutation or acknowledge successful replay."""
    admin = administration(harness, FakeWorkflowServiceClient())
    entry = await harness.store.put_dead_letter(capture())
    if operation == "expire":
        await harness.expire(entry, retention=True)
    elif operation == "finish":
        entry = await harness.store.claim_dead_letter(WS, "entry", "operator")
    before = await harness.store.get_dead_letter(WS, "entry")
    events_before = await audits(harness)
    if harness.pool is None:
        from unittest.mock import Mock

        monkeypatch.setattr(
            "custos_trigger.providers.dead_letter_transition_audits",
            Mock(side_effect=BackendUnavailable("audit unavailable")),
        )
    else:
        append = AsyncMock(side_effect=BackendUnavailable("audit unavailable"))
        monkeypatch.setattr(harness.store, "append_audit", append)
    if operation == "expire":
        with pytest.raises(DeadLetterAdminError) as failure:
            await admin.sweep(str(WS))
        assert failure.value.code == "trigger.deadletter.unavailable"
    elif operation == "discard":
        with pytest.raises(DeadLetterAdminError):
            await admin.discard(str(WS), "entry", "operator")
    elif operation == "finish":
        assert entry.claim is not None
        with pytest.raises(BackendUnavailable):
            await harness.store.finish_dead_letter(entry.claim, result_id="run")
    else:
        with pytest.raises(BackendUnavailable):
            await harness.store.put_dead_letter(capture("new"))
        assert await harness.store.get_dead_letter(WS, "new") is None
    assert await harness.store.get_dead_letter(WS, "entry") == before
    assert await audits(harness) == events_before


async def test_bulk_selection_and_concurrency_are_bounded(harness: Harness) -> None:
    """The maximum page issues exactly 100 attempts, never more than eight simultaneously."""
    active, peak = 0, 0
    barrier = asyncio.Event()

    class ConcurrentClient(FakeWorkflowServiceClient):
        """Track real dispatcher concurrency while avoiding time-based sleeps."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Release the first group when the documented concurrency ceiling is reached."""
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 8:
                barrier.set()
            try:
                await barrier.wait()
                return await super().start_run(request)
            finally:
                active -= 1

    client = ConcurrentClient()
    admin = administration(harness, client)
    data = capture()
    for index in range(101):
        await harness.store.put_dead_letter(
            replace(
                data,
                entry_id=f"entry-{index:03}",
                idempotency_key=f"key-{index:03}",
                target={**data.target, "deliveryProtocol": 2},
            )
        )
    async with asyncio.timeout(15):
        accepted, rejected, cursor = await admin.bulk_replay(
            str(WS), DeadLetterFilter(), "operator", limit=100
        )
        assert (accepted, rejected) == (100, 0) and cursor is not None
    assert len(client.start_run_calls) == 100 and peak == 8 and active == 0
    assert (await admin.inspect(str(WS), "entry-100")).status == "pending"
    assert await admin.bulk_replay(
        str(WS), DeadLetterFilter(), "operator", cursor=cursor.token, limit=100
    ) == (1, 0, None)
    with pytest.raises(DeadLetterAdminError):
        await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator", limit=101)


@pytest.mark.parametrize("pending_only", [False, True])
async def test_bulk_cursor_advances_past_unsafe_entries(
    harness: Harness, pending_only: bool
) -> None:
    """A rejected first entry cannot starve the rest of a stable filtered backlog."""
    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    unsafe = capture("a-unsafe")
    await harness.store.put_dead_letter(unsafe)
    await harness.store.put_dead_letter(
        replace(capture("z-safe"), target={**unsafe.target, "deliveryProtocol": 2})
    )
    filters = DeadLetterFilter(status="pending" if pending_only else None)
    accepted, rejected, cursor = await admin.bulk_replay(str(WS), filters, "operator", limit=1)
    assert (accepted, rejected) == (0, 1) and cursor is not None
    assert await admin.bulk_replay(str(WS), filters, "operator", cursor=cursor.token, limit=1) == (
        1,
        0,
        None,
    )
    with pytest.raises(DeadLetterAdminError) as invalid:
        await admin.bulk_replay(
            str(WS), DeadLetterFilter(status="replayed"), "operator", cursor=cursor.token, limit=1
        )
    assert invalid.value.code == "trigger.deadletter.invalid"
    assert len(client.start_run_calls) == 1


@pytest.mark.parametrize("boundary", ["lookup", "claim"])
async def test_bulk_rejects_entries_deleted_during_replay(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    """Retention racing either dispatcher boundary rejects only the vanished entry."""
    admin = administration(harness, FakeWorkflowServiceClient())
    data = capture("a-gone")
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    await harness.store.put_dead_letter(
        replace(capture("z-live"), target={**data.target, "deliveryProtocol": 2})
    )
    original_get = harness.store.get_dead_letter
    original_claim = harness.store.claim_dead_letter
    reads = 0

    async def erase() -> None:
        """Expire and remove the actual persisted entry, not a mocked missing response."""
        entry = await original_get(WS, "a-gone")
        assert entry is not None
        if harness.pool is None:
            assert isinstance(harness.store, InMemoryTriggerMetadataStore)
            harness.store._dead_letters[(str(WS), "a-gone")] = replace(
                entry, expires_at=harness.clock[0]
            )
        else:
            await harness.expire(entry, retention=True)
        assert await harness.store.delete_expired_dead_letter(WS, "a-gone")

    async def read(workspace: WorkspaceId, identifier: str) -> DeadLetterEntry | None:
        """Delete between the admin and dispatcher reads of the selected entry."""
        nonlocal reads
        if identifier == "a-gone":
            reads += 1
            if reads == 2:
                await erase()
        return await original_get(workspace, identifier)

    async def claim(
        workspace: WorkspaceId,
        identifier: str,
        actor: str,
        *,
        ttl_seconds: int = 60,
        bulk_id: str | None = None,
        bulk_token: str | None = None,
        expected_identity: str | None = None,
    ) -> DeadLetterEntry:
        """Delete after both reads but before acquiring replay ownership."""
        if identifier == "a-gone":
            await erase()
        return await original_claim(
            workspace,
            identifier,
            actor,
            ttl_seconds=ttl_seconds,
            bulk_id=bulk_id,
            bulk_token=bulk_token,
            expected_identity=expected_identity,
        )

    monkeypatch.setattr(
        harness.store,
        "get_dead_letter" if boundary == "lookup" else "claim_dead_letter",
        read if boundary == "lookup" else claim,
    )
    assert await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator") == (1, 1, None)


async def test_request_deadline_retains_recoverable_claims_and_awaits_children(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real expired request timeout returns unavailable without leaking replay tasks."""
    stopped = asyncio.Event()
    task: asyncio.Task[object] | None = None
    deadline: asyncio.Timeout | None = None
    actual_timeout = asyncio.timeout

    def observe_timeout(delay: float | None) -> asyncio.Timeout:
        """Capture the real command deadline rather than waiting on a timing heuristic."""
        nonlocal deadline
        timeout = actual_timeout(delay)
        if delay == 60 and deadline is None:
            deadline = timeout
        return timeout

    class TimedClient(FakeWorkflowServiceClient):
        """Block downstream execution until the actual administration deadline cancels it."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Record cleanup so the caller can prove all owned work was awaited."""
            nonlocal task
            task = asyncio.current_task()
            assert deadline is not None
            deadline.reschedule(asyncio.get_running_loop().time())
            try:
                await asyncio.Future[None]()
            finally:
                stopped.set()
            raise AssertionError("unreachable")

    client = TimedClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    monkeypatch.setattr(asyncio, "timeout", observe_timeout)
    with pytest.raises(DeadLetterAdminError) as expired:
        await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator")
    assert expired.value.code == "trigger.deadletter.unavailable"
    assert stopped.is_set() and task is not None and task.done()
    entry = await harness.store.get_dead_letter(WS, "entry")
    assert entry is not None and entry.status == "replaying"
    await harness.expire(entry)
    recovered = await harness.store.recover_dead_letter(WS, "entry")
    assert recovered.status == "pending"
    assert "trigger.deadletter.replay_failed" in await audits(harness)
