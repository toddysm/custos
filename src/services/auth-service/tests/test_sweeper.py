"""Tests for :mod:`custos_auth.sweeper` (AS-IMPL-016, GH-#251)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.ids import PrincipalId, ServiceTokenId, WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount, ServiceToken

from custos_auth.audit import EVENT_TOKEN_EXPIRED, PLATFORM_WORKSPACE_ID
from custos_auth.sweeper import (
    _jittered_interval,
    run_sweeper_loop,
    sweep_once,
)
from custos_auth.token_revoked_events import (
    LocalTokenRevokedBus,
    TokenRevokedEvent,
)
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter


def _seed_sa(store: FakeAuthAdapter, principal_id: str, workspace_id: str) -> None:
    now = datetime.now(UTC)
    store.principals[principal_id] = ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId(principal_id),
        workspace_id=WorkspaceId(workspace_id),
        display_name=principal_id,
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )


def _seed_token(
    store: FakeAuthAdapter,
    *,
    token_id: str,
    service_account_id: str,
    hash: str,
    ttl: timedelta,
) -> None:
    now = datetime.now(UTC)
    store.service_tokens[token_id] = ServiceToken(
        token_id=ServiceTokenId(token_id),
        service_account_id=PrincipalId(service_account_id),
        hash=hash,
        issued_at=now - timedelta(days=120),
        expires_at=now + ttl,
        revoked_at=None,
        revoked_by=None,
        revoked_reason=None,
    )


@pytest.mark.asyncio
async def test_sweep_once_emits_audit_and_event_per_expired_token() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()
    seen: list[TokenRevokedEvent] = []

    async def witness(event: TokenRevokedEvent) -> None:
        seen.append(event)

    bus.subscribe(witness)

    _seed_sa(auth, "sa-1", "ws-1")
    _seed_token(
        auth, token_id="tok-old", service_account_id="sa-1", hash="h1", ttl=timedelta(days=-1)
    )
    _seed_token(
        auth, token_id="tok-live", service_account_id="sa-1", hash="h2", ttl=timedelta(days=30)
    )

    deleted = await sweep_once(
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        publisher=bus,
    )

    # Only the expired row is deleted; the live row survives.
    assert deleted == 1
    assert "tok-old" not in auth.service_tokens
    assert "tok-live" in auth.service_tokens

    # Exactly one ``token.expired`` audit row, keyed under the SA's
    # workspace and carrying the original expires_at on the payload.
    expired_rows = [
        (ws, e) for ws, e in meta.append_audit_calls if e.event_type == EVENT_TOKEN_EXPIRED
    ]
    assert len(expired_rows) == 1
    ws_id, event = expired_rows[0]
    assert ws_id == "ws-1"
    assert event.actor == "sa-1"
    assert event.subject == {"token_id": "tok-old", "service_account_id": "sa-1"}
    assert "expires_at" in event.payload

    # And exactly one eviction event published, carrying the hash so
    # cross-replica subscribers don't have to re-hash.
    assert seen == [
        TokenRevokedEvent(token_id="tok-old", token_hash="h1", service_account_id="sa-1")
    ]


@pytest.mark.asyncio
async def test_sweep_once_falls_back_to_platform_when_sa_missing() -> None:
    # Defensive: SAs are never hard-deleted, but a TOCTOU race
    # between the snapshot and the SA lookup must not drop the
    # audit row. The sweeper buckets the orphan under the platform
    # sentinel so operators can re-bucket if needed.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()
    _seed_token(
        auth, token_id="tok-orphan", service_account_id="sa-gone", hash="h", ttl=timedelta(days=-1)
    )

    deleted = await sweep_once(
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        publisher=bus,
    )
    assert deleted == 1
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == PLATFORM_WORKSPACE_ID
    assert event.event_type == EVENT_TOKEN_EXPIRED


@pytest.mark.asyncio
async def test_sweep_once_is_idempotent_after_first_run() -> None:
    # AC: "Sweeper is at-least-once and idempotent." Running the
    # cycle a second time after the rows are gone is a no-op.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()
    _seed_sa(auth, "sa-1", "ws-1")
    _seed_token(
        auth, token_id="tok-old", service_account_id="sa-1", hash="h1", ttl=timedelta(days=-1)
    )

    deleted_1 = await sweep_once(
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        publisher=bus,
    )
    deleted_2 = await sweep_once(
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        publisher=bus,
    )
    assert deleted_1 == 1
    assert deleted_2 == 0
    # And only one audit row was emitted across the two cycles.
    assert sum(1 for _ws, e in meta.append_audit_calls if e.event_type == EVENT_TOKEN_EXPIRED) == 1


@pytest.mark.asyncio
async def test_sweep_once_emits_per_row_even_when_audit_fails() -> None:
    # Audit drops must not abort the sweep — the SPL physical
    # delete must still run so an unresponsive audit pipeline
    # cannot quietly disable housekeeping.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter(append_audit_should_fail=True)
    bus = LocalTokenRevokedBus()
    _seed_sa(auth, "sa-1", "ws-1")
    _seed_token(
        auth, token_id="tok-old", service_account_id="sa-1", hash="h1", ttl=timedelta(days=-1)
    )

    deleted = await sweep_once(
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        publisher=bus,
    )
    assert deleted == 1
    assert "tok-old" not in auth.service_tokens


def test_jittered_interval_is_bounded() -> None:
    # ±25 % jitter on a 100-second interval keeps the sleep in
    # [75, 125]. Running it many times must always fall in the
    # band — a regression here would cause replica thundering.
    for _ in range(100):
        v = _jittered_interval(100)
        assert 75.0 <= v <= 125.0


def test_jittered_interval_zero_is_zero() -> None:
    # Disabled sweeper has no jitter (defensive — the loop bails
    # before sleeping anyway).
    assert _jittered_interval(0) == 0.0


@pytest.mark.asyncio
async def test_run_sweeper_loop_disabled_returns_immediately() -> None:
    # The lifespan handler always spawns the task; ``interval=0``
    # is the only way to express "no sweeper". The coroutine must
    # therefore exit cleanly without scheduling a sleep so the
    # task object reaches the done state and the lifespan's
    # cancel/await doesn't deadlock.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()
    await asyncio.wait_for(
        run_sweeper_loop(
            auth_store=auth,  # type: ignore[arg-type]
            metadata_store=meta,  # type: ignore[arg-type]
            publisher=bus,
            interval_seconds=0,
        ),
        timeout=1.0,
    )


@pytest.mark.asyncio
async def test_run_sweeper_loop_cancellation_is_clean() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()
    task = asyncio.create_task(
        run_sweeper_loop(
            auth_store=auth,  # type: ignore[arg-type]
            metadata_store=meta,  # type: ignore[arg-type]
            publisher=bus,
            interval_seconds=60,
        )
    )
    await asyncio.sleep(0)  # let the loop reach its sleep
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_run_sweeper_loop_continues_after_unexpected_error(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # If a cycle raises an unexpected error the loop must log it
    # and continue rather than die silently — a flaky SPL must not
    # disable housekeeping.
    import custos_auth.sweeper as sweeper

    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()

    calls = {"n": 0}

    async def boom_once(**_kw: object) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient SPL hiccup")
        # Second call: cancel the loop to make the test
        # deterministic.
        raise asyncio.CancelledError

    monkeypatch.setattr(sweeper, "sweep_once", boom_once)
    monkeypatch.setattr(sweeper, "_jittered_interval", lambda _s: 0.0)

    with pytest.raises(asyncio.CancelledError):
        await sweeper.run_sweeper_loop(
            auth_store=auth,  # type: ignore[arg-type]
            metadata_store=meta,  # type: ignore[arg-type]
            publisher=bus,
            interval_seconds=1,
        )
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_sweep_once_emits_per_row_even_when_publish_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Publisher exceptions must not abort the sweep.

    Symmetric with the audit-fail test: the SPL physical delete
    must still complete so a flaky pub/sub backend cannot quietly
    disable housekeeping. The exception is logged at WARNING; the
    audit row is still written.
    """

    class FailingPublisher:
        async def publish(self, _event: TokenRevokedEvent) -> None:
            raise RuntimeError("simulated pub/sub outage")

    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    _seed_sa(auth, "sa-1", "ws-1")
    _seed_token(
        auth, token_id="tok-old", service_account_id="sa-1", hash="h1", ttl=timedelta(days=-1)
    )

    with caplog.at_level("WARNING", logger="custos_auth.sweeper"):
        deleted = await sweep_once(
            auth_store=auth,  # type: ignore[arg-type]
            metadata_store=meta,  # type: ignore[arg-type]
            publisher=FailingPublisher(),
        )

    assert deleted == 1
    assert "tok-old" not in auth.service_tokens

    # The audit row still landed despite the publisher exception.
    assert sum(1 for _ws, e in meta.append_audit_calls if e.event_type == EVENT_TOKEN_EXPIRED) == 1

    # And the failure surfaced at WARNING so operators can see the drop.
    assert any("publish failed" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_run_sweeper_loop_logs_count_after_nonzero_deletion(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a cycle deletes ≥1 row the loop emits an INFO summary.

    Pins the operator-facing visibility contract: dashboards and
    log-search alerts that key on the ``deleted N expired
    token(s)`` line keep working across refactors.
    """
    import custos_auth.sweeper as sweeper

    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    bus = LocalTokenRevokedBus()

    calls = {"n": 0}

    async def stub_sweep(**_kw: object) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            return 3  # non-zero deletion → triggers the INFO log
        raise asyncio.CancelledError

    monkeypatch.setattr(sweeper, "sweep_once", stub_sweep)
    monkeypatch.setattr(sweeper, "_jittered_interval", lambda _s: 0.0)

    with (
        caplog.at_level("INFO", logger="custos_auth.sweeper"),
        pytest.raises(asyncio.CancelledError),
    ):
        await sweeper.run_sweeper_loop(
            auth_store=auth,  # type: ignore[arg-type]
            metadata_store=meta,  # type: ignore[arg-type]
            publisher=bus,
            interval_seconds=1,
        )

    assert calls["n"] == 2
    assert any("deleted 3 expired token(s)" in record.getMessage() for record in caplog.records)
