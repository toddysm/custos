"""Tests for the durable idempotency ledger adapter (WF-IMPL-117).

Pins the :class:`~custos_workflow.validator.DurableIdempotencyLedger`
mapping over a :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`:

* Fresh reservation → ``replayed=False``.
* Same key + same fingerprint inside the TTL window (in-flight or
  completed reservation) → ``replayed=True``.
* Same key + *different* fingerprint inside the window →
  :class:`IdempotencyConflictError`.
* TTL expiry: ``purge_expired`` reaps lapsed rows, and a fresh reserve
  after expiry mints a brand-new ``replayed=False`` entry.
* Restart simulation: a new ledger over the *same* provider replays an
  earlier reservation (durability comes from the provider, not the
  ledger object).
* Constructor rejects non-positive ``ttl``; ``record_or_replay`` rejects
  empty ``workspace_id`` / ``idempotency_key`` / ``request_fingerprint``.
* The adapter pins ``route`` / ``principal_id`` to the published
  sentinels so the SPL row collapses onto the WF ``(workspace, key)``
  contract.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from custos_spl.ids import PrincipalId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    ExistingCompleted,
    ExistingInFlight,
    IdempotencyRecord,
    IdemReserved,
    KeyReuse,
    MetadataStoreProvider,
    ReserveIdempotencyResult,
)

from custos_workflow.validator import DurableIdempotencyLedger
from custos_workflow.validator.durable_idempotency_ledger import (
    LEDGER_PRINCIPAL,
    LEDGER_ROUTE,
)
from custos_workflow.validator.errors import IdempotencyConflictError

# ---------------------------------------------------------------------------
# Test double: an in-memory provider implementing only the idempotency
# subset the ledger drives, with an injectable clock so TTL boundaries
# are deterministic.
# ---------------------------------------------------------------------------


class _FakeIdempotencyProvider:
    """Minimal in-memory idempotency CAS mirroring the SPL contract."""

    def __init__(self, now: datetime) -> None:
        self._now = now
        self._rows: dict[tuple[str, str, str, str], IdempotencyRecord] = {}
        self.reserve_calls: list[tuple[str, str, str, str, str, int]] = []
        self.delete_calls: list[datetime] = []

    def advance(self, delta: timedelta) -> None:
        self._now = self._now + delta

    def set_completed(self, key: tuple[str, str, str, str]) -> None:
        """Flip a reserved row to ``completed`` to exercise the replay path."""
        row = self._rows[key]
        self._rows[key] = IdempotencyRecord(
            workspace_id=row.workspace_id,
            principal_id=row.principal_id,
            route=row.route,
            idempotency_key=row.idempotency_key,
            request_hash=row.request_hash,
            status="completed",
            response_snapshot={"ok": True},
            reserved_at=row.reserved_at,
            expires_at=row.expires_at,
        )

    async def reserve_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult:
        self.reserve_calls.append(
            (
                str(workspace_id),
                str(principal_id),
                route,
                idempotency_key,
                request_hash,
                ttl_seconds,
            )
        )
        key = (str(workspace_id), str(principal_id), route, idempotency_key)
        existing = self._rows.get(key)
        if existing is None or existing.expires_at <= self._now:
            record = IdempotencyRecord(
                workspace_id=workspace_id,
                principal_id=principal_id,
                route=route,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                status="in_progress",
                response_snapshot=None,
                reserved_at=self._now,
                expires_at=self._now + timedelta(seconds=ttl_seconds),
            )
            self._rows[key] = record
            return IdemReserved(record=record)
        if existing.request_hash != request_hash:
            return KeyReuse(record=existing)
        if existing.status == "in_progress":
            return ExistingInFlight(record=existing)
        return ExistingCompleted(
            record=existing,
            response_snapshot=existing.response_snapshot or {},
        )

    async def delete_expired_idempotency_records(self, before: datetime) -> int:
        self.delete_calls.append(before)
        stale = [k for k, r in self._rows.items() if r.expires_at <= before]
        for k in stale:
            del self._rows[k]
        return len(stale)


def _provider(now: datetime | None = None) -> _FakeIdempotencyProvider:
    return _FakeIdempotencyProvider(now or datetime(2025, 1, 1, tzinfo=UTC))


def _ledger(
    provider: _FakeIdempotencyProvider,
    *,
    ttl: timedelta = timedelta(hours=24),
) -> DurableIdempotencyLedger:
    return DurableIdempotencyLedger(
        cast(MetadataStoreProvider, provider),
        ttl=ttl,
        now=lambda: provider._now,
    )


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


def test_ttl_defaults_to_pt24h() -> None:
    """An unset ttl falls back to the published 24h default."""
    ledger = DurableIdempotencyLedger(cast(MetadataStoreProvider, _provider()))
    assert ledger.ttl == timedelta(hours=24)


@pytest.mark.parametrize("bad", [timedelta(0), timedelta(seconds=-1)])
def test_ttl_must_be_positive(bad: timedelta) -> None:
    with pytest.raises(ValueError, match="ttl must be positive"):
        DurableIdempotencyLedger(cast(MetadataStoreProvider, _provider()), ttl=bad)


@pytest.mark.asyncio
async def test_fractional_ttl_rounds_up_to_whole_seconds() -> None:
    """A sub-second-fractional TTL is rounded *up* so the window never shrinks."""
    provider = _provider()
    ledger = _ledger(provider, ttl=timedelta(seconds=1.5))
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    # The provider records the ttl_seconds it was reserved with.
    (*_head, ttl_seconds) = provider.reserve_calls[0]
    assert ttl_seconds == 2


# ---------------------------------------------------------------------------
# record_or_replay: argument validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workspace_id", "idempotency_key", "request_fingerprint", "match"),
    [
        ("", "key-1", "fp", "workspace_id must be non-empty"),
        ("ws-1", "", "fp", "idempotency_key must be non-empty"),
        ("ws-1", "key-1", "", "request_fingerprint must be non-empty"),
    ],
)
async def test_record_or_replay_rejects_empty_args(
    workspace_id: str,
    idempotency_key: str,
    request_fingerprint: str,
    match: str,
) -> None:
    ledger = _ledger(_provider())
    with pytest.raises(ValueError, match=match):
        await ledger.record_or_replay(
            workspace_id=workspace_id,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
        )


# ---------------------------------------------------------------------------
# record_or_replay: dedup state machine
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_reservation_is_not_replayed() -> None:
    provider = _provider()
    ledger = _ledger(provider)
    entry = await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert entry.replayed is False
    assert entry.workspace_id == "ws-1"
    assert entry.idempotency_key == "key-1"
    assert entry.request_fingerprint == "fp-1"


@pytest.mark.asyncio
async def test_reserve_pins_route_and_principal_sentinels() -> None:
    """The SPL row keys on the published ``StartRun`` / ``wf-start-run``."""
    provider = _provider()
    ledger = _ledger(provider)
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    (_, principal, route, *_rest) = provider.reserve_calls[0]
    assert route == LEDGER_ROUTE == "StartRun"
    assert principal == LEDGER_PRINCIPAL == "wf-start-run"


@pytest.mark.asyncio
async def test_same_fingerprint_in_flight_replays() -> None:
    provider = _provider()
    ledger = _ledger(provider)
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    entry = await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert entry.replayed is True


@pytest.mark.asyncio
async def test_same_fingerprint_completed_replays() -> None:
    provider = _provider()
    ledger = _ledger(provider)
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    provider.set_completed(("ws-1", LEDGER_PRINCIPAL, LEDGER_ROUTE, "key-1"))
    entry = await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert entry.replayed is True


@pytest.mark.asyncio
async def test_different_fingerprint_raises_conflict() -> None:
    provider = _provider()
    ledger = _ledger(provider)
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    with pytest.raises(IdempotencyConflictError) as excinfo:
        await ledger.record_or_replay(
            workspace_id="ws-1",
            idempotency_key="key-1",
            request_fingerprint="fp-2",
        )
    assert excinfo.value.workspace_id == "ws-1"
    assert excinfo.value.idempotency_key == "key-1"


# ---------------------------------------------------------------------------
# TTL expiry + purge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_after_expiry_mints_fresh_entry() -> None:
    provider = _provider()
    ledger = _ledger(provider, ttl=timedelta(seconds=60))
    first = await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert first.replayed is False
    provider.advance(timedelta(seconds=61))
    second = await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert second.replayed is False


@pytest.mark.asyncio
async def test_purge_expired_reaps_lapsed_rows() -> None:
    provider = _provider()
    ledger = _ledger(provider, ttl=timedelta(seconds=60))
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert await ledger.purge_expired() == 0
    provider.advance(timedelta(seconds=61))
    assert await ledger.purge_expired() == 1
    assert await ledger.purge_expired() == 0


@pytest.mark.asyncio
async def test_purge_expired_honours_explicit_before() -> None:
    provider = _provider()
    ledger = _ledger(provider, ttl=timedelta(seconds=60))
    await ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    far_future = provider._now + timedelta(days=1)
    assert await ledger.purge_expired(before=far_future) == 1
    assert provider.delete_calls[-1] == far_future


# ---------------------------------------------------------------------------
# Restart durability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_ledger_over_same_provider_replays() -> None:
    """Durability lives in the provider — a fresh ledger still dedups."""
    provider = _provider()
    first_ledger = _ledger(provider)
    await first_ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    # Simulate a process restart: brand-new ledger object, same provider.
    second_ledger = _ledger(provider)
    entry = await second_ledger.record_or_replay(
        workspace_id="ws-1",
        idempotency_key="key-1",
        request_fingerprint="fp-1",
    )
    assert entry.replayed is True
