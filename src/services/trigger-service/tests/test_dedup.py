"""Reserve-before-dispatch dedup component (TS-IMPL-009)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from custos_trigger.dedup import (
    DEDUP_KEY_PREFIX,
    DedupDecision,
    Deduplicator,
    compute_dedup_key,
)
from custos_trigger.providers import InMemoryTriggerMetadataStore

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now


# --- compute_dedup_key -------------------------------------------------------


def test_compute_dedup_key_is_deterministic() -> None:
    first = compute_dedup_key("sub-1", "evt-1")
    second = compute_dedup_key("sub-1", "evt-1")
    assert first == second
    assert first.startswith(f"{DEDUP_KEY_PREFIX}:")


def test_compute_dedup_key_varies_by_inputs() -> None:
    base = compute_dedup_key("sub-1", "evt-1")
    assert compute_dedup_key("sub-2", "evt-1") != base
    assert compute_dedup_key("sub-1", "evt-2") != base


def test_compute_dedup_key_no_delimiter_collision() -> None:
    # Length-prefixing must keep these two pairs distinct.
    assert compute_dedup_key("a", "b:c") != compute_dedup_key("a:b", "c")


# --- reserve -----------------------------------------------------------------


async def test_reserve_first_event_is_unseen() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)

    result = await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")

    assert result.decision is DedupDecision.UNSEEN
    assert result.is_unseen is True
    assert result.is_duplicate is False
    assert store.dedup_key("ws-1", result.key) is not None


async def test_reserve_replay_within_window_is_duplicate() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)

    first = await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")
    second = await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")

    assert first.is_unseen
    assert second.is_duplicate
    assert second.key == first.key


async def test_reserve_again_after_ttl_expiry_is_unseen() -> None:
    clock = _Clock(_NOW)
    store = InMemoryTriggerMetadataStore(now=clock)
    dedup = Deduplicator(store, default_ttl_seconds=60)

    await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")
    clock.now = _NOW + timedelta(seconds=61)

    again = await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")

    assert again.is_unseen


async def test_reserve_honors_ttl_override() -> None:
    clock = _Clock(_NOW)
    store = InMemoryTriggerMetadataStore(now=clock)
    dedup = Deduplicator(store, default_ttl_seconds=10)

    result = await dedup.reserve(
        workspace_id="ws-1",
        subscription_id="sub-1",
        event_id="evt-1",
        ttl_seconds=600,
    )

    row = store.dedup_key("ws-1", result.key)
    assert row is not None
    assert row.expires_at == _NOW + timedelta(seconds=600)


# --- guard -------------------------------------------------------------------


async def test_guard_unseen_keeps_reservation_on_success() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)
    dispatched = False

    async with dedup.guard(
        workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1"
    ) as reservation:
        assert reservation.is_unseen
        dispatched = True

    assert dispatched is True
    # The key remains committed after a successful dispatch.
    assert store.dedup_key("ws-1", reservation.key) is not None


async def test_guard_duplicate_suppresses_dispatch() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)
    await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")

    dispatched = False
    async with dedup.guard(
        workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1"
    ) as reservation:
        if reservation.is_unseen:  # pragma: no cover - must not run
            dispatched = True

    assert reservation.is_duplicate
    assert dispatched is False


async def test_guard_releases_reservation_when_dispatch_fails() -> None:
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)

    class _DispatchError(RuntimeError):
        pass

    with pytest.raises(_DispatchError):
        async with dedup.guard(
            workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1"
        ) as reservation:
            assert reservation.is_unseen
            raise _DispatchError

    # Key not committed when dispatch fails -> the redelivery sees it as unseen.
    assert store.dedup_key("ws-1", reservation.key) is None
    retry = await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")
    assert retry.is_unseen


async def test_guard_does_not_release_on_duplicate_failure() -> None:
    # When the event is a duplicate, the pre-existing reservation must survive
    # even if the (suppressed) body somehow raises.
    store = InMemoryTriggerMetadataStore(now=lambda: _NOW)
    dedup = Deduplicator(store)
    first = await dedup.reserve(workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1")

    with pytest.raises(RuntimeError):
        async with dedup.guard(
            workspace_id="ws-1", subscription_id="sub-1", event_id="evt-1"
        ) as reservation:
            assert reservation.is_duplicate
            raise RuntimeError("body error after duplicate")

    assert store.dedup_key("ws-1", first.key) is not None


async def test_release_is_noop_when_store_lacks_capability() -> None:
    # A store without ``release_dedup_key`` must not raise on rollback.
    class _MinimalStore:
        async def put_dedup_key(self, workspace_id: object, key: str, ttl_seconds: int) -> object:
            from custos_spl.interfaces.metadata_store import DedupKey, DedupReserved

            return DedupReserved(
                key=DedupKey(workspace_id=workspace_id, key=key, expires_at=_NOW)  # type: ignore[arg-type]
            )

    dedup = Deduplicator(_MinimalStore())  # type: ignore[arg-type]
    # Should not raise even though the store has no release hook.
    await dedup.release(workspace_id="ws-1", key="trigger.dedup.v1:abc")
