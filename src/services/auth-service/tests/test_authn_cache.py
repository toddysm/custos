"""Tests for :mod:`custos_auth.authn_cache` (AS-IMPL-014)."""

from __future__ import annotations

from datetime import UTC, datetime

from custos_spl.ids import PrincipalId, WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount

from custos_auth.authn_cache import AuthnCache, CachedAuthn


def _sa(principal_id: str = "sa-1", workspace_id: str = "ws-1") -> ServiceAccount:
    return ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId(principal_id),
        workspace_id=WorkspaceId(workspace_id),
        display_name=principal_id,
        disabled_at=None,
        disabled_reason=None,
        created_at=datetime.now(UTC),
    )


class _FakeClock:
    """Test clock with monotonic advance(). Mirrors authz_cache tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_get_returns_none_on_miss_and_counts_miss() -> None:
    clock = _FakeClock()
    cache = AuthnCache(ttl_seconds=30, time_source=clock)
    assert cache.get("h-missing") is None
    assert cache.misses == 1
    assert cache.hits == 0


def test_put_then_get_returns_principal_and_counts_hit() -> None:
    clock = _FakeClock()
    cache = AuthnCache(ttl_seconds=30, time_source=clock)
    sa = _sa()
    cache.put("h-1", principal=sa, token_id="tok-1")
    cached = cache.get("h-1")
    assert isinstance(cached, CachedAuthn)
    assert cached.principal is sa
    assert cached.token_id == "tok-1"
    assert cache.hits == 1
    assert cache.misses == 0


def test_get_drops_entry_after_ttl_expiry() -> None:
    clock = _FakeClock()
    cache = AuthnCache(ttl_seconds=30, time_source=clock)
    cache.put("h-1", principal=_sa(), token_id="tok-1")
    clock.advance(30.001)
    assert cache.get("h-1") is None
    assert cache.misses == 1
    # Subsequent lookups must not see the dropped row either.
    assert cache.get("h-1") is None


def test_invalidate_by_hash_returns_true_and_evicts() -> None:
    cache = AuthnCache(ttl_seconds=30)
    cache.put("h-1", principal=_sa(), token_id="tok-1")
    assert cache.invalidate_by_hash("h-1") is True
    assert cache.get("h-1") is None


def test_invalidate_by_hash_returns_false_when_absent() -> None:
    cache = AuthnCache(ttl_seconds=30)
    assert cache.invalidate_by_hash("h-ghost") is False


def test_invalidate_by_token_id_returns_true_and_evicts() -> None:
    cache = AuthnCache(ttl_seconds=30)
    cache.put("h-1", principal=_sa(), token_id="tok-1")
    assert cache.invalidate_by_token_id("tok-1") is True
    # Both indices are gone — re-invalidating by hash must report miss.
    assert cache.invalidate_by_hash("h-1") is False
    assert cache.invalidate_by_token_id("tok-1") is False


def test_invalidate_by_token_id_returns_false_when_absent() -> None:
    cache = AuthnCache(ttl_seconds=30)
    assert cache.invalidate_by_token_id("tok-ghost") is False


def test_flush_returns_count_and_clears_both_indices() -> None:
    cache = AuthnCache(ttl_seconds=30)
    cache.put("h-1", principal=_sa("sa-1"), token_id="tok-1")
    cache.put("h-2", principal=_sa("sa-2"), token_id="tok-2")
    assert cache.flush() == 2
    assert cache.invalidate_by_hash("h-1") is False
    assert cache.invalidate_by_token_id("tok-2") is False


def test_put_then_invalidate_then_put_keeps_indices_consistent() -> None:
    # Regression: the reverse ``token_id -> hash`` index must not
    # carry a stale entry after an evict-then-reinsert cycle, since
    # the hash key for the same token_id might change across
    # rotations (e.g. tests reuse ``tok-1`` with a new hash).
    cache = AuthnCache(ttl_seconds=30)
    cache.put("h-old", principal=_sa(), token_id="tok-1")
    cache.invalidate_by_token_id("tok-1")
    cache.put("h-new", principal=_sa(), token_id="tok-1")
    assert cache.get("h-old") is None
    assert cache.get("h-new") is not None
    # Invalidate by token id evicts the *new* hash, not the stale one.
    assert cache.invalidate_by_token_id("tok-1") is True
    assert cache.get("h-new") is None


def test_disabled_cache_bypasses_put_and_get() -> None:
    # ``ttl_seconds=0`` is the AS-IMPL-014 bypass-mode knob.
    cache = AuthnCache(ttl_seconds=0)
    assert cache.enabled is False
    cache.put("h-1", principal=_sa(), token_id="tok-1")
    assert cache.get("h-1") is None
    # Counters must not move in bypass mode — the cache pressure
    # dashboard would otherwise drown in synthetic misses.
    assert cache.misses == 0
    assert cache.hits == 0


def test_disabled_cache_invalidate_is_noop() -> None:
    cache = AuthnCache(ttl_seconds=0)
    # Both invalidation paths must report False because nothing is
    # ever stored in a disabled cache.
    assert cache.invalidate_by_hash("h-1") is False
    assert cache.invalidate_by_token_id("tok-1") is False
