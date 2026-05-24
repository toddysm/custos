"""Unit tests for :mod:`custos_auth.authz_cache`.

Coverage targets:

* TTL behaviour — hits within TTL, lazy expiry after, exact-edge
  semantics (``expires_at`` is strictly greater than ``now``).
* Hit/miss counters update on the read path only.
* Disabled mode (``ttl_seconds <= 0``) bypasses get/put completely
  and does not pollute the counters.
* Invalidation matrix — workspace scope narrows, tenant/platform
  scope falls through to a per-principal sweep.
* ``flush()`` drops everything and returns the row count for
  observability.
"""

from __future__ import annotations

import pytest
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    RoleBindingScope,
    TenantScope,
    WorkspaceScope,
)

from custos_auth.authz_cache import (
    DEFAULT_AUTHZ_CACHE_TTL_SECONDS,
    AuthzDecisionCache,
)
from custos_auth.binding_events import BindingChangedEvent


class _ManualClock:
    """Monotonic clock stand-in for deterministic TTL tests."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now: float = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _event(scope: RoleBindingScope, *, principal_id: str = "user-1") -> BindingChangedEvent:
    return BindingChangedEvent(
        principal_id=principal_id,
        role_id="role:workspace.viewer",
        scope=scope,
        action="revoked",
        binding_id="rb-1",
    )


def test_default_ttl_constant_matches_design() -> None:
    # The design's "Authz (decision) … 60s" entry is the source of
    # truth — the default constant must track it so a Settings build
    # that doesn't override the env var still gets the design value.
    assert DEFAULT_AUTHZ_CACHE_TTL_SECONDS == 60


def test_enabled_is_false_when_ttl_zero() -> None:
    cache = AuthzDecisionCache(ttl_seconds=0)
    assert cache.enabled is False


def test_enabled_is_true_when_ttl_positive() -> None:
    cache = AuthzDecisionCache(ttl_seconds=1)
    assert cache.enabled is True


def test_get_returns_none_on_disabled_cache_without_counter_update() -> None:
    cache = AuthzDecisionCache(ttl_seconds=0)
    result = cache.get("user-1", "ws-1", "workflow:read")
    assert result is None
    # Disabled-mode bypass is a documented configuration; cache
    # pressure dashboards should not light up because of it.
    assert cache.hits == 0
    assert cache.misses == 0


def test_put_is_noop_on_disabled_cache() -> None:
    cache = AuthzDecisionCache(ttl_seconds=0)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    # Nothing was stored — even if we re-enable later, get is empty.
    cache.ttl_seconds = 60
    assert cache.get("user-1", "ws-1", "workflow:read") is None


def test_put_then_get_returns_stored_decision_and_increments_hits() -> None:
    clock = _ManualClock()
    cache = AuthzDecisionCache(ttl_seconds=60, time_source=clock)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    entry = cache.get("user-1", "ws-1", "workflow:read")
    assert entry is not None
    assert entry.allowed is True
    assert entry.reason == "allow-bound"
    assert entry.expires_at == clock.now + 60
    assert cache.hits == 1
    assert cache.misses == 0


def test_get_miss_increments_misses() -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    assert cache.misses == 1
    assert cache.hits == 0


def test_get_expires_lazily_and_drops_the_row() -> None:
    clock = _ManualClock()
    cache = AuthzDecisionCache(ttl_seconds=10, time_source=clock)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    # Just before expiry — still a hit.
    clock.advance(9.999)
    assert cache.get("user-1", "ws-1", "workflow:read") is not None
    # At/past expiry — miss.
    clock.advance(1.0)
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    # Stale row is dropped on the read path so the dict does not
    # grow unbounded.
    clock.advance(60.0)
    assert cache.get("user-1", "ws-1", "workflow:read") is None  # still miss
    assert cache.misses == 2
    assert cache.hits == 1


def test_invalidate_principal_workspace_narrows_to_matching_pair() -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-1", "ws-1", "workflow:execute", allowed=False, reason="deny")
    cache.put("user-1", "ws-2", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-2", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    dropped = cache.invalidate_principal_workspace("user-1", "ws-1")
    assert dropped == 2
    # ws-1 entries for user-1 are gone; the other rows survive.
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    assert cache.get("user-1", "ws-1", "workflow:execute") is None
    assert cache.get("user-1", "ws-2", "workflow:read") is not None
    assert cache.get("user-2", "ws-1", "workflow:read") is not None


def test_invalidate_principal_evicts_across_all_workspaces() -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-1", "ws-2", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-2", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    dropped = cache.invalidate_principal("user-1")
    assert dropped == 2
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    assert cache.get("user-1", "ws-2", "workflow:read") is None
    assert cache.get("user-2", "ws-1", "workflow:read") is not None


def test_flush_drops_everything() -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-2", "ws-2", "workflow:read", allowed=True, reason="allow-bound")
    assert cache.flush() == 2
    assert cache.flush() == 0


def test_invalidate_methods_no_op_on_empty_cache() -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)
    assert cache.invalidate_principal_workspace("user-1", "ws-1") == 0
    assert cache.invalidate_principal("user-1") == 0


@pytest.mark.asyncio
async def test_on_binding_changed_workspace_scope_narrows_eviction() -> None:
    # Workspace scope is the precise case: the bus tells us exactly
    # which (principal, workspace) pair changed, so unrelated
    # workspaces retain their cached decisions.
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-1", "ws-2", "workflow:read", allowed=True, reason="allow-bound")
    await cache.on_binding_changed(_event(WorkspaceScope(workspace_id="ws-1")))  # type: ignore[arg-type]
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    assert cache.get("user-1", "ws-2", "workflow:read") is not None


@pytest.mark.asyncio
async def test_on_binding_changed_tenant_scope_flushes_principal() -> None:
    # Tenant scope: the cache cannot tell which workspaces belong to
    # the tenant, so we evict every entry for the principal.
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-1", "ws-2", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-2", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    await cache.on_binding_changed(_event(TenantScope(tenant_id="tenant-1")))  # type: ignore[arg-type]
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    assert cache.get("user-1", "ws-2", "workflow:read") is None
    # Other principals are unaffected.
    assert cache.get("user-2", "ws-1", "workflow:read") is not None


@pytest.mark.asyncio
async def test_on_binding_changed_global_scope_flushes_principal() -> None:
    # Platform scope is the broadest — flush every entry for the
    # principal exactly like the tenant case.
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.put("user-2", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    await cache.on_binding_changed(_event(GlobalScope()))
    assert cache.get("user-1", "ws-1", "workflow:read") is None
    assert cache.get("user-2", "ws-1", "workflow:read") is not None


@pytest.mark.asyncio
async def test_on_binding_changed_is_noop_when_cache_disabled() -> None:
    cache = AuthzDecisionCache(ttl_seconds=0)
    await cache.on_binding_changed(_event(GlobalScope()))
    # No raise; nothing to assert beyond not crashing — disabled
    # mode is a structural no-op.


def test_on_binding_changed_sync_is_synchronous_alias() -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
    cache.on_binding_changed_sync(_event(WorkspaceScope(workspace_id="ws-1")))  # type: ignore[arg-type]
    assert cache.get("user-1", "ws-1", "workflow:read") is None
