"""Per-pod TTL cache for ``authorize()`` decisions (AS-IMPL-012).

The cache sits in front of the binding-resolution path in
:func:`custos_auth.authorize.authorize`. Every authz call still emits
an :data:`~custos_auth.audit.EVENT_AUTHZ_DECISION` audit row — the
cache short-cuts the *resolution* step only, never the audit trail —
so the audit ledger remains the source of truth even when the cache
serves the answer.

Design reference: ``design/components/auth-service/design.md`` §
"Cache Invalidation Bus":

    | Cache | Key | TTL | Eviction event |
    |-------|-----|-----|----------------|
    | Authz | ``(principalId, roleVersion, workspaceId)`` | 60s | binding-changed |

M1 simplification
-----------------

The design's nominal key is ``(principal_id, role_version,
workspace_id)``. In M1 the cache widens the key to
``(principal_id, workspace_id, permission)`` so that:

* Cache invalidation by ``(principal, workspace)`` evicts every
  permission entry for that pair in a single sweep — the design's
  ``binding-changed`` semantic.
* Per-permission keys make hit/miss counters useful for observability
  (cache pressure correlates with the hottest permission) without
  shipping the resolved permission set on every miss.

``role-version-bumped`` events trigger a full flush — see
:meth:`AuthzDecisionCache.flush`.

Configuration
-------------

The TTL is read from :data:`custos_auth.settings.ENV_AUTHZ_CACHE_TTL`
(``CUSTOS_AUTH_AUTHZ_CACHE_TTL``) with a 60 s default. Setting the
env var to ``0`` puts the cache in **bypass mode** —
:meth:`AuthzDecisionCache.get` always returns ``None`` and
:meth:`AuthzDecisionCache.put` is a no-op. This is the
acceptance-criterion knob from AS-IMPL-012:

    > Cache-disabled mode (``CUSTOS_AUTH_AUTHZ_CACHE_TTL=0``)
    > bypasses the cache.

Threading
---------

The cache is intentionally **in-process** and lives one-per-replica.
Cross-replica consistency rides on the binding-changed pub/sub bus:
each replica subscribes and invalidates its own copy. The
in-process invalidation path
(:class:`~custos_auth.binding_events.LocalBindingChangedBus`) runs on
the same replica that performed the binding mutation so a single-
replica deployment passes the AS-IMPL-012 "revoke-then-recheck within
one round trip" acceptance criterion without standing up a real
transport.

Observability
-------------

Hits and misses are counted on the cache instance itself. Phase F
will lift these onto OTel meters; until then test suites assert
against the instance counters directly.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from custos_auth.binding_events import BindingChangedEvent

_LOGGER = logging.getLogger("custos_auth.authz_cache")


#: Cache key components. The tuple ordering puts ``(principal,
#: workspace)`` first so :meth:`AuthzDecisionCache.invalidate_principal_workspace`
#: can sweep entries with a prefix-match without indexing the
#: permission dimension.
CacheKey = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class CachedDecision:
    """One row in the authz decision cache.

    ``expires_at`` is in the time domain of the
    :class:`AuthzDecisionCache`'s ``time_source`` callable, which is
    ``time.monotonic()`` by default. Monotonic time is the right
    choice for TTL bookkeeping because it is immune to wall-clock
    jumps (NTP step, DST) that would otherwise either evict good
    entries early or keep stale ones forever.
    """

    allowed: bool
    reason: str
    expires_at: float


@dataclass(slots=True)
class AuthzDecisionCache:
    """TTL-bounded per-pod cache of authz decisions.

    The cache is keyed by ``(principal_id, workspace_id, permission)``.
    Workspace-scoped invalidation drops the matching prefix; tenant-
    and platform-scoped invalidation collapse to a full per-principal
    sweep because the cache does not retain the tenant membership
    needed to disambiguate. Over-invalidation is the conservative
    choice — the next call rebuilds the entry and audit truth is
    preserved either way.

    Attributes:
        ttl_seconds: Cache lifetime per entry, in monotonic seconds.
            ``0`` (and any negative value) disables the cache;
            :meth:`get` always returns ``None`` and :meth:`put` is a
            no-op. The disabled-mode flag is exposed via
            :attr:`enabled` for callers that want to skip the cache
            machinery entirely.
        time_source: Monotonic clock callable. Injectable for tests
            so TTL expiry can be exercised without wall-clock sleeps.
        hits: Cumulative cache-hit counter. Mutated by :meth:`get`.
        misses: Cumulative cache-miss counter. Mutated by :meth:`get`.
    """

    ttl_seconds: float
    time_source: Callable[[], float] = time.monotonic
    hits: int = 0
    misses: int = 0
    _entries: dict[CacheKey, CachedDecision] = field(default_factory=dict)

    @property
    def enabled(self) -> bool:
        """``True`` when the cache is configured to store entries."""
        return self.ttl_seconds > 0

    def get(
        self,
        principal_id: str,
        workspace_id: str,
        permission: str,
    ) -> CachedDecision | None:
        """Return a cached decision or ``None``.

        Increments :attr:`hits` on a live entry and :attr:`misses`
        otherwise. When the cache is disabled (``ttl_seconds <= 0``)
        the call short-circuits without touching the counters — the
        bypass path is the documented behaviour of the
        ``CUSTOS_AUTH_AUTHZ_CACHE_TTL=0`` configuration and we do not
        want it polluting cache-pressure dashboards.
        """
        if not self.enabled:
            return None
        entry = self._entries.get((principal_id, workspace_id, permission))
        if entry is None:
            self.misses += 1
            return None
        if entry.expires_at <= self.time_source():
            # Lazy expiry — drop the row on the read path so the
            # entry table does not grow unbounded for a workload that
            # cycles principals without invalidations.
            del self._entries[(principal_id, workspace_id, permission)]
            self.misses += 1
            return None
        self.hits += 1
        return entry

    def put(
        self,
        principal_id: str,
        workspace_id: str,
        permission: str,
        *,
        allowed: bool,
        reason: str,
    ) -> None:
        """Insert (or overwrite) a decision row.

        No-op when the cache is disabled — the AS-IMPL-012 acceptance
        criterion requires that ``CUSTOS_AUTH_AUTHZ_CACHE_TTL=0``
        bypass the cache completely; a put on a disabled cache must
        not allocate.
        """
        if not self.enabled:
            return
        expires_at = self.time_source() + self.ttl_seconds
        self._entries[(principal_id, workspace_id, permission)] = CachedDecision(
            allowed=allowed,
            reason=reason,
            expires_at=expires_at,
        )

    def invalidate_principal_workspace(
        self,
        principal_id: str,
        workspace_id: str,
    ) -> int:
        """Evict every entry for ``(principal_id, workspace_id)``.

        Returns the number of rows dropped so callers (the subscriber
        wired in :mod:`custos_auth.binding_events`) can surface a
        metric. The sweep iterates only the in-process dict so the
        cost is bounded by the per-replica cache size.
        """
        if not self._entries:
            return 0
        victims = [
            key for key in self._entries if key[0] == principal_id and key[1] == workspace_id
        ]
        for key in victims:
            del self._entries[key]
        return len(victims)

    def invalidate_principal(self, principal_id: str) -> int:
        """Evict every entry for ``principal_id`` across all workspaces.

        Used when a tenant- or platform-scoped binding flips. The
        cache does not retain tenant→workspace mapping, so the
        precise eviction set is unknown and the conservative choice
        is to drop every entry for the principal.
        """
        if not self._entries:
            return 0
        victims = [key for key in self._entries if key[0] == principal_id]
        for key in victims:
            del self._entries[key]
        return len(victims)

    def flush(self) -> int:
        """Drop every entry. Used by ``role-version-bumped`` events."""
        count = len(self._entries)
        self._entries.clear()
        return count

    def on_binding_changed_sync(self, event: BindingChangedEvent) -> None:
        """Synchronous form of :meth:`on_binding_changed`.

        Exposed for callers that need to invalidate without awaiting
        (the in-process publisher path uses the async form; this is
        for callers operating inside non-async code paths).
        """
        self._apply_binding_changed(event)

    async def on_binding_changed(self, event: BindingChangedEvent) -> None:
        """Apply :data:`event` to the cache.

        Dispatch:

        * ``workspace`` scope → invalidate the precise
          ``(principal, workspace)`` pair.
        * ``tenant`` or ``platform`` scope → invalidate every entry
          for the principal because the cache cannot tell which
          workspaces fall under the affected tenant / platform scope.

        Coroutine-shaped to match the
        :data:`~custos_auth.binding_events.BindingChangedHandler`
        signature; the body is synchronous because the in-memory
        dict does not require I/O. The bus subscribers and the cross
        -replica subscriber both invoke this coroutine.
        """
        self._apply_binding_changed(event)

    def _apply_binding_changed(self, event: BindingChangedEvent) -> None:
        if not self.enabled or not self._entries:
            return
        kind = event.scope_kind
        if kind == "workspace":
            # Workspace scope carries a concrete workspace_id; narrow
            # the eviction to that bucket so unrelated workspaces
            # retain their cached decisions.
            from custos_spl.interfaces.auth_store import WorkspaceScope

            assert isinstance(event.scope, WorkspaceScope)
            dropped = self.invalidate_principal_workspace(
                event.principal_id,
                str(event.scope.workspace_id),
            )
        else:
            dropped = self.invalidate_principal(event.principal_id)
        if dropped:
            _LOGGER.debug(
                "authz-cache: dropped %d row(s) for principal=%s scope=%s",
                dropped,
                event.principal_id,
                kind,
            )


#: Default TTL used when ``CUSTOS_AUTH_AUTHZ_CACHE_TTL`` is unset.
#: Matches the design's "Authz (decision) … 60s" entry.
DEFAULT_AUTHZ_CACHE_TTL_SECONDS: Final[int] = 60


__all__ = [
    "DEFAULT_AUTHZ_CACHE_TTL_SECONDS",
    "AuthzDecisionCache",
    "CacheKey",
    "CachedDecision",
]
