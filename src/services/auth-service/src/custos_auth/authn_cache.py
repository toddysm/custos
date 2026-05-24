"""Per-pod TTL cache for ``verify_token()`` results (AS-IMPL-014).

The cache sits in front of the SPL ``get_service_token_by_hash`` call
in :func:`custos_auth.authn.verify_token`. Every verify still emits
its own audit row (``authn.success`` / ``authn.failure`` at the
gateway entry path, ``token.used`` on a cache miss) — the cache
short-cuts the *resolution* step only.

Design reference: ``design/components/auth-service/design.md`` §
"Cache Invalidation Bus":

    | Cache | Key | TTL | Eviction event |
    |-------|-----|-----|----------------|
    | Authn | ``tokenHash`` | 30s | ``custos.auth.token-revoked`` |

Key choice
----------

Entries are keyed by the storage hash (the SHA-256 hex digest of the
plaintext bearer). This is the same string the SPL persists, so the
revoke path can produce a single eviction event carrying just the
hash and every replica's cache evicts the right row without leaking
the plaintext or the SA principal_id onto the bus.

The cache also indexes a parallel ``token_id → token_hash`` map so a
``custos.auth.token-revoked`` event carrying ``token_id`` (the
operator-facing identifier returned by the mint API) can be resolved
to a hash and evicted in O(1) without scanning every row. Both
directions stay in sync via the cache mutation methods only — the
test suite asserts the invariant.

Configuration
-------------

The TTL is read from :data:`custos_auth.settings.ENV_AUTHN_CACHE_TTL`
(``CUSTOS_AUTH_AUTHN_CACHE_TTL``) with a 30 s default. Setting the
env var to ``0`` puts the cache in **bypass mode** — every verify
call performs a full SPL lookup.

Threading
---------

The cache is intentionally **in-process** and lives one-per-replica.
Cross-replica consistency rides on the ``custos.auth.token-revoked``
pub/sub bus: each replica subscribes and invalidates its own copy.
The in-process invalidation path
(:class:`~custos_auth.token_revoked_events.LocalTokenRevokedBus`)
runs on the same replica that performed the revoke so a single-
replica deployment satisfies the AS-IMPL-014 "revoke + immediate re-
verify returns 401 with ≤100ms tail latency" acceptance criterion
without standing up a real transport.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from custos_spl.interfaces.auth_store import Principal

_LOGGER = logging.getLogger("custos_auth.authn_cache")


@dataclass(frozen=True, slots=True)
class CachedAuthn:
    """One row in the authn cache.

    Carries the verified :data:`Principal` (a snapshot taken at
    verify time) plus the ``token_id`` so the cache can be evicted
    by either dimension and so consumers can attribute audit events
    without a second SPL fetch.

    ``expires_at`` is in the time domain of the
    :class:`AuthnCache`'s ``time_source`` callable, which is
    :func:`time.monotonic` by default.
    """

    principal: Principal
    token_id: str
    expires_at: float


@dataclass(slots=True)
class AuthnCache:
    """TTL-bounded per-pod cache of token-verify results.

    Keyed by the storage hash. Carries a parallel ``token_id →
    hash`` index so revoke events arriving with the ``token_id``
    only can find their row in O(1).

    Attributes:
        ttl_seconds: Cache lifetime per entry, in monotonic seconds.
            ``0`` (and any negative value) disables the cache; both
            :meth:`get` and :meth:`put` short-circuit. The disabled-
            mode flag is exposed via :attr:`enabled`.
        time_source: Monotonic clock callable. Injectable for tests.
        hits: Cumulative cache-hit counter. Mutated by :meth:`get`.
        misses: Cumulative cache-miss counter. Mutated by :meth:`get`.
    """

    ttl_seconds: float
    time_source: Callable[[], float] = time.monotonic
    hits: int = 0
    misses: int = 0
    _entries: dict[str, CachedAuthn] = field(default_factory=dict)
    _by_token_id: dict[str, str] = field(default_factory=dict)

    @property
    def enabled(self) -> bool:
        """``True`` when the cache is configured to store entries."""
        return self.ttl_seconds > 0

    def get(self, token_hash: str) -> CachedAuthn | None:
        """Return a cached verify row, or ``None`` on miss / expiry.

        When the cache is disabled (``ttl_seconds <= 0``) the call
        short-circuits without touching the counters — the bypass
        path is the documented behaviour of the
        ``CUSTOS_AUTH_AUTHN_CACHE_TTL=0`` configuration and we do
        not want it polluting cache-pressure dashboards.
        """
        if not self.enabled:
            return None
        entry = self._entries.get(token_hash)
        if entry is None:
            self.misses += 1
            return None
        if entry.expires_at <= self.time_source():
            # Lazy expiry on the read path — keeps the in-memory
            # tables bounded for a workload that cycles tokens
            # without revoke events.
            self._drop_locked(token_hash, entry.token_id)
            self.misses += 1
            return None
        self.hits += 1
        return entry

    def put(
        self,
        token_hash: str,
        *,
        principal: Principal,
        token_id: str,
    ) -> None:
        """Insert (or refresh) a verify row.

        No-op when the cache is disabled — the AS-IMPL-014 acceptance
        criterion requires ``CUSTOS_AUTH_AUTHN_CACHE_TTL=0`` to
        bypass the cache completely; a put on a disabled cache must
        not allocate.
        """
        if not self.enabled:
            return
        expires_at = self.time_source() + self.ttl_seconds
        self._entries[token_hash] = CachedAuthn(
            principal=principal,
            token_id=token_id,
            expires_at=expires_at,
        )
        self._by_token_id[token_id] = token_hash

    def invalidate_by_hash(self, token_hash: str) -> bool:
        """Evict the entry for ``token_hash``. Returns ``True`` on hit."""
        entry = self._entries.get(token_hash)
        if entry is None:
            return False
        self._drop_locked(token_hash, entry.token_id)
        return True

    def invalidate_by_token_id(self, token_id: str) -> bool:
        """Evict the entry indexed by ``token_id``. Returns ``True`` on hit.

        Used by the ``custos.auth.token-revoked`` subscriber when
        the eviction event carries the operator-facing ``token_id``
        rather than the storage hash.
        """
        token_hash = self._by_token_id.get(token_id)
        if token_hash is None:
            return False
        self._drop_locked(token_hash, token_id)
        return True

    def flush(self) -> int:
        """Drop every entry. Returns the number of rows dropped."""
        count = len(self._entries)
        self._entries.clear()
        self._by_token_id.clear()
        return count

    def _drop_locked(self, token_hash: str, token_id: str) -> None:
        # Single point of mutation so the forward and reverse indices
        # stay in sync. Names ending in ``_locked`` are an idiom
        # borrowed from concurrent code to flag "the caller has
        # already established the precondition"; here, "the caller
        # already located the row."
        self._entries.pop(token_hash, None)
        self._by_token_id.pop(token_id, None)


__all__ = [
    "AuthnCache",
    "CachedAuthn",
]
