"""Async JWKS HTTP fetcher with TTL cache (AS-IMPL-020).

The OIDC verifier needs a fresh copy of every issuer's JWK set so it
can look up signing keys by ``kid``. Two operational realities shape
this module:

1. **JWKS responses are immutable per ``kid``** — providers add new
   keys for rotations and never re-use old ``kid`` values. A cached
   entry stays correct until the operator-relevant rotation event.
2. **Cold-cache lookups happen on the hot path** — every first verify
   for a freshly-rotated key fetches the JWKS, so the cache MUST be
   per-issuer (one slow provider does not block others) and the
   fetch MUST be async so the verifier does not block the event loop.

Strategy:

* One in-memory entry per ``jwks_uri``. Entries carry a ``fetched_at``
  timestamp and the provider-supplied TTL (parsed from
  ``Cache-Control: max-age=...`` or defaulted to
  :data:`DEFAULT_TTL_SECONDS`).
* :meth:`JwksCache.get_key(jwks_uri, kid)` consults the entry; on
  ``kid`` miss the cache forces a refresh (the issuer probably just
  rotated). On ``Cache-Control``-expiry the entry is also re-fetched
  proactively at the next lookup.
* The cache uses :class:`asyncio.Lock` to deduplicate concurrent
  refreshes — a thundering herd of verifies after a rotation issues
  one HTTP request, not N.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    import httpx

_LOG = logging.getLogger(__name__)

#: Fallback TTL when the JWKS response carries no ``Cache-Control``
#: header (or carries an unparseable one). 10 minutes is short
#: enough that operators see rotations in a reasonable window and
#: long enough that an idle deployment is not constantly fetching.
DEFAULT_TTL_SECONDS: Final[int] = 600

#: Cap on JWKS response size. JWKS documents are tiny (Google's is
#: ~2 KB) — anything pushing past 64 KB is either a misconfigured
#: provider or an attacker trying to DoS the cache.
MAX_RESPONSE_BYTES: Final[int] = 64 * 1024

_MAX_AGE_RE = re.compile(r"max-age\s*=\s*(\d+)", re.IGNORECASE)


class JwksCacheError(RuntimeError):
    """Raised when a JWKS fetch fails (network, HTTP error, malformed body)."""


@dataclass(slots=True)
class _Entry:
    """One cached JWK set."""

    keys_by_kid: dict[str, dict[str, Any]] = field(default_factory=dict)
    fetched_at: float = 0.0
    ttl_seconds: float = float(DEFAULT_TTL_SECONDS)

    def is_fresh(self, now: float) -> bool:
        return now - self.fetched_at < self.ttl_seconds


def _parse_max_age(cache_control: str | None) -> int | None:
    if not cache_control:
        return None
    match = _MAX_AGE_RE.search(cache_control)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:  # pragma: no cover — regex restricts to digits
        return None


class JwksCache:
    """Per-issuer JWKS HTTP fetcher with a TTL cache.

    The cache is constructed once per app and shared across every
    OIDC verifier. The underlying :class:`httpx.AsyncClient` is
    owned externally so the lifespan wiring controls the HTTP
    transport (timeouts, mTLS material, etc.) and tests can inject a
    transport that points at an in-process JWKS fixture.
    """

    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http_client = http_client
        self._entries: dict[str, _Entry] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, jwks_uri: str) -> asyncio.Lock:
        """Return the per-URI refresh lock, creating one on first use."""
        lock = self._locks.get(jwks_uri)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[jwks_uri] = lock
        return lock

    async def get_key(
        self,
        jwks_uri: str,
        kid: str,
        *,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Return the JWK with the matching ``kid``.

        * Cache hit (entry fresh and ``kid`` present): served from
          memory.
        * Cache miss (entry stale OR ``kid`` absent): refresh the
          JWKS HTTP-side and retry the lookup once. A ``kid`` still
          absent after refresh raises :class:`JwksCacheError`.

        The double-check inside the lock guards against a thundering
        herd: concurrent verifies for the same ``(jwks_uri, kid)``
        re-check the cache after acquiring the lock and skip the
        HTTP call if a peer already populated the entry.
        """
        current_time = time.monotonic() if now is None else now
        entry = self._entries.get(jwks_uri)
        if entry is not None and entry.is_fresh(current_time):
            jwk = entry.keys_by_kid.get(kid)
            if jwk is not None:
                return jwk

        lock = self._lock_for(jwks_uri)
        async with lock:
            now2 = time.monotonic() if now is None else now
            entry = self._entries.get(jwks_uri)
            if entry is not None and entry.is_fresh(now2):
                jwk = entry.keys_by_kid.get(kid)
                if jwk is not None:
                    return jwk
            entry = await self._refresh(jwks_uri)

        jwk = entry.keys_by_kid.get(kid)
        if jwk is None:
            raise JwksCacheError(f"JWKS at {jwks_uri!r} has no key with kid={kid!r} after refresh")
        return jwk

    async def _refresh(self, jwks_uri: str) -> _Entry:
        """Fetch ``jwks_uri`` and replace the in-memory entry."""
        try:
            response = await self._http_client.get(jwks_uri)
        except Exception as exc:
            raise JwksCacheError(f"JWKS fetch failed for {jwks_uri!r}: {exc}") from exc

        if response.status_code != 200:
            raise JwksCacheError(
                f"JWKS fetch returned HTTP {response.status_code} for {jwks_uri!r}"
            )

        # Defensive size cap before parsing — JWKS responses are tiny;
        # a multi-MB payload is either a misconfigured provider or an
        # attack surface we do not want to feed into the JSON parser.
        body = response.content
        if len(body) > MAX_RESPONSE_BYTES:
            raise JwksCacheError(
                f"JWKS response too large for {jwks_uri!r}: "
                f"{len(body)} bytes > {MAX_RESPONSE_BYTES} cap"
            )

        try:
            payload = response.json()
        except Exception as exc:
            raise JwksCacheError(f"JWKS body is not valid JSON for {jwks_uri!r}: {exc}") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("keys"), list):
            raise JwksCacheError(f"JWKS body for {jwks_uri!r} is missing the 'keys' array")

        keys_by_kid: dict[str, dict[str, Any]] = {}
        for key in payload["keys"]:
            if not isinstance(key, dict):
                continue
            kid = key.get("kid")
            if isinstance(kid, str) and kid:
                keys_by_kid[kid] = key

        ttl = _parse_max_age(response.headers.get("cache-control")) or DEFAULT_TTL_SECONDS
        entry = _Entry(
            keys_by_kid=keys_by_kid,
            fetched_at=time.monotonic(),
            ttl_seconds=float(ttl),
        )
        self._entries[jwks_uri] = entry
        _LOG.debug(
            "refreshed JWKS cache for %s: %d keys, ttl=%ds",
            jwks_uri,
            len(keys_by_kid),
            ttl,
        )
        return entry

    def snapshot(self) -> dict[str, tuple[int, float]]:
        """Diagnostic view: ``{jwks_uri: (key_count, age_seconds)}``."""
        now = time.monotonic()
        return {
            uri: (len(entry.keys_by_kid), now - entry.fetched_at)
            for uri, entry in self._entries.items()
        }


__all__ = [
    "DEFAULT_TTL_SECONDS",
    "MAX_RESPONSE_BYTES",
    "JwksCache",
    "JwksCacheError",
]
