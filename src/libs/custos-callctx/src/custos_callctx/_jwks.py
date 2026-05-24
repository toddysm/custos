"""JWKS cache with ``Cache-Control``-respecting refresh + kid-miss refetch.

The auth-service publishes the call-context signing keys at
``/.well-known/jwks.json`` with a ``Cache-Control: public, max-age=<half
rotation period>`` header (AS-IMPL-018). Receivers cache the JWKS body in
process for that duration; on a verification request whose JWT header
carries a ``kid`` that is not in the cache (key was rotated mid-request),
the cache forces a one-time refetch before declaring the key unknown —
that is what closes the rotation-overlap race.

The cache is hermetic for tests: callers inject an
``HttpJsonFetcher``-typed coroutine and a ``clock`` so the verifier
suite can drive cache hits, expiries, and kid-miss refetches without
standing up real HTTP transport.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any, Final

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from custos_callctx._errors import InvalidCallContextError, InvalidReason

logger = logging.getLogger(__name__)

#: Default cache TTL applied when the JWKS response omits a
#: ``Cache-Control`` ``max-age`` directive. 5 minutes mirrors a reasonable
#: midpoint between "freshness" and "load on the auth-service".
DEFAULT_CACHE_TTL_SECONDS: Final[int] = 300

#: Lower bound on the refetch interval applied to kid-miss refreshes.
#: Prevents a hot loop of refetches when an attacker submits a flood of
#: tokens with random kids.
MIN_REFETCH_INTERVAL_SECONDS: Final[float] = 1.0

#: Async HTTP fetcher signature accepted by :class:`JwksCache`. Receives the
#: fully-qualified JWKS URL and returns ``(headers, json_body)``. Production
#: wiring injects an ``httpx.AsyncClient`` shim; tests pass a pure-Python
#: coroutine.
HttpJsonFetcher = Callable[[str], Awaitable[tuple[dict[str, str], dict[str, Any]]]]

_MAX_AGE_PATTERN = re.compile(r"(?i)\bmax-age\s*=\s*(\d+)")


def _parse_max_age(cache_control: str | None) -> int | None:
    """Extract the integer ``max-age`` directive from a ``Cache-Control`` header."""
    if not cache_control:
        return None
    match = _MAX_AGE_PATTERN.search(cache_control)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:  # pragma: no cover — regex restricts to digits
        return None


def _decode_jwk_to_ed25519_public_key(jwk: dict[str, Any]) -> Ed25519PublicKey:
    """Reconstruct an Ed25519 public key from an RFC 8037 OKP JWK.

    The auth-service publishes ``kty=OKP`` / ``crv=Ed25519`` / ``x=<base64url
    no-pad raw pubkey>``. We tolerate the optional padding that some other
    JWKS publishers add.
    """
    if jwk.get("kty") != "OKP":
        raise InvalidCallContextError(
            InvalidReason.JWKS_UNAVAILABLE,
            f"JWK kty must be OKP for Ed25519; got {jwk.get('kty')!r}",
        )
    if jwk.get("crv") != "Ed25519":
        raise InvalidCallContextError(
            InvalidReason.JWKS_UNAVAILABLE,
            f"JWK crv must be Ed25519; got {jwk.get('crv')!r}",
        )
    x = jwk.get("x")
    if not isinstance(x, str) or not x:
        raise InvalidCallContextError(
            InvalidReason.JWKS_UNAVAILABLE,
            "JWK is missing the 'x' field",
        )
    padding = "=" * (-len(x) % 4)
    try:
        raw = base64.urlsafe_b64decode(x + padding)
    except (ValueError, TypeError) as exc:
        raise InvalidCallContextError(
            InvalidReason.JWKS_UNAVAILABLE,
            f"JWK 'x' field is not valid base64url: {exc}",
        ) from exc
    if len(raw) != 32:
        raise InvalidCallContextError(
            InvalidReason.JWKS_UNAVAILABLE,
            f"Ed25519 public key must be 32 raw bytes; got {len(raw)}",
        )
    return Ed25519PublicKey.from_public_bytes(raw)


async def default_http_fetcher(
    url: str,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Production JWKS fetcher backed by :mod:`httpx`.

    Returns ``(headers, body)``. Headers are lower-cased so the cache
    can pick up ``Cache-Control`` regardless of upstream casing.
    """
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        headers = {k.lower(): v for k, v in response.headers.items()}
        body = response.json()
        if not isinstance(body, dict):  # pragma: no cover — defensive
            raise InvalidCallContextError(
                InvalidReason.JWKS_UNAVAILABLE,
                f"JWKS body at {url} is not a JSON object",
            )
        return headers, body


class JwksCache:
    """In-memory JWKS cache shared by verifier coroutines.

    The cache stores a mapping of ``kid`` -> :class:`Ed25519PublicKey` plus
    the next refresh timestamp derived from the response's ``max-age``. A
    verification with an unknown ``kid`` triggers an out-of-band refetch
    before the cache reports the key as missing — that is the AS-IMPL-018
    rotation-overlap recovery path.

    The class is asyncio-safe (single ``asyncio.Lock`` guards the refresh
    path) but is not designed for cross-process sharing; each verifier
    instance owns its cache.

    Args:
        jwks_url: Fully-qualified URL to the auth-service JWKS endpoint.
        fetcher: Coroutine returning ``(headers, body)``. Default is
            :func:`default_http_fetcher`; tests inject a hermetic fake.
        clock: Wall-clock callable returning Unix seconds. Default is
            :func:`time.monotonic` so the cache TTL is unaffected by
            wall-clock skew.
        default_ttl_seconds: Cache TTL applied when the JWKS response
            omits a ``max-age`` directive.
        min_refetch_interval_seconds: Lower bound on the kid-miss
            refetch interval, defending against flood-of-random-kids
            attacks.
    """

    def __init__(
        self,
        *,
        jwks_url: str,
        fetcher: HttpJsonFetcher | None = None,
        clock: Callable[[], float] = time.monotonic,
        default_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        min_refetch_interval_seconds: float = MIN_REFETCH_INTERVAL_SECONDS,
    ) -> None:
        if not jwks_url:
            raise ValueError("jwks_url must be a non-empty URL")
        self._jwks_url = jwks_url
        self._fetcher = fetcher if fetcher is not None else default_http_fetcher
        self._clock = clock
        self._default_ttl_seconds = default_ttl_seconds
        self._min_refetch_interval_seconds = min_refetch_interval_seconds
        self._keys: dict[str, Ed25519PublicKey] = {}
        self._expires_at: float = 0.0
        self._last_kid_miss_refetch_at: float = float("-inf")
        self._lock = asyncio.Lock()

    @property
    def jwks_url(self) -> str:
        return self._jwks_url

    async def get_key(self, kid: str) -> Ed25519PublicKey:
        """Return the public key for ``kid``, refreshing the cache if needed.

        Raises:
            InvalidCallContextError: with reason
                :attr:`InvalidReason.UNKNOWN_KID` when the JWKS does not
                advertise the key id, or
                :attr:`InvalidReason.JWKS_UNAVAILABLE` when the JWKS
                fetch fails.
        """
        now = self._clock()
        if now >= self._expires_at:
            await self._refresh(force=False)
        key = self._keys.get(kid)
        if key is not None:
            return key
        # kid-miss: force a one-time refetch unless we've already
        # refetched recently for an unknown kid (flood defense).
        since_last_miss_refetch = self._clock() - self._last_kid_miss_refetch_at
        if since_last_miss_refetch >= self._min_refetch_interval_seconds:
            await self._refresh(force=True)
            self._last_kid_miss_refetch_at = self._clock()
            key = self._keys.get(kid)
        if key is None:
            raise InvalidCallContextError(
                InvalidReason.UNKNOWN_KID,
                f"kid {kid!r} not found in JWKS at {self._jwks_url}",
                kid=kid,
            )
        return key

    async def _refresh(self, *, force: bool) -> None:
        async with self._lock:
            now = self._clock()
            # Re-check under the lock to coalesce concurrent refreshes.
            if not force and now < self._expires_at:
                return
            try:
                headers, body = await self._fetcher(self._jwks_url)
            except asyncio.CancelledError:
                # Preserve task-cancellation semantics. asyncio.CancelledError
                # derives from BaseException on Python 3.8+, so the broader
                # ``except Exception`` below already misses it — but an
                # injected ``HttpJsonFetcher`` could (incorrectly) raise a
                # custom Exception-derived cancellation marker, and being
                # explicit makes the intent obvious to readers and to future
                # maintainers swapping the fetcher.
                raise
            except InvalidCallContextError:
                raise
            except Exception as exc:
                raise InvalidCallContextError(
                    InvalidReason.JWKS_UNAVAILABLE,
                    f"failed to fetch JWKS at {self._jwks_url}: {exc}",
                ) from exc
            keys_field = body.get("keys")
            if not isinstance(keys_field, list):
                raise InvalidCallContextError(
                    InvalidReason.JWKS_UNAVAILABLE,
                    f"JWKS body at {self._jwks_url} has no 'keys' array",
                )
            new_keys: dict[str, Ed25519PublicKey] = {}
            for entry in keys_field:
                if not isinstance(entry, dict):
                    continue
                kid = entry.get("kid")
                if not isinstance(kid, str) or not kid:
                    continue
                try:
                    new_keys[kid] = _decode_jwk_to_ed25519_public_key(entry)
                except InvalidCallContextError:
                    # Skip individual bad entries — the rest of the
                    # JWKS is still usable. Forensic detail goes into
                    # the log.
                    logger.warning("skipping malformed JWK at %s (kid=%s)", self._jwks_url, kid)
            self._keys = new_keys
            max_age = _parse_max_age(headers.get("cache-control"))
            ttl = max_age if max_age is not None else self._default_ttl_seconds
            self._expires_at = now + max(0, ttl)
            logger.debug(
                "refreshed JWKS at %s; %d keys cached for %ds",
                self._jwks_url,
                len(new_keys),
                ttl,
            )


__all__ = [
    "DEFAULT_CACHE_TTL_SECONDS",
    "MIN_REFETCH_INTERVAL_SECONDS",
    "HttpJsonFetcher",
    "JwksCache",
    "default_http_fetcher",
]
