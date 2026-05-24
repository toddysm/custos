"""Unit tests for :class:`custos_callctx.JwksCache`."""

from __future__ import annotations

from typing import Any

import pytest

from custos_callctx import InvalidCallContextError, JwksCache
from custos_callctx._errors import InvalidReason
from tests._helpers import (
    SigningKeyFixture,
    derive_kid,
    jwks_from_keys,
    public_key_to_jwk,
)


class _FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += seconds


_UNSET: Any = object()


class _RecordingFetcher:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.bodies: list[dict[str, Any]] = []
        self.headers: list[dict[str, str]] = []

    def enqueue(self, body: dict[str, Any], *, max_age: Any = _UNSET) -> None:
        """Enqueue a JWKS body.

        ``max_age=_UNSET`` (default) attaches no ``Cache-Control`` header so
        the cache falls back to its configured default TTL. ``max_age=None``
        also omits the header — both forms read naturally at call sites.
        ``max_age=<int>`` attaches ``Cache-Control: public, max-age=<int>``.
        """
        self.bodies.append(body)
        if max_age is _UNSET or max_age is None:
            self.headers.append({})
        else:
            self.headers.append({"cache-control": f"public, max-age={int(max_age)}"})

    async def __call__(self, url: str) -> tuple[dict[str, str], dict[str, Any]]:
        self.calls.append(url)
        if not self.bodies:
            raise AssertionError(f"unexpected JWKS fetch for {url!r}; no body enqueued")
        return self.headers.pop(0), self.bodies.pop(0)


@pytest.fixture
def signing_key() -> SigningKeyFixture:
    return SigningKeyFixture.generate()


async def test_jwks_cache_caches_until_max_age_elapses(signing_key: SigningKeyFixture) -> None:
    clock = _FakeClock()
    fetcher = _RecordingFetcher()
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=60)
    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
        clock=clock,
    )

    await cache.get_key(signing_key.kid)
    await cache.get_key(signing_key.kid)
    clock.tick(59)
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 1, "JWKS should be served from cache while max-age is fresh"

    # After max-age elapses the cache refetches.
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=60)
    clock.tick(2)
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 2


async def test_jwks_cache_kid_miss_triggers_refetch(signing_key: SigningKeyFixture) -> None:
    """The AS-IMPL-018 rotation-overlap path: cache must refetch on unknown kid."""
    other_key = SigningKeyFixture.generate()
    clock = _FakeClock()
    fetcher = _RecordingFetcher()
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=600)
    fetcher.enqueue(
        jwks_from_keys(signing_key.public_key, other_key.public_key),
        max_age=600,
    )

    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
        clock=clock,
    )

    # Warm the cache with the first JWKS body.
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 1

    # Ask for the rotated kid that wasn't in the warm body.
    clock.tick(0.1)  # below max-age, but kid miss must still refetch
    key = await cache.get_key(other_key.kid)
    assert key is not None
    assert len(fetcher.calls) == 2, "kid miss must force a JWKS refetch"


async def test_jwks_cache_kid_miss_throttles_repeat_refetches(
    signing_key: SigningKeyFixture,
) -> None:
    """Two unknown-kid lookups within the throttle window must share one refetch."""
    clock = _FakeClock()
    fetcher = _RecordingFetcher()
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=600)

    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
        clock=clock,
        min_refetch_interval_seconds=5.0,
    )

    # Warm the cache.
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 1

    # First unknown-kid lookup forces a refetch — we re-enqueue the
    # same body so the refetch still doesn't contain the requested kid.
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=600)
    with pytest.raises(InvalidCallContextError) as exc1:
        await cache.get_key("ghost-kid")
    assert exc1.value.reason is InvalidReason.UNKNOWN_KID
    assert len(fetcher.calls) == 2

    # Second unknown-kid lookup within the throttle window must NOT
    # refetch — protects against flood-of-random-kids attacks.
    with pytest.raises(InvalidCallContextError) as exc2:
        await cache.get_key("ghost-kid-2")
    assert exc2.value.reason is InvalidReason.UNKNOWN_KID
    assert len(fetcher.calls) == 2


async def test_jwks_cache_uses_default_ttl_when_no_cache_control(
    signing_key: SigningKeyFixture,
) -> None:
    clock = _FakeClock()
    fetcher = _RecordingFetcher()
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=None)
    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
        clock=clock,
        default_ttl_seconds=30,
    )

    await cache.get_key(signing_key.kid)
    clock.tick(29)
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 1

    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=None)
    clock.tick(2)
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 2


async def test_jwks_cache_unknown_kid_when_jwks_does_not_contain_key(
    signing_key: SigningKeyFixture,
) -> None:
    fetcher = _RecordingFetcher()
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=600)
    fetcher.enqueue(jwks_from_keys(signing_key.public_key), max_age=600)
    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
        clock=_FakeClock(),
    )

    with pytest.raises(InvalidCallContextError) as exc_info:
        await cache.get_key("nope")
    assert exc_info.value.reason is InvalidReason.UNKNOWN_KID
    assert exc_info.value.kid == "nope"


async def test_jwks_cache_wraps_transport_failures() -> None:
    class _BoomFetcher:
        async def __call__(self, url: str) -> tuple[dict[str, str], dict[str, Any]]:
            raise RuntimeError("connection refused")

    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=_BoomFetcher(),
    )
    with pytest.raises(InvalidCallContextError) as exc:
        await cache.get_key("anything")
    assert exc.value.reason is InvalidReason.JWKS_UNAVAILABLE


async def test_jwks_cache_rejects_non_okp_keys(signing_key: SigningKeyFixture) -> None:
    bad_body: dict[str, Any] = {
        "keys": [
            {"kty": "RSA", "kid": "rsa-1", "n": "AA", "e": "AQAB"},
            public_key_to_jwk(signing_key.public_key, kid=derive_kid(signing_key.public_key)),
        ]
    }
    fetcher = _RecordingFetcher()
    fetcher.enqueue(bad_body, max_age=600)
    # Re-enqueue the same body for the kid-miss refetch path.
    fetcher.enqueue(bad_body, max_age=600)
    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
    )

    # Good key still resolves.
    await cache.get_key(signing_key.kid)
    # RSA entry is skipped (not crashed on).
    with pytest.raises(InvalidCallContextError) as exc:
        await cache.get_key("rsa-1")
    assert exc.value.reason is InvalidReason.UNKNOWN_KID


async def test_jwks_cache_skips_individual_malformed_entries(
    signing_key: SigningKeyFixture,
) -> None:
    """Bad ``x`` / missing kid / wrong crv entries are skipped, not fatal."""
    good_jwk = public_key_to_jwk(signing_key.public_key, kid=derive_kid(signing_key.public_key))
    body: dict[str, Any] = {
        "keys": [
            {"kty": "OKP", "crv": "P-256", "kid": "wrong-crv", "x": "AAAA"},
            {"kty": "OKP", "crv": "Ed25519", "kid": "missing-x"},
            {"kty": "OKP", "crv": "Ed25519", "kid": "bad-b64", "x": "!!!"},
            {"kty": "OKP", "crv": "Ed25519", "kid": "short", "x": "AAAA"},
            {"kty": "OKP", "crv": "Ed25519", "x": good_jwk["x"]},  # no kid
            {"kty": "OKP", "crv": "Ed25519", "kid": "", "x": good_jwk["x"]},
            "not-an-object",
            good_jwk,
        ]
    }
    fetcher = _RecordingFetcher()
    fetcher.enqueue(body, max_age=600)
    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
    )
    key = await cache.get_key(signing_key.kid)
    assert key is not None


async def test_jwks_cache_rejects_body_with_no_keys_array() -> None:
    fetcher = _RecordingFetcher()
    fetcher.enqueue({"not-keys": []}, max_age=600)
    cache = JwksCache(jwks_url="https://auth/jwks", fetcher=fetcher)
    with pytest.raises(InvalidCallContextError) as exc:
        await cache.get_key("anything")
    assert exc.value.reason is InvalidReason.JWKS_UNAVAILABLE


async def test_jwks_cache_ignores_non_integer_max_age(
    signing_key: SigningKeyFixture,
) -> None:
    """A garbage ``Cache-Control`` directive falls back to the default TTL."""
    fetcher = _RecordingFetcher()
    body = jwks_from_keys(signing_key.public_key)
    fetcher.bodies.append(body)
    fetcher.headers.append({"cache-control": "public, no-store"})
    clock = _FakeClock()
    cache = JwksCache(
        jwks_url="https://auth/jwks",
        fetcher=fetcher,
        clock=clock,
        default_ttl_seconds=42,
    )
    await cache.get_key(signing_key.kid)
    clock.tick(41)
    await cache.get_key(signing_key.kid)
    assert len(fetcher.calls) == 1


def test_jwks_cache_requires_jwks_url() -> None:
    with pytest.raises(ValueError, match="jwks_url"):
        JwksCache(jwks_url="")
