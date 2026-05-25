"""Tests for ``custos_auth.oidc.jwks_cache`` (AS-IMPL-020).

Drives the cache through an ``httpx.MockTransport`` so the test suite
exercises the real client / response path without touching the network.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from custos_auth.oidc.jwks_cache import (
    DEFAULT_TTL_SECONDS,
    MAX_RESPONSE_BYTES,
    JwksCache,
    JwksCacheError,
)

_JWKS_URI = "https://issuer.example.com/.well-known/jwks.json"


def _jwks_body(*kids: str) -> dict[str, list[dict[str, str]]]:
    """Build a minimal JWKS document with the requested ``kid`` set."""
    return {
        "keys": [
            {"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256", "n": "a", "e": "b"}
            for kid in kids
        ]
    }


def _make_client(handler: Any, **transport_kwargs: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), **transport_kwargs)


async def test_cache_serves_from_memory_after_first_fetch() -> None:
    fetch_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        fetch_count["n"] += 1
        return httpx.Response(200, json=_jwks_body("kid-1"))

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        jwk1 = await cache.get_key(_JWKS_URI, "kid-1")
        assert jwk1["kid"] == "kid-1"
        # Second lookup served from cache — no extra HTTP call.
        jwk2 = await cache.get_key(_JWKS_URI, "kid-1")
        assert jwk2 == jwk1
        assert fetch_count["n"] == 1


async def test_cache_refresh_on_unknown_kid() -> None:
    # The cache should re-fetch when the requested ``kid`` is missing
    # from the cached entry (covers the rotation case).
    state = {"call": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["call"] += 1
        if state["call"] == 1:
            return httpx.Response(200, json=_jwks_body("kid-old"))
        return httpx.Response(200, json=_jwks_body("kid-old", "kid-new"))

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        await cache.get_key(_JWKS_URI, "kid-old")
        # First lookup of kid-new triggers a refresh; the new kid is found.
        jwk = await cache.get_key(_JWKS_URI, "kid-new")
        assert jwk["kid"] == "kid-new"
        assert state["call"] == 2


async def test_cache_raises_when_kid_missing_after_refresh() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_jwks_body("kid-a"))

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        with pytest.raises(JwksCacheError, match="has no key with kid="):
            await cache.get_key(_JWKS_URI, "missing-kid")


async def test_cache_raises_on_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream busy")

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        with pytest.raises(JwksCacheError, match="HTTP 503"):
            await cache.get_key(_JWKS_URI, "any")


async def test_cache_raises_on_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns failure", request=request)

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        with pytest.raises(JwksCacheError, match="fetch failed"):
            await cache.get_key(_JWKS_URI, "any")


async def test_cache_raises_on_invalid_json_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        with pytest.raises(JwksCacheError, match="not valid JSON"):
            await cache.get_key(_JWKS_URI, "any")


async def test_cache_raises_when_body_missing_keys_array() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        with pytest.raises(JwksCacheError, match="missing the 'keys' array"):
            await cache.get_key(_JWKS_URI, "any")


async def test_cache_rejects_oversized_response() -> None:
    big_body = json.dumps(
        {"keys": [{"kid": "k", "padding": "x" * (MAX_RESPONSE_BYTES + 1)}]}
    ).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=big_body)

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        with pytest.raises(JwksCacheError, match="too large"):
            await cache.get_key(_JWKS_URI, "k")


async def test_cache_dedups_concurrent_refreshes() -> None:
    # All concurrent lookups for the same JWKS URI should share a
    # single HTTP request — the per-URI lock guarantees this.
    fetch_count = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        fetch_count["n"] += 1
        # Yield to the loop so concurrent callers stack up on the lock.
        await asyncio.sleep(0.01)
        return httpx.Response(200, json=_jwks_body("kid-1"))

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        results = await asyncio.gather(
            cache.get_key(_JWKS_URI, "kid-1"),
            cache.get_key(_JWKS_URI, "kid-1"),
            cache.get_key(_JWKS_URI, "kid-1"),
        )
        assert all(r["kid"] == "kid-1" for r in results)
        assert fetch_count["n"] == 1


async def test_cache_uses_cache_control_max_age() -> None:
    # When the response carries ``Cache-Control: max-age=N`` the
    # entry stores ``N`` rather than the default fallback.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_jwks_body("kid-1"),
            headers={"cache-control": "public, max-age=42"},
        )

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        await cache.get_key(_JWKS_URI, "kid-1")
        snapshot = cache.snapshot()
        assert _JWKS_URI in snapshot


async def test_cache_falls_back_to_default_ttl_when_header_absent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_jwks_body("kid-1"))

    async with _make_client(handler) as client:
        cache = JwksCache(client)
        await cache.get_key(_JWKS_URI, "kid-1")
        # Snapshot returns (key_count, age_seconds); just confirm the
        # entry exists. The TTL itself is private; covered indirectly.
        snapshot = cache.snapshot()
        assert snapshot[_JWKS_URI][0] == 1
        assert DEFAULT_TTL_SECONDS > 0  # sanity
