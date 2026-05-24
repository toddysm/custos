"""Tests for the call-context JWKS endpoint (AS-IMPL-018, GH-#253)."""

from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.callctx_keyring import JWKS_CACHE_FRACTION, KeyRing
from custos_auth.callctx_signer import SigningKey
from custos_auth.providers import Providers
from custos_auth.settings import load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

_ENV = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
    # Disable the in-process rotation loop so tests do not race against
    # the scheduler; the JWKS body is what we are exercising.
    "CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION": "0",
}


def _providers() -> Providers:
    return Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def test_jwks_endpoint_returns_active_key() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        response = client.get("/.well-known/jwks.json")
    assert response.status_code == 200
    body = response.json()
    keys = body["keys"]
    assert len(keys) == 1
    jwk = keys[0]
    assert jwk["kty"] == "OKP"
    assert jwk["crv"] == "Ed25519"
    assert jwk["alg"] == "EdDSA"
    assert jwk["use"] == "sig"
    assert len(jwk["kid"]) == 16
    assert _b64url_decode(jwk["x"]) and len(_b64url_decode(jwk["x"])) == 32


def test_jwks_endpoint_lists_active_then_retired() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        ring: KeyRing = app.state.call_context_key_ring
        initial_kid = ring.active.kid
        # Rotate so we have a retired entry to advertise. The JWKS
        # route reads the live ring on each request, so a second GET
        # immediately reflects the new state — no app re-creation.
        ring.rotate(SigningKey.generate())
        response = client.get("/.well-known/jwks.json")
    body = response.json()
    kids = [jwk["kid"] for jwk in body["keys"]]
    # Active first; then the retired entry.
    assert kids[0] == ring.active.kid
    assert initial_kid in kids[1:]


def test_jwks_endpoint_emits_cache_control_header() -> None:
    rotation = "600"
    env = dict(_ENV, CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION=rotation)
    app = create_app(settings=load_settings(env), providers=_providers())
    with TestClient(app) as client:
        response = client.get("/.well-known/jwks.json")
    expected_max_age = max(1, int(int(rotation) * JWKS_CACHE_FRACTION))
    cache_control = response.headers["cache-control"]
    assert "public" in cache_control
    assert f"max-age={expected_max_age}" in cache_control


def test_jwks_endpoint_is_listed_as_callctx_bypass_path() -> None:
    # Routes behind the call-context middleware require an
    # X-Custos-Callctx header outside development. The JWKS route
    # must be reachable without one (verifiers fetch it *before*
    # they hold a context), so it has to live in the middleware's
    # bypass set.
    from custos_auth.middleware.callctx import _BYPASS_PATHS

    assert "/.well-known/jwks.json" in _BYPASS_PATHS
