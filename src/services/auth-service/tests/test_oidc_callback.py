"""Tests for ``POST /v1/auth/login/oidc/callback`` (AS-IMPL-020..023).

The route is wired end-to-end on app creation when
``CUSTOS_AUTH_OIDC_ENABLED=true`` and at least one issuer is configured
via ``CUSTOS_AUTH_OIDC_ISSUERS``. Tests exercise the four major paths:

* Flag off → 503 ``oidc_not_enabled``.
* Flag on but no issuers → 503 ``oidc_not_configured``.
* Issuer present but token verification fails → 401
  ``oidc_verification_failed`` + ``authn.failure`` audit row.
* Happy path → 200 with ``PrincipalResponse`` + ``authn.success`` audit row.

The verifier and the OAuth token-endpoint exchange use an injected
``httpx.MockTransport`` so the test stays hermetic.
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

import httpx
import jwt
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from custos_auth import create_app
from custos_auth.providers import Providers
from custos_auth.settings import Settings, load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

_ENV_BASE = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
}

_ISSUER_URL = "https://issuer.example.com"
_AUDIENCE = "api://custos"
_JWKS_URI = "https://issuer.example.com/.well-known/jwks.json"
_TOKEN_ENDPOINT = "https://issuer.example.com/oauth2/token"


def _providers() -> Providers:
    return Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )


def _app_with_settings(
    settings: Settings,
    *,
    providers: Providers | None = None,
) -> TestClient:
    return TestClient(create_app(settings=settings, providers=providers or _providers()))


def _valid_body() -> dict[str, str]:
    return {
        "issuer": "primary",
        "code": "auth-code-xyz",
        "state": "opaque-state",
    }


def _issuers_env(token_endpoint: str | None = _TOKEN_ENDPOINT) -> str:
    entry: dict[str, Any] = {
        "id": "primary",
        "issuer_url": _ISSUER_URL,
        "jwks_uri": _JWKS_URI,
        "audiences": [_AUDIENCE],
    }
    if token_endpoint is not None:
        entry["token_endpoint"] = token_endpoint
        entry["client_id"] = "test-client"
        entry["client_secret_env"] = "TEST_OIDC_CLIENT_SECRET"
    return json.dumps({"issuers": [entry]})


# ---------------------------------------------------------------------------
# Crypto helpers (mint a real signed ID token for the happy-path test)
# ---------------------------------------------------------------------------


def _generate_rsa_keypair() -> tuple[Any, Any]:
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    return private_key, private_key.public_key()


def _jwk_from_public_key(public_key: Any, kid: str) -> dict[str, Any]:
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    jwk_str = RSAAlgorithm.to_jwk(RSAAlgorithm(RSAAlgorithm.SHA256).prepare_key(pem))
    jwk: dict[str, Any] = json.loads(jwk_str)
    jwk["kid"] = kid
    jwk["alg"] = "RS256"
    jwk["use"] = "sig"
    return jwk


def _mint_id_token(
    private_key: Any,
    *,
    kid: str = "kid-1",
    sub: str = "user-42",
    extra: dict[str, Any] | None = None,
) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": _ISSUER_URL,
        "aud": _AUDIENCE,
        "sub": sub,
        "iat": now,
        "exp": now + 300,
        **(extra or {}),
    }
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": kid})


def _make_mock_transport(
    *,
    jwk: dict[str, Any],
    id_token: str,
    exchange_status: int = 200,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == _TOKEN_ENDPOINT:
            if exchange_status != 200:
                return httpx.Response(exchange_status, text="provider error")
            return httpx.Response(200, json={"id_token": id_token})
        if str(request.url) == _JWKS_URI:
            return httpx.Response(200, json={"keys": [jwk]})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def _patch_oidc_client(client: TestClient, transport: httpx.MockTransport) -> None:
    """Point the lifespan-built httpx client at a mock transport.

    The lifespan in ``create_app()`` builds the real
    :class:`httpx.AsyncClient` and registers ``await client.aclose()``
    against the *local reference captured by closure*. If we
    replaced ``app.state.oidc_http_client`` with a brand-new client
    here, the lifespan teardown would still close the original
    (now-detached) instance and our replacement would leak — leaving
    an unclosed asyncio resource and producing flaky
    ``ResourceWarning`` noise across the suite.

    Instead we mutate the existing client's transport in place. The
    :class:`JwksCache` and :class:`OidcVerifier` already hold the
    client by reference (they were built around it at lifespan time)
    so no downstream rebuild is needed — they automatically pick up
    the new transport on the next request.
    """
    from fastapi import FastAPI

    app = cast(FastAPI, client.app)
    http_client: httpx.AsyncClient = app.state.oidc_http_client
    # ``_transport`` is the documented httpx test-injection point and
    # has been stable across the 0.27.x line (our pinned range).
    http_client._transport = transport


# ---------------------------------------------------------------------------
# Disabled / misconfigured states
# ---------------------------------------------------------------------------


def test_oidc_callback_returns_503_oidc_not_enabled_by_default() -> None:
    settings = load_settings(_ENV_BASE)
    assert settings.oidc_enabled is False
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"]["code"] == "oidc_not_enabled"
    assert "CUSTOS_AUTH_OIDC_ENABLED" in body["error"]["detail"]


def test_oidc_callback_returns_503_oidc_not_configured_when_flag_on_no_issuers() -> None:
    settings = load_settings({**_ENV_BASE, "CUSTOS_AUTH_OIDC_ENABLED": "true"})
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"]["code"] == "oidc_not_configured"


def test_oidc_callback_returns_401_for_unknown_issuer_id() -> None:
    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    with _app_with_settings(settings) as client:
        resp = client.post(
            "/v1/auth/login/oidc/callback",
            json={**_valid_body(), "issuer": "no-such-issuer"},
        )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "oidc_verification_failed"


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def test_oidc_callback_does_not_require_call_context_header() -> None:
    settings = load_settings(_ENV_BASE)
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    # 503 (oidc_not_enabled) — what matters: not 401 from callctx middleware.
    assert resp.status_code == 503


def test_oidc_callback_rejects_unknown_fields() -> None:
    settings = load_settings(_ENV_BASE)
    bad_body = {**_valid_body(), "evil_field": "x"}
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=bad_body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "request_validation_failed"


def test_oidc_callback_accepts_optional_redirect_uri() -> None:
    settings = load_settings(_ENV_BASE)
    body = {**_valid_body(), "redirect_uri": "https://app.example.com/callback"}
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=body)
    # 503 (flag off) — the body shape was accepted.
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "oidc_not_enabled"


def test_oidc_callback_rejects_empty_required_fields() -> None:
    settings = load_settings(_ENV_BASE)
    bad_body = {"issuer": "", "code": "", "state": ""}
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=bad_body)
    assert resp.status_code == 422


def test_oidc_callback_is_listed_in_middleware_bypass_paths() -> None:
    from custos_auth.middleware.callctx import _BYPASS_PATHS

    assert "/v1/auth/login/oidc/callback" in _BYPASS_PATHS


# ---------------------------------------------------------------------------
# End-to-end: exchange + verify + provision + audit
# ---------------------------------------------------------------------------


def test_oidc_callback_happy_path_provisions_and_returns_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    id_token = _mint_id_token(priv, sub="user-42", extra={"name": "Alice"})

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    providers = _providers()
    with _app_with_settings(settings, providers=providers) as client:
        _patch_oidc_client(
            client,
            _make_mock_transport(jwk=jwk, id_token=id_token),
        )
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["newly_provisioned"] is True
    assert body["principal"]["kind"] == "user"
    assert body["principal"]["display_name"] == "Alice"

    # authn.success audit row landed with authentication_type=oidc.
    events = [event for _, event in providers.metadata_store.append_audit_calls]  # type: ignore[attr-defined]
    success_rows = [e for e in events if e.event_type == "authn.success"]
    assert len(success_rows) == 1
    assert success_rows[0].payload["authentication_type"] == "oidc"
    assert success_rows[0].payload["issuer"] == _ISSUER_URL


def test_oidc_callback_audit_failure_on_verification_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    # Mint a token signed by a *different* private key → bad_signature.
    other_priv, _ = _generate_rsa_keypair()
    bogus_token = _mint_id_token(other_priv, sub="user-42")
    del priv  # legitimate keypair generated for completeness, not used

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    providers = _providers()
    with _app_with_settings(settings, providers=providers) as client:
        _patch_oidc_client(
            client,
            _make_mock_transport(jwk=jwk, id_token=bogus_token),
        )
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "oidc_verification_failed"

    events = [event for _, event in providers.metadata_store.append_audit_calls]  # type: ignore[attr-defined]
    failure_rows = [e for e in events if e.event_type == "authn.failure"]
    assert len(failure_rows) == 1
    assert failure_rows[0].payload["reason"] == "bad_signature"


def test_oidc_callback_502_on_exchange_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    id_token = _mint_id_token(priv, sub="user-42")

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    providers = _providers()
    with _app_with_settings(settings, providers=providers) as client:
        _patch_oidc_client(
            client,
            _make_mock_transport(jwk=jwk, id_token=id_token, exchange_status=503),
        )
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "oidc_exchange_failed"

    # Audit row distinguishes token-endpoint outages from JWKS outages —
    # the audit reason MUST be ``exchange_failed`` (not the legacy
    # ``jwks_fetch_failed`` that pollutes the JWKS SLO dashboards).
    events = [event for _, event in providers.metadata_store.append_audit_calls]  # type: ignore[attr-defined]
    failure_rows = [e for e in events if e.event_type == "authn.failure"]
    assert len(failure_rows) == 1
    assert failure_rows[0].payload["reason"] == "exchange_failed"


def test_oidc_callback_503_when_token_endpoint_missing() -> None:
    # Workload-token-only issuers (no token_endpoint) are rejected at the
    # callback — those go through /v1/auth/verify instead.
    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(token_endpoint=None),
        }
    )
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "oidc_not_configured"


def test_oidc_callback_503_when_client_secret_env_unset() -> None:
    # CUSTOS_AUTH_OIDC_ISSUERS references TEST_OIDC_CLIENT_SECRET but the
    # env var is empty/unset → the route returns oidc_not_configured.
    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "oidc_not_configured"
