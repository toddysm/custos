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


# ---------------------------------------------------------------------------
# Supplemental branch coverage (Phase K / AS-IMPL-027)
# ---------------------------------------------------------------------------


def test_oidc_callback_bare_app_returns_503_oidc_not_implemented() -> None:
    """A bare ``FastAPI`` (no lifespan) is missing ``app.state.oidc_*``.

    The route falls back to parsing ``settings.oidc_issuers_raw``
    inline so it still type-checks, then short-circuits with
    ``oidc_not_implemented``. Used to pin the fixture-friendly
    contract — bare-app tests must never crash on a missing
    ``app.state`` attribute.
    """
    from fastapi import FastAPI

    from custos_auth.api.routes.oidc import router as oidc_router

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )

    app = FastAPI()
    # Mount the route at its real prefix and override both
    # ``get_settings`` and ``get_metadata_store``. Even though the
    # ``oidc_not_implemented`` branch does not use the metadata store,
    # FastAPI still resolves ``get_metadata_store`` as a dependency,
    # and the default provider would raise on a bare app with no
    # ``app.state.providers``.
    from custos_auth.api.dependencies import get_metadata_store, get_settings

    # ``oidc_router`` already declares its ``/v1`` prefix on the
    # ``APIRouter`` constructor — including it without an extra prefix
    # is what the production app does in ``create_app()``.
    app.include_router(oidc_router)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_metadata_store] = lambda: FakeMetadataAdapter()

    with TestClient(app) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "oidc_not_implemented"


def test_oidc_callback_503_when_issuer_missing_client_id() -> None:
    """Issuer entry has a ``token_endpoint`` but no ``client_id`` →
    ``oidc_not_configured``. The exchange step requires both.

    The parser permits ``client_id`` to be omitted (workload-token
    issuers don't need it); the route is the one that demands the
    pair when a code-flow exchange is requested.
    """
    raw_config = json.dumps(
        {
            "issuers": [
                {
                    "id": "primary",
                    "issuer_url": _ISSUER_URL,
                    "jwks_uri": _JWKS_URI,
                    "audiences": [_AUDIENCE],
                    "token_endpoint": _TOKEN_ENDPOINT,
                    # client_id + client_secret_env intentionally absent.
                }
            ]
        }
    )
    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": raw_config,
        }
    )
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "oidc_not_configured"
    assert "client_id" in resp.json()["error"]["detail"]


def test_oidc_callback_502_on_exchange_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transport-level failure during the OAuth code exchange.

    The route catches the ``httpx`` exception and re-raises it as
    :class:`OidcVerificationError` with ``reason=exchange_failed``,
    which the handler maps to a 502 + ``authn.failure_oidc`` audit
    row. Distinct branch from the non-200 path (covered by the
    existing 503-status test).
    """
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    def _raising(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("simulated DNS failure")

    transport = httpx.MockTransport(_raising)

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    providers = _providers()
    with _app_with_settings(settings, providers=providers) as client:
        _patch_oidc_client(client, transport)
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "oidc_exchange_failed"

    events = [event for _, event in providers.metadata_store.append_audit_calls]  # type: ignore[attr-defined]
    failure_rows = [e for e in events if e.event_type == "authn.failure"]
    assert len(failure_rows) == 1
    assert failure_rows[0].payload["reason"] == "exchange_failed"


def test_oidc_callback_502_on_exchange_response_not_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Token endpoint returns 200 but the body is not JSON.

    Provider misconfiguration (HTML error page, gateway-rewritten
    body) collapses to the same closed-set ``exchange_failed``
    reason so dashboards keep one bucket for code-exchange health.
    """
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    def _handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == _TOKEN_ENDPOINT:
            return httpx.Response(
                200,
                content=b"<html>maintenance mode</html>",
                headers={"content-type": "text/html"},
            )
        return httpx.Response(404)

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    providers = _providers()
    with _app_with_settings(settings, providers=providers) as client:
        _patch_oidc_client(client, httpx.MockTransport(_handler))
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "oidc_exchange_failed"


def test_oidc_callback_502_on_exchange_missing_id_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Token endpoint returns 200 JSON but no ``id_token`` field.

    Some providers return ``{"access_token": "..."}`` without an
    ``id_token`` when the ``openid`` scope is missing — same closed-
    set reason so the audit feed stays consistent.
    """
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    def _handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == _TOKEN_ENDPOINT:
            return httpx.Response(200, json={"access_token": "abc"})
        return httpx.Response(404)

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    providers = _providers()
    with _app_with_settings(settings, providers=providers) as client:
        _patch_oidc_client(client, httpx.MockTransport(_handler))
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "oidc_exchange_failed"


def test_oidc_callback_forwards_redirect_uri_to_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the caller passes ``redirect_uri`` the exchange POST must
    forward it.

    Some providers (notably GitHub OAuth) refuse the code exchange
    when ``redirect_uri`` was sent on the ``/authorize`` call but is
    omitted on the token exchange.
    """
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    id_token = _mint_id_token(priv, sub="user-42")

    captured: dict[str, str] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == _TOKEN_ENDPOINT:
            # ``request.content`` is the form-encoded payload.
            body = request.content.decode("utf-8")
            for piece in body.split("&"):
                if "=" in piece:
                    k, v = piece.split("=", 1)
                    captured[k] = v
            return httpx.Response(200, json={"id_token": id_token})
        if str(request.url) == _JWKS_URI:
            return httpx.Response(200, json={"keys": [jwk]})
        return httpx.Response(404)

    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": _issuers_env(),
        }
    )
    body = {**_valid_body(), "redirect_uri": "https://app.example.com/cb"}
    with _app_with_settings(settings) as client:
        _patch_oidc_client(client, httpx.MockTransport(_handler))
        resp = client.post("/v1/auth/login/oidc/callback", json=body)

    assert resp.status_code == 200, resp.text
    # ``urllib.parse.quote_plus``-style encoding by httpx: ``:`` → ``%3A`` etc.
    from urllib.parse import unquote_plus

    assert "redirect_uri" in captured
    assert unquote_plus(captured["redirect_uri"]) == "https://app.example.com/cb"


def test_oidc_callback_github_preset_attaches_extras_to_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the issuer carries ``preset: github`` the audit success
    row must include the GitHub preset's extra claims.

    Pins the wire shape for downstream audit-feed consumers: a
    GitHub workload-token login surfaces ``repository`` /
    ``workflow`` / ``ref`` on the audit row so operators can pivot
    on origin pipeline.
    """
    monkeypatch.setenv("TEST_OIDC_CLIENT_SECRET", "shhh")

    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    id_token = _mint_id_token(
        priv,
        sub="repo:acme/sandbox:ref:refs/heads/main",
        extra={
            "repository": "acme/sandbox",
            "repository_id": 12345,
            "workflow": "ci",
            "ref": "refs/heads/main",
        },
    )

    raw_config = json.dumps(
        {
            "issuers": [
                {
                    "id": "primary",
                    "preset": "github",
                    "issuer_url": _ISSUER_URL,
                    "jwks_uri": _JWKS_URI,
                    "audiences": [_AUDIENCE],
                    "token_endpoint": _TOKEN_ENDPOINT,
                    "client_id": "test-client",
                    "client_secret_env": "TEST_OIDC_CLIENT_SECRET",
                }
            ]
        }
    )
    settings = load_settings(
        {
            **_ENV_BASE,
            "CUSTOS_AUTH_OIDC_ENABLED": "true",
            "CUSTOS_AUTH_OIDC_ISSUERS": raw_config,
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

    events = [event for _, event in providers.metadata_store.append_audit_calls]  # type: ignore[attr-defined]
    success_rows = [e for e in events if e.event_type == "authn.success"]
    assert len(success_rows) == 1
    payload = success_rows[0].payload
    assert payload["repository"] == "acme/sandbox"
    assert payload["repository_id"] == "12345"  # int → str in preset
    assert payload["workflow"] == "ci"
    assert payload["ref"] == "refs/heads/main"
