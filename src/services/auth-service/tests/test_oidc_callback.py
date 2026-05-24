"""Tests for ``POST /v1/auth/login/oidc/callback`` (AS-IMPL-024 stub)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.providers import Providers
from custos_auth.settings import Settings, load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

_ENV_BASE = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
}


def _providers() -> Providers:
    return Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )


def _app_with_settings(settings: Settings) -> TestClient:
    return TestClient(create_app(settings=settings, providers=_providers()))


def _valid_body() -> dict[str, str]:
    return {
        "issuer": "https://login.example.com/",
        "code": "auth-code-xyz",
        "state": "opaque-state",
    }


def test_oidc_callback_returns_503_oidc_not_enabled_by_default() -> None:
    # The Phase I stub gates the route behind CUSTOS_AUTH_OIDC_ENABLED so an
    # M1 deployment ships with OIDC routes mounted but disabled. The
    # callback returns the canonical 503 envelope with code
    # ``oidc_not_enabled`` so clients can branch on it.
    settings = load_settings(_ENV_BASE)
    assert settings.oidc_enabled is False
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"]["code"] == "oidc_not_enabled"
    assert "CUSTOS_AUTH_OIDC_ENABLED" in body["error"]["detail"]


def test_oidc_callback_returns_503_oidc_not_implemented_when_flag_on() -> None:
    # When an operator flips the flag before Phase H ships the handler
    # implementation, the route returns a *different* 503 code so the
    # diagnostic (and the audit row) disambiguates "flag off" from
    # "flag on but verifier missing".
    settings = load_settings({**_ENV_BASE, "CUSTOS_AUTH_OIDC_ENABLED": "true"})
    assert settings.oidc_enabled is True
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"]["code"] == "oidc_not_implemented"
    assert "Phase H" in body["error"]["detail"]


def test_oidc_callback_does_not_require_call_context_header() -> None:
    # OIDC callback is an external IdP redirect — the call-context
    # middleware must let it through unauthenticated. Bypass path
    # already registered in ``custos_auth.middleware.callctx``.
    settings = load_settings(_ENV_BASE)
    with _app_with_settings(settings) as client:
        # No X-Custos-Callctx header anywhere on this request.
        resp = client.post("/v1/auth/login/oidc/callback", json=_valid_body())
    # The 503 is the stub response — what matters here is that the
    # middleware did not pre-empt with a 401/403 for missing context.
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] in {"oidc_not_enabled", "oidc_not_implemented"}


def test_oidc_callback_rejects_extra_fields() -> None:
    # The OidcCallbackRequest model is extra=forbid so a misconfigured
    # client cannot smuggle additional fields into the contract.
    settings = load_settings(_ENV_BASE)
    bad_body = {**_valid_body(), "redirect_uri": "https://attacker.example/"}
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=bad_body)
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "request_validation_failed"


def test_oidc_callback_rejects_empty_required_fields() -> None:
    # min_length=1 on every field — empty strings fail Pydantic
    # validation, surfaced through the shared
    # ``request_validation_failed`` envelope.
    settings = load_settings(_ENV_BASE)
    bad_body = {"issuer": "", "code": "", "state": ""}
    with _app_with_settings(settings) as client:
        resp = client.post("/v1/auth/login/oidc/callback", json=bad_body)
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "request_validation_failed"


def test_oidc_callback_is_listed_in_middleware_bypass_paths() -> None:
    # Defensive cross-reference: the bypass set must include the OIDC
    # callback path so the test above is not relying on an
    # implementation detail of the route order.
    from custos_auth.middleware.callctx import _BYPASS_PATHS

    assert "/v1/auth/login/oidc/callback" in _BYPASS_PATHS
