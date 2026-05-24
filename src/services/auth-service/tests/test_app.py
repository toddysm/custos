"""Integration smoke for :func:`custos_auth.create_app` (AS-IMPL-004)."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired
from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.providers import Providers
from custos_auth.settings import load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

_ENV = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
}


def _providers(
    *,
    auth_revs: set[int] | None = None,
    meta_revs: set[int] | None = None,
) -> Providers:
    return Providers(
        auth_store=FakeAuthAdapter(applied_revisions=auth_revs),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(applied_revisions=meta_revs),  # type: ignore[arg-type]
    )


def test_create_app_returns_a_fastapi_instance() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    from fastapi import FastAPI

    assert isinstance(app, FastAPI)


def test_healthz_returns_200_before_any_startup_work() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_returns_200_when_schema_gate_passes() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_readyz_returns_503_when_schema_gate_fails() -> None:
    app = create_app(
        settings=load_settings(_ENV),
        providers=_providers(auth_revs=set(), meta_revs={1, 2, 3, 4}),
    )
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert "AuthStoreProvider@rev1" in body["detail"]
    assert "custos migrate up" in body["detail"]


def test_app_state_carries_schema_gate_error_on_failure() -> None:
    app = create_app(
        settings=load_settings(_ENV),
        providers=_providers(auth_revs=set(), meta_revs={1, 2, 3, 4}),
    )
    with TestClient(app):
        assert app.state.ready is False
        assert isinstance(app.state.schema_gate_error, MigrationRequired)


def test_app_state_carries_providers_after_startup() -> None:
    providers = _providers()
    app = create_app(settings=load_settings(_ENV), providers=providers)
    with TestClient(app):
        assert app.state.providers is providers


def test_app_state_carries_settings_after_startup() -> None:
    settings = load_settings(_ENV)
    app = create_app(settings=settings, providers=_providers())
    with TestClient(app):
        assert app.state.settings is settings


@pytest.mark.parametrize("path", ["/healthz", "/readyz"])
def test_probes_do_not_require_authentication(path: str) -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        resp = client.get(path)
    assert resp.status_code in (200, 503)
