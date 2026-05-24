"""Integration smoke for :func:`custos_catalog.create_app`."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired
from fastapi.testclient import TestClient

from custos_catalog import create_app
from custos_catalog.providers import Providers
from custos_catalog.settings import load_settings
from tests._fakes import FakeCatalogAdapter, FakeDefinitionAdapter, FakeMetadataAdapter

_ENV = {
    "CAT_DEFINITION_STORE": "postgresql://u:p@h:5432/def",
    "CAT_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
    "CAT_METADATA_STORE": "postgresql://u:p@h:5432/meta",
    "CAT_CONNECTOR_ENDPOINT": "http://connector-service:8080",
    # CAT_AUTHZ_ENDPOINT intentionally unset — exercises the dev shim.
}


def _providers(
    *,
    def_revs: set[int] | None = None,
    cat_revs: set[int] | None = None,
    meta_revs: set[int] | None = None,
) -> Providers:
    return Providers(
        definition_store=FakeDefinitionAdapter(applied_revisions=def_revs),  # type: ignore[arg-type]
        catalog_store=FakeCatalogAdapter(applied_revisions=cat_revs),  # type: ignore[arg-type]
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
        providers=_providers(def_revs=set(), cat_revs={1}),
    )
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert "DefinitionStoreProvider@rev1" in body["detail"]
    assert "custos migrate up" in body["detail"]


def test_app_state_carries_schema_gate_error_on_failure() -> None:
    app = create_app(
        settings=load_settings(_ENV),
        providers=_providers(def_revs=set(), cat_revs={1}),
    )
    with TestClient(app):
        assert app.state.ready is False
        assert isinstance(app.state.schema_gate_error, MigrationRequired)


def test_app_state_carries_providers_after_startup() -> None:
    providers = _providers()
    app = create_app(settings=load_settings(_ENV), providers=providers)
    with TestClient(app):
        assert app.state.providers is providers


def test_create_app_propagates_dev_shim_production_guard() -> None:
    # When the operator forgets to set CAT_AUTHZ_ENDPOINT in production,
    # the middleware refuses to construct. FastAPI builds the middleware
    # stack lazily on first request / lifespan, so the guard surfaces
    # when the TestClient enters the lifespan context.
    from custos_catalog.middleware import DevShimDisabledInProductionError

    settings = load_settings({**_ENV, "ENVIRONMENT": "production"})
    app = create_app(settings=settings, providers=_providers())
    with pytest.raises(DevShimDisabledInProductionError), TestClient(app):
        pass


@pytest.mark.parametrize("path", ["/healthz", "/readyz"])
def test_probes_do_not_require_callctx_header(path: str) -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        resp = client.get(path)
    assert resp.status_code in (200, 503)
