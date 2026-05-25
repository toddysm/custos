"""Tests for the ``create_app`` factory + ``/healthz`` + ``/readyz`` wiring.

Exercises the FastAPI lifespan that runs the schema-revision startup
gate: ``/readyz`` flips to 200 when the gate passes and 503 (with the
operator-actionable ``MigrationRequired`` text) when it fails.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.providers import Providers
from custos_connector.settings import Settings
from tests._fakes import FakeCatalogAdapter, FakeMetadataAdapter

_BASE_SETTINGS = Settings(
    catalog_store_dsn="postgresql://u:p@h:5432/cat",
    metadata_store_dsn="postgresql://u:p@h:5432/meta",
    catalog_endpoint="http://catalog-service:8080",
    authz_endpoint="",  # dev shim
    oci_referrers_timeout_ms=5000,
    publish_max_body_mb=4,
    sidecar_default_ttl_sec=600,
    lease_max_concurrent=16,
    pull_loop_min_interval_sec=10,
    sidecar_mtls_issuer=None,
    environment="development",
)


def _providers(*, behind: bool = False) -> Providers:
    catalog = FakeCatalogAdapter(applied_revisions=set() if behind else {1})
    metadata = FakeMetadataAdapter(applied_revisions=set() if behind else {1, 2, 3, 4})
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
    )


def test_healthz_is_always_200() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_providers())
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_returns_200_when_schema_gate_passes() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_providers())
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_readyz_returns_503_with_explainer_when_schema_gate_fails() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_providers(behind=True))
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert "CatalogStoreProvider@rev1" in body["detail"]
    assert "MetadataStoreProvider@rev4" in body["detail"]
    assert "CONN_CATALOG_STORE" in body["detail"]


def test_lifespan_records_schema_gate_error_on_app_state() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_providers(behind=True))
    with TestClient(app):
        assert app.state.ready is False
        assert app.state.schema_gate_error is not None
