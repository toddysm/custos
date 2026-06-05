"""Scaffold smoke tests for the Observability and Audit Service (OBS-IMPL-001).

These pin the package's import contract, the version string, and the
``/healthz`` + ``/readyz`` probe wire shapes so later OBS-IMPL phases can grow
the runtime in place without regressing the deployment-probe contract.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

import custos_obs
from custos_obs import __version__, create_app
from custos_obs.providers import Providers
from custos_obs.settings import Settings, load_settings

# A fully ``noop`` configuration whose providers wire without touching a
# backend: the metadata pool is deferred (no socket until first query) and the
# log/metrics providers are noop adapters. Lets the lifespan boot under
# ``TestClient`` with no Postgres/Loki/Prometheus reachable.
_NOOP_ENV = {
    "CUSTOS_LOG_QUERY_PROVIDER": "noop",
    "CUSTOS_LOGS_EXTERNAL_URL": "https://logs.example.com",
    "CUSTOS_METRICS_QUERY_PROVIDER": "noop",
    "CUSTOS_METRICS_EXTERNAL_URL": "https://metrics.example.com",
    "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://noop/noop",
}


def _noop_settings() -> Settings:
    return load_settings(_NOOP_ENV)


def test_package_exports_create_app_and_version() -> None:
    assert custos_obs.__version__ == "0.1.0"
    assert __version__ == "0.1.0"
    assert callable(create_app)


def test_create_app_is_import_safe_and_idempotent() -> None:
    # Constructing the app must not open sockets or read required env vars.
    first = create_app()
    second = create_app()
    assert first is not second
    assert first.title == "Custos Observability and Audit Service"
    assert first.version == "0.1.0"


def test_healthz_is_always_ok() -> None:
    app = create_app()
    # Bypass the lifespan: ``/healthz`` is a flat liveness signal.
    client = TestClient(app)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_is_not_ready_before_lifespan_runs() -> None:
    app = create_app()
    client = TestClient(app)  # no context-manager -> lifespan not run
    resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert "observability-audit-service" in body["detail"]


def test_readyz_becomes_ready_inside_lifespan() -> None:
    app = create_app(settings=_noop_settings())
    with TestClient(app) as client:  # context-manager runs the lifespan
        resp = client.get("/readyz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ready"}
        # The lifespan stashes the resolved settings + provider bundle.
        assert app.state.settings.log_query_provider == "noop"
        assert isinstance(app.state.providers, Providers)


def test_readyz_resets_after_lifespan_shutdown() -> None:
    app = create_app(settings=_noop_settings())
    with TestClient(app):
        pass
    # After shutdown the readiness flag is cleared again.
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 503
