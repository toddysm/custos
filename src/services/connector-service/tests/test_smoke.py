"""Smoke tests for the connector-service package (CONN-IMPL-001 scaffold).

These tests assert the package imports cleanly and that the ``/healthz`` +
``/readyz`` probes return 200 so the IMPL-002 Helm chart can pass its
liveness / readiness gates. Per-component behaviour lands in dedicated test
modules in subsequent CONN-IMPL-* phases.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient


def test_package_imports() -> None:
    import custos_connector

    assert hasattr(custos_connector, "__version__")
    assert isinstance(custos_connector.__version__, str)
    assert custos_connector.__version__ == "0.1.0"


def test_create_app_builds_a_fastapi_instance() -> None:
    """``create_app`` returns a minimal FastAPI app during the scaffold phase."""
    import custos_connector

    app = custos_connector.create_app()
    assert isinstance(app, FastAPI)


def test_healthz_and_readyz_return_ok() -> None:
    """Probes return 200 OK so IMPL-002's chart liveness/readiness gates pass."""
    import custos_connector

    client = TestClient(custos_connector.create_app())

    health = client.get("/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ok"}
