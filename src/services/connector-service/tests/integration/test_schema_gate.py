"""End-to-end integration tests for the schema-revision startup gate and
the SPL adapter wiring (CONN-IMPL-003).

These tests drive ``custos_connector.create_app`` mounted on real
``PgCatalogAdapter`` / ``PgConnectorInstanceAdapter`` /
``PgMetadataAdapter`` instances pointed at a live Postgres database. The
intent is to catch regressions in the wiring between
:func:`custos_connector.providers.load_providers`, the FastAPI lifespan
in :func:`custos_connector.create_app`, and the SPL ledger that
``refresh_declared`` reads.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def test_healthz_returns_200_against_live_postgres(client: TestClient) -> None:
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_returns_200_when_migrations_applied(client: TestClient) -> None:
    """With ``apply_pending`` run against the test database, the schema
    gate passes and the app is ready."""
    resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_readyz_returns_503_when_ledger_is_empty(stale_client: TestClient) -> None:
    """When the migration ledger has been wiped after the schemas were
    created (simulating a downgrade), the startup gate raises
    ``MigrationRequired`` and ``/readyz`` returns 503 with the
    operator-actionable explainer."""
    resp = stale_client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    # All three interfaces should be flagged.
    assert "CatalogStoreProvider@rev1" in body["detail"]
    assert "ConnectorInstanceStoreProvider@rev1" in body["detail"]
    assert "MetadataStoreProvider@rev4" in body["detail"]
    assert "custos migrate up" in body["detail"]
