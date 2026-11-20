"""Tests for the app factory + health probes (AGW-IMPL-002)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from custos_pg import PgMetadataAdapter
from custos_pg.pool import LazyPool
from custos_spl.errors import BackendUnavailable, MigrationRequired
from fastapi.testclient import TestClient

from custos_gateway.app import create_app
from custos_gateway.clients.auth import FakeAuthServiceClient
from custos_gateway.settings import Settings


def test_healthz_always_ok(settings: Settings, auth_client: FakeAuthServiceClient) -> None:
    app = create_app(settings=settings, auth_client=auth_client)
    # No lifespan entered: app.state.ready is False, but liveness is independent.
    with TestClient(app, raise_server_exceptions=True) as client:
        # Inside the context the lifespan has run, so probe liveness directly.
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_503_before_ready_then_200_after(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    app = create_app(settings=settings, auth_client=auth_client)

    # Before the lifespan runs the readiness gate is closed.
    assert app.state.ready is False
    pre_client = TestClient(app)  # does not enter lifespan
    pre = pre_client.get("/readyz")
    assert pre.status_code == 503
    assert pre.json()["status"] == "not_ready"

    # Entering the lifespan flips the gate to ready.
    with TestClient(app) as client:
        post = client.get("/readyz")
    assert post.status_code == 200
    assert post.json() == {"status": "ready"}


def test_settings_attached_to_app_state(settings: Settings) -> None:
    app = create_app(settings=settings)
    assert app.state.settings is settings


@pytest.mark.parametrize("valid", [False, True])
def test_execution_file_before_gateway_readiness(
    valid: bool,
    settings: Settings,
    auth_client: FakeAuthServiceClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate managed ingress's pinned registry before opening transports or admission."""
    raw = json.dumps(
        {
            "bindings": [
                {
                    "workspaceId": "ws",
                    "principalId": "executor",
                    "bindingId": "one",
                    "secretRef": "source",
                    "secretKey": "token",
                }
            ],
            "defaults": {"ws": "one"},
        }
    ).encode()
    path = tmp_path / "bindings.json"
    path.write_bytes(raw)
    monkeypatch.setenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", "true")
    monkeypatch.delenv("CUSTOS_EXECUTION_BINDINGS", raising=False)
    monkeypatch.setenv("CUSTOS_EXECUTION_BINDINGS_FILE", str(path))
    monkeypatch.setenv(
        "CUSTOS_EXECUTION_BINDINGS_SHA256", hashlib.sha256(raw).hexdigest() if valid else "0" * 64
    )
    app = create_app(settings=settings, auth_client=auth_client)
    if valid:
        with TestClient(app) as client:
            assert client.get("/readyz").status_code == 200
            assert app.state.execution_bindings.select("ws").principal_id == "executor"
    else:
        with pytest.raises(ValueError, match="SHA-256 mismatch"), TestClient(app):
            pytest.fail("invalid snapshot opened Gateway admission")
        assert app.state.ready is False


@pytest.mark.parametrize("failure", [None, "migration", "backend"])
async def test_owned_metadata_readiness_and_cleanup(
    settings: Settings,
    auth_client: FakeAuthServiceClient,
    monkeypatch: pytest.MonkeyPatch,
    failure: str | None,
) -> None:
    """Configured stores gate readiness and close owned resources on success or partial startup."""
    pool = LazyPool("postgresql://unused")
    monkeypatch.setattr("custos_pg.pool.LazyPool", lambda dsn: pool)

    async def refresh(adapter: PgMetadataAdapter) -> None:
        """Supply schema evidence or an explicit backend failure without opening a socket."""
        if failure == "backend":
            raise BackendUnavailable("metadata unavailable")
        adapter._applied_revisions = set() if failure == "migration" else {1, 2, 3, 4}

    monkeypatch.setattr(PgMetadataAdapter, "refresh_declared", refresh)
    app = create_app(
        settings=replace(settings, metadata_store_dsn="postgresql://unused"),
        auth_client=auth_client,
    )
    if failure is None:
        async with app.router.lifespan_context(app):
            assert app.state.ready and isinstance(app.state.metadata_store, PgMetadataAdapter)
            assert app.state.metadata_store._lazy is pool
    else:
        expected = MigrationRequired if failure == "migration" else BackendUnavailable
        with pytest.raises(expected):
            async with app.router.lifespan_context(app):
                raise AssertionError("unready metadata must not serve requests")
    assert not app.state.ready
    assert app.state.downstream_router.http_client.is_closed
    with pytest.raises(RuntimeError, match="closed"):
        await pool.get()


async def test_injected_metadata_remains_caller_owned(
    settings: Settings,
    auth_client: FakeAuthServiceClient,
) -> None:
    """An injected store overrides the DSN without transferring ownership of its pool."""
    pool = LazyPool("postgresql://unused")
    store = PgMetadataAdapter(lazy=pool)
    app = create_app(
        settings=replace(settings, metadata_store_dsn="postgresql://unused"),
        auth_client=auth_client,
        metadata_store=store,
    )
    try:
        async with app.router.lifespan_context(app):
            assert app.state.metadata_store is store
        assert not pool._closed
    finally:
        await pool.aclose()
