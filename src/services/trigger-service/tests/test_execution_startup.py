"""Fail-closed execution activation and cleanup before the normal application yield."""

import json

import pytest
from custos_spl.errors import MigrationRequired
from fastapi.testclient import TestClient

from custos_trigger import create_app
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers


def configure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install only operator reference configuration, never a service credential."""
    monkeypatch.setenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", "true")
    monkeypatch.setenv(
        "CUSTOS_EXECUTION_BINDINGS",
        json.dumps(
            {
                "bindings": [
                    {
                        "workspaceId": "ws",
                        "principalId": "executor",
                        "bindingId": "one",
                        "secretRef": "execution-source",
                        "secretKey": "token",
                    }
                ],
                "defaults": {"ws": "one"},
            }
        ),
    )


def test_execution_rejects_implicit_memory_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing metadata binding cannot turn durable execution into a development fallback."""
    configure(monkeypatch)
    monkeypatch.delenv("TRIGGER_METADATA_STORE", raising=False)
    app = create_app()
    with pytest.raises(ValueError, match="durable metadata"), TestClient(app):
        pass
    assert app.state.ready is False


def test_failed_identity_readiness_closes_owned_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed revision gate must release the provider opened before yield."""
    configure(monkeypatch)
    monkeypatch.setenv("TRIGGER_METADATA_STORE", "postgresql://localhost/unit")
    calls: list[str] = []

    class NotMigrated(InMemoryTriggerMetadataStore):
        """Model an opened provider whose optional identity migration is absent."""

        async def check_execution_identity_ready(self) -> None:
            """Record acquisition and reject activation through the real schema-gate error."""
            calls.append("check")
            raise MigrationRequired([("MetadataStoreProvider", 14)])

    async def close() -> None:
        """Record release of the resource owned by the application."""
        calls.append("close")

    def factory(_dsn: str) -> Providers:
        """Supply the owned provider without accessing a developer's database."""
        return Providers(metadata_store=NotMigrated(), close=close)

    monkeypatch.setattr("custos_trigger.app.load_providers", factory)
    app = create_app()
    with pytest.raises(MigrationRequired), TestClient(app):
        pass
    assert calls == ["check", "close"]
    assert app.state.ready is False
