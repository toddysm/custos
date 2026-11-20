"""Fail-closed execution activation and cleanup before the normal application yield."""

import hashlib
import json
from pathlib import Path

import pytest
from custos_spl.errors import MigrationRequired
from fastapi.testclient import TestClient

from custos_trigger import create_app
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers


def configure(monkeypatch: pytest.MonkeyPatch, path: Path | None = None) -> None:
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
    if path is not None:
        import os

        raw = os.environ["CUSTOS_EXECUTION_BINDINGS"].encode()
        path.write_bytes(raw)
        monkeypatch.delenv("CUSTOS_EXECUTION_BINDINGS")
        monkeypatch.setenv("CUSTOS_EXECUTION_BINDINGS_FILE", str(path))
        monkeypatch.setenv("CUSTOS_EXECUTION_BINDINGS_SHA256", hashlib.sha256(raw).hexdigest())


def test_execution_rejects_implicit_memory_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing metadata binding cannot turn durable execution into a development fallback."""
    configure(monkeypatch)
    monkeypatch.delenv("TRIGGER_METADATA_STORE", raising=False)
    app = create_app()
    with pytest.raises(ValueError, match="durable metadata"), TestClient(app):
        pass
    assert app.state.ready is False


@pytest.mark.parametrize("file_source", [False, True])
def test_failed_identity_readiness_closes_owned_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, file_source: bool
) -> None:
    """A failed revision gate must release the provider opened before yield."""
    configure(monkeypatch, tmp_path / "bindings.json" if file_source else None)
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


def test_file_digest_failure_precedes_admission(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A bad file snapshot cannot construct a receiver or open its metadata provider."""
    configure(monkeypatch, tmp_path / "bindings.json")
    monkeypatch.setenv("CUSTOS_EXECUTION_BINDINGS_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        create_app()
