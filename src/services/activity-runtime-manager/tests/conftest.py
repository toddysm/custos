"""Shared pytest fixtures for the Activity Runtime Manager tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from custos_arm.config import Settings, load_settings

#: The minimal set of required ``ARM_*`` variables a valid environment must
#: carry (design § Configuration). Optional variables fall back to their
#: documented defaults.
_REQUIRED_ENV: dict[str, str] = {
    "ARM_ARTIFACT_STORE": "artifacts",
    "ARM_METADATA_STORE": "metadata",
    "ARM_CATALOG_ENDPOINT": "http://catalog.svc:8080",
    "ARM_CONNECTOR_ENDPOINT": "http://connector.svc:8080",
    "ARM_SANDBOX_NAMESPACE": "custos-activities",
    "ARM_SIDECAR_IMAGE": "ghcr.io/custos/connector-sidecar:0.1.0",
}


@pytest.fixture(autouse=True)
def _required_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Populate the required ``ARM_*`` variables for tests that build the app.

    Tests that exercise the failure paths clear specific variables with
    ``monkeypatch.delenv``; the dev-shim/production tests override
    ``ENVIRONMENT`` and ``ARM_AUTHZ_ENDPOINT`` explicitly.
    """
    for name, value in _REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    # Ensure a clean, dev-shim, non-production baseline regardless of the
    # developer's shell environment.
    monkeypatch.delenv("ARM_AUTHZ_ENDPOINT", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    yield


@pytest.fixture
def settings() -> Settings:
    """A valid :class:`Settings` loaded from the autouse test environment."""
    return load_settings()
