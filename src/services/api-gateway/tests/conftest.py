"""Shared test fixtures for the API Gateway suite."""

from __future__ import annotations

import pytest

from custos_gateway.clients.auth import DeclaredPermission, FakeAuthServiceClient
from custos_gateway.routes.registry import registry_required_permissions
from custos_gateway.settings import Settings, load_settings


def minimal_gateway_env() -> dict[str, str]:
    """Return an env mapping carrying only the required gateway variables."""
    return {
        "CUSTOS_GATEWAY_TLS_CERT_REF": "secretref://tls/cert",
        "CUSTOS_GATEWAY_TLS_KEY_REF": "secretref://tls/key",
        "CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": '["https://ui.custos.example"]',
    }


@pytest.fixture
def gateway_env() -> dict[str, str]:
    """A minimal valid environment for ``load_settings``."""
    return minimal_gateway_env()


@pytest.fixture
def settings(gateway_env: dict[str, str]) -> Settings:
    """A :class:`Settings` parsed from the minimal valid environment."""
    return load_settings(gateway_env)


@pytest.fixture
def auth_client() -> FakeAuthServiceClient:
    """A fake Auth client declaring every registry permission so startup passes.

    ``create_app``'s lifespan always runs the startup permission cross-check, so
    tests that enter the lifespan inject this double to avoid a real Dapr call.
    """
    return FakeAuthServiceClient(
        permissions=[
            DeclaredPermission(name=name, description=name, declared_by="test")
            for name in registry_required_permissions()
        ]
    )
