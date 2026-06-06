"""Shared test fixtures for the API Gateway suite."""

from __future__ import annotations

import pytest

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
