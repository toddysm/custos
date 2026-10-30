"""Configured hook selection, shared invoker wiring and readiness failure boundaries."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from kubernetes_asyncio import config

from custos_connector.hook_runtime import InClusterHookRunner
from custos_connector.providers import load_providers
from custos_connector.settings import SettingsError, load_settings

BASE = {
    "CONN_CATALOG_STORE": "postgresql://localhost/catalog",
    "CONN_METADATA_STORE": "postgresql://localhost/metadata",
    "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
}
KUBERNETES = {
    **BASE,
    "CONN_HOOK_BACKEND": "kubernetes",
    "CONN_HOOK_NAMESPACE": "custos-hooks",
    "CONN_HOOK_HELPER_IMAGE": "registry.example/helper@sha256:" + "1" * 64,
    "CONN_CONNECTOR_REGISTRY_URL": "https://registry.example",
}


@pytest.mark.parametrize(
    "missing",
    [
        "CONN_HOOK_NAMESPACE",
        "CONN_HOOK_HELPER_IMAGE",
        "CONN_CONNECTOR_REGISTRY_URL",
    ],
)
def test_missing_kubernetes_prerequisites_are_rejected(missing: str) -> None:
    """Selected production execution cannot silently fall back to Docker."""
    with pytest.raises(SettingsError):
        load_settings({**KUBERNETES, missing: ""})


def test_production_does_not_implicitly_select_docker() -> None:
    """Reject the development backend before constructing production resources."""
    with pytest.raises(SettingsError, match="CONN_HOOK_BACKEND"):
        load_providers(load_settings({**BASE, "ENVIRONMENT": "production"}))


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
async def test_kubernetes_invoker_and_readiness_are_bound_once(
    monkeypatch: pytest.MonkeyPatch,
    architecture: str,
) -> None:
    """All production hook services share the same configured execution authority."""
    monkeypatch.setattr(config, "load_incluster_config", lambda **kwargs: None)
    ready = AsyncMock()
    monkeypatch.setattr(InClusterHookRunner, "check_ready", ready)
    bundle = load_providers(
        load_settings(
            {
                **KUBERNETES,
                "ENVIRONMENT": "production",
                "CONN_HOOK_ARCHITECTURE": architecture,
            }
        )
    )
    try:
        assert bundle.plugin_invoker is not None
        assert bundle.hook_readiness is not None
        assert bundle.instance_service is not None
        assert bundle.bind_for_step_service._plugin is bundle.plugin_invoker
        assert bundle.instance_service._runtime is bundle.plugin_invoker
        await bundle.hook_readiness()
        ready.assert_awaited_once()
    finally:
        await bundle.identity_registry.aclose()
        if bundle.registration_registry_client is not None:
            await bundle.registration_registry_client.aclose()
