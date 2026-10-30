"""Configured hook selection, shared invoker wiring and readiness failure boundaries."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from kubernetes_asyncio import client, config

from custos_connector.hook_images import RegistryHookImageResolver
from custos_connector.hook_runtime import InClusterHookRunner
from custos_connector.hook_sandbox import HookSandboxSettings
from custos_connector.providers import load_providers
from custos_connector.runtime import HookRunResult, PluginCleanupFailed
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


@pytest.mark.parametrize("allowed", [True, False])
async def test_readiness_checks_authority_and_closes_its_session(
    monkeypatch: pytest.MonkeyPatch,
    allowed: bool,
) -> None:
    """Missing authority fails startup; permission probes never leave an API session open."""
    api = AsyncMock()
    core = SimpleNamespace(
        list_namespaced_pod=AsyncMock(),
        read_namespaced_service_account=AsyncMock(),
    )
    reviews = SimpleNamespace(
        create_self_subject_access_review=AsyncMock(
            return_value=SimpleNamespace(status=SimpleNamespace(allowed=allowed)),
        )
    )
    configuration = client.Configuration()
    monkeypatch.setattr(client, "ApiClient", lambda *args: api)
    monkeypatch.setattr(client, "CoreV1Api", lambda *args: core)
    monkeypatch.setattr(client, "AuthorizationV1Api", lambda *args: reviews)
    async with httpx.AsyncClient(base_url="https://registry.example") as registry:
        runner = InClusterHookRunner(
            configuration,
            RegistryHookImageResolver(registry, architecture="amd64"),
            HookSandboxSettings(
                "custos-hooks", KUBERNETES["CONN_HOOK_HELPER_IMAGE"], "connector-hook"
            ),
        )
        if allowed:
            await runner.check_ready()
            assert reviews.create_self_subject_access_review.await_count == 7
        else:
            with pytest.raises(RuntimeError, match="required permissions"):
                await runner.check_ready()
        api.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("uncertain", [False, True])
@pytest.mark.parametrize("close_error", [False, True])
async def test_invocation_api_close_is_not_detached_by_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    uncertain: bool,
    close_error: bool,
) -> None:
    """Finalization cannot replace a pending uncertainty fence with cancellation or close errors."""
    from custos_connector import hook_runtime

    started, allowed = asyncio.Event(), asyncio.Event()

    async def close() -> None:
        """Hold actual session disposal until the test permits it."""
        started.set()
        await allowed.wait()
        if close_error:
            raise RuntimeError("API session close failed")

    api = SimpleNamespace(close=AsyncMock(side_effect=close))
    pending = PluginCleanupFailed("custos-hooks/owned")
    child = SimpleNamespace(
        run=AsyncMock(
            return_value=HookRunResult(0, b"{}", b""),
            side_effect=pending if uncertain else None,
        )
    )
    configuration = client.Configuration()
    monkeypatch.setattr(client, "ApiClient", lambda *args: api)
    monkeypatch.setattr(client, "CoreV1Api", lambda *args: object())
    monkeypatch.setattr(hook_runtime, "KubernetesHookTransport", lambda *args: object())
    monkeypatch.setattr(hook_runtime, "KubernetesHookRunner", lambda *args: child)
    async with httpx.AsyncClient(base_url="https://registry.example") as registry:
        runner = InClusterHookRunner(
            configuration,
            RegistryHookImageResolver(registry, architecture="amd64"),
            HookSandboxSettings(
                "custos-hooks", KUBERNETES["CONN_HOOK_HELPER_IMAGE"], "connector-hook"
            ),
        )
        task = asyncio.create_task(
            runner.run(
                image_ref="registered",
                hook="health",
                payload={},
                timeout_seconds=30,
            )
        )
        try:
            async with asyncio.timeout(2):
                await started.wait()
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            allowed.set()
            if uncertain:
                with pytest.raises(PluginCleanupFailed) as failure:
                    await task
                assert failure.value is pending and failure.value.cancellation_requested
            else:
                with pytest.raises(RuntimeError if close_error else asyncio.CancelledError):
                    await task
            api.close.assert_awaited_once()
        finally:
            allowed.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
