"""Execution-aware startup never silently substitutes in-memory or Noop collaborators."""

import json
from collections.abc import Mapping

import pytest
from custos_callctx import CallContextVerifier
from custos_callctx.execution import load_execution_bindings
from custos_spl import MetadataStoreProvider
from fastapi.testclient import TestClient

import custos_workflow.app as app_module
from custos_workflow.providers import MetadataStorePool, RunComponents, load_run_components
from custos_workflow.runs.errors import WorkflowRuntimeUnavailableError


def execution_env() -> dict[str, str]:
    """Provide operator references and service addresses without credentials."""
    return {
        "CUSTOS_EXECUTION_IDENTITY_ENABLED": "true",
        "CUSTOS_EXECUTION_BINDINGS": json.dumps(
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
        "WF_TRIGGER_DELIVERY_ENABLED": "true",
        "WF_TS_ENDPOINT": "trigger-service",
        "WF_ARM_ENDPOINT": "activity-runtime-manager",
        "WF_CONNECTOR_ENDPOINT": "connector-service",
        "WF_CATALOG_ENDPOINT": "catalog-service",
    }


def test_execution_requires_explicit_metadata_provider() -> None:
    """Opting into execution must not create an implicit in-process receipt store."""
    with pytest.raises(ValueError, match="explicit durable metadata"):
        load_run_components(env=execution_env())


@pytest.mark.parametrize(
    "variable",
    [
        "WF_ARM_ENDPOINT",
        "WF_CONNECTOR_ENDPOINT",
        "WF_CATALOG_ENDPOINT",
    ],
)
def test_execution_requires_real_default_transports(
    variable: str, fake_run_components: RunComponents
) -> None:
    """Missing production endpoints fail before a Noop client can claim an executed activity."""
    env = execution_env()
    env.pop(variable)
    assert fake_run_components.metadata_store is not None
    with pytest.raises(ValueError, match=variable):
        load_run_components(env=env, metadata_store=fake_run_components.metadata_store)


@pytest.mark.parametrize("injected", [False, True])
def test_execution_environment_requires_an_opened_durable_store(
    injected: bool, monkeypatch: pytest.MonkeyPatch, fake_run_components: RunComponents
) -> None:
    """The actual environment fallback is rejected; explicit test bundles remain supported."""
    monkeypatch.delenv("WF_METADATA_STORE", raising=False)
    monkeypatch.setenv("WF_DAPR_WORKFLOW_COMPONENT", "component")
    monkeypatch.setenv("WF_TS_ENDPOINT", "trigger")

    def fake_load(*, metadata_store: MetadataStoreProvider) -> RunComponents:
        """Avoid starting the external SDK while retaining the real metadata-open path."""
        assert metadata_store is not None
        return fake_run_components

    monkeypatch.setattr(app_module, "load_run_components", fake_load)
    app = app_module.create_app(
        run_components=fake_run_components if injected else None,
        execution_bindings=load_execution_bindings(execution_env()),
        execution_verifier=CallContextVerifier(jwks_url="http://auth/.well-known/jwks.json"),
        app_api_token="test-app-token",
        trigger_namespace="test",
    )
    if injected:
        with TestClient(app) as client:
            assert client.get("/readyz").status_code == 200
    else:
        with (
            pytest.raises(WorkflowRuntimeUnavailableError, match="opened durable metadata"),
            TestClient(app),
        ):
            pytest.fail("execution mode accepted an implicit memory store")


async def test_component_construction_failure_closes_opened_metadata_pool(
    monkeypatch: pytest.MonkeyPatch, fake_run_components: RunComponents
) -> None:
    """A configuration exception after opening storage must not leak the owned pool."""
    store = fake_run_components.metadata_store
    assert store is not None
    pool = MetadataStorePool(dsn="postgresql://test/custos")
    closed: list[MetadataStorePool] = []

    async def open_store(
        env: Mapping[str, str],
    ) -> tuple[MetadataStoreProvider, MetadataStorePool, None]:
        """Return the owned pool without opening an external connection."""
        return store, pool, None

    async def close_pool(self: MetadataStorePool) -> None:
        """Record cleanup of precisely the pool opened for this startup."""
        closed.append(self)

    def fail_load(*, metadata_store: MetadataStoreProvider) -> RunComponents:
        """Reproduce a new execution-configuration guard raising during construction."""
        raise ValueError("execution configuration failed")

    monkeypatch.setattr(app_module, "open_metadata_store", open_store)
    monkeypatch.setattr(app_module, "load_run_components", fail_load)
    monkeypatch.setattr(MetadataStorePool, "aclose", close_pool)
    monkeypatch.setenv("WF_DAPR_WORKFLOW_COMPONENT", "component")
    monkeypatch.setenv("WF_TS_ENDPOINT", "trigger")
    with pytest.raises(ValueError, match="execution configuration failed"):
        await app_module._resolve_run_components(None)
    assert closed == [pool]
