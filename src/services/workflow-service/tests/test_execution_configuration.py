"""Execution-aware startup never silently substitutes in-memory or Noop collaborators."""

import json

import pytest

from custos_workflow.providers import RunComponents, load_run_components


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
