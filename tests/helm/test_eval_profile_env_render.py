"""Render-time assertions for the connected-eval service config wiring.

The connected-eval profile has no external secret store, so it synthesises
each stateful service's Postgres DSNs directly from the CNPG application
secret (``custos-app``, key ``uri``) and supplies the remaining non-secret
config (provider selectors, in-cluster endpoints, the sandbox namespace and
the connector sidecar image) as literal env. These tests shell out to
``helm template`` for the ``connected-eval`` profile and assert every
service that previously crash-looped on missing config now receives it.
"""

from __future__ import annotations

from typing import Any

import pytest

PROFILE = "connected-eval"

# Service Deployment name -> required DSN env vars sourced from custos-app.uri.
DSN_ENV: dict[str, tuple[str, ...]] = {
    "custos-auth-service": (
        "CUSTOS_AUTH_STORE_DSN",
        "CUSTOS_AUTH_METADATA_STORE_DSN",
    ),
    "custos-catalog-service": (
        "CAT_DEFINITION_STORE",
        "CAT_CATALOG_STORE",
        "CAT_METADATA_STORE",
    ),
    "custos-connector-service": (
        "CONN_CATALOG_STORE",
        "CONN_METADATA_STORE",
    ),
    "custos-trigger-service": ("TRIGGER_METADATA_STORE",),
    "custos-observability-audit-service": ("CUSTOS_OBS_METADATA_STORE_DSN",),
    "custos-activity-runtime-manager": ("ARM_METADATA_STORE",),
}

# Service Deployment name -> required literal (non-secret) env values.
LITERAL_ENV: dict[str, dict[str, str]] = {
    "custos-observability-audit-service": {
        "CUSTOS_LOG_QUERY_PROVIDER": "loki",
        "CUSTOS_METRICS_QUERY_PROVIDER": "prometheus",
        "CUSTOS_LOKI_URL": "http://custos-loki:3100",
        "CUSTOS_PROMETHEUS_URL": "http://custos-prometheus-server:80",
    },
    "custos-activity-runtime-manager": {
        "ARM_ARTIFACT_STORE": "s3://custos-artifacts",
        "ARM_CATALOG_ENDPOINT": "http://custos-catalog-service:8080",
        "ARM_CONNECTOR_ENDPOINT": "http://custos-connector-service:8080",
        "ARM_SANDBOX_NAMESPACE": "custos-system",
        "ARM_SIDECAR_IMAGE": "ghcr.io/toddysm/custos/connector-sidecar:dev",
    },
}


def _find(
    docs: list[dict[str, Any]], kind: str, name: str
) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _container_env(
    rendered: dict[str, list[dict[str, Any]]], name: str
) -> dict[str, dict[str, Any]]:
    dep = _find(rendered[PROFILE], "Deployment", name)
    assert dep is not None, f"{name} Deployment missing in {PROFILE}"
    container = dep["spec"]["template"]["spec"]["containers"][0]
    return {entry["name"]: entry for entry in (container.get("env") or [])}


@pytest.mark.parametrize("name,env_vars", sorted(DSN_ENV.items()))
def test_dsn_env_sourced_from_cnpg_secret(
    rendered: dict[str, list[dict[str, Any]]],
    name: str,
    env_vars: tuple[str, ...],
) -> None:
    env = _container_env(rendered, name)
    for var in env_vars:
        assert var in env, f"{name} missing DSN env {var} in {PROFILE}"
        ref = env[var].get("valueFrom", {}).get("secretKeyRef", {})
        assert ref.get("name") == "custos-app", (
            f"{name}/{var} must source from the custos-app secret"
        )
        assert ref.get("key") == "uri", (
            f"{name}/{var} must read the 'uri' key of custos-app"
        )


@pytest.mark.parametrize("name,values", sorted(LITERAL_ENV.items()))
def test_literal_config_env_values(
    rendered: dict[str, list[dict[str, Any]]],
    name: str,
    values: dict[str, str],
) -> None:
    env = _container_env(rendered, name)
    for var, expected in values.items():
        assert var in env, f"{name} missing config env {var} in {PROFILE}"
        assert env[var].get("value") == expected, (
            f"{name}/{var} expected {expected!r}, got {env[var].get('value')!r}"
        )
