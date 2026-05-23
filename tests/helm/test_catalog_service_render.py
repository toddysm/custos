"""Render-time assertions for the catalog-service subchart (CS-IMPL-002).

These tests shell out to ``helm template`` against the umbrella chart and
walk the parsed manifests. They assert the wiring contract documented in
``deploy/helm/charts/catalog-service/README.md``:

- The Deployment for ``catalog-service`` pulls a ConfigMap carrying the
  documented ``CAT_*`` defaults.
- HA profiles additionally pull a Secret materialized by the ExternalSecret.
- The ExternalSecret projects ``CAT_DEFINITION_STORE`` and ``CAT_CATALOG_STORE``.
- ``CAT_CONNECTOR_ENDPOINT`` / ``CAT_AUTHZ_ENDPOINT`` carry their documented
  in-cluster defaults.
- ``CAT_PUBLISH_MAX_BODY_MB`` and ``CAT_CEL_PARSE_TIMEOUT_MS`` carry the
  design-documented defaults (``4`` / ``500``).
"""

from __future__ import annotations

from typing import Any

import pytest

HA_PROFILES = ("connected-ha", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
ALL_PROFILES = HA_PROFILES + EVAL_PROFILES


def _find(
    docs: list[dict[str, Any]], kind: str, name: str
) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_configmap_has_documented_defaults(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    cm = _find(rendered[profile], "ConfigMap", "custos-catalog-service")
    assert cm is not None, f"catalog-service ConfigMap missing in {profile}"
    data = cm["data"]
    assert data["CAT_PUBLISH_MAX_BODY_MB"] == "4"
    assert data["CAT_CEL_PARSE_TIMEOUT_MS"] == "500"
    assert data["CAT_CONNECTOR_ENDPOINT"] == "http://connector-service:8080"
    assert data["CAT_AUTHZ_ENDPOINT"] == "http://auth-service:8080"
    # DSN env vars MUST flow through the Secret, never the ConfigMap.
    assert "CAT_DEFINITION_STORE" not in data
    assert "CAT_CATALOG_STORE" not in data


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_envfrom_includes_configmap(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-catalog-service")
    assert dep is not None, f"catalog-service Deployment missing in {profile}"
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    cm_refs = [src for src in sources if "configMapRef" in src]
    assert any(
        ref["configMapRef"]["name"] == "custos-catalog-service" for ref in cm_refs
    ), f"ConfigMap envFrom missing for catalog-service in {profile}"


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_deployment_envfrom_includes_secret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-catalog-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    secret_refs = [src for src in sources if "secretRef" in src]
    assert any(
        ref["secretRef"]["name"] == "custos-catalog-service" for ref in secret_refs
    ), f"Secret envFrom missing for catalog-service in {profile}"


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_deployment_omits_secret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-catalog-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    secret_refs = [src for src in sources if "secretRef" in src]
    catalog_secret_refs = [
        ref for ref in secret_refs if ref["secretRef"]["name"] == "custos-catalog-service"
    ]
    assert not catalog_secret_refs, (
        f"eval profile {profile} should not project the catalog-service Secret "
        "(externalSecret is disabled by default)"
    )


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_externalsecret_projects_both_dsns(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    es = _find(rendered[profile], "ExternalSecret", "custos-catalog-service")
    assert es is not None, f"ExternalSecret missing in {profile}"
    keys = {entry["secretKey"] for entry in es["spec"]["data"]}
    assert "CAT_DEFINITION_STORE" in keys
    assert "CAT_CATALOG_STORE" in keys


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_profiles_emit_no_externalsecret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    es = _find(rendered[profile], "ExternalSecret", "custos-catalog-service")
    assert es is None, (
        f"{profile}: catalog-service ExternalSecret should be disabled by default"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_image_points_at_catalog_service(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-catalog-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    image = container["image"]
    assert "catalog-service" in image, (
        f"{profile}: catalog-service image was {image!r}, expected the chart "
        "to point at <registry>/catalog-service or an override of the same name"
    )
