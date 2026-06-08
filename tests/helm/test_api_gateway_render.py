"""Render-time assertions for the api-gateway subchart (AGW-IMPL-019).

These tests shell out to ``helm template`` against the umbrella chart and
walk the parsed manifests. They assert the wiring contract documented in
``deploy/helm/charts/api-gateway/README.md``:

- The Deployment for ``api-gateway`` pulls a ConfigMap carrying the documented
  ``CUSTOS_GATEWAY_*`` defaults from ``design.md`` § Configuration, including
  the TLS cert/key Dapr secret references and the CORS allow-list (JSON array).
- No ``CUSTOS_GATEWAY_*`` variable carries a secret value, so the ExternalSecret
  block stays a disabled stub and no profile projects a gateway Secret envFrom.
- The Deployment image points at the chart's ``api-gateway`` repository.
- The Dapr sidecar annotations (``dapr.io/app-id: api-gateway``) +
  ``/healthz`` / ``/readyz`` probe contract are locked.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

HA_PROFILES = ("connected-ha", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
ALL_PROFILES = HA_PROFILES + EVAL_PROFILES


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_configmap_has_documented_defaults(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    cm = _find(rendered[profile], "ConfigMap", "custos-api-gateway")
    assert cm is not None, f"api-gateway ConfigMap missing in {profile}"
    data = cm["data"]
    # design.md § Configuration defaults.
    assert data["CUSTOS_GATEWAY_LISTEN_ADDR"] == ":8443"
    assert data["CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT"] == "1048576"
    assert data["CUSTOS_GATEWAY_BODY_MAX_BYTES_PUBLISH"] == "5242880"
    assert data["CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_RPS"] == "20"
    assert data["CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_BURST"] == "40"
    assert data["CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_RPS"] == "200"
    assert data["CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_BURST"] == "400"
    assert data["CUSTOS_GATEWAY_IDEMPOTENCY_TTL"] == "24h"
    assert data["CUSTOS_GATEWAY_DEVICE_CODE_TTL"] == "15m"
    assert data["CUSTOS_GATEWAY_DEVICE_CODE_POLL_INTERVAL"] == "5s"
    # Device-code flow ships disabled in M1 (REQ-035 API tokens only).
    assert data["CUSTOS_GATEWAY_OIDC_DEFAULT_ISSUER"] == ""


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_configmap_carries_tls_secret_refs(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The TLS cert/key refs are Dapr secret *references* (lookup keys), not the
    cert material, so they ship as non-secret ConfigMap env."""
    cm = _find(rendered[profile], "ConfigMap", "custos-api-gateway")
    assert cm is not None
    data = cm["data"]
    assert data["CUSTOS_GATEWAY_TLS_CERT_REF"], (
        f"{profile}: api-gateway must wire a TLS cert secret reference"
    )
    assert data["CUSTOS_GATEWAY_TLS_KEY_REF"], (
        f"{profile}: api-gateway must wire a TLS key secret reference"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_configmap_cors_origins_is_json_array(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """CORS origins render as a JSON array of strings with no wildcard."""
    cm = _find(rendered[profile], "ConfigMap", "custos-api-gateway")
    assert cm is not None
    raw = cm["data"]["CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS"]
    parsed = json.loads(raw)
    assert isinstance(parsed, list) and parsed, (
        f"{profile}: CORS origins must be a non-empty JSON array (got {raw!r})"
    )
    assert all(isinstance(origin, str) for origin in parsed)
    assert "*" not in parsed, f"{profile}: CORS allow-list must not contain a wildcard"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_envfrom_includes_configmap(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None, f"api-gateway Deployment missing in {profile}"
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    cm_refs = [src for src in sources if "configMapRef" in src]
    assert any(
        ref["configMapRef"]["name"] == "custos-api-gateway" for ref in cm_refs
    ), f"ConfigMap envFrom missing for api-gateway in {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_profile_projects_gateway_secret_today(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """No CUSTOS_GATEWAY_* variable carries a secret value (the TLS refs are
    lookup keys), so the ExternalSecret block stays a disabled stub and no
    profile projects an api-gateway Secret envFrom."""
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    secret_refs = [src for src in sources if "secretRef" in src]
    gateway_secret_refs = [
        ref for ref in secret_refs if ref["secretRef"]["name"] == "custos-api-gateway"
    ]
    assert not gateway_secret_refs, (
        f"{profile}: api-gateway must not project a Secret envFrom while the "
        "ExternalSecret block stays a disabled stub."
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_profile_emits_gateway_externalsecret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    es = _find(rendered[profile], "ExternalSecret", "custos-api-gateway")
    assert es is None, (
        f"{profile}: api-gateway ExternalSecret should be disabled in the "
        "shipped profiles."
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_image_points_at_api_gateway(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    image = container["image"]
    assert "api-gateway" in image, (
        f"{profile}: api-gateway image was {image!r}, expected the chart to "
        "point at <registry>/api-gateway or an override of the same name"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_probes_hit_healthz_and_readyz(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    liveness = container["livenessProbe"]["httpGet"]
    readiness = container["readinessProbe"]["httpGet"]
    assert liveness["path"] == "/healthz"
    assert readiness["path"] == "/readyz"
    assert liveness["port"] == "http"
    assert readiness["port"] == "http"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_has_startup_probe_for_resilient_startup(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """A startupProbe must cover the cold-start HTTP-serving window.

    The startupProbe hits the flat ``/healthz`` liveness endpoint, so it gates
    the ``livenessProbe`` until uvicorn is serving HTTP and keeps a slow cold
    start from crash-looping the pod. (Readiness convergence — the background
    startup permission cross-check, issue #815 — is surfaced separately via
    ``/readyz`` and is not bounded by this probe.) The probe must hit
    ``/healthz`` with a failure budget large enough to cover a realistic
    cold-start window.
    """
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    startup = container.get("startupProbe")
    assert startup is not None, f"{profile}: api-gateway must define a startupProbe"
    assert startup["httpGet"]["path"] == "/healthz"
    assert startup["httpGet"]["port"] == "http"
    budget = startup["periodSeconds"] * startup["failureThreshold"]
    assert budget >= 60, (
        f"{profile}: startupProbe budget {budget}s is too small to cover the "
        "cold-start HTTP-serving window"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_service_renders_with_expected_port(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    svc = _find(rendered[profile], "Service", "custos-api-gateway")
    assert svc is not None, f"api-gateway Service missing in {profile}"
    ports = svc["spec"]["ports"]
    assert any(
        p.get("port") == 8080
        and p.get("name") == "http"
        and p.get("targetPort") == "http"
        for p in ports
    ), f"{profile}: api-gateway Service must expose name=http port=8080"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_dapr_sidecar_annotations_present(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    annotations = dep["spec"]["template"]["metadata"].get("annotations") or {}
    assert annotations.get("dapr.io/enabled") == "true"
    assert annotations.get("dapr.io/app-id") == "api-gateway"
    assert annotations.get("dapr.io/app-port") == "8080"


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_profiles_inherit_ha_replica_count(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    assert "replicas" not in dep["spec"], (
        f"{profile}: api-gateway Deployment must not set static replicas "
        "when the HPA is active"
    )
    hpa = _find(
        rendered[profile], "HorizontalPodAutoscaler", "custos-api-gateway"
    )
    assert hpa is not None
    assert hpa["spec"]["minReplicas"] == 3, (
        f"{profile}: expected api-gateway HPA floor of 3"
    )


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_profiles_inherit_single_replica(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-api-gateway")
    assert dep is not None
    assert dep["spec"]["replicas"] == 1, (
        f"{profile}: expected api-gateway to inherit global.replicaCount=1"
    )
