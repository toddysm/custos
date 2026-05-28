"""Render-time assertions for the workflow-service subchart (WF-IMPL-014).

These tests shell out to ``helm template`` against the umbrella chart and
walk the parsed manifests. They assert the wiring contract documented in
``deploy/helm/charts/workflow-service/README.md``:

- The Deployment for ``workflow-service`` pulls a ConfigMap carrying the
  documented ``WF_*`` defaults from ``design.md`` § Configuration.
- The ConfigMap projects every non-secret ``WF_*`` env var with the
  design-documented default (or the in-cluster sibling-Service endpoint
  default where the design table marks the value Required).
- ``WF_*`` env vars never appear in a Secret in v1 — the ExternalSecret
  block is a forward-compatible stub and stays disabled in every profile.
- The Deployment image points at the chart's ``workflow-service`` repository
  (no accidental override pointing at the umbrella image registry root).
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
    cm = _find(rendered[profile], "ConfigMap", "custos-workflow-service")
    assert cm is not None, f"workflow-service ConfigMap missing in {profile}"
    data = cm["data"]
    # design.md § Configuration — Dapr Workflow + Pub/Sub.
    assert data["WF_DAPR_WORKFLOW_COMPONENT"] == "workflow-dapr"
    assert data["WF_PUBLISH_TOPIC"] == "custos.workflow.events"
    # In-cluster sibling-service endpoint defaults must match the Service
    # names emitted by the corresponding subcharts.
    assert data["WF_ARM_ENDPOINT"] == "http://activity-runtime-manager:8080"
    assert data["WF_TS_ENDPOINT"] == "http://trigger-service:8080"
    assert data["WF_CONNECTOR_ENDPOINT"] == "http://connector-service:8080"
    assert data["WF_CATALOG_ENDPOINT"] == "http://catalog-service:8080"
    # Retention + idempotency windows (defaults documented in design.md).
    assert data["WF_RUN_HISTORY_RETENTION"] == "90d"
    assert data["WF_RESUME_SUB_DEFAULT_TTL"] == "PT24H"
    assert data["WF_REGISTER_SUB_MAX_RETRIES"] == "5"
    assert data["WF_EXPR_TIMEOUT_MS"] == "100"
    assert data["WF_IDEMPOTENCY_KEY_TTL"] == "PT24H"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_envfrom_includes_configmap(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None, f"workflow-service Deployment missing in {profile}"
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    cm_refs = [src for src in sources if "configMapRef" in src]
    assert any(
        ref["configMapRef"]["name"] == "custos-workflow-service" for ref in cm_refs
    ), f"ConfigMap envFrom missing for workflow-service in {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_profile_projects_workflow_secret_today(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """v1 has no secret WF_* env vars — no profile should project a Secret
    for the workflow-service. The ExternalSecret block is a forward-
    compatible stub and must stay disabled across the shipped profiles.
    """
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    secret_refs = [src for src in sources if "secretRef" in src]
    workflow_secret_refs = [
        ref for ref in secret_refs
        if ref["secretRef"]["name"] == "custos-workflow-service"
    ]
    assert not workflow_secret_refs, (
        f"{profile}: workflow-service must not project a Secret envFrom in v1 — "
        "the ExternalSecret block is intentionally a disabled stub."
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_profile_emits_workflow_externalsecret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    es = _find(rendered[profile], "ExternalSecret", "custos-workflow-service")
    assert es is None, (
        f"{profile}: workflow-service ExternalSecret should be disabled in v1 — "
        "no secret WF_* env vars are defined yet."
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_image_points_at_workflow_service(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    image = container["image"]
    assert "workflow-service" in image, (
        f"{profile}: workflow-service image was {image!r}, expected the chart "
        "to point at <registry>/workflow-service or an override of the same name"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_probes_hit_healthz_and_readyz(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The liveness/readiness probes are the deploy-time contract for the
    FastAPI surface that lands in WF-IMPL-015. Lock the probe paths now so
    the application skeleton can be wired in confidently.
    """
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    liveness = container["livenessProbe"]["httpGet"]
    readiness = container["readinessProbe"]["httpGet"]
    assert liveness["path"] == "/healthz"
    assert readiness["path"] == "/readyz"
    # Both probes hit the named "http" port wired to the Service port.
    assert liveness["port"] == "http"
    assert readiness["port"] == "http"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_service_renders_with_expected_port(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    svc = _find(rendered[profile], "Service", "custos-workflow-service")
    assert svc is not None, f"workflow-service Service missing in {profile}"
    ports = svc["spec"]["ports"]
    assert any(
        p.get("port") == 8080 and p.get("name") == "http" and p.get("targetPort") == "http"
        for p in ports
    ), f"{profile}: workflow-service Service must expose name=http port=8080"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_dapr_sidecar_annotations_present(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The Step Coordinator and Pub/Sub publication path both require Dapr;
    the umbrella default enables the Dapr sidecar via subchart values.
    """
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None
    annotations = dep["spec"]["template"]["metadata"].get("annotations") or {}
    assert annotations.get("dapr.io/enabled") == "true"
    assert annotations.get("dapr.io/app-id") == "workflow-service"
    assert annotations.get("dapr.io/app-port") == "8080"


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_profiles_inherit_ha_replica_count(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """HA umbrella profiles set ``global.replicaCount: 3``; the subchart
    inherits that by leaving ``replicaCount: ""`` in its own values.yaml.
    """
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None
    assert dep["spec"]["replicas"] == 3, (
        f"{profile}: expected workflow-service to inherit global.replicaCount=3"
    )


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_profiles_inherit_single_replica(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Eval umbrella profiles set ``global.replicaCount: 1``; the subchart
    inherits that.
    """
    dep = _find(rendered[profile], "Deployment", "custos-workflow-service")
    assert dep is not None
    assert dep["spec"]["replicas"] == 1, (
        f"{profile}: expected workflow-service to inherit global.replicaCount=1"
    )
