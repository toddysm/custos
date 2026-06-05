"""Render-time assertions for the trigger-service subchart (TS-IMPL-002).

These tests shell out to ``helm template`` against the umbrella chart and
walk the parsed manifests. They assert the wiring contract documented in
``deploy/helm/charts/trigger-service/README.md``:

- The Deployment for ``trigger-service`` pulls a ConfigMap carrying the
  documented ``TRIGGER_*`` defaults from ``design.md`` § Configuration plus
  the Dapr Pub/Sub component + topic refs and the sibling-service endpoints.
- The single secret ``TRIGGER_*`` env var (the metadata-store DSN) flows
  through the ExternalSecret, which is a disabled stub in every shipped
  profile.
- The Deployment image points at the chart's ``trigger-service`` repository.
- The Dapr sidecar annotations + ``/healthz`` / ``/readyz`` probe contract
  are locked so the FastAPI skeleton (TS-IMPL-003) can be wired confidently.
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
    cm = _find(rendered[profile], "ConfigMap", "custos-trigger-service")
    assert cm is not None, f"trigger-service ConfigMap missing in {profile}"
    data = cm["data"]
    # design.md § Configuration defaults.
    assert data["TRIGGER_WEBHOOK_BASE_URL"] == "http://trigger-service:8080"
    assert data["TRIGGER_DEDUP_TTL_SECONDS"] == "86400"
    assert data["TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS"] == "60"
    assert data["TRIGGER_RESUME_DEFAULT_TTL_SECONDS"] == "604800"
    assert data["TRIGGER_DISPATCH_MAX_RETRIES"] == "5"
    assert data["TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS"] == "30"
    # Dapr Pub/Sub component + topic refs.
    assert data["TRIGGER_PUBSUB_COMPONENT"] == "custos-pubsub"
    assert data["TRIGGER_NORMALIZED_TOPIC"] == "custos.triggers.normalized"
    assert data["TRIGGER_WORKFLOW_EVENTS_TOPIC"] == "custos.workflow.events"
    # In-cluster sibling-service endpoint defaults must match the Service
    # names emitted by the corresponding subcharts.
    assert data["TRIGGER_WF_ENDPOINT"] == "http://workflow-service:8080"
    assert data["TRIGGER_CONNECTOR_ENDPOINT"] == "http://connector-service:8080"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_envfrom_includes_configmap(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None, f"trigger-service Deployment missing in {profile}"
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    cm_refs = [src for src in sources if "configMapRef" in src]
    assert any(
        ref["configMapRef"]["name"] == "custos-trigger-service" for ref in cm_refs
    ), f"ConfigMap envFrom missing for trigger-service in {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_profile_projects_trigger_secret_today(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The metadata-store DSN ships behind a disabled ExternalSecret stub in
    every shipped profile (eval synthesises the DSN from the CNPG app
    secret; HA profiles opt in out-of-band). No profile should project a
    trigger-service Secret envFrom yet.
    """
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    sources = container.get("envFrom") or []
    secret_refs = [src for src in sources if "secretRef" in src]
    trigger_secret_refs = [
        ref
        for ref in secret_refs
        if ref["secretRef"]["name"] == "custos-trigger-service"
    ]
    assert not trigger_secret_refs, (
        f"{profile}: trigger-service must not project a Secret envFrom while "
        "the ExternalSecret block stays a disabled stub."
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_profile_emits_trigger_externalsecret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    es = _find(rendered[profile], "ExternalSecret", "custos-trigger-service")
    assert es is None, (
        f"{profile}: trigger-service ExternalSecret should be disabled in the "
        "shipped profiles."
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_image_points_at_trigger_service(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    image = container["image"]
    assert "trigger-service" in image, (
        f"{profile}: trigger-service image was {image!r}, expected the chart "
        "to point at <registry>/trigger-service or an override of the same name"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_deployment_probes_hit_healthz_and_readyz(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The liveness/readiness probes are the deploy-time contract for the
    FastAPI surface that lands in TS-IMPL-003. Lock the probe paths now so
    the application skeleton can be wired in confidently.
    """
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None
    container = dep["spec"]["template"]["spec"]["containers"][0]
    liveness = container["livenessProbe"]["httpGet"]
    readiness = container["readinessProbe"]["httpGet"]
    assert liveness["path"] == "/healthz"
    assert readiness["path"] == "/readyz"
    assert liveness["port"] == "http"
    assert readiness["port"] == "http"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_service_renders_with_expected_port(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    svc = _find(rendered[profile], "Service", "custos-trigger-service")
    assert svc is not None, f"trigger-service Service missing in {profile}"
    ports = svc["spec"]["ports"]
    assert any(
        p.get("port") == 8080 and p.get("name") == "http" and p.get("targetPort") == "http"
        for p in ports
    ), f"{profile}: trigger-service Service must expose name=http port=8080"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_dapr_sidecar_annotations_present(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The Internal Event Receiver (Dapr Pub/Sub subscription) and the
    StartRun / RaiseExternalEvent dispatch path both require the Dapr
    sidecar; the umbrella default enables it via subchart values.
    """
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None
    annotations = dep["spec"]["template"]["metadata"].get("annotations") or {}
    assert annotations.get("dapr.io/enabled") == "true"
    assert annotations.get("dapr.io/app-id") == "trigger-service"
    assert annotations.get("dapr.io/app-port") == "8080"


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_profiles_inherit_ha_replica_count(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """HA umbrella profiles set ``global.replicaCount: 3``; the subchart
    inherits that by leaving ``replicaCount: ""`` in its own values.yaml.
    """
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None
    assert dep["spec"]["replicas"] == 3, (
        f"{profile}: expected trigger-service to inherit global.replicaCount=3"
    )


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_profiles_inherit_single_replica(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Eval umbrella profiles set ``global.replicaCount: 1``; the subchart
    inherits that.
    """
    dep = _find(rendered[profile], "Deployment", "custos-trigger-service")
    assert dep is not None
    assert dep["spec"]["replicas"] == 1, (
        f"{profile}: expected trigger-service to inherit global.replicaCount=1"
    )
