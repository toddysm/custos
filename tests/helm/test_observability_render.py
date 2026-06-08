"""Render-time assertions for the observability pipeline (DEPLOY-IMPL-014).

The umbrella vendors Prometheus, the OpenTelemetry Collector, and Loki as
subcharts gated on ``observability.<backend>.enabled``. Loki runs single-binary
with filesystem storage on eval profiles and switches to the MinIO
``custos-loki`` bucket on HA. Air-gapped profiles mirror every image to
``registry.internal``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
UMBRELLA = REPO_ROOT / "deploy" / "helm" / "custos"

ALL_PROFILES = ("connected-eval", "connected-ha", "airgapped-eval", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
HA_PROFILES = ("connected-ha", "airgapped-ha")
AIRGAPPED_PROFILES = ("airgapped-eval", "airgapped-ha")

PROMETHEUS_SERVER = "custos-prometheus-server"
OTEL_COLLECTOR = "custos-opentelemetry-collector"
LOKI = "custos-loki"

# Prometheus bundled components that the chart disables to keep the footprint to
# a single scraping server.
DISABLED_PROM_COMPONENTS = (
    "custos-prometheus-alertmanager",
    "custos-prometheus-pushgateway",
    "custos-prometheus-node-exporter",
    "custos-prometheus-kube-state-metrics",
)


def _by_kind(docs: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [d for d in docs if d.get("kind") == kind]


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _images(docs: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for doc in docs:
        if doc.get("kind") not in ("Deployment", "StatefulSet", "DaemonSet"):
            continue
        spec = doc["spec"]["template"]["spec"]
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            out.add(container["image"])
    return out


def _loki_config(docs: list[dict[str, Any]]) -> dict[str, Any]:
    cm = _find(docs, "ConfigMap", "loki")
    assert cm is not None, "Loki ConfigMap missing"
    return yaml.safe_load(cm["data"]["config.yaml"])


_DEPS_UPDATED = False


def _ensure_dependencies() -> None:
    """Vendor the umbrella subcharts once per test process."""
    global _DEPS_UPDATED
    if _DEPS_UPDATED:
        return
    subprocess.run(
        ["helm", "dependency", "update", str(UMBRELLA)],
        check=True,
        capture_output=True,
    )
    _DEPS_UPDATED = True


def _render_with(profile: str, *sets: str) -> list[dict[str, Any]]:
    """Render one profile with extra ``--set`` overrides."""
    _ensure_dependencies()
    cmd = [
        "helm",
        "template",
        "custos",
        str(UMBRELLA),
        "-f",
        str(UMBRELLA / f"values-{profile}.yaml"),
    ]
    for override in sets:
        cmd += ["--set", override]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc is not None]


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_prometheus_server_renders(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Every profile renders a single Prometheus server."""
    docs = rendered[profile]
    assert _find(docs, "Deployment", PROMETHEUS_SERVER) is not None, (
        f"Prometheus server missing from {profile}"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_prometheus_extras_disabled(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Only the server runs — alertmanager/pushgateway/exporters are off."""
    docs = rendered[profile]
    workloads = {
        d["metadata"]["name"]
        for d in docs
        if d.get("kind") in ("Deployment", "StatefulSet", "DaemonSet")
    }
    for extra in DISABLED_PROM_COMPONENTS:
        assert extra not in workloads, f"{extra} should be disabled in {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_otel_collector_renders(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Every profile renders the OpenTelemetry Collector deployment."""
    docs = rendered[profile]
    assert _find(docs, "Deployment", OTEL_COLLECTOR) is not None, (
        f"OTel Collector missing from {profile}"
    )


def test_prometheus_scrapes_custos_services() -> None:
    """Prometheus carries a pod-discovery job for the Custos /metrics ports."""
    docs = _render_with("connected-eval")
    cm = _find(docs, "ConfigMap", PROMETHEUS_SERVER)
    assert cm is not None
    cfg = yaml.safe_load(cm["data"]["prometheus.yml"])
    job = next(
        (j for j in cfg["scrape_configs"] if j["job_name"] == "custos-services"),
        None,
    )
    assert job is not None, "custos-services scrape job missing"
    assert job["metrics_path"] == "/metrics"
    # Only the app container port named "http" is kept (Dapr / sink ports differ).
    keep = [
        r
        for r in job["relabel_configs"]
        if r.get("action") == "keep"
        and r.get("source_labels") == ["__meta_kubernetes_pod_container_port_name"]
    ]
    assert keep and keep[0]["regex"] == "http"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_loki_single_binary(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Loki runs as a single-binary StatefulSet (no scalable components)."""
    docs = rendered[profile]
    assert _find(docs, "StatefulSet", LOKI) is not None, f"Loki missing from {profile}"
    workloads = {
        d["metadata"]["name"]
        for d in docs
        if d.get("kind") in ("Deployment", "StatefulSet")
    }
    for scalable in ("custos-loki-read", "custos-loki-write", "custos-loki-backend"):
        assert scalable not in workloads, f"{scalable} should not render in {profile}"


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_loki_filesystem_storage_on_eval(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Eval profiles keep Loki on filesystem storage."""
    cfg = _loki_config(rendered[profile])
    assert "filesystem" in cfg["common"]["storage"]
    object_stores = {c["object_store"] for c in cfg["schema_config"]["configs"]}
    assert object_stores == {"filesystem"}


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_loki_minio_storage_on_ha(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """HA profiles switch Loki to the MinIO object-store bucket."""
    cfg = _loki_config(rendered[profile])
    assert "s3" in cfg["common"]["storage"]
    assert cfg["common"]["storage"]["s3"]["endpoint"] == "minio:9000"
    object_stores = {c["object_store"] for c in cfg["schema_config"]["configs"]}
    assert object_stores == {"s3"}


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_loki_minio_credentials_from_secret(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """HA Loki reads MinIO credentials from the custos-minio Secret, not plaintext."""
    docs = rendered[profile]
    sts = _find(docs, "StatefulSet", LOKI)
    assert sts is not None
    env = sts["spec"]["template"]["spec"]["containers"][0].get("env", [])
    secret_refs = {
        e["valueFrom"]["secretKeyRef"]["name"]
        for e in env
        if e.get("valueFrom", {}).get("secretKeyRef")
    }
    assert "custos-minio" in secret_refs
    # No plaintext keys should be embedded in the rendered config.
    cfg = _loki_config(docs)
    s3 = cfg["common"]["storage"]["s3"]
    assert s3["access_key_id"].startswith("${")
    assert s3["secret_access_key"].startswith("${")


def test_connected_uses_public_registries() -> None:
    """Connected profiles pull the observability images from their upstreams."""
    images = _images(_render_with("connected-eval"))
    assert "docker.io/grafana/loki:3.6.7" in images
    assert any(i.startswith("otel/opentelemetry-collector-contrib") for i in images)
    assert any(i.startswith("quay.io/prometheus/prometheus") for i in images)
    # Third-party images are not rewritten under the Custos GHCR namespace.
    assert not any("toddysm/custos/grafana" in i for i in images)
    assert not any("toddysm/custos/otel" in i for i in images)


@pytest.mark.parametrize("profile", AIRGAPPED_PROFILES)
def test_airgapped_mirrors_observability_images(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Air-gapped profiles route every observability image to the mirror."""
    obs_images = {
        i
        for i in _images(rendered[profile])
        if any(k in i for k in ("loki", "prometheus", "opentelemetry", "otel"))
    }
    assert obs_images, f"no observability images found in {profile}"
    for image in obs_images:
        assert image.startswith("registry.internal/"), f"unmirrored image: {image}"


def test_prometheus_toggle_skips_subchart() -> None:
    """``observability.prometheus.enabled=false`` drops Prometheus."""
    docs = _render_with("connected-eval", "observability.prometheus.enabled=false")
    assert _find(docs, "Deployment", PROMETHEUS_SERVER) is None


def test_loki_toggle_skips_subchart() -> None:
    """``observability.loki.enabled=false`` drops Loki."""
    docs = _render_with("connected-eval", "observability.loki.enabled=false")
    assert _find(docs, "StatefulSet", LOKI) is None


def test_otel_toggle_skips_subchart() -> None:
    """``observability.otelCollector.enabled=false`` drops the OTel Collector."""
    docs = _render_with("connected-eval", "observability.otelCollector.enabled=false")
    assert _find(docs, "Deployment", OTEL_COLLECTOR) is None
