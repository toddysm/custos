"""Render-time assertions for the observability pipeline.

Since #851/#852 the umbrella no longer vendors Prometheus, the OpenTelemetry
Collector, or Loki — they are installed out-of-band by
``scripts/install-prereqs.sh`` before ``helm install custos`` (the bundled
subcharts pushed the packaged release past Helm's 1 MB release-Secret limit).
What the umbrella *does* own is the Grafana dashboard bundle: each JSON under
``dashboards/`` ships as a ConfigMap carrying the Grafana sidecar discovery
label so an operator-supplied Grafana imports them automatically. These tests
assert the dashboards render and that the observability backends are not
bundled by the umbrella.
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

# Grafana dashboard ConfigMaps the umbrella ships (one per dashboards/*.json).
DASHBOARD_CONFIGMAPS = (
    "custos-dashboard-custos-audit-events",
    "custos-dashboard-custos-components",
    "custos-dashboard-custos-drainer-lag",
)
DASHBOARD_LABEL = "grafana_dashboard"
DASHBOARD_LABEL_VALUE = "1"

# Observability backend workloads that used to ship in the umbrella and now come
# from the out-of-band prerequisites install.
OUT_OF_BAND_WORKLOADS = (
    "custos-prometheus-server",
    "custos-loki",
    "custos-opentelemetry-collector",
)

WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet")


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _workload_names(docs: list[dict[str, Any]]) -> set[str]:
    return {d["metadata"]["name"] for d in docs if d.get("kind") in WORKLOAD_KINDS}


_DEPS_UPDATED = False


def _ensure_dependencies() -> None:
    """Populate ./charts/ once per test process (all deps are local)."""
    global _DEPS_UPDATED
    if _DEPS_UPDATED:
        return
    subprocess.run(
        ["helm", "dependency", "build", str(UMBRELLA)],
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


# --- Grafana dashboards (umbrella-owned) ------------------------------------


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_grafana_dashboards_render(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Every profile ships the Grafana dashboard ConfigMaps."""
    docs = rendered[profile]
    for name in DASHBOARD_CONFIGMAPS:
        assert _find(docs, "ConfigMap", name) is not None, (
            f"{name} missing from {profile}"
        )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_grafana_dashboards_carry_sidecar_label(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Dashboards carry the Grafana sidecar discovery label for auto-import."""
    docs = rendered[profile]
    for name in DASHBOARD_CONFIGMAPS:
        cm = _find(docs, "ConfigMap", name)
        assert cm is not None
        labels = cm["metadata"].get("labels", {})
        assert labels.get(DASHBOARD_LABEL) == DASHBOARD_LABEL_VALUE, (
            f"{name} missing sidecar label in {profile}"
        )


def test_grafana_dashboards_toggle_off() -> None:
    """``observability.grafanaDashboards.enabled=false`` drops the ConfigMaps."""
    docs = _render_with(
        "connected-eval", "observability.grafanaDashboards.enabled=false"
    )
    for name in DASHBOARD_CONFIGMAPS:
        assert _find(docs, "ConfigMap", name) is None


# --- Observability backends are not bundled ---------------------------------


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_observability_backends_not_bundled(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Prometheus, Loki, and the OTel Collector must not be bundled."""
    names = _workload_names(rendered[profile])
    for workload in OUT_OF_BAND_WORKLOADS:
        assert workload not in names, (
            f"{workload} is bundled in {profile}; observability backends must be "
            "installed out-of-band (scripts/install-prereqs.sh)"
        )
