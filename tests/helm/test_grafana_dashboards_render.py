"""Render-time assertions for the Grafana dashboard bundle (DEPLOY-IMPL-020).

These tests shell out to ``helm template`` (via the session ``rendered``
fixture in ``conftest.py``) and assert the dashboard ConfigMap contract from
``design/architecture/reference-deployment.md`` § Observability and design
TODO-005:

- Three ConfigMaps render in every profile: a per-component overview, an
  audit-event dashboard, and a drainer-lag dashboard.
- Each carries the Grafana sidecar discovery label ``grafana_dashboard: "1"``
  and the ``grafana_folder`` annotation so an operator-supplied Grafana imports
  them automatically.
- Each ConfigMap's single data key holds valid Grafana dashboard JSON with a
  stable ``uid`` and at least one panel.
- The drainer-lag dashboard queries the ``custos_obs_audit_outbox_lag_rows``
  gauge that backs the ``audit-drain-lagging`` alert rule.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

HA_PROFILES = ("connected-ha", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
ALL_PROFILES = HA_PROFILES + EVAL_PROFILES

# ConfigMap name -> (expected dashboard uid, data filename).
DASHBOARDS = {
    "custos-dashboard-custos-components": (
        "custos-components",
        "custos-components.json",
    ),
    "custos-dashboard-custos-audit-events": (
        "custos-audit-events",
        "custos-audit-events.json",
    ),
    "custos-dashboard-custos-drainer-lag": (
        "custos-drainer-lag",
        "custos-drainer-lag.json",
    ),
}


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


@pytest.mark.parametrize("profile", ALL_PROFILES)
@pytest.mark.parametrize("cm_name", sorted(DASHBOARDS))
def test_dashboard_configmap_renders_with_sidecar_label(
    rendered: dict[str, list[dict[str, Any]]], profile: str, cm_name: str
) -> None:
    cm = _find(rendered[profile], "ConfigMap", cm_name)
    assert cm is not None, f"{cm_name} missing in {profile}"

    labels = cm["metadata"]["labels"]
    assert labels.get("grafana_dashboard") == "1", (
        f"{cm_name} must carry the sidecar discovery label in {profile}"
    )
    annotations = cm["metadata"].get("annotations", {})
    assert annotations.get("grafana_folder") == "Custos"


@pytest.mark.parametrize("profile", ALL_PROFILES)
@pytest.mark.parametrize("cm_name", sorted(DASHBOARDS))
def test_dashboard_json_is_valid(
    rendered: dict[str, list[dict[str, Any]]], profile: str, cm_name: str
) -> None:
    expected_uid, filename = DASHBOARDS[cm_name]
    cm = _find(rendered[profile], "ConfigMap", cm_name)
    assert cm is not None, f"{cm_name} missing in {profile}"

    assert filename in cm["data"], f"{cm_name} must expose {filename}"
    dashboard = json.loads(cm["data"][filename])
    assert dashboard["uid"] == expected_uid
    assert dashboard["title"].startswith("Custos")
    assert len(dashboard["panels"]) >= 1


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_drainer_lag_dashboard_queries_outbox_gauge(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    cm = _find(rendered[profile], "ConfigMap", "custos-dashboard-custos-drainer-lag")
    assert cm is not None
    body = cm["data"]["custos-drainer-lag.json"]
    assert "custos_obs_audit_outbox_lag_rows" in body


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_dashboards_can_be_disabled(profile: str) -> None:
    """When ``observability.grafanaDashboards.enabled=false`` no ConfigMap renders."""
    import subprocess
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    umbrella = repo_root / "deploy" / "helm" / "custos"
    result = subprocess.run(
        [
            "helm",
            "template",
            "custos",
            str(umbrella),
            "-f",
            str(umbrella / f"values-{profile}.yaml"),
            "--set",
            "observability.grafanaDashboards.enabled=false",
            "--show-only",
            "templates/grafana-dashboards.yaml",
        ],
        capture_output=True,
        text=True,
    )
    # helm exits non-zero with "could not find template ... in chart" when the
    # template renders empty, which is the expected "disabled" outcome.
    combined = result.stdout + result.stderr
    assert "kind: ConfigMap" not in result.stdout, (
        f"dashboards should not render when disabled in {profile}: {combined}"
    )
