"""Render-time assertions for the vendored Dapr control-plane subchart
(DEPLOY-IMPL-011).

The umbrella vendors Dapr as a dependency gated by ``dapr.install``. Defaults
must bring the control plane up; ``dapr.install=false`` must skip it entirely.
Images route through the Dapr ``global.registry`` knob, which the air-gapped
profile overlays point at the internal mirror. The HA profiles flip the Dapr
control plane to multi-replica.
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
HA_PROFILES = ("connected-ha", "airgapped-ha")
AIRGAPPED_PROFILES = ("airgapped-eval", "airgapped-ha")

# Dapr control-plane Deployments the chart must stand up.
CONTROL_PLANE_DEPLOYMENTS = ("dapr-operator", "dapr-sentry", "dapr-sidecar-injector")


def _by_kind(docs: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [d for d in docs if d.get("kind") == kind]


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _render_with(profile: str, *sets: str) -> list[dict[str, Any]]:
    """Render one profile with extra ``--set`` overrides."""
    # Populate ./charts/ first so the vendored Dapr dependency exists when this
    # file is run in isolation or before the session fixture has rendered.
    subprocess.run(
        ["helm", "dependency", "update", str(UMBRELLA)],
        check=True,
        capture_output=True,
    )
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
def test_control_plane_installed_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Each profile renders the Dapr control-plane Deployments by default."""
    docs = rendered[profile]
    names = {d["metadata"]["name"] for d in _by_kind(docs, "Deployment")}
    for dep in CONTROL_PLANE_DEPLOYMENTS:
        assert dep in names, f"{dep} missing from {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_install_false_skips_control_plane(profile: str) -> None:
    """``dapr.install=false`` removes every Dapr object."""
    docs = _render_with(profile, "dapr.install=false")
    dapr_objs = [
        d
        for d in docs
        if str(d.get("metadata", {}).get("name", "")).startswith("dapr-")
    ]
    assert dapr_objs == [], f"Dapr objects leaked into {profile} with install=false"


def test_default_registry_is_public() -> None:
    """Connected profiles pull Dapr images from the public Dapr registry."""
    operator = _find(_render_with("connected-eval"), "Deployment", "dapr-operator")
    assert operator is not None
    image = operator["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image.startswith("ghcr.io/dapr/"), image
    assert image.endswith(":1.14.0"), image


@pytest.mark.parametrize("profile", AIRGAPPED_PROFILES)
def test_airgapped_registry_mirrored(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Air-gapped profiles redirect Dapr images to the internal mirror."""
    operator = _find(rendered[profile], "Deployment", "dapr-operator")
    assert operator is not None
    image = operator["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image.startswith("registry.internal/dapr/"), image


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_scales_control_plane(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """HA profiles run the Dapr control plane with multiple replicas."""
    for dep in CONTROL_PLANE_DEPLOYMENTS:
        doc = _find(rendered[profile], "Deployment", dep)
        assert doc is not None, f"{dep} missing in {profile}"
        assert doc["spec"]["replicas"] == 3, dep


@pytest.mark.parametrize("profile", ("connected-eval", "airgapped-eval"))
def test_eval_runs_single_control_plane_replica(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Eval profiles keep the Dapr control plane single-replica."""
    for dep in CONTROL_PLANE_DEPLOYMENTS:
        doc = _find(rendered[profile], "Deployment", dep)
        assert doc is not None, f"{dep} missing in {profile}"
        assert doc["spec"]["replicas"] == 1, dep
