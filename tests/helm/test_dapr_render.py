"""Render-time assertions that the Dapr control plane is installed out-of-band.

The umbrella used to vendor the Dapr control-plane subchart. Since #851/#852 the
chart no longer bundles it — the packaged release exceeded Helm's 1 MB release
Secret limit — so Dapr is installed out-of-band by ``scripts/install-prereqs.sh``
before ``helm install custos``. The umbrella still templates the Dapr building-
block CRs (Components/Subscriptions, gated on ``dapr.install`` — covered by
``test_dapr_components_render.py``). These tests pin the new boundary: the
control-plane *workloads* must NOT come from the umbrella, in any profile.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
UMBRELLA = REPO_ROOT / "deploy" / "helm" / "custos"

ALL_PROFILES = ("connected-eval", "connected-ha", "airgapped-eval", "airgapped-ha")

# Dapr control-plane workloads that used to ship in the umbrella and now come
# from the out-of-band prerequisites install.
CONTROL_PLANE_WORKLOADS = (
    "dapr-operator",
    "dapr-sentry",
    "dapr-sidecar-injector",
    "dapr-placement-server",
)

WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet")


def _workload_names(docs: list[dict[str, Any]]) -> set[str]:
    return {d["metadata"]["name"] for d in docs if d.get("kind") in WORKLOAD_KINDS}


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_control_plane_not_bundled(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The umbrella must not bundle the Dapr control-plane workloads."""
    names = _workload_names(rendered[profile])
    for dep in CONTROL_PLANE_WORKLOADS:
        assert dep not in names, (
            f"{dep} is bundled in {profile}; the Dapr control plane must be "
            "installed out-of-band (scripts/install-prereqs.sh)"
        )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_no_dapr_control_plane_workloads(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """No ``dapr-*`` workload should be rendered by the umbrella at all."""
    leaked = sorted(
        n for n in _workload_names(rendered[profile]) if n.startswith("dapr-")
    )
    assert leaked == [], f"Dapr workloads leaked into {profile}: {leaked}"
