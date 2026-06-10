"""Render-time assertions for the Dapr injector readiness gate (issue #847).

Dapr is installed out-of-band into ``dapr-system`` before the umbrella, so on a
cold start the sidecar-injector webhook may not yet be serving when the service
pods are created. Because the webhook runs ``failurePolicy: Ignore`` those pods
come up with no sidecar (``1/1`` not ``2/2``) and ``helm install --wait`` times
out. The umbrella ships a ``pre-install``/``pre-upgrade`` hook Job that blocks
until the injector Deployment is Ready, so the Dapr-enabled service Deployments
that follow are injected on first creation.

These tests assert the hook Job, its ServiceAccount, and the cross-namespace
RBAC into the Dapr control-plane namespace render correctly (and that the gate is
toggleable / mirrored for air-gapped).
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
GATE_NAME = "custos-dapr-injector-wait"
DAPR_NAMESPACE = "dapr-system"


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _render_with(profile: str, *sets: str) -> list[dict[str, Any]]:
    """Render one profile with extra ``--set`` overrides."""
    subprocess.run(
        ["helm", "dependency", "build", str(UMBRELLA)],
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
def test_injector_gate_resources_rendered_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Each profile renders the gate Job, SA, Role, and RoleBinding by default."""
    docs = rendered[profile]
    for kind in ("Job", "ServiceAccount", "Role", "RoleBinding"):
        assert _find(docs, kind, GATE_NAME) is not None, (
            f"{profile}: {kind}/{GATE_NAME} missing"
        )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_injector_gate_job_is_pre_install_hook_before_migrate(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The gate must run as a pre-install/upgrade hook, ahead of the migrate hook.

    The injector has to be Ready *before* the service pods are created, so the
    gate Job runs at a negative hook-weight (migrate runs at weight 0).
    """
    job = _find(rendered[profile], "Job", GATE_NAME)
    assert job is not None
    ann = job["metadata"]["annotations"]
    assert ann["helm.sh/hook"] == "pre-install,pre-upgrade"
    assert int(ann["helm.sh/hook-weight"]) < 0


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_injector_gate_rbac_scoped_to_dapr_namespace(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """RBAC is least-privilege read of deployments in the Dapr control-plane ns."""
    docs = rendered[profile]
    role = _find(docs, "Role", GATE_NAME)
    binding = _find(docs, "RoleBinding", GATE_NAME)
    assert role is not None and binding is not None
    assert role["metadata"]["namespace"] == DAPR_NAMESPACE
    assert binding["metadata"]["namespace"] == DAPR_NAMESPACE
    rules = role["rules"]
    assert rules == [
        {
            "apiGroups": ["apps"],
            "resources": ["deployments"],
            "verbs": ["get", "list", "watch"],
        }
    ]
    # The binding must target the gate ServiceAccount.
    subjects = binding["subjects"]
    assert any(
        s["kind"] == "ServiceAccount" and s["name"] == GATE_NAME for s in subjects
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_injector_gate_job_waits_on_injector_deployment(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The Job command must roll-status the injector Deployment in dapr-system.

    The kubectl image is distroless (no shell), so the command is the kubectl
    entrypoint plus args rather than a ``/bin/sh -c`` script.
    """
    job = _find(rendered[profile], "Job", GATE_NAME)
    assert job is not None
    container = job["spec"]["template"]["spec"]["containers"][0]
    argv = " ".join(container.get("command", []) + container.get("args", []))
    assert "rollout" in argv and "status" in argv
    assert "dapr-sidecar-injector" in argv
    assert DAPR_NAMESPACE in argv


@pytest.mark.parametrize("profile", ("airgapped-eval", "airgapped-ha"))
def test_injector_gate_image_mirrored_for_airgapped(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Air-gapped profiles pull the kubectl image from the internal mirror."""
    job = _find(rendered[profile], "Job", GATE_NAME)
    assert job is not None
    image = job["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image.startswith("registry.internal/"), image


def test_injector_gate_can_be_disabled() -> None:
    """Setting ``dapr.injectorReadyGate.enabled=false`` drops every gate resource."""
    docs = _render_with("connected-eval", "dapr.injectorReadyGate.enabled=false")
    for kind in ("Job", "ServiceAccount", "Role", "RoleBinding"):
        assert _find(docs, kind, GATE_NAME) is None, f"{kind}/{GATE_NAME} still rendered"
