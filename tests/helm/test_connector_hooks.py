"""Render the opt-in hook namespace, least-privilege controller and fail-closed configuration."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[2] / "deploy/helm/charts/connector-service"
HELPER = "registry.example/helper@sha256:" + "1" * 64


def render(*overrides: str) -> subprocess.CompletedProcess[str]:
    """Render only the owned subchart, without installing resources into a cluster."""
    args = [
        "helm",
        "template",
        "hook-test",
        str(CHART),
        "--namespace",
        "custos-system",
        "--set",
        "hookRuntime.enabled=true",
        "--set",
        "hookRuntime.namespace=custos-hooks",
        "--set",
        f"hookRuntime.helperImage={HELPER}",
        "--set",
        "hookRuntime.registryUrl=https://registry.example",
    ]
    for override in overrides:
        args.extend(("--set", override))
    return subprocess.run(args, capture_output=True, text=True, check=False)


def test_sandbox_role_and_private_namespace() -> None:
    """Grant only required namespaced operations and keep plugin tokens disabled."""
    result = render()
    assert result.returncode == 0, result.stderr
    rows = [row for row in yaml.safe_load_all(result.stdout) if row]
    role = next(row for row in rows if row["kind"] == "Role")
    assert role["metadata"]["namespace"] == "custos-hooks"
    assert role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["create", "get", "list", "delete", "patch"],
        },
        {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get", "create"]},
        {
            "apiGroups": [""],
            "resources": ["serviceaccounts"],
            "resourceNames": ["connector-hook"],
            "verbs": ["get"],
        },
    ]
    account = next(
        row
        for row in rows
        if row["kind"] == "ServiceAccount"
        and row["metadata"].get("namespace") == "custos-hooks"
    )
    assert account["automountServiceAccountToken"] is False
    policy = next(row for row in rows if row["kind"] == "NetworkPolicy")
    assert policy["spec"]["ingress"] == [] and policy["spec"]["egress"] == []
    assert policy["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    config = next(row for row in rows if row["kind"] == "ConfigMap")["data"]
    assert config["CONN_HOOK_BACKEND"] == "kubernetes"
    assert config["CONN_HOOK_HELPER_IMAGE"] == HELPER


@pytest.mark.parametrize(
    "override",
    [
        "hookRuntime.namespace=custos-system",
        "hookRuntime.helperImage=helper:mutable",
        "hookRuntime.architecture=invalid",
        "hookRuntime.registryUrl=",
    ],
)
def test_invalid_hook_configuration_fails_render(override: str) -> None:
    """Incomplete/unsafe activation fails instead of producing a partly usable backend."""
    assert render(override).returncode != 0


def test_default_off_does_not_create_sandbox_authority() -> None:
    """Profiles keep their current behavior until explicit lifecycle activation."""
    result = render("hookRuntime.enabled=false")
    assert result.returncode == 0, result.stderr
    rows = [row for row in yaml.safe_load_all(result.stdout) if row]
    assert not any(row["kind"] in {"Namespace", "Role", "RoleBinding"} for row in rows)
