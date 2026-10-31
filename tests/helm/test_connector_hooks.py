"""Render the opt-in hook namespace, least-privilege controller and fail-closed configuration."""

from __future__ import annotations

import json
import os
import subprocess
import sys
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


def test_image_matrix_publishes_both_real_helper_platforms() -> None:
    """Execute the actual matrix generator and verify platform, target and offline inclusion."""
    root = CHART.parents[3]
    workflow = yaml.safe_load((root / ".github/workflows/build-images.yml").read_text())
    step = next(
        row
        for row in workflow["jobs"]["setup"]["steps"]
        if row.get("name") == "Build matrix"
    )
    source = step["run"].split("<<'PY'", 1)[1].split("\n", 1)[1].rsplit("\nPY", 1)[0]
    result = subprocess.run(
        [sys.executable, "-", "false", "false"],
        input=source,
        env={**os.environ, "CHANGED_JSON": json.dumps({"connector-service": "true"})},
        capture_output=True,
        text=True,
        check=True,
    )
    output = dict(line.split("=", 1) for line in result.stdout.splitlines())
    entries = json.loads(output["matrix"])["include"]
    assert len(entries) == 3
    inventory = (root / "deploy/offline/images.txt").read_text().splitlines()
    for architecture in ("amd64", "arm64"):
        helper = next(
            row
            for row in entries
            if row["key"] == f"connector-hook-bridge-{architecture}"
        )
        assert (
            helper["platform"] == f"linux/{architecture}"
            and helper["target"] == "hook-runtime"
        )
        assert f"ghcr.io/toddysm/{helper['image']}:dev" in inventory
    build = next(
        row for row in workflow["jobs"]["build"]["steps"] if row.get("id") == "build"
    )
    assert build["with"]["target"] == "${{ matrix.target || '' }}"
    assert build["with"]["platforms"] == "${{ matrix.platform || 'linux/amd64' }}"
