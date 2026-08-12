"""Render-time assertions for the bootstrap Job's permission-registry wiring.

The bootstrap Job seeds the platform permission registry at install time. Like
the auth-service pod, it aggregates each component's ``permissions.yaml`` baked
into its image when ``CUSTOS_AUTH_PERMISSIONS_PATHS`` is set (#867 / AS-IMPL-032),
so the post-install seed and the running auth-service load the identical registry
surface. These tests pin that the umbrella chart wires the env var by default and
omits it when ``bootstrap.permissionsPaths`` is cleared (bundled-aggregate
fallback).
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
UMBRELLA = REPO_ROOT / "deploy" / "helm" / "custos"

HA_PROFILES = ("connected-ha", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
ALL_PROFILES = HA_PROFILES + EVAL_PROFILES

_EXPECTED_PATHS = (
    "/opt/custos/permissions/auth-service.yaml:"
    "/opt/custos/permissions/catalog-service.yaml:"
    "/opt/custos/permissions/workflow-service.yaml:"
    "/opt/custos/permissions/trigger-service.yaml:"
    "/opt/custos/permissions/connector-service.yaml:"
    "/opt/custos/permissions/observability-audit-service.yaml"
)


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _bootstrap_env(docs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    job = _find(docs, "Job", "custos-bootstrap")
    assert job is not None, "bootstrap Job missing from rendered manifests"
    container = job["spec"]["template"]["spec"]["containers"][0]
    return {entry["name"]: entry for entry in container.get("env", [])}


def _render(profile: str, *set_args: str) -> list[dict[str, Any]]:
    # Subchart dependencies are vendored once per session by the autouse
    # ``chart_dependencies`` fixture in conftest.py, so this only templates.
    cmd = [
        "helm",
        "template",
        "custos",
        str(UMBRELLA),
        "-f",
        str(UMBRELLA / f"values-{profile}.yaml"),
    ]
    for arg in set_args:
        cmd.extend(["--set", arg])
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc is not None]


def _render_error(profile: str, *set_args: str) -> str:
    cmd = [
        "helm",
        "template",
        "custos",
        str(UMBRELLA),
        "-f",
        str(UMBRELLA / f"values-{profile}.yaml"),
    ]
    for arg in set_args:
        cmd.extend(["--set", arg])
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert result.returncode != 0
    return result.stderr


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_bootstrap_permissions_paths_wired_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    env = _bootstrap_env(rendered[profile])
    assert "CUSTOS_AUTH_PERMISSIONS_PATHS" in env, (
        f"{profile}: bootstrap Job must set CUSTOS_AUTH_PERMISSIONS_PATHS so the "
        "seeder aggregates the per-service permissions.yaml files"
    )
    assert env["CUSTOS_AUTH_PERMISSIONS_PATHS"]["value"] == _EXPECTED_PATHS


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_bootstrap_permissions_paths_omitted_when_empty(profile: str) -> None:
    docs = _render(profile, "bootstrap.permissionsPaths=")
    env = _bootstrap_env(docs)
    assert "CUSTOS_AUTH_PERMISSIONS_PATHS" not in env, (
        f"{profile}: clearing bootstrap.permissionsPaths must omit the env var so "
        "the seeder falls back to the bundled aggregate"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_bootstrap_admin_token_disabled_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    env = _bootstrap_env(rendered[profile])
    assert not any(name.startswith("CUSTOS_BOOTSTRAP_ADMIN_TOKEN") for name in env)
    assert "CUSTOS_BOOTSTRAP_ADMIN_PRINCIPAL_ID" not in env
    assert "CUSTOS_BOOTSTRAP_ADMIN_WORKSPACE_ID" not in env


@pytest.mark.parametrize("profile", ALL_PROFILES)
@pytest.mark.parametrize("mode", ("init", "recover"))
def test_bootstrap_admin_token_secret_wired(profile: str, mode: str) -> None:
    docs = _render(
        profile,
        f"bootstrap.adminToken.mode={mode}",
        "bootstrap.adminToken.secretName=bootstrap-credential",
        "bootstrap.adminToken.secretKey=admin-token",
    )
    env = _bootstrap_env(docs)
    assert env["CUSTOS_BOOTSTRAP_ADMIN_TOKEN_MODE"]["value"] == mode
    assert env["CUSTOS_BOOTSTRAP_ADMIN_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "bootstrap-credential",
        "key": "admin-token",
    }
    assert env["CUSTOS_BOOTSTRAP_ADMIN_PRINCIPAL_ID"]["value"] == (
        "custos-bootstrap-admin"
    )
    assert env["CUSTOS_BOOTSTRAP_ADMIN_WORKSPACE_ID"]["value"] == "workspace-default"
    assert env["CUSTOS_BOOTSTRAP_ADMIN_TOKEN_TTL_SECONDS"]["value"] == "7776000"
    rendered_text = yaml.safe_dump_all(docs)
    assert "custos_plaintext-must-never-render" not in rendered_text
    assert "valueFrom" in rendered_text


def test_bootstrap_admin_values_have_no_plaintext_field() -> None:
    values = yaml.safe_load((UMBRELLA / "values.yaml").read_text())
    admin_token = values["bootstrap"]["adminToken"]
    assert set(admin_token) == {
        "mode",
        "secretName",
        "secretKey",
        "principalId",
        "workspaceId",
        "ttlSeconds",
    }


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_bootstrap_admin_rejects_plaintext_value(profile: str) -> None:
    plaintext = "custos_plaintext-must-never-enter-helm-values"
    error = _render_error(profile, f"bootstrap.adminToken.token={plaintext}")
    assert "additional properties 'token' not allowed" in error
    assert plaintext not in error


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_bootstrap_admin_token_rejects_invalid_mode(profile: str) -> None:
    error = _render_error(profile, "bootstrap.adminToken.mode=replace")
    assert "/bootstrap/adminToken/mode" in error
    assert "value must be one of" in error


@pytest.mark.parametrize("profile", ALL_PROFILES)
@pytest.mark.parametrize("ttl", ("0", "31536001"))
def test_bootstrap_admin_token_rejects_invalid_ttl(profile: str, ttl: str) -> None:
    error = _render_error(profile, f"bootstrap.adminToken.ttlSeconds={ttl}")
    assert "/bootstrap/adminToken/ttlSeconds" in error
    assert ("minimum" if ttl == "0" else "maximum") in error


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_bootstrap_admin_token_requires_secret_reference(profile: str) -> None:
    error = _render_error(profile, "bootstrap.adminToken.mode=init")
    assert "secretName is required" in error
