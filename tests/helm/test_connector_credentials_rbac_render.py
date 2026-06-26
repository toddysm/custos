"""Render-time assertions for the connector-credential RBAC (CONN-DAPRSEC-02).

The umbrella renders a dedicated credential namespace plus a namespace-scoped
``get secrets`` Role/RoleBinding so operators can add connector credentials
(registry PATs read via the ``x-dapr-secret`` resolver) to a *running* platform
without a ``helm upgrade``. The Role intentionally carries **no**
``resourceNames`` in the default (dynamic) mode; a strict per-secret mode is
available via ``connectorCredentials.strictSecretNames``. The whole bundle is
gated on the Dapr secret store being Kubernetes-backed.
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
RBAC_NAME = "custos-connector-credential-reader"
CREDENTIAL_NS = "custos-connectors"
CONNECTOR_SA = "custos-connector-service"


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _render_with(profile: str, *sets: str) -> list[dict[str, Any]]:
    """Render one profile with extra ``--set`` overrides."""
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
def test_credential_namespace_and_rbac_rendered_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    docs = rendered[profile]

    ns = _find(docs, "Namespace", CREDENTIAL_NS)
    assert ns is not None, f"credential namespace missing in {profile}"

    role = _find(docs, "Role", RBAC_NAME)
    assert role is not None, f"credential Role missing in {profile}"
    assert role["metadata"]["namespace"] == CREDENTIAL_NS
    rule = role["rules"][0]
    assert rule["resources"] == ["secrets"]
    assert rule["verbs"] == ["get"]
    # Dynamic mode: no resourceNames, so new credential Secrets created later in
    # the namespace are readable without a redeploy.
    assert "resourceNames" not in rule

    binding = _find(docs, "RoleBinding", RBAC_NAME)
    assert binding is not None, f"credential RoleBinding missing in {profile}"
    assert binding["roleRef"]["name"] == RBAC_NAME
    subject = binding["subjects"][0]
    assert subject["kind"] == "ServiceAccount"
    assert subject["name"] == CONNECTOR_SA


def test_strict_mode_scopes_role_to_resource_names() -> None:
    docs = _render_with(
        "connected-eval",
        "connectorCredentials.strictSecretNames={dockerhub-pat,ghcr-pat}",
    )
    role = _find(docs, "Role", RBAC_NAME)
    assert role is not None
    assert role["rules"][0]["resourceNames"] == ["dockerhub-pat", "ghcr-pat"]


def test_disabled_renders_no_rbac() -> None:
    docs = _render_with("connected-eval", "connectorCredentials.enabled=false")
    assert _find(docs, "Role", RBAC_NAME) is None
    assert _find(docs, "RoleBinding", RBAC_NAME) is None
    assert _find(docs, "Namespace", CREDENTIAL_NS) is None


def test_not_rendered_when_secret_store_is_not_kubernetes() -> None:
    docs = _render_with(
        "connected-eval",
        "dapr.components.secretStore.type=secretstores.hashicorp.vault",
    )
    assert _find(docs, "Role", RBAC_NAME) is None


def test_create_namespace_false_keeps_rbac_but_skips_namespace() -> None:
    docs = _render_with("connected-eval", "connectorCredentials.createNamespace=false")
    assert _find(docs, "Namespace", CREDENTIAL_NS) is None
    assert _find(docs, "Role", RBAC_NAME) is not None
