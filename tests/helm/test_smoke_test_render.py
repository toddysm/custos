"""Render-time assertions for the helm test smoke hook (DEPLOY-IMPL-018).

These tests shell out to ``helm template`` against the umbrella chart and walk
the parsed manifests. They lock the wiring contract for
``deploy/helm/custos/templates/tests/smoke-test.yaml``:

- The ``helm.sh/hook: test`` ConfigMap (ships ``smoke.py``) + Pod render across
  every profile, with release-scoped names so two releases can share a namespace.
- The Pod runs the api-gateway image (redirected to the mirror for air-gapped
  profiles), targets the in-cluster gateway Service, and runs hardened
  (non-root, read-only root FS, all caps dropped).
- The optional bearer-token env is omitted by default and wired via
  ``secretKeyRef`` when ``tests.auth.tokenSecret`` is set.
"""

from __future__ import annotations

from typing import Any

import pytest

ALL_PROFILES = ("connected-eval", "connected-ha", "airgapped-eval", "airgapped-ha")

CONFIGMAP_NAME = "custos-smoke-test"  # release name in the suite is "custos"
POD_NAME = "custos-smoke-test"


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_hook_resources_render(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    cm = _find(rendered[profile], "ConfigMap", CONFIGMAP_NAME)
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert cm is not None, f"smoke-test ConfigMap missing in {profile}"
    assert pod is not None, f"smoke-test Pod missing in {profile}"
    # Both are helm test hooks.
    assert cm["metadata"]["annotations"]["helm.sh/hook"] == "test"
    assert pod["metadata"]["annotations"]["helm.sh/hook"] == "test"
    # The script is shipped inline so it needs no extra image layer.
    assert "smoke.py" in cm["data"]
    assert "Custos helm-test smoke scenario" in cm["data"]["smoke.py"]


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_pod_mounts_script_configmap(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert pod is not None
    volumes = pod["spec"]["volumes"]
    script_vol = next(v for v in volumes if v["name"] == "smoke-script")
    # The volume must reference the same release-scoped ConfigMap name.
    assert script_vol["configMap"]["name"] == CONFIGMAP_NAME


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_pod_runs_hardened(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert pod is not None
    assert pod["spec"]["securityContext"]["runAsNonRoot"] is True
    container = pod["spec"]["containers"][0]
    sec = container["securityContext"]
    assert sec["allowPrivilegeEscalation"] is False
    assert sec["readOnlyRootFilesystem"] is True
    assert sec["capabilities"]["drop"] == ["ALL"]


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_pod_targets_gateway(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert pod is not None
    env = {e["name"]: e for e in pod["spec"]["containers"][0]["env"]}
    assert env["CUSTOS_GATEWAY_URL"]["value"] == "http://custos-api-gateway:8080"
    assert env["CUSTOS_API_PREFIX"]["value"] == "/v1"


@pytest.mark.parametrize("profile", ("connected-eval", "connected-ha"))
def test_image_uses_public_registry(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert pod is not None
    image = pod["spec"]["containers"][0]["image"]
    assert image == "ghcr.io/toddysm/custos/api-gateway:dev"


@pytest.mark.parametrize("profile", ("airgapped-eval", "airgapped-ha"))
def test_image_redirected_for_airgapped(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert pod is not None
    image = pod["spec"]["containers"][0]["image"]
    # Air-gapped overlays repoint global.imageRegistry to the mirror.
    assert image.startswith("registry.internal/custos/api-gateway:")


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_token_env_absent_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pod = _find(rendered[profile], "Pod", POD_NAME)
    assert pod is not None
    names = {e["name"] for e in pod["spec"]["containers"][0]["env"]}
    # No token Secret configured by default -> the authenticated scenario is
    # opt-in and the env var is omitted.
    assert "CUSTOS_TEST_TOKEN" not in names
