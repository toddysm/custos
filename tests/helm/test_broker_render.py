"""Render-time assertions for the pub/sub broker and optional auth/secrets
subcharts (DEPLOY-IMPL-015).

The umbrella vendors Redis as the Dapr pub/sub broker (always on with the Redis
pub/sub component), plus Keycloak and Sealed Secrets as optional, default-off
backends for air-gapped installs. Redis runs standalone on eval profiles and
switches to replication on HA. Keycloak and Sealed Secrets render only when
their feature flags (``oidc.keycloak.enabled`` / ``secrets.sealed.enabled``) are
set, which the air-gapped overlays do. Air-gapped profiles mirror every image to
``registry.internal``.
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
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
HA_PROFILES = ("connected-ha", "airgapped-ha")
CONNECTED_PROFILES = ("connected-eval", "connected-ha")
AIRGAPPED_PROFILES = ("airgapped-eval", "airgapped-ha")

REDIS_MASTER = "custos-redis-master"
REDIS_REPLICAS = "custos-redis-replicas"
REDIS_SECRET = "custos-redis"
KEYCLOAK = "custos-keycloak"
SEALED_SECRETS = "custos-sealed-secrets"

# Prefix the air-gapped overlays mirror every image under.
MIRROR_PREFIX = "registry.internal/"


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _has_workload(docs: list[dict[str, Any]], name: str) -> bool:
    """True when a Deployment or StatefulSet with ``name`` is present."""
    return any(
        doc.get("kind") in ("Deployment", "StatefulSet")
        and doc.get("metadata", {}).get("name") == name
        for doc in docs
    )


def _images(docs: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for doc in docs:
        if doc.get("kind") not in ("Deployment", "StatefulSet", "DaemonSet"):
            continue
        spec = doc["spec"]["template"]["spec"]
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            out.add(container["image"])
    return out


_DEPS_UPDATED = False


def _ensure_dependencies() -> None:
    """Vendor the umbrella subcharts once per test process."""
    global _DEPS_UPDATED
    if _DEPS_UPDATED:
        return
    subprocess.run(
        ["helm", "dependency", "update", str(UMBRELLA)],
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


# --- Redis broker -----------------------------------------------------------


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_redis_master_renders(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Every profile installs the Redis primary the pub/sub component targets."""
    docs = rendered[profile]
    assert _find(docs, "StatefulSet", REDIS_MASTER) is not None, (
        f"Redis master missing from {profile}"
    )


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_redis_password_secret_present(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The Dapr component reads the password from the chart-managed Secret."""
    docs = rendered[profile]
    secret = _find(docs, "Secret", REDIS_SECRET)
    assert secret is not None, f"Redis Secret missing from {profile}"
    assert "redis-password" in secret.get("data", {})


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_redis_standalone_on_eval(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Eval profiles run a single standalone broker (no replicas)."""
    docs = rendered[profile]
    assert _find(docs, "StatefulSet", REDIS_REPLICAS) is None, (
        f"Unexpected Redis replicas in {profile}"
    )


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_redis_replication_on_ha(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """HA profiles add Redis read replicas alongside the primary."""
    docs = rendered[profile]
    assert _find(docs, "StatefulSet", REDIS_REPLICAS) is not None, (
        f"Redis replicas missing from {profile}"
    )


def test_dapr_pubsub_component_targets_broker(
    rendered: dict[str, list[dict[str, Any]]],
) -> None:
    """The Dapr Redis pub/sub Component points at the broker Service + Secret."""
    component = _find(rendered["connected-eval"], "Component", "custos-pubsub")
    assert component is not None, "custos-pubsub Component missing"
    metadata = {m["name"]: m for m in component["spec"]["metadata"]}
    assert metadata["redisHost"]["value"] == f"{REDIS_MASTER}:6379"
    secret_ref = metadata["redisPassword"]["secretKeyRef"]
    assert secret_ref["name"] == REDIS_SECRET
    assert secret_ref["key"] == "redis-password"


@pytest.mark.parametrize("profile", CONNECTED_PROFILES)
def test_connected_uses_upstream_redis_registry(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Connected installs keep Redis on the upstream registry (not mirrored)."""
    redis = [i for i in _images(rendered[profile]) if "bitnami/redis" in i]
    assert redis, f"No Redis image in {profile}"
    # Assert the image is NOT redirected to the air-gapped mirror rather than
    # pinning an exact Docker Hub prefix (which varies between
    # `docker.io/` and `registry-1.docker.io/`).
    assert not any(i.startswith(MIRROR_PREFIX) for i in redis), redis


@pytest.mark.parametrize("profile", AIRGAPPED_PROFILES)
def test_airgapped_mirrors_redis(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Air-gapped installs mirror Redis via the umbrella global registry."""
    redis = [i for i in _images(rendered[profile]) if "bitnami/redis" in i]
    assert redis, f"No Redis image in {profile}"
    assert all(i.startswith(MIRROR_PREFIX) for i in redis), redis


# --- Optional Keycloak + Sealed Secrets -------------------------------------


@pytest.mark.parametrize("profile", CONNECTED_PROFILES)
def test_keycloak_off_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Keycloak does not render unless explicitly enabled."""
    assert not _has_workload(rendered[profile], KEYCLOAK)


@pytest.mark.parametrize("profile", CONNECTED_PROFILES)
def test_sealed_secrets_off_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Sealed Secrets does not render unless explicitly enabled."""
    assert not _has_workload(rendered[profile], SEALED_SECRETS)


def test_keycloak_renders_when_enabled() -> None:
    """Enabling the feature flag renders the Keycloak workload."""
    docs = _render_with("connected-eval", "oidc.keycloak.enabled=true")
    assert _has_workload(docs, KEYCLOAK)


def test_sealed_secrets_renders_when_enabled() -> None:
    """Enabling the feature flag renders the Sealed Secrets controller."""
    docs = _render_with("connected-eval", "secrets.sealed.enabled=true")
    assert _find(docs, "Deployment", SEALED_SECRETS) is not None


@pytest.mark.parametrize("profile", AIRGAPPED_PROFILES)
def test_airgapped_enables_keycloak_and_sealed_secrets(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The air-gapped overlays turn both optional backends on."""
    docs = rendered[profile]
    assert _has_workload(docs, KEYCLOAK), f"Keycloak missing from {profile}"
    assert _find(docs, "Deployment", SEALED_SECRETS) is not None, (
        f"Sealed Secrets missing from {profile}"
    )


@pytest.mark.parametrize("profile", AIRGAPPED_PROFILES)
def test_airgapped_mirrors_sealed_secrets_image(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Sealed Secrets pins its own registry, so the overlay repoints the mirror."""
    sealed = [i for i in _images(rendered[profile]) if "sealed-secrets" in i]
    assert sealed, f"No Sealed Secrets image in {profile}"
    assert all(i.startswith(MIRROR_PREFIX) for i in sealed), sealed
