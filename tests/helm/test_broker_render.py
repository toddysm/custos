"""Render-time assertions for the pub/sub broker + optional auth/secrets backends.

Since #851/#852 the umbrella no longer vendors Redis, Keycloak, or Sealed
Secrets — they are installed out-of-band by ``scripts/install-prereqs.sh`` before
``helm install custos`` (the bundled subcharts pushed the packaged release past
Helm's 1 MB release-Secret limit). The umbrella still templates the Dapr Redis
pub/sub ``Component`` CR, which pins the contract the out-of-band Redis install
must satisfy: a ``custos-redis-master:6379`` Service and a ``custos-redis``
Secret. These tests assert that contract and that none of the broker/auth
backends are bundled by the umbrella in any profile.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
UMBRELLA = REPO_ROOT / "deploy" / "helm" / "custos"

ALL_PROFILES = ("connected-eval", "connected-ha", "airgapped-eval", "airgapped-ha")

REDIS_MASTER = "custos-redis-master"
REDIS_SECRET = "custos-redis"
KEYCLOAK = "custos-keycloak"
SEALED_SECRETS = "custos-sealed-secrets"

# Broker / auth workloads that used to ship in the umbrella and now come from
# the out-of-band prerequisites install.
OUT_OF_BAND_WORKLOADS = (
    REDIS_MASTER,
    "custos-redis-replicas",
    KEYCLOAK,
    SEALED_SECRETS,
)

WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet")


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _workload_names(docs: list[dict[str, Any]]) -> set[str]:
    return {d["metadata"]["name"] for d in docs if d.get("kind") in WORKLOAD_KINDS}


# --- Dapr pub/sub broker contract -------------------------------------------


def test_dapr_pubsub_component_targets_broker(
    rendered: dict[str, list[dict[str, Any]]],
) -> None:
    """The Dapr Redis pub/sub Component pins the out-of-band broker's names.

    The umbrella renders the Component CR but not Redis itself; the
    out-of-band Redis install must expose the ``custos-redis-master`` Service
    and the ``custos-redis`` Secret this Component references.
    """
    component = _find(rendered["connected-eval"], "Component", "custos-pubsub")
    assert component is not None, "custos-pubsub Component missing"
    metadata = {m["name"]: m for m in component["spec"]["metadata"]}
    assert metadata["redisHost"]["value"] == f"{REDIS_MASTER}:6379"
    secret_ref = metadata["redisPassword"]["secretKeyRef"]
    assert secret_ref["name"] == REDIS_SECRET
    assert secret_ref["key"] == "redis-password"


# --- Out-of-band backends are not bundled -----------------------------------


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_broker_and_auth_backends_not_bundled(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Redis, Keycloak, and Sealed Secrets must not be bundled in any profile."""
    names = _workload_names(rendered[profile])
    for workload in OUT_OF_BAND_WORKLOADS:
        assert workload not in names, (
            f"{workload} is bundled in {profile}; broker/auth backends must be "
            "installed out-of-band (scripts/install-prereqs.sh)"
        )
