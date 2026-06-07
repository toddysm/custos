"""Render-time assertions for the Dapr building-block CRs (DEPLOY-IMPL-012).

The umbrella renders Dapr ``Component`` CRs (state store, secret store, and the
Redis + Postgres pub/sub brokers) plus declarative ``Subscription`` CRs for the
trigger-service and observability-audit-service consumers. Everything is gated
on ``dapr.install`` and individually toggleable. Secret-backed metadata must be
resolved through a Dapr secret store rather than inlined.
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

# Component names the umbrella must render by default, keyed by Dapr type.
EXPECTED_COMPONENTS = {
    "custos-statestore": "state.postgresql",
    "custos-secretstore": "secretstores.kubernetes",
    "custos-pubsub": "pubsub.redis",
    "custos-pubsub-durable": "pubsub.postgres",
}

# Subscription name -> (topic, scope app-id).
EXPECTED_SUBSCRIPTIONS = {
    "trigger-workflow-events": ("custos.workflow.events", "trigger-service"),
    "observability-workflow-events": (
        "custos.workflow.events",
        "observability-audit-service",
    ),
}


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
def test_components_rendered_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Each profile renders the state/secret/pubsub Components by default."""
    docs = rendered[profile]
    components = {
        d["metadata"]["name"]: d.get("spec", {}).get("type")
        for d in _by_kind(docs, "Component")
        if d["metadata"]["name"] in EXPECTED_COMPONENTS
    }
    assert components == EXPECTED_COMPONENTS, f"unexpected Components in {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_pubsub_uses_redis_and_postgres(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Pub/sub spans a Redis fan-out broker and a Postgres durable broker (D3)."""
    docs = rendered[profile]
    pubsub_types = {
        d.get("spec", {}).get("type")
        for d in _by_kind(docs, "Component")
        if str(d["metadata"]["name"]).startswith("custos-pubsub")
    }
    assert "pubsub.redis" in pubsub_types
    assert "pubsub.postgres" in pubsub_types


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_subscriptions_rendered_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Each profile renders the trigger + observability consumer Subscriptions."""
    docs = rendered[profile]
    for name, (topic, scope) in EXPECTED_SUBSCRIPTIONS.items():
        sub = _find(docs, "Subscription", name)
        assert sub is not None, f"{name} missing from {profile}"
        spec = sub["spec"]
        assert spec["pubsubname"] == "custos-pubsub"
        assert spec["topic"] == topic
        assert spec["routes"]["default"] == "/internal/events/workflow"
        assert sub["scopes"] == [scope]


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_secrets_resolved_via_secret_store(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Credential-bearing Components reference a secret store, never inline values."""
    docs = rendered[profile]
    for name in ("custos-statestore", "custos-pubsub", "custos-pubsub-durable"):
        comp = _find(docs, "Component", name)
        assert comp is not None, f"{name} missing from {profile}"
        assert comp["auth"]["secretStore"] == "kubernetes"
        secret_refs = [
            entry for entry in comp["spec"]["metadata"] if "secretKeyRef" in entry
        ]
        assert secret_refs, f"{name} has no secretKeyRef metadata"


def test_statestore_binds_to_cnpg_secret() -> None:
    """The state store reads its connection URI from the CNPG app secret."""
    docs = _render_with("connected-eval")
    comp = _find(docs, "Component", "custos-statestore")
    assert comp is not None
    entry = next(e for e in comp["spec"]["metadata"] if e["name"] == "connectionString")
    assert entry["secretKeyRef"] == {"name": "custos-app", "key": "uri"}


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_install_false_removes_components_and_subscriptions(profile: str) -> None:
    """``dapr.install=false`` strips every umbrella Component + Subscription CR."""
    docs = _render_with(profile, "dapr.install=false")
    names = {d["metadata"]["name"] for d in _by_kind(docs, "Component")}
    assert not (names & set(EXPECTED_COMPONENTS)), f"Components leaked in {profile}"
    assert not _by_kind(docs, "Subscription"), f"Subscriptions leaked in {profile}"


def test_component_can_be_disabled_individually() -> None:
    """A single component toggle removes only that Component."""
    docs = _render_with(
        "connected-eval", "dapr.components.pubsub.postgres.enabled=false"
    )
    names = {d["metadata"]["name"] for d in _by_kind(docs, "Component")}
    assert "custos-pubsub-durable" not in names
    assert "custos-pubsub" in names
    assert "custos-statestore" in names


def test_secret_store_type_is_overridable() -> None:
    """HA installs can point the secret store at an ESO-aligned backend."""
    docs = _render_with(
        "connected-ha",
        "dapr.components.secretStore.type=secretstores.hashicorp.vault",
        "dapr.components.secretStore.metadata[0].name=vaultAddr",
        "dapr.components.secretStore.metadata[0].value=https://vault:8200",
    )
    comp = _find(docs, "Component", "custos-secretstore")
    assert comp is not None
    assert comp["spec"]["type"] == "secretstores.hashicorp.vault"
    assert comp["spec"]["metadata"] == [
        {"name": "vaultAddr", "value": "https://vault:8200"}
    ]
