"""Render-time assertions for the per-service NetworkPolicy allow set
(DEPLOY-IMPL-009).

These tests render the umbrella chart with
``global.networkPolicies.enabled=true`` and assert the design's Networking
allow matrix (reference-deployment.md, resolves TODO-004):

- The umbrella ships a default ``deny-all`` (Ingress+Egress) policy.
- Every service subchart ships exactly one NetworkPolicy selecting its own
  pods, and the per-service allow rules encode the documented east-west /
  north-south / scrape / DNS / Dapr / Postgres flows.
- With the umbrella toggle OFF (the shipped default) NO NetworkPolicy renders,
  so enabling deny-all is an explicit, all-or-nothing operator decision.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
UMBRELLA = REPO_ROOT / "deploy" / "helm" / "custos"

SERVICES = (
    "api-gateway",
    "auth-service",
    "workflow-service",
    "trigger-service",
    "connector-service",
    "activity-runtime-manager",
    "catalog-service",
    "observability-audit-service",
)

# Expected egress east-west targets per service (caller -> callees), driving
# the design allow matrix. Ingress is asserted as the transpose below.
EGRESS_MATRIX: dict[str, set[str]] = {
    "api-gateway": {
        "auth-service",
        "workflow-service",
        "trigger-service",
        "connector-service",
        "catalog-service",
        "activity-runtime-manager",
        "observability-audit-service",
    },
    "auth-service": {"observability-audit-service"},
    "workflow-service": {
        "connector-service",
        "activity-runtime-manager",
        "catalog-service",
        "auth-service",
        "observability-audit-service",
    },
    "trigger-service": {
        "workflow-service",
        "connector-service",
        "auth-service",
        "observability-audit-service",
    },
    "connector-service": {
        "activity-runtime-manager",
        "catalog-service",
        "auth-service",
        "observability-audit-service",
    },
    "catalog-service": {
        "connector-service",
        "auth-service",
        "observability-audit-service",
    },
    "activity-runtime-manager": {"auth-service", "observability-audit-service"},
    "observability-audit-service": set(),
}

# Services that hold a CNPG-backed store (egress to Postgres on 5432).
POSTGRES_SERVICES = {
    "auth-service",
    "workflow-service",
    "trigger-service",
    "connector-service",
    "catalog-service",
    "observability-audit-service",
}


def _render(enabled: bool) -> list[dict[str, Any]]:
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
        str(UMBRELLA / "values-connected-eval.yaml"),
    ]
    if enabled:
        cmd += ["--set", "global.networkPolicies.enabled=true"]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return [d for d in yaml.safe_load_all(result.stdout) if d is not None]


@pytest.fixture(scope="module")
def policies() -> dict[str, dict[str, Any]]:
    docs = _render(enabled=True)
    return {d["metadata"]["name"]: d for d in docs if d.get("kind") == "NetworkPolicy"}


def _names_from_rules(rules: list[dict[str, Any]]) -> set[str]:
    """Collect app.kubernetes.io/name values from a rule's pod matchExpressions."""
    names: set[str] = set()
    for rule in rules:
        for peer in rule.get("to", []) + rule.get("from", []):
            sel = peer.get("podSelector")
            if not sel:
                continue
            for expr in sel.get("matchExpressions", []):
                if expr.get("key") == "app.kubernetes.io/name":
                    names.update(expr.get("values", []))
    return names


def test_disabled_by_default_emits_no_policies() -> None:
    docs = _render(enabled=False)
    nps = [d for d in docs if d.get("kind") == "NetworkPolicy"]
    assert nps == [], "NetworkPolicies must not render unless explicitly enabled"


def test_deny_all_present(policies: dict[str, dict[str, Any]]) -> None:
    deny = policies.get("deny-all")
    assert deny is not None, "umbrella deny-all NetworkPolicy missing"
    assert deny["spec"]["podSelector"] == {}
    assert set(deny["spec"]["policyTypes"]) == {"Ingress", "Egress"}


def test_one_policy_per_service(policies: dict[str, dict[str, Any]]) -> None:
    for svc in SERVICES:
        name = f"custos-{svc}"
        assert name in policies, f"NetworkPolicy missing for {svc}"
        spec = policies[name]["spec"]
        assert spec["podSelector"]["matchLabels"]["app.kubernetes.io/name"] == svc
        assert set(spec["policyTypes"]) == {"Ingress", "Egress"}
    # 8 services + deny-all, nothing else.
    assert len(policies) == len(SERVICES) + 1


@pytest.mark.parametrize("svc", SERVICES)
def test_egress_matrix(policies: dict[str, dict[str, Any]], svc: str) -> None:
    egress = policies[f"custos-{svc}"]["spec"].get("egress", [])
    assert _names_from_rules(egress) == EGRESS_MATRIX[svc]


@pytest.mark.parametrize("svc", SERVICES)
def test_ingress_is_egress_transpose(
    policies: dict[str, dict[str, Any]], svc: str
) -> None:
    """A service's ingress callers == every service that egresses to it."""
    expected = {caller for caller, callees in EGRESS_MATRIX.items() if svc in callees}
    ingress = policies[f"custos-{svc}"]["spec"].get("ingress", [])
    assert _names_from_rules(ingress) == expected


@pytest.mark.parametrize("svc", SERVICES)
def test_dns_and_dapr_egress_always_present(
    policies: dict[str, dict[str, Any]], svc: str
) -> None:
    egress = policies[f"custos-{svc}"]["spec"].get("egress", [])
    ns_targets = {
        peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        for rule in egress
        for peer in rule.get("to", [])
        if "namespaceSelector" in peer
    }
    assert "kube-system" in ns_targets, f"{svc} missing DNS egress"
    assert "dapr-system" in ns_targets, f"{svc} missing Dapr control-plane egress"


@pytest.mark.parametrize("svc", SERVICES)
def test_postgres_egress(policies: dict[str, dict[str, Any]], svc: str) -> None:
    egress = policies[f"custos-{svc}"]["spec"].get("egress", [])
    has_pg = any(
        peer.get("podSelector", {}).get("matchLabels", {}).get("cnpg.io/cluster")
        == "custos"
        for rule in egress
        for peer in rule.get("to", [])
    )
    assert has_pg == (svc in POSTGRES_SERVICES), (
        f"{svc} Postgres egress = {has_pg}, expected {svc in POSTGRES_SERVICES}"
    )


@pytest.mark.parametrize("svc", SERVICES)
def test_prometheus_scrape_ingress(
    policies: dict[str, dict[str, Any]], svc: str
) -> None:
    ingress = policies[f"custos-{svc}"]["spec"].get("ingress", [])
    ns_targets = {
        peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        for rule in ingress
        for peer in rule.get("from", [])
        if "namespaceSelector" in peer
    }
    assert "monitoring" in ns_targets, f"{svc} missing Prometheus scrape ingress"


def test_only_gateway_accepts_north_south(
    policies: dict[str, dict[str, Any]],
) -> None:
    for svc in SERVICES:
        ingress = policies[f"custos-{svc}"]["spec"].get("ingress", [])
        ns_targets = {
            peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
            for rule in ingress
            for peer in rule.get("from", [])
            if "namespaceSelector" in peer
        }
        if svc == "api-gateway":
            assert "custos-system" in ns_targets
        else:
            assert "custos-system" not in ns_targets, (
                f"{svc} must not accept north-south gateway ingress"
            )
