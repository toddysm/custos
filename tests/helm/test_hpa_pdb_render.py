"""Render-time assertions for the HA-gated HPA + PDB templates (DEPLOY-IMPL-010).

The eval profiles must render neither a HorizontalPodAutoscaler nor a
PodDisruptionBudget; the HA profiles must render one of each per service and
the service Deployment must drop its static ``replicas`` field (so the HPA owns
the replica count). Defaults come from each subchart's ``autoscaling`` /
``podDisruptionBudget`` values block.
"""

from __future__ import annotations

from typing import Any

import pytest

HA_PROFILES = ("connected-ha", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
ALL_PROFILES = HA_PROFILES + EVAL_PROFILES

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


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


def _by_kind(docs: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [d for d in docs if d.get("kind") == kind]


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_renders_no_hpa_or_pdb(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    docs = rendered[profile]
    assert _by_kind(docs, "HorizontalPodAutoscaler") == []
    # Scope to Custos-owned PDBs; vendored subcharts (e.g. Dapr) may ship their
    # own disruption budgets independent of the HA-gated service templates.
    custos_pdbs = [
        d
        for d in _by_kind(docs, "PodDisruptionBudget")
        if d["metadata"]["name"].startswith("custos-")
    ]
    assert custos_pdbs == []


@pytest.mark.parametrize("profile", EVAL_PROFILES)
def test_eval_deployment_sets_static_replicas(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    for svc in SERVICES:
        dep = _find(rendered[profile], "Deployment", f"custos-{svc}")
        assert dep is not None, f"{svc} Deployment missing in {profile}"
        assert dep["spec"].get("replicas") == 1


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_renders_hpa_per_service(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    hpas = {
        d["metadata"]["name"]
        for d in _by_kind(rendered[profile], "HorizontalPodAutoscaler")
    }
    assert hpas == {f"custos-{svc}" for svc in SERVICES}


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_renders_pdb_per_service(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    pdbs = {
        d["metadata"]["name"]
        for d in _by_kind(rendered[profile], "PodDisruptionBudget")
        if d["metadata"]["name"].startswith("custos-")
    }
    assert pdbs == {f"custos-{svc}" for svc in SERVICES}


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_ha_deployment_omits_static_replicas(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    for svc in SERVICES:
        dep = _find(rendered[profile], "Deployment", f"custos-{svc}")
        assert dep is not None, f"{svc} Deployment missing in {profile}"
        assert "replicas" not in dep["spec"], (
            f"{svc} Deployment must not set static replicas under HPA"
        )


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_hpa_targets_its_own_deployment(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    for svc in SERVICES:
        hpa = _find(rendered[profile], "HorizontalPodAutoscaler", f"custos-{svc}")
        assert hpa is not None
        spec = hpa["spec"]
        ref = spec["scaleTargetRef"]
        assert ref["kind"] == "Deployment"
        assert ref["apiVersion"] == "apps/v1"
        assert ref["name"] == f"custos-{svc}"
        assert spec["minReplicas"] == 3
        assert spec["maxReplicas"] == 6
        metric_names = {m["resource"]["name"] for m in spec["metrics"]}
        assert metric_names == {"cpu", "memory"}


@pytest.mark.parametrize("profile", HA_PROFILES)
def test_pdb_max_unavailable_and_selector(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    for svc in SERVICES:
        pdb = _find(rendered[profile], "PodDisruptionBudget", f"custos-{svc}")
        assert pdb is not None
        assert pdb["spec"]["maxUnavailable"] == 1
        sel = pdb["spec"]["selector"]["matchLabels"]
        assert sel["app.kubernetes.io/name"] == svc
        assert sel["app.kubernetes.io/instance"] == "custos"
