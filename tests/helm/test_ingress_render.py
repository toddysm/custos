"""Render-time assertions for the north-south ingress stack (DEPLOY-IMPL-013).

The umbrella vendors Envoy Gateway and cert-manager as subcharts gated on
``envoyGateway.install`` / ``certManager.install``. It also templates the
Gateway API ``GatewayClass`` bound to the Envoy Gateway controller and the
cert-manager ``Issuer`` + ``Certificate`` that issue the Gateway's serving
TLS material.
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
AIRGAPPED_PROFILES = ("airgapped-eval", "airgapped-ha")

ENVOY_CONTROLLER = "gateway.envoyproxy.io/gatewayclass-controller"


def _by_kind(docs: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [d for d in docs if d.get("kind") == kind]


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
def test_gatewayclass_bound_to_envoy_controller(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Each profile renders a GatewayClass bound to the Envoy Gateway controller."""
    docs = rendered[profile]
    gc = _find(docs, "GatewayClass", "envoy")
    assert gc is not None, f"GatewayClass missing from {profile}"
    assert gc["spec"]["controllerName"] == ENVOY_CONTROLLER


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_gateway_uses_rendered_class(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The Gateway binds to the GatewayClass the chart renders."""
    docs = rendered[profile]
    gw = _find(docs, "Gateway", "custos")
    assert gw is not None, f"Gateway missing from {profile}"
    assert gw["spec"]["gatewayClassName"] == "envoy"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_certmanager_issues_gateway_tls(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """cert-manager Issuer + Certificate populate the Gateway's TLS Secret."""
    docs = rendered[profile]
    gw = _find(docs, "Gateway", "custos")
    assert gw is not None
    tls_secret = gw["spec"]["listeners"][0]["tls"]["certificateRefs"][0]["name"]

    cert = _find(docs, "Certificate", tls_secret)
    assert cert is not None, f"Certificate for {tls_secret} missing from {profile}"
    assert cert["spec"]["secretName"] == tls_secret
    issuer_name = cert["spec"]["issuerRef"]["name"]

    issuer = _find(docs, "Issuer", issuer_name)
    assert issuer is not None, f"Issuer {issuer_name} missing from {profile}"
    # Default issuer is self-signed for portability across eval / air-gapped.
    assert "selfSigned" in issuer["spec"]


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_subcharts_installed_by_default(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Envoy Gateway + cert-manager controllers come up by default."""
    docs = rendered[profile]
    deployments = {d["metadata"]["name"] for d in _by_kind(docs, "Deployment")}
    assert "envoy-gateway" in deployments, f"envoy-gateway missing from {profile}"
    assert "custos-cert-manager" in deployments, f"cert-manager missing from {profile}"
    # cert-manager ships its CRDs in-chart.
    crds = {d["metadata"]["name"] for d in _by_kind(docs, "CustomResourceDefinition")}
    assert crds.issuperset({"certificates.cert-manager.io"}), (
        f"CRDs missing from {profile}"
    )


def test_install_toggles_skip_subcharts() -> None:
    """``*.install=false`` removes the subcharts and the GatewayClass."""
    docs = _render_with(
        "connected-eval",
        "envoyGateway.install=false",
        "certManager.install=false",
    )
    deployments = {d["metadata"]["name"] for d in _by_kind(docs, "Deployment")}
    assert "envoy-gateway" not in deployments
    assert "custos-cert-manager" not in deployments
    assert _find(docs, "GatewayClass", "envoy") is None
    # Scope to cert-manager's own CRDs so the test stays focused on the toggle
    # rather than failing if an unrelated subchart starts shipping CRDs.
    crds = {d["metadata"]["name"] for d in _by_kind(docs, "CustomResourceDefinition")}
    assert not {c for c in crds if c.endswith("cert-manager.io")}


def test_certmanager_off_keeps_issuer_for_cluster_provided() -> None:
    """With cert-manager cluster-provided, the chart still renders its Issuer/Cert."""
    docs = _render_with("connected-eval", "certManager.install=false")
    assert _find(docs, "Certificate", "custos-gateway-tls") is not None
    assert _find(docs, "Issuer", "custos-gateway-issuer") is not None


def test_tls_certmanager_disabled_drops_issuer_and_cert() -> None:
    """Disabling cert-manager TLS issuance removes the Issuer + Certificate."""
    docs = _render_with("connected-eval", "gateway.tls.certManager.enabled=false")
    assert _find(docs, "Certificate", "custos-gateway-tls") is None
    assert _find(docs, "Issuer", "custos-gateway-issuer") is None


def test_acme_issuer_renders_solver() -> None:
    """Switching the issuer to ACME renders an HTTP-01 gatewayHTTPRoute solver."""
    docs = _render_with(
        "connected-eval",
        "gateway.tls.certManager.issuer.type=acme",
        "gateway.tls.certManager.issuer.acme.email=ops@example.com",
    )
    issuer = _find(docs, "Issuer", "custos-gateway-issuer")
    assert issuer is not None
    acme = issuer["spec"]["acme"]
    assert acme["email"] == "ops@example.com"
    solver = acme["solvers"][0]["http01"]["gatewayHTTPRoute"]
    assert solver["parentRefs"][0]["name"] == "custos"


@pytest.mark.parametrize("profile", AIRGAPPED_PROFILES)
def test_airgapped_mirrors_ingress_images(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Air-gapped profiles pull the ingress images from the internal mirror."""
    docs = rendered[profile]
    images = [
        c["image"]
        for d in _by_kind(docs, "Deployment")
        for c in d["spec"]["template"]["spec"]["containers"]
    ]
    envoy = [i for i in images if "envoyproxy/gateway" in i]
    certmgr = [i for i in images if "cert-manager-controller" in i]
    assert envoy and all(i.startswith("registry.internal") for i in envoy)
    assert certmgr and all(i.startswith("registry.internal") for i in certmgr)
