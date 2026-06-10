"""Render-time assertions for the north-south ingress stack.

Since #851/#852 the umbrella no longer vendors Envoy Gateway or cert-manager —
they are installed out-of-band by ``scripts/install-prereqs.sh`` before
``helm install custos`` (the bundled subcharts pushed the packaged release past
Helm's 1 MB release-Secret limit). The umbrella still templates its own
Gateway API ``Gateway`` + ``GatewayClass`` (bound to the Envoy Gateway
controller) and the cert-manager ``Issuer`` + ``Certificate`` that issue the
Gateway's serving TLS material. These tests assert that umbrella-owned glue and
that the ingress controllers themselves are not bundled.
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

ENVOY_CONTROLLER = "gateway.envoyproxy.io/gatewayclass-controller"

# Ingress controller workloads that used to ship in the umbrella and now come
# from the out-of-band prerequisites install.
OUT_OF_BAND_WORKLOADS = ("envoy-gateway", "custos-cert-manager")


def _by_kind(docs: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [d for d in docs if d.get("kind") == kind]


def _find(docs: list[dict[str, Any]], kind: str, name: str) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


_DEPS_UPDATED = False


def _ensure_dependencies() -> None:
    """Populate ./charts/ once per test process (all deps are local)."""
    global _DEPS_UPDATED
    if _DEPS_UPDATED:
        return
    subprocess.run(
        ["helm", "dependency", "build", str(UMBRELLA)],
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
def test_ingress_controllers_not_bundled(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """Envoy Gateway + cert-manager controllers must not be bundled."""
    docs = rendered[profile]
    deployments = {d["metadata"]["name"] for d in _by_kind(docs, "Deployment")}
    for workload in OUT_OF_BAND_WORKLOADS:
        assert workload not in deployments, (
            f"{workload} is bundled in {profile}; the ingress controllers must "
            "be installed out-of-band (scripts/install-prereqs.sh)"
        )
    # cert-manager ships its own CRDs out-of-band too — none come from the chart.
    crds = {d["metadata"]["name"] for d in _by_kind(docs, "CustomResourceDefinition")}
    assert not {c for c in crds if "cert-manager" in c}, (
        f"cert-manager CRDs leaked into {profile}"
    )


def test_envoy_install_false_drops_gatewayclass() -> None:
    """``envoyGateway.install=false`` removes the umbrella-owned GatewayClass."""
    docs = _render_with("connected-eval", "envoyGateway.install=false")
    assert _find(docs, "GatewayClass", "envoy") is None


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
