"""Render-time assertions for the auth-service subchart Dapr app-id.

The api-gateway reaches the Auth Service via Dapr service invocation under the
app-id ``custos-auth`` (``docs/developers/api-gateway.md``; it is also the
call-context JWT issuer). Every *other* service's Dapr app-id defaults to its
chart name, but the Auth Service is the documented exception and must register
``custos-auth`` so ``custos-auth-dapr`` resolves and the gateway's startup
permission check (``/readyz``) succeeds.

These tests also guard the Helm footgun that motivated the override: the
``svc.*`` named templates live in one global namespace across the umbrella, so
the auth-service app-id can only differ from its chart name when *all* service
subcharts share an identical ``svc.daprAnnotations`` define that reads
``.Values.dapr.appId``. A sibling reverting to ``include "svc.name"`` would
silently win and regress the app-id back to ``auth-service`` — these assertions
catch that.
"""

from __future__ import annotations

from typing import Any

import pytest

HA_PROFILES = ("connected-ha", "airgapped-ha")
EVAL_PROFILES = ("connected-eval", "airgapped-eval")
ALL_PROFILES = HA_PROFILES + EVAL_PROFILES


def _find(
    docs: list[dict[str, Any]], kind: str, name: str
) -> dict[str, Any] | None:
    for doc in docs:
        if doc.get("kind") == kind and doc.get("metadata", {}).get("name") == name:
            return doc
    return None


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_auth_dapr_app_id_is_custos_auth(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    dep = _find(rendered[profile], "Deployment", "custos-auth-service")
    assert dep is not None, f"auth-service Deployment missing in {profile}"
    annotations = dep["spec"]["template"]["metadata"].get("annotations") or {}
    assert annotations.get("dapr.io/enabled") == "true"
    assert annotations.get("dapr.io/app-id") == "custos-auth", (
        f"{profile}: auth-service must register Dapr app-id 'custos-auth' so "
        "the api-gateway can resolve 'custos-auth-dapr'; got "
        f"{annotations.get('dapr.io/app-id')!r}"
    )
    assert annotations.get("dapr.io/app-port") == "8080"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_auth_identity_labels_keep_chart_name(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The app-id override must NOT leak into the workload identity: the
    Service name, selector labels, and ``app.kubernetes.io/name`` stay
    ``auth-service`` (chart name) so NetworkPolicies and the
    ``http://auth-service:8080`` sibling endpoints keep resolving.
    """
    dep = _find(rendered[profile], "Deployment", "custos-auth-service")
    assert dep is not None
    selector = dep["spec"]["selector"]["matchLabels"]
    assert selector.get("app.kubernetes.io/name") == "auth-service"

    svc = _find(rendered[profile], "Service", "custos-auth-service")
    assert svc is not None, f"auth-service Service missing in {profile}"


@pytest.mark.parametrize("profile", ALL_PROFILES)
def test_sibling_services_keep_chart_name_app_id(
    rendered: dict[str, list[dict[str, Any]]], profile: str
) -> None:
    """The shared ``svc.daprAnnotations`` change is backward compatible: every
    non-auth service still derives its app-id from its chart name.
    """
    expected = {
        "custos-api-gateway": "api-gateway",
        "custos-catalog-service": "catalog-service",
        "custos-workflow-service": "workflow-service",
        "custos-trigger-service": "trigger-service",
        "custos-connector-service": "connector-service",
        "custos-activity-runtime-manager": "activity-runtime-manager",
        "custos-observability-audit-service": "observability-audit-service",
    }
    for dep_name, app_id in expected.items():
        dep = _find(rendered[profile], "Deployment", dep_name)
        assert dep is not None, f"{dep_name} Deployment missing in {profile}"
        annotations = dep["spec"]["template"]["metadata"].get("annotations") or {}
        assert annotations.get("dapr.io/app-id") == app_id, (
            f"{profile}: {dep_name} app-id regressed to "
            f"{annotations.get('dapr.io/app-id')!r}, expected {app_id!r}"
        )
