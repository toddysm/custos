"""Tests for the Phase D ``GET /v1/roles`` + ``GET /v1/permissions`` routes (AS-IMPL-009)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import callctx_header


@pytest.fixture
def auth_header() -> dict[str, str]:
    """Default authenticated dev-shim header. No permissions required
    beyond being authenticated."""
    return callctx_header(principal_id="alice")


def test_list_roles_returns_all_six_builtins(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.get("/v1/roles", headers=auth_header)
    assert response.status_code == 200
    body = response.json()
    assert "roles" in body
    role_ids = [r["role_id"] for r in body["roles"]]
    assert role_ids == [
        "role:workspace.viewer",
        "role:workspace.author",
        "role:workspace.operator",
        "role:workspace.admin",
        "role:tenant.admin",
        "role:platform.admin",
    ]


def test_list_roles_includes_allowed_scopes(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.get("/v1/roles", headers=auth_header)
    body = response.json()
    by_id = {r["role_id"]: r for r in body["roles"]}
    assert by_id["role:workspace.viewer"]["allowed_scopes"] == ["workspace"]
    assert by_id["role:tenant.admin"]["allowed_scopes"] == ["tenant"]
    assert by_id["role:platform.admin"]["allowed_scopes"] == ["platform"]


def test_list_roles_includes_permission_names(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.get("/v1/roles", headers=auth_header)
    by_id = {r["role_id"]: r for r in response.json()["roles"]}
    viewer = by_id["role:workspace.viewer"]
    assert "catalog:workflows:read" in viewer["permission_names"]
    assert "audit:read" in viewer["permission_names"]
    # platform.admin has the empty permission tuple — it short-circuits
    # in the authorize engine rather than listing every permission.
    assert by_id["role:platform.admin"]["permission_names"] == []


def test_list_roles_requires_callctx(client: TestClient) -> None:
    response = client.get("/v1/roles")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "callctx_missing"


def test_post_roles_returns_501(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.post("/v1/roles", headers=auth_header, json={})
    assert response.status_code == 501
    assert response.json()["error"]["code"] == "not_implemented"


def test_list_permissions_returns_bundled_registry(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.get("/v1/permissions", headers=auth_header)
    assert response.status_code == 200
    body = response.json()
    names = {p["name"] for p in body["permissions"]}
    # Spot-check both auth-service-owned and cross-component entries.
    assert "admin:role-binding" in names
    assert "catalog:workflows:read" in names
    assert "audit:read" in names


def test_list_permissions_carries_multi_declarer_attribution(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.get("/v1/permissions", headers=auth_header)
    by_name = {p["name"]: p for p in response.json()["permissions"]}
    # ``audit:read`` is declared by both auth-service and
    # observability-audit-service in the bundled registry.
    parts = by_name["audit:read"]["declared_by"].split("|")
    assert "auth-service" in parts
    assert "observability-audit-service" in parts


def test_list_permissions_is_public_bootstrap_artefact(client: TestClient) -> None:
    """The permission registry is a public read — no call-context required.

    The API Gateway fetches it at startup to cross-check its route grants
    *before* it holds any call-context (the path is on the middleware
    bypass list). ``GET /v1/roles`` stays authenticated by contrast.
    """
    response = client.get("/v1/permissions")
    assert response.status_code == 200
    assert "permissions" in response.json()


def test_list_permissions_is_sorted_by_name(
    client: TestClient,
    auth_header: dict[str, str],
) -> None:
    response = client.get("/v1/permissions", headers=auth_header)
    names = [p["name"] for p in response.json()["permissions"]]
    assert names == sorted(names)
