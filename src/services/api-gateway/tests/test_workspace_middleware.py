"""Tests for the Workspace Resolver dependency (AGW-IMPL-006)."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.errors import register_exception_handlers
from custos_gateway.middleware.workspace import (
    WORKSPACE_STATE_ATTR,
    ResolvedWorkspace,
    resolve_workspace,
)


def _build_app() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)

    @app.post("/v1/workspaces/{workspaceId}/things")
    async def scoped(
        request: Request,
        resolved: ResolvedWorkspace = Depends(resolve_workspace),
    ) -> dict[str, object]:
        return {
            "workspace_id": resolved.workspace_id,
            "is_scoped": resolved.is_scoped,
            "state_workspace_id": getattr(request.state, WORKSPACE_STATE_ATTR, None),
        }

    @app.get("/v1/workspaces/{workspaceId}/things")
    async def scoped_get(
        resolved: ResolvedWorkspace = Depends(resolve_workspace),
    ) -> dict[str, object]:
        return {"workspace_id": resolved.workspace_id, "is_scoped": resolved.is_scoped}

    @app.get("/v1/principals/me")
    async def unscoped(
        request: Request,
        resolved: ResolvedWorkspace = Depends(resolve_workspace),
    ) -> dict[str, object]:
        return {
            "workspace_id": resolved.workspace_id,
            "is_scoped": resolved.is_scoped,
            "state_workspace_id": getattr(request.state, WORKSPACE_STATE_ATTR, None),
        }

    @app.post("/v1/principals/me")
    async def unscoped_post(
        resolved: ResolvedWorkspace = Depends(resolve_workspace),
    ) -> dict[str, object]:
        return {"workspace_id": resolved.workspace_id, "is_scoped": resolved.is_scoped}

    return app


# --- ResolvedWorkspace value object ------------------------------------------


def test_resolved_workspace_is_scoped_when_present() -> None:
    assert ResolvedWorkspace(workspace_id="ws_1").is_scoped is True


def test_resolved_workspace_unscoped_when_none() -> None:
    assert ResolvedWorkspace(workspace_id=None).is_scoped is False


# --- scoped routes -----------------------------------------------------------


def test_scoped_route_resolves_path_workspace() -> None:
    client = TestClient(_build_app())
    resp = client.get("/v1/workspaces/ws_42/things")
    assert resp.status_code == 200
    body = resp.json()
    assert body["workspace_id"] == "ws_42"
    assert body["is_scoped"] is True


def test_scoped_route_binds_state_for_downstream() -> None:
    client = TestClient(_build_app())
    resp = client.post("/v1/workspaces/ws_42/things", json={"name": "thing"})
    assert resp.status_code == 200
    assert resp.json()["state_workspace_id"] == "ws_42"


def test_body_matching_url_workspace_is_allowed() -> None:
    client = TestClient(_build_app())
    resp = client.post("/v1/workspaces/ws_42/things", json={"workspaceId": "ws_42"})
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws_42"


def test_body_referencing_other_workspace_is_mismatch() -> None:
    client = TestClient(_build_app())
    resp = client.post("/v1/workspaces/ws_42/things", json={"workspaceId": "ws_99"})
    assert resp.status_code == 400
    assert resp.json()["code"] == "workspace-mismatch"


@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "application/json; charset=utf-8",
        "application/vnd.custos.thing+json",
        "application/problem+json",
    ],
)
def test_mismatch_detected_for_all_json_media_types(content_type: str) -> None:
    client = TestClient(_build_app())
    resp = client.post(
        "/v1/workspaces/ws_42/things",
        content=b'{"workspaceId": "ws_99"}',
        headers={"content-type": content_type},
    )
    assert resp.status_code == 400
    assert resp.json()["code"] == "workspace-mismatch"


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "thing"},  # no workspaceId field
        {"workspaceId": ""},  # blank workspaceId is ignored
        {"workspaceId": 7},  # non-string workspaceId is ignored
        ["ws_99"],  # non-object body
    ],
)
def test_body_without_conflicting_workspace_passes(payload: object) -> None:
    client = TestClient(_build_app())
    resp = client.post("/v1/workspaces/ws_42/things", json=payload)
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws_42"


def test_non_json_body_is_not_inspected() -> None:
    client = TestClient(_build_app())
    resp = client.post(
        "/v1/workspaces/ws_42/things",
        content=b"workspaceId=ws_99",
        headers={"content-type": "text/plain"},
    )
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws_42"


def test_malformed_json_body_is_ignored() -> None:
    client = TestClient(_build_app())
    resp = client.post(
        "/v1/workspaces/ws_42/things",
        content=b"{not json",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws_42"


def test_empty_json_body_is_ignored() -> None:
    client = TestClient(_build_app())
    resp = client.post(
        "/v1/workspaces/ws_42/things",
        content=b"",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws_42"


# --- unscoped routes ---------------------------------------------------------


def test_unscoped_route_resolves_to_no_workspace() -> None:
    client = TestClient(_build_app())
    resp = client.get("/v1/principals/me")
    assert resp.status_code == 200
    body = resp.json()
    assert body["workspace_id"] is None
    assert body["is_scoped"] is False
    assert body["state_workspace_id"] is None


def test_unscoped_route_ignores_body_workspace() -> None:
    client = TestClient(_build_app())
    resp = client.post("/v1/principals/me", json={"workspaceId": "ws_99"})
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] is None
