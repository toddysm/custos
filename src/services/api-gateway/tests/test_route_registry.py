"""Tests for the M1 route registry (AGW-IMPL-013)."""

from __future__ import annotations

import httpx
import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.clients.auth import AUTH_APP_ID
from custos_gateway.errors import register_exception_handlers
from custos_gateway.middleware.auth import AUTH_STATE_ATTR, AuthorizedCaller
from custos_gateway.middleware.callctx_mint import OUTBOUND_METADATA_STATE_ATTR
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes import registry
from custos_gateway.routes.registry import (
    CATALOG_APP_ID,
    CONNECTOR_APP_ID,
    DOWNSTREAM_ROUTER_STATE_ATTR,
    M1_ROUTE_REGISTRY,
    OBSERVABILITY_APP_ID,
    TRIGGER_APP_ID,
    WORKFLOW_APP_ID,
    RateLimitClass,
    RouteSpec,
    build_registry_router,
    registry_required_permissions,
)
from custos_gateway.settings import (
    DEFAULT_BODY_MAX_BYTES_DEFAULT,
    DEFAULT_BODY_MAX_BYTES_PUBLISH,
)
from custos_gateway.startup import collect_required_permissions

# The complete set of permissions the M1 registry enforces. Pinning it here makes
# any drift in the contract a loud test failure (the same set the startup check
# validates against the Auth Service registry).
EXPECTED_PERMISSIONS = {
    "admin:service-account",
    "admin:role-binding",
    "catalog:workflows:write",
    "catalog:workflows:read",
    "catalog:templates:write",
    "catalog:templates:read",
    "catalog:activity-types:write",
    "catalog:activity-types:read",
    "catalog:connector-types:write",
    "catalog:connector-types:read",
    "workflow:execute",
    "run:read",
    "run:cancel",
    "trigger:subscriptions:write",
    "trigger:subscriptions:read",
    "trigger:subscriptions:delete",
    "trigger:subscriptions:fire",
    "admin:connector",
    "connector:read",
    "audit:read",
    "logs:read",
    "metrics:read",
}

# Every downstream owner the design's M1 route table mounts a prefix for.
ALL_APP_IDS = {
    AUTH_APP_ID,
    CATALOG_APP_ID,
    WORKFLOW_APP_ID,
    TRIGGER_APP_ID,
    CONNECTOR_APP_ID,
    OBSERVABILITY_APP_ID,
}


def _spec(method: str, path: str) -> RouteSpec:
    for spec in M1_ROUTE_REGISTRY:
        if spec.method == method and spec.path == path:
            return spec
    raise AssertionError(f"no registry entry for {method} {path}")


# --- registry contract -------------------------------------------------------


def test_registry_is_non_empty() -> None:
    assert len(M1_ROUTE_REGISTRY) > 0


def test_every_design_owner_is_mounted() -> None:
    # Each of the six downstream components in the design's M1 route table owns
    # at least one mounted route.
    mounted = {spec.app_id for spec in M1_ROUTE_REGISTRY}
    assert mounted == ALL_APP_IDS


def test_no_unknown_app_ids() -> None:
    for spec in M1_ROUTE_REGISTRY:
        assert spec.app_id in ALL_APP_IDS


def test_method_path_pairs_are_unique() -> None:
    pairs = [(spec.method, spec.path) for spec in M1_ROUTE_REGISTRY]
    assert len(pairs) == len(set(pairs))


@pytest.mark.parametrize(
    ("method", "path", "app_id", "permission"),
    [
        ("POST", "/v1/service-accounts", AUTH_APP_ID, "admin:service-account"),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/role-bindings",
            AUTH_APP_ID,
            "admin:role-binding",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/workflows",
            CATALOG_APP_ID,
            "catalog:workflows:write",
        ),
        (
            "GET",
            "/v1/workspaces/{workspaceId}/workflows/{nameOrRef}",
            CATALOG_APP_ID,
            "catalog:workflows:read",
        ),
        ("POST", "/v1/catalog/connector-types", CATALOG_APP_ID, "catalog:connector-types:write"),
        (
            "POST",
            "/v1/catalog/connector-types/{ref}:deprecate",
            CATALOG_APP_ID,
            "catalog:connector-types:write",
        ),
        (
            "GET",
            "/v1/workflows/{workflowVersionId:path}",
            CATALOG_APP_ID,
            "catalog:workflows:read",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/workflows/{ref}:deprecate",
            CATALOG_APP_ID,
            "catalog:workflows:write",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/templates/{ref}:materialize",
            CATALOG_APP_ID,
            "catalog:templates:write",
        ),
        ("POST", "/v1/workspaces/{workspaceId}/runs", WORKFLOW_APP_ID, "workflow:execute"),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/runs/{runId}:cancel",
            WORKFLOW_APP_ID,
            "run:cancel",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/triggers",
            TRIGGER_APP_ID,
            "trigger:subscriptions:write",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/triggers/{subscriptionId}:fire",
            TRIGGER_APP_ID,
            "trigger:subscriptions:fire",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/connectors",
            CONNECTOR_APP_ID,
            "admin:connector",
        ),
        (
            "GET",
            "/v1/workspaces/{workspaceId}/runs/{runId}/leases",
            CONNECTOR_APP_ID,
            "connector:read",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/connectors/{connectorId}/cursor:rewind",
            CONNECTOR_APP_ID,
            "admin:connector",
        ),
        (
            "POST",
            "/v1/workspaces/{workspaceId}/connectors/{connectorId}/pull-loop:pause",
            CONNECTOR_APP_ID,
            "admin:connector",
        ),
        (
            "GET",
            "/v1/workspaces/{workspaceId}/audit/leases",
            CONNECTOR_APP_ID,
            "audit:read",
        ),
        (
            "GET",
            "/v1/workspaces/{workspaceId}/runs/{runId}/logs",
            OBSERVABILITY_APP_ID,
            "logs:read",
        ),
        (
            "GET",
            "/v1/workspaces/{workspaceId}/runs/{runId}/metrics",
            OBSERVABILITY_APP_ID,
            "metrics:read",
        ),
        (
            "GET",
            "/v1/workspaces/{workspaceId}/audit/{eventId}",
            OBSERVABILITY_APP_ID,
            "audit:read",
        ),
    ],
)
def test_representative_routes_map_to_owner_and_permission(
    method: str, path: str, app_id: str, permission: str
) -> None:
    spec = _spec(method, path)
    assert spec.app_id == app_id
    assert spec.required_permission == permission


def test_audit_leases_precedes_audit_event_lookup() -> None:
    # The connector ``/audit/leases`` literal must be registered before the
    # observability ``/audit/{eventId}`` parameter so the literal wins the match.
    paths = [(spec.method, spec.path) for spec in M1_ROUTE_REGISTRY]
    leases = paths.index(("GET", "/v1/workspaces/{workspaceId}/audit/leases"))
    event = paths.index(("GET", "/v1/workspaces/{workspaceId}/audit/{eventId}"))
    assert leases < event


# --- derived attributes ------------------------------------------------------


def test_required_permissions_match_expected_set() -> None:
    assert registry_required_permissions() == EXPECTED_PERMISSIONS


def test_publish_routes_get_raised_body_cap() -> None:
    for path in (
        "/v1/workspaces/{workspaceId}/workflows",
        "/v1/workspaces/{workspaceId}/templates",
    ):
        assert _spec("POST", path).max_body_bytes == DEFAULT_BODY_MAX_BYTES_PUBLISH


def test_non_publish_writes_use_default_body_cap() -> None:
    # A sub-resource action write (``:deprecate``) is not a publish route, so it
    # keeps the 1 MiB default cap.
    spec = _spec("POST", "/v1/workspaces/{workspaceId}/workflows/{ref}:deprecate")
    assert spec.max_body_bytes == DEFAULT_BODY_MAX_BYTES_DEFAULT


def test_write_routes_require_idempotency_and_write_bucket() -> None:
    for spec in M1_ROUTE_REGISTRY:
        if spec.method in {"POST", "PUT", "PATCH", "DELETE"}:
            assert spec.requires_idempotency_key is True
            assert spec.rate_limit_class is RateLimitClass.WRITE


def test_read_routes_skip_idempotency_and_use_read_bucket() -> None:
    for spec in M1_ROUTE_REGISTRY:
        if spec.method == "GET":
            assert spec.requires_idempotency_key is False
            assert spec.rate_limit_class is RateLimitClass.READ
            assert spec.max_body_bytes == DEFAULT_BODY_MAX_BYTES_DEFAULT


def test_workspace_scoped_routes_name_the_workspace_param() -> None:
    # The workspace resolver extracts ``{workspaceId}``; any workspace-scoped
    # route must use that exact path-parameter name.
    for spec in M1_ROUTE_REGISTRY:
        if "/workspaces/" in spec.path and not spec.path.startswith("/v1/catalog/"):
            assert "{workspaceId}" in spec.path


# --- RouteSpec validation ----------------------------------------------------


def _valid_kwargs() -> dict[str, object]:
    return {
        "method": "GET",
        "path": "/v1/things",
        "app_id": "catalog-service",
        "required_permission": "things:read",
        "requires_idempotency_key": False,
        "max_body_bytes": 1024,
        "rate_limit_class": RateLimitClass.READ,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("method", "get"),
        ("method", "GET1"),
        ("path", "v1/things"),
        ("app_id", ""),
        ("required_permission", ""),
        ("max_body_bytes", 0),
        ("max_body_bytes", -1),
    ],
)
def test_route_spec_rejects_invalid_field(field: str, value: object) -> None:
    kwargs = _valid_kwargs()
    kwargs[field] = value
    with pytest.raises(ValueError):
        RouteSpec(**kwargs)  # type: ignore[arg-type]


# --- router construction -----------------------------------------------------


def test_router_mounts_every_spec() -> None:
    router = build_registry_router()
    mounted = {
        (method, route.path)  # type: ignore[attr-defined]
        for route in router.routes
        for method in route.methods  # type: ignore[attr-defined]
        if method != "HEAD"  # Starlette auto-adds HEAD for every GET route.
    }
    expected = {(spec.method, spec.path) for spec in M1_ROUTE_REGISTRY}
    assert mounted == expected


def test_router_declares_every_permission_for_startup_check() -> None:
    app = FastAPI()
    app.include_router(build_registry_router())
    assert collect_required_permissions(app) == registry_required_permissions()


# --- forwarding seam ---------------------------------------------------------


def _router_with(handler: httpx.MockTransport) -> DownstreamRouter:
    return DownstreamRouter(
        http_client=httpx.AsyncClient(transport=handler),
        host="127.0.0.1",
        http_port=3500,
    )


def _seam_spec(app_id: str, method: str, path: str) -> RouteSpec:
    """Build a minimal :class:`RouteSpec` for exercising the forwarder seam."""
    return RouteSpec(
        method=method,
        path=path,
        app_id=app_id,
        required_permission="things:write" if method != "GET" else "things:read",
        requires_idempotency_key=False,
        max_body_bytes=DEFAULT_BODY_MAX_BYTES_DEFAULT,
        rate_limit_class=RateLimitClass.WRITE if method != "GET" else RateLimitClass.READ,
    )


async def _stamp_auth(request: Request) -> None:
    """Bind a synthetic :class:`AuthorizedCaller` the way ``require_permission`` does."""
    setattr(
        request.state,
        AUTH_STATE_ATTR,
        AuthorizedCaller(
            principal_id="principal-1",
            audit_event_id="audit-1",
            permission="things:write",
            workspace_id="ws-1",
        ),
    )


def test_forwarder_passes_request_through_to_downstream() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["method"] = request.method
        captured["body"] = request.content
        captured["content_type"] = request.headers.get("content-type")
        captured["callctx"] = request.headers.get("x-custos-callctx")
        return httpx.Response(201, headers={"x-downstream": "yes"}, content=b"created")

    app = FastAPI()
    register_exception_handlers(app)

    async def _stamp_metadata(request: Request) -> None:
        setattr(request.state, OUTBOUND_METADATA_STATE_ATTR, {"x-custos-callctx": "signed"})

    app.add_api_route(
        "/v1/workspaces/{workspaceId}/workflows",
        registry._make_forwarder(
            _seam_spec(CATALOG_APP_ID, "POST", "/v1/workspaces/{workspaceId}/workflows")
        ),
        methods=["POST"],
        dependencies=[Depends(_stamp_auth), Depends(_stamp_metadata)],
    )
    app.state.downstream_router = _router_with(httpx.MockTransport(handler))

    client = TestClient(app)
    response = client.post(
        "/v1/workspaces/ws-1/workflows",
        content=b'{"name":"demo"}',
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 201
    assert response.content == b"created"
    assert response.headers["x-downstream"] == "yes"
    assert captured["method"] == "POST"
    assert captured["body"] == b'{"name":"demo"}'
    assert captured["content_type"] == "application/json"
    assert captured["callctx"] == "signed"
    assert str(captured["url"]).endswith(
        "/v1.0/invoke/catalog-service/method/v1/workspaces/ws-1/workflows"
    )


def test_forwarder_carries_query_string() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, content=b"[]")

    app = FastAPI()
    register_exception_handlers(app)
    app.add_api_route(
        "/v1/workspaces/{workspaceId}/runs",
        registry._make_forwarder(
            _seam_spec(WORKFLOW_APP_ID, "GET", "/v1/workspaces/{workspaceId}/runs")
        ),
        methods=["GET"],
        dependencies=[Depends(_stamp_auth)],
    )
    app.state.downstream_router = _router_with(httpx.MockTransport(handler))

    client = TestClient(app)
    response = client.get("/v1/workspaces/ws-1/runs", params={"status": "running", "limit": "10"})

    assert response.status_code == 200
    assert str(captured["url"]).endswith(
        "/v1.0/invoke/workflow-service/method/v1/workspaces/ws-1/runs?status=running&limit=10"
    )


def test_forwarder_returns_503_when_router_unbound() -> None:
    app = FastAPI()
    register_exception_handlers(app)
    app.add_api_route(
        "/v1/workspaces/{workspaceId}/runs",
        registry._make_forwarder(
            _seam_spec(WORKFLOW_APP_ID, "GET", "/v1/workspaces/{workspaceId}/runs")
        ),
        methods=["GET"],
        dependencies=[Depends(_stamp_auth)],
    )

    client = TestClient(app)
    response = client.get("/v1/workspaces/ws-1/runs")

    assert response.status_code == 503


def test_downstream_router_attr_name_is_stable() -> None:
    assert DOWNSTREAM_ROUTER_STATE_ATTR == "downstream_router"
