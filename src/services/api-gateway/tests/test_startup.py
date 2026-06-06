"""Tests for the startup permission validator (AGW-IMPL-008)."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from custos_gateway.app import create_app
from custos_gateway.clients.auth import DeclaredPermission, FakeAuthServiceClient
from custos_gateway.errors import GatewayErrorCode
from custos_gateway.middleware.auth import (
    AUTH_CLIENT_STATE_ATTR,
    require_permission,
    route_required_permission,
)
from custos_gateway.routes.registry import registry_required_permissions
from custos_gateway.settings import Settings
from custos_gateway.startup import (
    GatewayStartupError,
    collect_required_permissions,
    validate_route_permissions,
)


def _permission(name: str) -> DeclaredPermission:
    return DeclaredPermission(name=name, description=name, declared_by="catalog-service")


def _client(*names: str) -> FakeAuthServiceClient:
    return FakeAuthServiceClient(permissions=[_permission(n) for n in names])


def _add_protected_route(app: FastAPI, path: str, permission: str) -> None:
    async def _handler() -> dict[str, bool]:
        return {"ok": True}

    app.add_api_route(
        path,
        _handler,
        methods=["POST"],
        dependencies=[Depends(require_permission(permission))],
    )


# --- route_required_permission ----------------------------------------------


def test_route_required_permission_reads_declared_name() -> None:
    dependency = require_permission("things:write")
    assert route_required_permission(dependency) == "things:write"


def test_route_required_permission_none_for_plain_callable() -> None:
    assert route_required_permission(lambda: None) is None
    assert route_required_permission(object()) is None
    assert route_required_permission(None) is None


# --- collect_required_permissions -------------------------------------------


def test_collect_required_permissions_gathers_route_dependencies() -> None:
    app = FastAPI()
    _add_protected_route(app, "/v1/workspaces/{workspaceId}/things", "things:write")
    _add_protected_route(app, "/v1/workspaces/{workspaceId}/widgets", "widgets:read")
    assert collect_required_permissions(app) == {"things:write", "widgets:read"}


def test_collect_required_permissions_empty_without_protected_routes() -> None:
    app = FastAPI()

    @app.get("/v1/anon")
    async def _anon() -> dict[str, bool]:
        return {"ok": True}

    assert collect_required_permissions(app) == set()


def test_collect_required_permissions_dedupes_repeated_permission() -> None:
    app = FastAPI()
    _add_protected_route(app, "/v1/a", "shared:write")
    _add_protected_route(app, "/v1/b", "shared:write")
    assert collect_required_permissions(app) == {"shared:write"}


# --- validate_route_permissions ---------------------------------------------


async def test_validate_passes_when_all_permissions_declared() -> None:
    app = FastAPI()
    _add_protected_route(app, "/v1/things", "things:write")
    client = _client("things:write", "things:read")

    await validate_route_permissions(app=app, client=client)

    assert client.get_permissions_calls == 1


async def test_validate_skips_registry_fetch_without_protected_routes() -> None:
    app = FastAPI()

    @app.get("/v1/anon")
    async def _anon() -> dict[str, bool]:
        return {"ok": True}

    client = _client("things:write")
    await validate_route_permissions(app=app, client=client)

    # No route declares a permission → no need to consult the registry.
    assert client.get_permissions_calls == 0


async def test_validate_raises_for_undeclared_permission() -> None:
    app = FastAPI()
    _add_protected_route(app, "/v1/things", "things:write")
    _add_protected_route(app, "/v1/ghosts", "ghosts:summon")
    client = _client("things:write")

    with pytest.raises(GatewayStartupError) as exc_info:
        await validate_route_permissions(app=app, client=client)

    assert exc_info.value.code is GatewayErrorCode.GATEWAY_STARTUP_PERMISSION_MISSING
    assert "ghosts:summon" in str(exc_info.value)
    assert "things:write" not in str(exc_info.value)


async def test_validate_lists_missing_permissions_sorted() -> None:
    app = FastAPI()
    _add_protected_route(app, "/v1/b", "b:write")
    _add_protected_route(app, "/v1/a", "a:write")
    client = _client()  # empty registry

    with pytest.raises(GatewayStartupError) as exc_info:
        await validate_route_permissions(app=app, client=client)

    message = str(exc_info.value)
    assert message.index("a:write") < message.index("b:write")


# --- create_app lifespan wiring ---------------------------------------------


def test_lifespan_validates_and_binds_client(settings: Settings) -> None:
    # create_app mounts the full M1 registry, so the injected client must declare
    # every registry permission plus the extra protected route added below.
    client = _client("things:write", *registry_required_permissions())
    app = create_app(settings=settings, auth_client=client)
    _add_protected_route(app, "/v1/workspaces/{workspaceId}/things", "things:write")

    with TestClient(app) as http:
        assert http.get("/readyz").status_code == 200

    assert getattr(app.state, AUTH_CLIENT_STATE_ATTR) is client
    assert client.get_permissions_calls == 1


def test_lifespan_refuses_to_start_on_undeclared_permission(settings: Settings) -> None:
    client = _client()  # empty registry
    app = create_app(settings=settings, auth_client=client)
    _add_protected_route(app, "/v1/workspaces/{workspaceId}/things", "things:write")

    with pytest.raises(GatewayStartupError) as exc_info, TestClient(app):
        pass

    assert exc_info.value.code is GatewayErrorCode.GATEWAY_STARTUP_PERMISSION_MISSING
    assert app.state.ready is False


def test_lifespan_without_auth_client_skips_validation(settings: Settings) -> None:
    app = create_app(settings=settings)
    with TestClient(app) as http:
        assert http.get("/readyz").status_code == 200
    assert not hasattr(app.state, AUTH_CLIENT_STATE_ATTR)
