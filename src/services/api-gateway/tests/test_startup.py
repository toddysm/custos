"""Tests for the startup permission validator (AGW-IMPL-008, AGW-IMPL-022)."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from custos_gateway.app import create_app
from custos_gateway.clients.auth import (
    AuthServiceClientError,
    AuthServiceClientStatusError,
    AuthServiceClientTransportError,
    DeclaredPermission,
    FakeAuthServiceClient,
)
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
    StartupCheckOutcome,
    collect_required_permissions,
    converge_route_permissions,
    run_startup_permission_check,
    validate_route_permissions,
)


class _FlakyAuthServiceClient(FakeAuthServiceClient):
    """Auth client that raises ``error`` for the first ``fail_times`` permission
    fetches, then serves the declared registry — emulating an Auth Service / Dapr
    sidecar that is unreachable at boot and recovers later (issue #815).
    """

    def __init__(
        self,
        *names: str,
        fail_times: int,
        error: AuthServiceClientError | None = None,
    ) -> None:
        super().__init__(permissions=[_permission(n) for n in names])
        self._fail_times = fail_times
        self._error = error or AuthServiceClientTransportError("sidecar not ready")

    async def get_permissions(self) -> list[DeclaredPermission]:
        if self.get_permissions_calls < self._fail_times:
            self.get_permissions_calls += 1
            raise self._error
        return await super().get_permissions()


class _RecordingSleeper:
    """Awaitable sleep stub that records the requested delays without waiting."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


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
    # Permission drift is a *permanent* failure, but per issue #815 the gateway
    # must not crash-loop: it stays up and permanently not-ready, surfacing an
    # operator-actionable /readyz detail instead of aborting the lifespan.
    client = _client()  # empty registry
    app = create_app(settings=settings, auth_client=client)
    _add_protected_route(app, "/v1/workspaces/{workspaceId}/things", "things:write")

    with TestClient(app) as http:
        response = http.get("/readyz")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert "startup permission check failed" in body["detail"]
    assert app.state.ready is False


def test_lifespan_builds_default_auth_client(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    # With no auth_client injected the lifespan owns a Dapr-backed client and
    # still runs the startup cross-check. Patch the client class so the cross-
    # check consults a fake registry instead of opening a socket.
    fake = _client(*registry_required_permissions())
    monkeypatch.setattr("custos_gateway.app.DaprAuthServiceClient", lambda **_: fake)
    app = create_app(settings=settings)

    with TestClient(app) as http:
        assert http.get("/readyz").status_code == 200

    assert getattr(app.state, AUTH_CLIENT_STATE_ATTR) is fake
    assert fake.get_permissions_calls == 1


def test_lifespan_stays_up_when_auth_initially_unreachable(settings: Settings) -> None:
    # A transient Auth Service / Dapr-sidecar outage at boot must not crash the
    # process (issue #815): the gateway comes up not-ready with a transport
    # detail and converges in the background. ``fail_times`` is effectively
    # unbounded here so the background task is still running — and therefore
    # cancelled cleanly — when the lifespan shuts down.
    client = _FlakyAuthServiceClient(
        "things:write", *registry_required_permissions(), fail_times=10_000
    )
    app = create_app(settings=settings, auth_client=client)
    _add_protected_route(app, "/v1/workspaces/{workspaceId}/things", "things:write")

    with TestClient(app) as http:
        response = http.get("/readyz")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert "auth-service unreachable" in body["detail"]
    assert app.state.ready is False


# --- run_startup_permission_check -------------------------------------------


async def test_run_startup_check_ready_on_success() -> None:
    app = FastAPI()
    app.state.ready = False
    _add_protected_route(app, "/v1/things", "things:write")
    client = _client("things:write")

    outcome = await run_startup_permission_check(app=app, client=client)

    assert outcome is StartupCheckOutcome.READY
    assert app.state.ready is True
    assert app.state.ready_detail is None


async def test_run_startup_check_transient_on_transport_error() -> None:
    app = FastAPI()
    app.state.ready = False
    _add_protected_route(app, "/v1/things", "things:write")
    client = _FlakyAuthServiceClient("things:write", fail_times=1)

    outcome = await run_startup_permission_check(app=app, client=client)

    assert outcome is StartupCheckOutcome.TRANSIENT_FAILURE
    assert app.state.ready is False
    assert "auth-service unreachable" in app.state.ready_detail


async def test_run_startup_check_permanent_on_permission_drift() -> None:
    app = FastAPI()
    app.state.ready = False
    _add_protected_route(app, "/v1/ghosts", "ghosts:summon")
    client = _client()  # empty registry

    outcome = await run_startup_permission_check(app=app, client=client)

    assert outcome is StartupCheckOutcome.PERMANENT_FAILURE
    assert app.state.ready is False
    assert "startup permission check failed" in app.state.ready_detail


async def test_run_startup_check_permanent_on_non_retryable_status() -> None:
    app = FastAPI()
    app.state.ready = False
    _add_protected_route(app, "/v1/things", "things:write")
    client = _FlakyAuthServiceClient(
        "things:write",
        fail_times=1,
        error=AuthServiceClientStatusError("bad request", status_code=400),
    )

    outcome = await run_startup_permission_check(app=app, client=client)

    assert outcome is StartupCheckOutcome.PERMANENT_FAILURE
    assert app.state.ready is False
    assert "contract error" in app.state.ready_detail


# --- converge_route_permissions ---------------------------------------------


async def test_converge_recovers_after_transient_failures() -> None:
    app = FastAPI()
    app.state.ready = False
    _add_protected_route(app, "/v1/things", "things:write")
    client = _FlakyAuthServiceClient("things:write", fail_times=2)
    sleeper = _RecordingSleeper()

    await converge_route_permissions(
        app=app,
        client=client,
        initial_backoff_seconds=1,
        max_backoff_seconds=4,
        sleep=sleeper,
    )

    assert app.state.ready is True
    assert app.state.ready_detail is None
    # Three attempts (two transient + one success); backoff doubles and caps at 4.
    assert client.get_permissions_calls == 3
    assert sleeper.delays == [1, 2, 4]


async def test_converge_stops_on_permanent_failure() -> None:
    app = FastAPI()
    app.state.ready = False
    _add_protected_route(app, "/v1/ghosts", "ghosts:summon")
    client = _client()  # empty registry → permanent drift, never recoverable
    sleeper = _RecordingSleeper()

    await converge_route_permissions(
        app=app,
        client=client,
        initial_backoff_seconds=1,
        max_backoff_seconds=4,
        sleep=sleeper,
    )

    assert app.state.ready is False
    assert "startup permission check failed" in app.state.ready_detail
    # Stops after the first (permanent) attempt — no infinite retry on drift.
    assert client.get_permissions_calls == 1
    assert sleeper.delays == [1]
