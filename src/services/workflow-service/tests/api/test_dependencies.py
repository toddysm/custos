"""Tests for the FastAPI dependency factories (WF-IMPL-064).

Pins:

* Every factory returns the bundled component when ``app.state`` is
  wired the way :func:`custos_workflow.create_app` wires it.
* Missing-state failures raise
  :class:`~custos_workflow.runs.errors.WorkflowRuntimeUnavailableError`,
  which the existing WF-IMPL-061 handler chain renders as a 503
  :class:`~custos_workflow.api.errors.ProblemDetail` with the
  ``workflow.workflow_runtime_unavailable`` kind — never a 500.
* The dependencies are usable from a FastAPI route through
  :class:`fastapi.Depends`, exercised end-to-end with
  :class:`httpx.AsyncClient` so the 503 envelope is observed on the
  wire as well as at the function level.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import Depends, FastAPI, Request
from httpx import ASGITransport

from custos_workflow.api import (
    get_call_context,
    get_run_components,
    get_run_controller,
    get_validator,
    register_exception_handlers,
    workspace_path,
)
from custos_workflow.call_context import CallContext
from custos_workflow.providers import RunComponents
from custos_workflow.runs.controller import RunController
from custos_workflow.runs.errors import WorkflowRuntimeUnavailableError
from custos_workflow.validator import (
    InMemoryIdempotencyLedger,
    StartRunValidator,
)

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _fake_run_components() -> RunComponents:
    """Return a :class:`RunComponents` whose attributes are all stubs.

    The dependency factories never call any method on the bundle —
    they hand the singleton back to the caller — so plain
    :class:`AsyncMock` placeholders are enough to satisfy the
    ``isinstance`` check on the bundle itself and on the controller
    type. The :class:`RunController` is the only attribute the
    tests need to assert on, so it is the only field with a
    concrete spec.
    """
    controller = AsyncMock(spec=RunController)
    return RunComponents(
        workflow_runtime=AsyncMock(),
        workflow_client=AsyncMock(),
        run_store=AsyncMock(),
        lifecycle_publisher=AsyncMock(),
        replay_reconciler=AsyncMock(),
        run_controller=controller,
        activity_client=AsyncMock(),
        connector_client=AsyncMock(),
    )


def _fake_validator() -> StartRunValidator:
    """Return a real :class:`StartRunValidator` with stub collaborators."""
    return StartRunValidator(
        catalog=AsyncMock(),
        ledger=InMemoryIdempotencyLedger(),
    )


def _request_with_app_state(**state: Any) -> Request:
    """Build a minimal :class:`Request` whose ``app.state`` carries ``state``.

    The dependency factories only touch
    ``request.app.state.<attr>`` and ``request.state.<attr>``; a
    full ASGI scope is unnecessary for the function-level tests.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "headers": [],
        "query_string": b"",
        "path": "/v1/workspaces/ws-a/runs",
    }
    request = Request(scope)
    app_state = type("AppState", (), {})()
    for name, value in state.items():
        setattr(app_state, name, value)
    app = type("AppShim", (), {"state": app_state})()
    request.scope["app"] = app
    return request


# ---------------------------------------------------------------------------
# get_run_components
# ---------------------------------------------------------------------------


def test_get_run_components_returns_bundle_when_wired() -> None:
    """Happy path: ``app.state.run_components`` is honoured verbatim."""
    bundle = _fake_run_components()
    request = _request_with_app_state(run_components=bundle)
    assert get_run_components(request) is bundle


def test_get_run_components_raises_runtime_unavailable_when_missing() -> None:
    """Missing state raises the 503-mapped run-controller error, not 500."""
    request = _request_with_app_state()
    with pytest.raises(WorkflowRuntimeUnavailableError) as info:
        get_run_components(request)
    assert "run_components" in str(info.value)


def test_get_run_components_rejects_wrong_type() -> None:
    """A non-:class:`RunComponents` binding is also a 503, not a 500."""
    request = _request_with_app_state(run_components="not-a-bundle")
    with pytest.raises(WorkflowRuntimeUnavailableError) as info:
        get_run_components(request)
    assert "str" in str(info.value) or "expected RunComponents" in str(info.value)


# ---------------------------------------------------------------------------
# get_run_controller
# ---------------------------------------------------------------------------


def test_get_run_controller_returns_bundle_controller() -> None:
    """The transitive :func:`get_run_components` dependency feeds the
    controller through; the factory must not rebuild anything."""
    bundle = _fake_run_components()
    assert get_run_controller(components=bundle) is bundle.run_controller


# ---------------------------------------------------------------------------
# get_validator
# ---------------------------------------------------------------------------


def test_get_validator_returns_app_state_validator() -> None:
    """Happy path: ``app.state.start_run_validator`` is returned verbatim."""
    validator = _fake_validator()
    request = _request_with_app_state(start_run_validator=validator)
    assert get_validator(request) is validator


def test_get_validator_raises_runtime_unavailable_when_missing() -> None:
    """Missing validator raises the 503-mapped error, not a 500."""
    request = _request_with_app_state()
    with pytest.raises(WorkflowRuntimeUnavailableError) as info:
        get_validator(request)
    assert "start_run_validator" in str(info.value)


def test_get_validator_rejects_wrong_type() -> None:
    """A non-:class:`StartRunValidator` binding is a 503, not a 500."""
    request = _request_with_app_state(start_run_validator="not-a-validator")
    with pytest.raises(WorkflowRuntimeUnavailableError) as info:
        get_validator(request)
    assert "str" in str(info.value) or "expected StartRunValidator" in str(info.value)


# ---------------------------------------------------------------------------
# get_call_context
# ---------------------------------------------------------------------------


def test_get_call_context_returns_request_state_context() -> None:
    """Happy path: ``request.state.call_context`` is returned verbatim."""
    ctx = CallContext(workspace="ws-a", principal="user-1")
    request = _request_with_app_state()
    request.state.call_context = ctx
    assert get_call_context(request) is ctx


def test_get_call_context_raises_runtime_unavailable_when_middleware_missing() -> None:
    """Absent middleware is a misconfiguration; surface as 503 not 500."""
    request = _request_with_app_state()
    with pytest.raises(WorkflowRuntimeUnavailableError) as info:
        get_call_context(request)
    assert "call_context" in str(info.value)


def test_get_call_context_rejects_wrong_type() -> None:
    """Foreign value on ``request.state.call_context`` is a 503."""
    request = _request_with_app_state()
    request.state.call_context = "not-a-callctx"
    with pytest.raises(WorkflowRuntimeUnavailableError) as info:
        get_call_context(request)
    assert "expected CallContext" in str(info.value)


# ---------------------------------------------------------------------------
# workspace_path
# ---------------------------------------------------------------------------


def test_workspace_path_returns_segment_verbatim() -> None:
    """The default behaviour echoes the URL segment as-is."""
    assert workspace_path(ws="ws-a") == "ws-a"


# ---------------------------------------------------------------------------
# End-to-end: 503 envelope is rendered on the wire
# ---------------------------------------------------------------------------


def _build_probe_app(*, wire_state: bool) -> FastAPI:
    """Build a one-route FastAPI app that exercises every accessor.

    When ``wire_state`` is true, the app's ``state`` carries all the
    fields the dependency factories expect; when false, every
    factory raises and the WF-IMPL-061 exception handlers render
    the 503 envelope. The single route depends on every factory so
    one failure surfaces immediately.
    """
    app = FastAPI()
    register_exception_handlers(app)

    if wire_state:
        app.state.run_components = _fake_run_components()
        app.state.start_run_validator = _fake_validator()

    @app.get("/probe/{ws}")
    async def _probe(
        request: Request,
        ws: str = Depends(workspace_path),
        components: RunComponents = Depends(get_run_components),
        controller: RunController = Depends(get_run_controller),
        validator: StartRunValidator = Depends(get_validator),
    ) -> dict[str, str | bool]:
        # Test middleware attaches the call-context lazily so the
        # missing-middleware path can be exercised separately. Wire
        # it here when the app is fully configured.
        request.state.call_context = CallContext(workspace=ws, principal="user-1")
        ctx = get_call_context(request)
        # The bundle attaches an ``AsyncMock(spec=RunController)`` for
        # the controller, so ``type(...).__name__`` is "AsyncMock";
        # ``isinstance`` is the contract the dependency advertises.
        return {
            "workspace": ws,
            "controller_is_run_controller": isinstance(controller, RunController),
            "validator_is_start_run_validator": isinstance(validator, StartRunValidator),
            "components_is_run_components": isinstance(components, RunComponents),
            "context": ctx.workspace or "<unset>",
        }

    return app


@pytest.fixture
def _wired_client() -> Any:
    """ASGI client with a fully-wired app."""
    app = _build_probe_app(wire_state=True)
    transport = ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture
def _bare_client() -> Any:
    """ASGI client whose app has no ``run_components`` on state."""
    app = _build_probe_app(wire_state=False)
    transport = ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_wired_app_renders_happy_path(_wired_client: httpx.AsyncClient) -> None:
    """A fully-wired app surfaces every dependency."""
    async with _wired_client as client:
        resp = await client.get("/probe/ws-a")
    assert resp.status_code == 200
    body = resp.json()
    assert body["workspace"] == "ws-a"
    assert body["controller_is_run_controller"] is True
    assert body["validator_is_start_run_validator"] is True
    assert body["components_is_run_components"] is True
    assert body["context"] == "ws-a"


async def test_bare_app_renders_503_problem_envelope(_bare_client: httpx.AsyncClient) -> None:
    """A bare app (no lifespan run) yields a 503 RFC 7807 envelope.

    The envelope must carry the locked
    ``workflow.workflow_runtime_unavailable`` kind so SDK branch
    logic stays uniform with the rest of the public taxonomy.
    """
    async with _bare_client as client:
        resp = await client.get("/probe/ws-a")
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    assert body["status"] == 503
    assert body["code"] == "workflow.workflow_runtime_unavailable"
    assert "run_components" in body["detail"]
