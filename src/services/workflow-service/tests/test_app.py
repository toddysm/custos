"""Tests for ``custos_workflow.create_app`` (WF-IMPL-015).

These tests assert the factory shape — routes, middleware, lifespan
behaviour — without exercising the request path semantics (those live
in ``test_healthz.py`` and ``test_call_context.py``).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_workflow import create_app
from custos_workflow.call_context import CallContextMiddleware
from custos_workflow.providers import RunComponents


def test_create_app_returns_fastapi_with_expected_metadata() -> None:
    app = create_app(require_call_context=False)
    assert isinstance(app, FastAPI)
    assert app.title == "Custos Workflow Service"
    assert app.version == "0.1.0"


def test_create_app_registers_health_routes() -> None:
    app = create_app(require_call_context=False)
    paths = {route.path for route in app.routes if hasattr(route, "path")}
    assert "/healthz" in paths
    assert "/readyz" in paths


def test_create_app_installs_call_context_middleware() -> None:
    app = create_app(require_call_context=False)
    middleware_classes = [m.cls for m in app.user_middleware]
    assert CallContextMiddleware in middleware_classes  # type: ignore[comparison-overlap]


def test_lifespan_flips_ready_to_true(fake_run_components: RunComponents) -> None:
    app = create_app(require_call_context=False, run_components=fake_run_components)
    with TestClient(app) as client:
        # Inside the ``with`` block FastAPI has run the lifespan startup.
        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}
        assert app.state.ready is True
        assert app.state.ready_detail is None


@pytest.fixture
def _clean_env() -> Iterator[None]:
    """Snapshot + restore ``WF_REQUIRE_CALL_CONTEXT`` around env-driven tests."""
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("WF_REQUIRE_CALL_CONTEXT", None)
        yield


def test_create_app_honours_env_flag_dev_default(
    _clean_env: None, fake_run_components: RunComponents
) -> None:
    """Unset env → dev shim; create_app with no explicit flag does not raise."""
    app = create_app(run_components=fake_run_components)
    # Smoke through TestClient to confirm the middleware is installed in the
    # dev shape (no headers required).
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200


def test_create_app_honours_env_flag_production(
    _clean_env: None, fake_run_components: RunComponents
) -> None:
    """``WF_REQUIRE_CALL_CONTEXT=1`` → production-mode middleware."""
    os.environ["WF_REQUIRE_CALL_CONTEXT"] = "1"
    app = create_app(run_components=fake_run_components)
    with TestClient(app) as client:
        # /healthz still passes — probes are bypassed even in production mode.
        assert client.get("/healthz").status_code == 200
        # A non-bypass route without headers would be 401, but WF-IMPL-015
        # has no non-probe routes yet, so we assert the middleware
        # construction picked up the strict flag via attribute inspection.
    installed = next(
        m
        for m in app.user_middleware
        if m.cls is CallContextMiddleware  # type: ignore[comparison-overlap]
    )
    # Starlette stores the kwargs on the wrapper's ``kwargs`` mapping.
    assert installed.kwargs == {"require_call_context": True}


def test_create_app_env_flag_only_truthy_on_exact_1(_clean_env: None) -> None:
    """``true`` / ``yes`` must NOT silently flip into production mode."""
    for non_truthy in ("true", "yes", "TRUE", "1 ", "", "0"):
        os.environ["WF_REQUIRE_CALL_CONTEXT"] = non_truthy
        app = create_app()
        installed = next(
            m
            for m in app.user_middleware
            if m.cls is CallContextMiddleware  # type: ignore[comparison-overlap]
        )
        assert installed.kwargs == {"require_call_context": False}, (
            f"WF_REQUIRE_CALL_CONTEXT={non_truthy!r} must not enable production "
            "mode — only the literal string '1' may flip the gate."
        )


def test_explicit_kwarg_overrides_env(_clean_env: None) -> None:
    """The explicit ``require_call_context`` kwarg wins over the env var."""
    os.environ["WF_REQUIRE_CALL_CONTEXT"] = "1"
    app = create_app(require_call_context=False)
    installed = next(
        m
        for m in app.user_middleware
        if m.cls is CallContextMiddleware  # type: ignore[comparison-overlap]
    )
    assert installed.kwargs == {"require_call_context": False}


# ---------------------------------------------------------------------------
# WF-IMPL-043: FastAPI lifespan worker wiring
# ---------------------------------------------------------------------------


class _RecordingFakeRuntime:
    """Adapter wrapping :class:`FakeWorkflowRuntime` for lifespan probes.

    Records every lifecycle method invocation so tests can assert
    the lifespan calls them in the documented order (register →
    start → wait_for_worker_ready → shutdown). The lifecycle
    methods delegate to the underlying fake so the structural
    :class:`~custos_workflow.providers.WorkflowRuntimeProtocol`
    contract still holds.
    """

    def __init__(
        self,
        *,
        wait_returns: bool = True,
        start_raises: BaseException | None = None,
        shutdown_raises: BaseException | None = None,
        shutdown_hangs: bool = False,
    ) -> None:
        from custos_workflow.runtime import FakeWorkflowRuntime

        self._inner = FakeWorkflowRuntime()
        self._wait_returns = wait_returns
        self._start_raises = start_raises
        self._shutdown_raises = shutdown_raises
        self._shutdown_hangs = shutdown_hangs
        self.calls: list[str] = []
        self.registered: list[tuple[str, Any]] = []

    def register_workflow(self, fn: Any, *, name: str | None = None) -> None:
        self.calls.append("register_workflow")
        self.registered.append((name or fn.__name__, fn))
        self._inner.register_workflow(fn, name=name)

    async def start(self) -> None:
        self.calls.append("start")
        if self._start_raises is not None:
            raise self._start_raises
        await self._inner.start()

    async def shutdown(self) -> None:
        self.calls.append("shutdown")
        if self._shutdown_hangs:
            import asyncio as _aio

            await _aio.sleep(3600)  # outlive any plausible test timeout
        if self._shutdown_raises is not None:
            raise self._shutdown_raises
        await self._inner.shutdown()

    async def wait_for_worker_ready(self, *, timeout: float = 30.0) -> bool:
        self.calls.append("wait_for_worker_ready")
        # Mirror the fake's latching semantics on the truthy path so
        # ``is_ready`` flips correctly downstream.
        if self._wait_returns:
            await self._inner.wait_for_worker_ready(timeout=timeout)
        return self._wait_returns

    @property
    def is_ready(self) -> bool:
        return self._inner.is_ready

    def client(self) -> Any:
        return self._inner.client()


def _components_with(runtime: _RecordingFakeRuntime) -> RunComponents:
    from custos_workflow.providers import load_run_components

    return load_run_components(env={}, workflow_runtime=runtime)


def test_lifespan_raises_when_workflow_component_env_missing(_clean_env: None) -> None:
    """Without an injected bundle, missing env var fails the lifespan.

    Import must remain side-effect-free — the factory itself does
    not consult the env var — and the failure must happen inside
    the lifespan startup with a message that names the missing
    variable.
    """
    os.environ.pop("WF_DAPR_WORKFLOW_COMPONENT", None)
    # Construction is side-effect-free even with the env var unset.
    app = create_app(require_call_context=False)
    with pytest.raises(RuntimeError, match="WF_DAPR_WORKFLOW_COMPONENT"), TestClient(app):
        pass  # pragma: no cover - lifespan startup must raise above


def test_lifespan_registers_orchestrator_and_starts_runtime(
    fake_run_components: RunComponents,
) -> None:
    """Lifespan registers ``run_orchestrator`` and starts the worker."""
    from custos_workflow.runs.orchestrator import WORKFLOW_NAME

    runtime = _RecordingFakeRuntime()
    components = _components_with(runtime)
    app = create_app(require_call_context=False, run_components=components)
    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200
    # Lifecycle ordering: register → start → wait → shutdown
    assert runtime.calls == ["register_workflow", "start", "wait_for_worker_ready", "shutdown"]
    # The orchestrator is registered under the canonical workflow name.
    assert any(name == WORKFLOW_NAME for name, _fn in runtime.registered)


def test_lifespan_exposes_run_components_on_app_state(
    fake_run_components: RunComponents,
) -> None:
    """The full bundle is reachable from ``app.state.run_components``."""
    app = create_app(require_call_context=False, run_components=fake_run_components)
    with TestClient(app):
        bundle = app.state.run_components
    assert bundle is fake_run_components
    # API-layer dependencies the lifespan promises to wire:
    assert bundle.run_controller is not None
    assert bundle.run_store is not None
    assert bundle.lifecycle_publisher is not None
    assert bundle.replay_reconciler is not None


def test_readyz_503_when_worker_never_reports_ready() -> None:
    """``wait_for_worker_ready`` returning False keeps ``/readyz`` at 503.

    The WF-IMPL-043 acceptance criterion requires the readiness
    gate to track ``workflow_runtime.is_ready()``. We assert that
    the lifespan completes (so requests can be served) but
    ``/readyz`` still reports 503 with an operator-actionable
    detail string naming the timeout.
    """
    runtime = _RecordingFakeRuntime(wait_returns=False)
    components = _components_with(runtime)
    app = create_app(
        require_call_context=False,
        run_components=components,
        worker_ready_timeout_s=5.0,
    )
    with TestClient(app) as client:
        response = client.get("/readyz")
    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "not_ready"
    assert "5.0" in payload["detail"]
    assert app.state.ready is False


def test_lifespan_swallows_runtime_shutdown_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``runtime.shutdown()`` must not crash the lifespan exit."""
    runtime = _RecordingFakeRuntime(shutdown_raises=RuntimeError("simulated"))
    components = _components_with(runtime)
    app = create_app(require_call_context=False, run_components=components)
    caplog.set_level("ERROR", logger="custos_workflow")
    # The lifespan exit must complete cleanly even though shutdown raises.
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
    assert "shutdown" in runtime.calls
    # The exception is logged (not raised) so operators can correlate.
    assert any("workflow runtime shutdown raised" in r.getMessage() for r in caplog.records)


def test_lifespan_swallows_runtime_shutdown_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A slow ``runtime.shutdown()`` is abandoned at the grace deadline."""
    runtime = _RecordingFakeRuntime(shutdown_hangs=True)
    components = _components_with(runtime)
    app = create_app(
        require_call_context=False,
        run_components=components,
        worker_shutdown_timeout_s=0.05,
    )
    caplog.set_level("ERROR", logger="custos_workflow")
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
    assert any("workflow runtime shutdown exceeded" in r.getMessage() for r in caplog.records)


def test_lifespan_handles_runtime_start_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``runtime.start()`` is logged but does not crash the lifespan.

    ``/readyz`` keeps reporting 503 and the bundle is still
    pinned on ``app.state.run_components`` so the API layer can
    surface useful errors on every request.
    """
    runtime = _RecordingFakeRuntime(start_raises=RuntimeError("dapr is asleep"))
    components = _components_with(runtime)
    app = create_app(require_call_context=False, run_components=components)
    caplog.set_level("ERROR", logger="custos_workflow")
    with TestClient(app) as client:
        # Lifespan completes, /readyz still 503, app.state has the bundle.
        response = client.get("/readyz")
        assert response.status_code == 503
        assert app.state.ready is False
        assert app.state.run_components is components
    assert any("failed to start" in r.getMessage() for r in caplog.records)


def test_create_app_is_import_side_effect_free_with_production_env(
    _clean_env: None,
) -> None:
    """Constructing the app must not start the worker, even when env vars are set.

    Both ``WF_REQUIRE_CALL_CONTEXT=1`` and
    ``WF_DAPR_WORKFLOW_COMPONENT`` are set; the factory must
    return without contacting the runtime. The worker only fires
    inside the lifespan, where the env-driven defaults would then
    fail because there is no Dapr sidecar in the test environment.
    """
    os.environ["WF_REQUIRE_CALL_CONTEXT"] = "1"
    os.environ["WF_DAPR_WORKFLOW_COMPONENT"] = "wf-component"
    app = create_app()
    # The factory returned a configured app object with no app.state.ready set,
    # confirming the lifespan has not yet been entered.
    assert not getattr(app.state, "ready", False)
    assert not hasattr(app.state, "run_components")


def _components_with_owned_http_client(
    runtime: _RecordingFakeRuntime,
    http_client: Any,
) -> RunComponents:
    """Build a bundle that pins ``http_client`` as the owned Dapr HTTP client."""
    from dataclasses import replace

    base = _components_with(runtime)
    return replace(base, dapr_http_client=http_client)


def test_lifespan_closes_owned_dapr_http_client_on_shutdown() -> None:
    """The lifespan must release any HTTP client owned by the bundle.

    When ``WF_PUBLISH_TOPIC`` is set the publisher factory owns
    its own :class:`httpx.AsyncClient`; the lifespan is the only
    party that knows the bundle is being torn down, so it owns
    the corresponding ``aclose()`` call.
    """
    closed = {"n": 0}

    class _RecordingClient:
        async def aclose(self) -> None:
            closed["n"] += 1

    runtime = _RecordingFakeRuntime()
    components = _components_with_owned_http_client(runtime, _RecordingClient())
    app = create_app(require_call_context=False, run_components=components)
    with TestClient(app):
        assert closed["n"] == 0  # not closed until lifespan exit
    assert closed["n"] == 1


def test_lifespan_swallows_dapr_http_client_close_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``http_client.aclose()`` must not crash the lifespan exit."""

    class _ExplodingClient:
        async def aclose(self) -> None:
            raise RuntimeError("connection refused")

    runtime = _RecordingFakeRuntime()
    components = _components_with_owned_http_client(runtime, _ExplodingClient())
    app = create_app(require_call_context=False, run_components=components)
    caplog.set_level("ERROR", logger="custos_workflow")
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
    assert any("dapr publisher http client aclose failed" in r.getMessage() for r in caplog.records)


def test_lifespan_closes_workflow_client_on_shutdown() -> None:
    """The lifespan must call ``workflow_client.aclose()`` if present.

    The default Dapr-backed ``WorkflowClient`` opens a lazy gRPC
    channel via ``_ensure_client()`` the first time the API layer
    issues a workflow RPC; the lifespan is the only party that
    knows the bundle is being torn down and so owns the matching
    ``aclose`` call.
    """
    from dataclasses import replace

    closed = {"n": 0}

    class _RecordingWorkflowClient:
        async def aclose(self) -> None:
            closed["n"] += 1

    runtime = _RecordingFakeRuntime()
    base = _components_with(runtime)
    components = replace(base, workflow_client=_RecordingWorkflowClient())  # type: ignore[arg-type]
    app = create_app(require_call_context=False, run_components=components)
    with TestClient(app):
        assert closed["n"] == 0  # not closed until lifespan exit
    assert closed["n"] == 1


def test_lifespan_swallows_workflow_client_close_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``workflow_client.aclose()`` must not crash the lifespan exit."""
    from dataclasses import replace

    class _ExplodingWorkflowClient:
        async def aclose(self) -> None:
            raise RuntimeError("grpc channel already torn down")

    runtime = _RecordingFakeRuntime()
    base = _components_with(runtime)
    components = replace(base, workflow_client=_ExplodingWorkflowClient())  # type: ignore[arg-type]
    app = create_app(require_call_context=False, run_components=components)
    caplog.set_level("ERROR", logger="custos_workflow")
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
    assert any("workflow client aclose failed" in r.getMessage() for r in caplog.records)
