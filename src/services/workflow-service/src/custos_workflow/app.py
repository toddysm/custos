"""FastAPI application factory for the Workflow Service (WF-IMPL-015, WF-IMPL-043).

The factory is import-safe: no environment lookups beyond the
``WF_REQUIRE_CALL_CONTEXT`` flag, no socket connections, no Postgres
DSN parsing. All side-effecting work (workflow runtime construction,
worker start, Dapr Pub/Sub HTTP client construction) lands inside
the FastAPI lifespan.

The exposed surface is:

- ``GET /healthz`` — flat liveness probe.
- ``GET /readyz`` — readiness gate (503 until the lifespan flips
  ``app.state.ready``; the lifespan flips that flag only after the
  Dapr Workflow worker reports ready via
  :attr:`WorkflowRuntimeProtocol.is_ready`).
- :class:`custos_workflow.call_context.CallContextMiddleware` —
  extracts ``X-Custos-Workspace`` / ``X-Custos-Principal`` headers
  onto ``request.state.call_context`` (dev shim by default;
  production mode opts in via ``WF_REQUIRE_CALL_CONTEXT=1``).

WF-IMPL-043 extends the lifespan to:

- Read ``WF_DAPR_WORKFLOW_COMPONENT`` and fail-fast with
  :class:`RuntimeError` if unset (lifespan startup, not import).
- Build (or accept an injected) :class:`WorkflowRuntimeProtocol`,
  register the WF-IMPL-035 ``run_orchestrator`` workflow function
  on it, start the worker, and wait for worker readiness.
- Inject the full :class:`RunComponents` bundle onto
  ``app.state.run_components`` so the API layer (WF-IMPL-037+) can
  pull every collaborator off a single attribute.
- Stop the worker with a 10 s grace period on shutdown; never
  crash the lifespan on worker-shutdown errors.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_workflow._version import __version__
from custos_workflow.call_context import CallContextMiddleware
from custos_workflow.healthz import router as health_router
from custos_workflow.providers import (
    ENV_DAPR_WORKFLOW_COMPONENT,
    RunComponents,
    load_run_components,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME, make_run_orchestrator
from custos_workflow.runs.step_handler import NoopStepHandler

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["create_app"]

logger = logging.getLogger("custos_workflow")


#: Maximum seconds the lifespan blocks waiting for the worker to
#: report ready. Matches the Dapr SDK default and the Kubernetes
#: readinessProbe ``initialDelaySeconds`` budget the design.md
#: § Configuration entry assumes.
DEFAULT_WORKER_READY_TIMEOUT_S = 30.0

#: Maximum seconds the lifespan waits for ``runtime.shutdown()`` on
#: pod termination. design.md § Configuration calls this the "10 s
#: grace period"; tuned to fit inside the default Kubernetes
#: ``terminationGracePeriodSeconds`` of 30 s while leaving slack for
#: HTTP client aclose() and the WSGI server's own drain.
DEFAULT_WORKER_SHUTDOWN_TIMEOUT_S = 10.0


def _read_require_call_context_flag() -> bool:
    """Resolve ``WF_REQUIRE_CALL_CONTEXT`` to a bool.

    Truthy only on the exact string ``"1"`` so accidental values like
    ``"true"`` / ``"yes"`` do not silently flip production-mode
    behaviour. The strict comparison matches the design.md § Configuration
    convention used by sibling services.
    """
    return os.environ.get("WF_REQUIRE_CALL_CONTEXT", "") == "1"


def create_app(
    *,
    require_call_context: bool | None = None,
    run_components: RunComponents | None = None,
    worker_ready_timeout_s: float = DEFAULT_WORKER_READY_TIMEOUT_S,
    worker_shutdown_timeout_s: float = DEFAULT_WORKER_SHUTDOWN_TIMEOUT_S,
) -> FastAPI:
    """Build and return the Workflow Service FastAPI application.

    Args:
        require_call_context: Override for the ``WF_REQUIRE_CALL_CONTEXT``
            env-driven default. Tests pass this explicitly; production
            and CI rely on the env-var path. The flag is only consulted
            at app construction time so a running app never silently
            flips identity-enforcement behaviour.
        run_components: Pre-built dependency bundle (used by tests to
            inject a :class:`~custos_workflow.runtime.FakeWorkflowRuntime`
            and in-memory adapters). When ``None`` the lifespan
            constructs the real defaults via
            :func:`~custos_workflow.providers.load_run_components`,
            reading ``WF_DAPR_WORKFLOW_COMPONENT`` from the
            environment. The factory itself never reads the env;
            missing variables surface as :class:`RuntimeError`
            inside the lifespan startup so import remains
            side-effect-free.
        worker_ready_timeout_s: Seconds the lifespan blocks on
            :meth:`WorkflowRuntimeProtocol.wait_for_worker_ready`
            before giving up. A timeout leaves ``app.state.ready``
            at ``False`` so ``/readyz`` keeps returning 503 until
            an operator restarts the pod.
        worker_shutdown_timeout_s: Seconds the lifespan waits for
            ``runtime.shutdown()`` on termination. Exceeding the
            budget is logged but never crashes the lifespan.

    Returns:
        A configured :class:`FastAPI` instance with the call-context
        middleware mounted and the ``/healthz`` / ``/readyz`` router
        included.
    """
    from fastapi import FastAPI

    effective_require = (
        require_call_context
        if require_call_context is not None
        else _read_require_call_context_flag()
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ready = False
        app.state.ready_detail = "workflow-service has not finished startup"
        components = _resolve_run_components(run_components)
        runtime = components.workflow_runtime

        # Register the WF-IMPL-035 ``run_orchestrator`` under the
        # canonical :data:`WORKFLOW_NAME`. We bind a stateless
        # :class:`NoopStepHandler` here — the Phase E Step
        # Coordinator (WF-IMPL-046) will swap in the real handler
        # without touching the lifespan. ``WaitStepHandler`` is
        # constructed inline by :func:`make_run_orchestrator`, so
        # there is nothing to register separately for the wait
        # step kind.
        orchestrator_fn = make_run_orchestrator(
            NoopStepHandler(),
            on_replay=components.replay_reconciler.on_replay,
        )
        runtime.register_workflow(orchestrator_fn, name=WORKFLOW_NAME)

        try:
            await runtime.start()
        except Exception:
            logger.exception("workflow runtime failed to start; /readyz will remain 503")
            app.state.ready_detail = "workflow runtime failed to start"
            app.state.run_components = components
            try:
                yield
            finally:
                await _shutdown_components(components, worker_shutdown_timeout_s)
            return

        worker_ready = await runtime.wait_for_worker_ready(timeout=worker_ready_timeout_s)
        if worker_ready and runtime.is_ready:
            app.state.ready = True
            app.state.ready_detail = None
            logger.info(
                "workflow-service ready (call-context dev shim %s)",
                "disabled" if effective_require else "active",
            )
        else:
            # Leave ``ready`` at ``False`` so ``/readyz`` keeps
            # returning 503. An operator-visible detail string
            # surfaces in the probe body.
            app.state.ready_detail = (
                f"workflow worker did not report ready within {worker_ready_timeout_s:.1f}s"
            )
            logger.error(
                "workflow worker not ready after %.1fs; /readyz will remain 503",
                worker_ready_timeout_s,
            )

        app.state.run_components = components
        try:
            yield
        finally:
            await _shutdown_components(components, worker_shutdown_timeout_s)

    app = FastAPI(
        title="Custos Workflow Service",
        version=__version__,
        lifespan=lifespan,
    )
    app.add_middleware(
        CallContextMiddleware,
        require_call_context=effective_require,
    )
    app.include_router(health_router)
    return app


def _resolve_run_components(injected: RunComponents | None) -> RunComponents:
    """Return the caller-injected bundle or build the env-driven default.

    When no override is supplied, ``WF_DAPR_WORKFLOW_COMPONENT`` is
    required: the design's fail-fast convention surfaces missing
    configuration as :class:`RuntimeError` at lifespan startup
    instead of letting the Dapr SDK raise a less actionable error
    once the worker tries to register against an unknown component.
    """
    if injected is not None:
        return injected
    component = os.environ.get(ENV_DAPR_WORKFLOW_COMPONENT, "")
    if not component:
        raise RuntimeError(
            f"{ENV_DAPR_WORKFLOW_COMPONENT} environment variable is required to "
            "start the workflow worker (design.md § Configuration). Set it to "
            "the name of the Dapr Workflow component registered with the sidecar."
        )
    return load_run_components()


async def _shutdown_components(components: RunComponents, worker_shutdown_timeout_s: float) -> None:
    """Stop the worker (with a grace period) and release owned resources.

    Never raises — the lifespan exit must not crash on a slow Dapr
    sidecar shutdown or a transient HTTP client close error.
    """
    runtime = components.workflow_runtime
    try:
        await asyncio.wait_for(
            runtime.shutdown(),
            timeout=worker_shutdown_timeout_s,
        )
    except TimeoutError:
        logger.error(
            "workflow runtime shutdown exceeded %.1fs grace; abandoning",
            worker_shutdown_timeout_s,
        )
    except Exception:
        logger.exception("workflow runtime shutdown raised; ignoring")

    dapr_http_client = components.dapr_http_client
    if dapr_http_client is not None:
        try:
            await dapr_http_client.aclose()
        except Exception:
            logger.exception("dapr publisher http client aclose failed during shutdown")
