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
    ENV_TS_ENDPOINT,
    RunComponents,
    load_run_components,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME, make_run_orchestrator
from custos_workflow.steps import StepCoordinator
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.resume import ResumeSubscriptionTtlSweeper
from custos_workflow.steps.sub_orchestration import (
    CHILD_STEP_WORKFLOW_NAME,
    SubOrchestrationManager,
    make_child_step_orchestrator,
)

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
        # canonical :data:`WORKFLOW_NAME`. WF-IMPL-057 wires the
        # real :class:`StepCoordinator` (WF-IMPL-055) so every
        # non-wait node dispatches through the production
        # ``let:`` / ``activity:`` handlers; ``wait:`` is still
        # served inline by :class:`WaitStepHandler` (constructed
        # by :func:`make_run_orchestrator`). The coordinator's
        # :class:`ActivityStepHandler` is wired against the
        # ``ActivityRuntimeClient`` / ``ConnectorClient`` stubs
        # the bundle ships with — production replaces those
        # noop stubs with the deferred *Real ARM Client* /
        # *Real Connector Client* sub-modules behind the same
        # Protocols, so swapping in the real adapters does NOT
        # require touching the lifespan. The previous
        # :class:`NoopStepHandler` default (WF-IMPL-043) is no
        # longer instantiated; if a future task needs it, the
        # symbol remains importable from
        # :mod:`custos_workflow.runs.step_handler`.
        activity_handler = ActivityStepHandler(
            activity_client=components.activity_client,
            connector_client=components.connector_client,
        )
        step_coordinator = StepCoordinator(activity_handler=activity_handler)
        # WF-IMPL-094: build the Sub-Orchestration Manager from the
        # env-resolved knobs (``WF_MAX_FANOUT_WIDTH`` /
        # ``WF_APPROVAL_DEFAULT_TIMEOUT``) the provider parsed onto the
        # bundle, register the WF-IMPL-088 child-step workflow under
        # :data:`CHILD_STEP_WORKFLOW_NAME` so ``forEach`` fan-out can
        # spawn it, and thread the manager into the top-level
        # orchestrator so ``PrimitiveHandler.SUB_ORCHESTRATION`` nodes
        # dispatch through it. The child orchestrator shares the same
        # :class:`ActivityStepHandler` so an ``activity:`` loop body is
        # driven through the WF-IMPL-074 yield protocol exactly as the
        # top-level orchestrator drives ACTIVITY nodes.
        sub_orchestration_manager = SubOrchestrationManager(
            max_fanout_width=components.max_fanout_width,
            approval_default_timeout=components.approval_default_timeout,
        )
        runtime.register_workflow(
            make_child_step_orchestrator(step_coordinator, activity_handler=activity_handler),
            name=CHILD_STEP_WORKFLOW_NAME,
        )
        orchestrator_fn = make_run_orchestrator(
            step_coordinator,
            on_replay=components.replay_reconciler.on_replay,
            sub_orchestration_manager=sub_orchestration_manager,
            resume_handler=components.resume_handler,
        )
        runtime.register_workflow(orchestrator_fn, name=WORKFLOW_NAME)

        try:
            await runtime.start()
        except Exception:
            logger.exception("workflow runtime failed to start; /readyz will remain 503")
            app.state.ready_detail = "workflow runtime failed to start"
            app.state.run_components = components
            # WF-IMPL-069: bind the validator alongside the bundle
            # so the ``api.dependencies.get_validator`` Depends can
            # resolve it even on the failed-startup path; the
            # health probes stay 503 via ``app.state.ready``
            # already.
            app.state.start_run_validator = components.start_run_validator
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
        # WF-IMPL-069: also bind the WF-IMPL-063
        # :class:`~custos_workflow.validator.StartRunValidator`
        # so the ``Depends(get_validator)`` factory on every
        # ``StartRun`` route can resolve it off ``app.state``.
        # The validator is built by
        # :func:`~custos_workflow.providers.load_run_components`
        # so it shares the same Catalog client the Run Controller
        # drives.
        app.state.start_run_validator = components.start_run_validator
        # WF-IMPL-109: launch the background TTL-expiry sweep that
        # garbage-collects expired resume-subscription mirror rows on a
        # wall-clock interval, independent of the WF mirror writes the
        # ``waitFor:`` handler performs. The task is cancelled (and
        # awaited) on lifespan exit so it never outlives the worker.
        sweep_task = _start_resume_sweep(components)
        app.state.resume_sweep_task = sweep_task
        try:
            yield
        finally:
            await _cancel_resume_sweep(sweep_task)
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

    # WF-IMPL-069: mount the public REST + Internal RPC surface.
    # ``register_exception_handlers`` installs the WF-IMPL-061
    # ``application/problem+json`` envelope for every
    # :class:`~custos_workflow.runs.errors.RunControllerError` /
    # :class:`~custos_workflow.validator.errors.ValidatorError`
    # subclass so handlers never have to construct envelopes by
    # hand. ``all_routers`` is the single tuple
    # (:mod:`custos_workflow.api.routes`) that holds the
    # ``runs`` / ``steps`` / ``internal-rpc`` routers — adding a
    # new resource module only requires appending to that
    # registry; it never has to touch this bootstrap.
    #
    # Imported lazily inside the factory because the API package
    # transitively imports :class:`RunController`, which closes a
    # ``runs.controller -> _telemetry -> steps.events ->
    # runs.controller`` cycle when pulled in before the
    # ``from custos_workflow.steps import StepCoordinator`` line
    # above runs to completion.
    from custos_workflow.api import all_routers, register_exception_handlers
    from custos_workflow.api.observability import register_http_observability

    register_exception_handlers(app)
    # WF-IMPL-070: install the OTel HTTP-server middleware. The
    # call order vs ``CallContextMiddleware`` (added earlier in
    # this function via ``app.add_middleware``) matters because
    # Starlette stacks middlewares LIFO: the LAST one registered
    # runs FIRST (outermost). Registering the observability
    # middleware here — after ``CallContextMiddleware`` — makes
    # observability the OUTER layer, so the span + duration
    # sample wrap the call-context resolution (auth + workspace
    # check) along with the routed request handler. That keeps
    # ``http.status_code`` consistent with what the client
    # actually saw on the wire, even when an auth failure short
    # -circuits routing. ``register_http_observability`` is
    # idempotent so repeated ``create_app`` calls in the test
    # fixtures don't stack duplicate middlewares.
    register_http_observability(app)
    for router in all_routers:
        app.include_router(router)

    return app


def _resolve_run_components(injected: RunComponents | None) -> RunComponents:
    """Return the caller-injected bundle or build the env-driven default.

    When no override is supplied, ``WF_DAPR_WORKFLOW_COMPONENT`` is
    required: the design's fail-fast convention surfaces missing
    configuration as :class:`RuntimeError` at lifespan startup
    instead of letting the Dapr SDK raise a less actionable error
    once the worker tries to register against an unknown component.
    ``WF_TS_ENDPOINT`` (the Trigger Service app-id) is likewise
    required on the production path (WF-IMPL-108): the resume
    subscription manager re-registers surviving ``waitFor:``
    subscriptions against the Trigger Service on replay, so a worker
    started without it would silently drop resume registrations. The
    check fires only here on the env-driven path — tests that inject a
    :class:`RunComponents` bundle bypass it and keep wiring the
    in-process Noop trigger client.
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
    if not os.environ.get(ENV_TS_ENDPOINT, "").strip():
        raise RuntimeError(
            f"{ENV_TS_ENDPOINT} environment variable is required to start the "
            "workflow worker (design.md § Configuration). Set it to the Trigger "
            "Service Dapr app-id so the resume subscription manager can register "
            "and reconcile waitFor: subscriptions."
        )
    return load_run_components()


def _start_resume_sweep(components: RunComponents) -> asyncio.Task[None]:
    """Launch the WF-IMPL-109 TTL-expiry mirror sweep as a background task.

    The sweeper garbage-collects expired
    :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror`
    rows over the *same* mirror repository the ``waitFor:`` handler
    (and, when active, the replay reconciler) is built on, so the table
    does not grow without bound for runs that parked on a ``waitFor:``
    step and were never resumed before TTL. The interval is the
    env-resolved :attr:`RunComponents.resume_sweep_interval_seconds`.
    """
    sweeper = ResumeSubscriptionTtlSweeper(components.resume_handler.mirror_repo)
    return asyncio.create_task(
        sweeper.run_forever(components.resume_sweep_interval_seconds),
        name="resume-subscription-ttl-sweep",
    )


async def _cancel_resume_sweep(task: asyncio.Task[None]) -> None:
    """Cancel and await the TTL-expiry sweep task on lifespan exit.

    Never raises — the lifespan teardown must not crash on the sweep
    task. The expected outcome is :class:`asyncio.CancelledError` (the
    sweep loop runs until cancelled); any other exception means a sweep
    raised outside its own swallow-and-continue guard and is logged
    rather than propagated.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("resume subscription TTL sweep task raised during shutdown")


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

    # The default Dapr-backed ``WorkflowClient`` opens a lazy gRPC channel via
    # ``_ensure_client()`` the first time the API layer schedules / terminates
    # / inspects a workflow; we own the matching ``aclose()`` so the channel
    # is released on shutdown. The structural ``_WorkflowClient`` Protocol the
    # controller depends on does not declare ``aclose`` (only the lifecycle
    # RPCs), so we discover it reflectively — exotic clients that lack a close
    # hook are simply left alone.
    workflow_client_close = getattr(components.workflow_client, "aclose", None)
    if callable(workflow_client_close):
        try:
            await workflow_client_close()
        except Exception:
            logger.exception("workflow client aclose failed during shutdown")

    dapr_http_client = components.dapr_http_client
    if dapr_http_client is not None:
        try:
            await dapr_http_client.aclose()
        except Exception:
            logger.exception("dapr publisher http client aclose failed during shutdown")
