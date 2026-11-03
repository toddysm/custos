"""FastAPI application factory for the Activity Runtime Manager.

The factory performs no network I/O and opens no sockets, but as of
ARM-IMPL-002 it loads the typed :class:`~custos_arm.config.Settings`
from the environment (unless settings are injected) so it can mount the
call-context middleware with the right AuthZ wiring. A missing required
variable therefore fails fast at construction with a clear message.

The exposed surface is:

- ``GET /healthz`` — flat liveness probe.
- ``GET /readyz`` — readiness gate (503 until the lifespan flips
  ``app.state.ready``).
- Every other request passes through
  :class:`~custos_arm.middleware.CallContextMiddleware`, which trusts the
  ``x-custos-callctx`` dev-shim header when ``ARM_AUTHZ_ENDPOINT`` is empty
  and refuses to start when ``ENVIRONMENT=production``. The two probes are
  deliberately bypassed by the middleware so liveness/readiness checks need
  no call-context header.

Startup cannot claim readiness without a configured scheduler. Explicit runtime
activation assembles/probes the real graph and drains it before resource closure.
When disabled or unwired, liveness remains available but readiness fails closed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from custos_arm._version import __version__
from custos_arm.config import Settings, load_settings
from custos_arm.credential_proxy import router as credential_router
from custos_arm.healthz import SCHEDULER_NOT_CONFIGURED
from custos_arm.healthz import router as health_router
from custos_arm.middleware import (
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
)
from custos_arm.rpc import router as rpc_router
from custos_arm.rpc.router import schedule_validation_error_handler
from custos_arm.scheduler import ActivityScheduler

if TYPE_CHECKING:
    from custos_arm.resources import RuntimeResources

__all__ = ["create_app"]

logger = logging.getLogger("custos_arm")


def create_app(
    settings: Settings | None = None,
    *,
    resource_factory: Callable[[Settings], AbstractAsyncContextManager[RuntimeResources]]
    | None = None,
) -> FastAPI:
    """Build and return the Activity Runtime Manager FastAPI application.

    Args:
        settings: Pre-loaded configuration. When ``None`` (the default),
            :func:`custos_arm.config.load_settings` reads the process
            environment — a missing required variable raises
            :class:`custos_arm.config.SettingsError` here, failing fast at
            startup. Tests inject a constructed :class:`Settings` to avoid
            touching the real environment.
        resource_factory: Explicit lifecycle test seam; production uses the real
            resource graph and never substitutes test adapters on failure.

    Returns:
        A configured :class:`fastapi.FastAPI` instance. The ``/healthz`` and
        ``/readyz`` probes are intentionally bypassed by the call-context
        middleware; every other request passes through it.

    Raises:
        custos_arm.config.SettingsError: A required variable is missing or
            malformed (raised here when ``settings`` is ``None``).
        custos_arm.middleware.DevShimDisabledInProductionError: The
            call-context dev shim is active (empty ``ARM_AUTHZ_ENDPOINT``)
            while ``ENVIRONMENT=production``. Starlette builds the
            middleware stack lazily, so this surfaces when the application
            starts up (or serves its first request), not from
            ``create_app`` itself.
    """
    resolved = settings if settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Probe owned resources before admission; drain them before closing clients on shutdown."""
        async with AsyncExitStack() as stack:
            if resolved.runtime_enabled:
                from custos_arm.resources import open_runtime

                factory = resource_factory or open_runtime
                async with asyncio.timeout(30):
                    resources = await stack.enter_async_context(factory(resolved))
                app.state.scheduler = resources.scheduler
                app.state.credential_proxy = resources.credential_proxy
                app.state.readiness_probe = resources.probe
            app.state.ready = isinstance(app.state.scheduler, ActivityScheduler)
            app.state.ready_detail = "ready" if app.state.ready else SCHEDULER_NOT_CONFIGURED
            if not app.state.ready:
                logger.warning("Activity runtime is not ready: %s", app.state.ready_detail)
            try:
                yield
            finally:
                app.state.ready = False
                app.state.ready_detail = "activity-runtime-manager is shutting down"
        app.state.credential_proxy = None
        app.state.readiness_probe = None
        if resolved.runtime_enabled:
            app.state.scheduler = None

    app = FastAPI(
        title="Custos Activity Runtime Manager",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.ready = False
    app.state.settings = resolved
    app.state.runtime_managed = resolved.runtime_enabled
    app.state.credential_proxy = None
    app.state.readiness_probe = None
    app.state.dependencies_ready = True
    # The Activity Scheduler is attached by the composition root during
    # startup; until then the RPC surface (rpc_router) reports 503.
    app.state.scheduler = None
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=resolved.authz_endpoint,
        environment=resolved.environment,
    )
    app.add_exception_handler(CallContextError, call_context_error_handler)
    app.add_exception_handler(RequestValidationError, schedule_validation_error_handler)
    app.include_router(health_router)
    app.include_router(rpc_router)
    app.include_router(credential_router)
    return app
