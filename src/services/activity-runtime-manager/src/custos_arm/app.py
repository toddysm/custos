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

The lifespan currently flips ``app.state.ready`` to ``True`` with no
dependencies; ARM-IMPL-003+ replace the trivial startup with the runtime
driver probe, Dapr worker start, and collaborator wiring.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from custos_arm._version import __version__
from custos_arm.config import Settings, load_settings
from custos_arm.healthz import router as health_router
from custos_arm.middleware import (
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
)

__all__ = ["create_app"]

logger = logging.getLogger("custos_arm")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build and return the Activity Runtime Manager FastAPI application.

    Args:
        settings: Pre-loaded configuration. When ``None`` (the default),
            :func:`custos_arm.config.load_settings` reads the process
            environment — a missing required variable raises
            :class:`custos_arm.config.SettingsError` here, failing fast at
            startup. Tests inject a constructed :class:`Settings` to avoid
            touching the real environment.

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
        # Startup: ARM-IMPL-002 wires configuration and the call-context
        # middleware but has no collaborators to warm up, so the gate
        # opens immediately. Later tasks gate readiness on the
        # runtime-driver probe and Dapr worker readiness.
        app.state.ready = True
        app.state.ready_detail = "ready"
        logger.info(
            "activity-runtime-manager %s ready (environment=%s, dev_shim=%s)",
            __version__,
            resolved.environment,
            resolved.use_callctx_dev_shim,
        )
        try:
            yield
        finally:
            # Close the gate on shutdown and keep ``ready_detail`` aligned
            # with the closed state so ``/readyz`` does not report 503 with
            # a stale "ready" detail while the process drains in-flight
            # requests.
            app.state.ready = False
            app.state.ready_detail = "activity-runtime-manager is shutting down"

    app = FastAPI(
        title="Custos Activity Runtime Manager",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.ready = False
    app.state.settings = resolved
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=resolved.authz_endpoint,
        environment=resolved.environment,
    )
    app.add_exception_handler(CallContextError, call_context_error_handler)
    app.include_router(health_router)
    return app
