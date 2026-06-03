"""FastAPI application factory for the Activity Runtime Manager (ARM-IMPL-001).

The factory is import-safe: no environment lookups, no socket
connections, no Dapr sidecar calls. All side-effecting work (runtime
driver probe, Dapr worker start, resolver/store warm-up) lands inside
the FastAPI lifespan in later ARM-IMPL-* tasks.

The exposed surface for ARM-IMPL-001 is:

- ``GET /healthz`` — flat liveness probe.
- ``GET /readyz`` — readiness gate (503 until the lifespan flips
  ``app.state.ready``).

The lifespan currently flips ``app.state.ready`` to ``True`` with no
dependencies; ARM-IMPL-002+ replace the trivial startup with the real
configuration load, AuthZ dev-shim middleware, and collaborator wiring.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_arm._version import __version__
from custos_arm.healthz import router as health_router

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["create_app"]

logger = logging.getLogger("custos_arm")


def create_app() -> FastAPI:
    """Build and return the Activity Runtime Manager FastAPI application.

    The factory performs no I/O and reads no environment variables, so it
    is safe to import and construct in tests and tooling. The readiness
    gate opens inside the lifespan, which :class:`fastapi.testclient.TestClient`
    enters on ``__enter__``.

    Returns:
        A configured :class:`fastapi.FastAPI` instance exposing the
        ``/healthz`` and ``/readyz`` probes.
    """
    # Imported here (not at module top) so the package ``__init__`` can
    # re-export ``create_app`` without importing FastAPI at package-import
    # time for non-runtime tooling.
    from fastapi import FastAPI

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Startup: ARM-IMPL-001 has no collaborators to warm up, so the
        # gate opens immediately. Later tasks gate readiness on the
        # runtime-driver probe and Dapr worker readiness.
        app.state.ready = True
        app.state.ready_detail = "ready"
        logger.info("activity-runtime-manager %s ready", __version__)
        try:
            yield
        finally:
            app.state.ready = False

    app = FastAPI(
        title="Custos Activity Runtime Manager",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.ready = False
    app.include_router(health_router)
    return app
