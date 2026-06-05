"""Observability and Audit Service FastAPI application factory (OBS-IMPL-001).

:func:`create_app` builds the FastAPI application: the ``/healthz`` +
``/readyz`` probes and a lifespan hook that marks the app ready once startup
completes.

The factory is import-safe — no socket connections, no env-dependent failures
at construction time. All side-effecting work belongs in the FastAPI lifespan
context. The typed settings loader (OBS-IMPL-002), the SPL provider bundle
(OBS-IMPL-004), the audit pipeline (OBS-IMPL-005/006), the retention worker
(OBS-IMPL-007), alerting (OBS-IMPL-008/009), the External Exporter Loader
(OBS-IMPL-010/011), and the read-back API surface (OBS-IMPL-012..014) grow in
place atop this factory without changing its import-safety contract.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_obs._version import __version__
from custos_obs.health import router as health_router

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger("custos_obs")


def create_app() -> FastAPI:
    """Build and return the Observability and Audit Service FastAPI app.

    The application exposes only the ``/healthz`` + ``/readyz`` probes at this
    phase. The lifespan hook marks ``app.state.ready`` once startup completes;
    later phases extend it to own the SPL provider bundle, the audit-outbox
    drainer, the retention worker, and the alerting dispatcher.
    """
    from fastapi import FastAPI

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ready = False
        logger.info("observability-audit-service starting (v%s)", __version__)
        # Provider wiring + background workers attach here in later phases.
        app.state.ready = True
        logger.info("observability-audit-service ready")
        try:
            yield
        finally:
            app.state.ready = False
            logger.info("observability-audit-service stopped")

    app = FastAPI(
        title="Custos Observability and Audit Service",
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app


__all__ = ["create_app"]
