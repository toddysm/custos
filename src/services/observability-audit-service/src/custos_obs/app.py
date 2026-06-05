"""Observability and Audit Service FastAPI application factory (OBS-IMPL-001).

:func:`create_app` builds the FastAPI application: the ``/healthz`` +
``/readyz`` probes and a lifespan hook that owns the SPL provider bundle and
marks the app ready once startup completes.

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

    from custos_obs.providers import Providers
    from custos_obs.settings import Settings

logger = logging.getLogger("custos_obs")


def create_app(
    settings: Settings | None = None,
    providers: Providers | None = None,
) -> FastAPI:
    """Build and return the Observability and Audit Service FastAPI app.

    The application exposes the ``/healthz`` + ``/readyz`` probes and a lifespan
    hook that owns the SPL provider bundle. The hook marks ``app.state.ready``
    once startup completes and closes provider resources on shutdown; later
    phases extend it with the audit-outbox drainer, the retention worker, and
    the alerting dispatcher.

    Args:
        settings: Pre-parsed :class:`Settings`. When ``None`` the lifespan reads
            the process environment via
            :func:`custos_obs.settings.load_settings`.
        providers: Pre-built :class:`Providers` (used by tests to inject fakes).
            When ``None`` the lifespan constructs the real SPL adapters from the
            settings via :func:`custos_obs.providers.load_providers`.

    The factory is import-safe: no DSN lookups, no socket connections. All
    side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        from custos_obs.providers import aclose_providers, load_providers
        from custos_obs.settings import load_settings

        app.state.ready = False
        effective_settings = settings if settings is not None else load_settings()
        app.state.settings = effective_settings
        logger.info("observability-audit-service starting (v%s)", __version__)

        local_providers: Providers | None = None
        try:
            local_providers = (
                providers if providers is not None else load_providers(effective_settings)
            )
            app.state.providers = local_providers
            # Background workers (drainer, retention, alerting) attach here in later phases.
            app.state.ready = True
            logger.info("observability-audit-service ready")
            yield
        finally:
            app.state.ready = False
            # Only close providers this lifespan owns; injected fakes are the
            # caller's responsibility. ``local_providers`` stays None if
            # construction raised before assignment, so the guard avoids an
            # UnboundLocalError masking the real startup failure.
            if providers is None and local_providers is not None:
                await aclose_providers(local_providers)
            logger.info("observability-audit-service stopped")

    app = FastAPI(
        title="Custos Observability and Audit Service",
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app


__all__ = ["create_app"]
