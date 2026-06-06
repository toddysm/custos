"""API Gateway FastAPI application factory (AGW-IMPL-002).

:func:`create_app` builds the FastAPI application: the ``/healthz`` + ``/readyz``
probes and a lifespan hook that marks the app ready once startup completes.

The factory is import-safe — no socket connections, no env-dependent failures at
construction time beyond resolving :class:`Settings`. All side-effecting work
belongs in the FastAPI lifespan context, which later phases extend to build the
Auth Service client, the SPL provider bundle, and the downstream router onto
``app.state`` without changing the factory's import-safety contract.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_gateway._version import __version__
from custos_gateway.errors import register_exception_handlers
from custos_gateway.health import router as health_router
from custos_gateway.middleware import CorrelationIdMiddleware
from custos_gateway.middleware.auth import AUTH_CLIENT_STATE_ATTR
from custos_gateway.settings import Settings, load_settings
from custos_gateway.startup import validate_route_permissions

if TYPE_CHECKING:
    from fastapi import FastAPI

    from custos_gateway.clients.auth import AuthServiceClient

logger = logging.getLogger("custos_gateway")


def create_app(
    *,
    settings: Settings | None = None,
    auth_client: AuthServiceClient | None = None,
) -> FastAPI:
    """Build and return the API Gateway FastAPI application.

    Args:
        settings: Pre-built :class:`Settings`. When ``None`` (the default) the
            configuration is parsed from the process environment via
            :func:`custos_gateway.settings.load_settings`, which raises
            :class:`~custos_gateway.settings.SettingsError` for missing required
            vars. Tests inject a settings instance to avoid touching the
            environment.
        auth_client: Lifespan-owned Auth Service client. When provided it is
            bound to ``app.state`` and the startup permission check
            (AGW-IMPL-008) validates every route's declared permission against
            the Auth Service registry before readiness flips, refusing to boot
            on any undeclared permission. When ``None`` the real Dapr client is
            wired by a later phase (AGW-IMPL-016); tests inject a fake.

    The factory is import-safe: no DSN lookups, no socket connections. All
    side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI

    effective_settings = settings if settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Later phases bind the SPL provider bundle and downstream router here.
        # When an Auth Service client is available, the gateway validates that
        # every route declares a permission the Auth Service knows about and
        # refuses to become ready otherwise.
        if auth_client is not None:
            setattr(app.state, AUTH_CLIENT_STATE_ATTR, auth_client)
            await validate_route_permissions(app=app, client=auth_client)
        app.state.ready = True
        logger.info("api-gateway is ready")
        yield

    app = FastAPI(
        title="Custos API Gateway",
        version=__version__,
        description="Single uniform HTTPS entrypoint for Custos (COMP-001).",
        lifespan=lifespan,
    )
    app.state.settings = effective_settings
    app.state.ready = False

    # The correlation middleware runs outermost so every response — success,
    # health probe, or Problem+JSON error — carries the x-correlation-id header.
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)

    app.include_router(health_router)
    return app


__all__ = ["create_app"]
