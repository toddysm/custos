"""API Gateway FastAPI application factory (AGW-IMPL-002, AGW-IMPL-016).

:func:`create_app` builds the FastAPI application: the ``/healthz`` + ``/readyz``
probes, the full ingress pipeline (declarative route registry, anonymous webhook
pass-through, auth-bootstrap device-code routes), the cross-cutting CORS and
correlation middleware, and a lifespan hook that wires the downstream router,
rate limiter, idempotency metadata store and Auth Service client onto
``app.state`` before flipping readiness.

The factory is import-safe — no socket connections, no env-dependent failures at
construction time beyond resolving :class:`Settings`. All side-effecting work
(building the owned ``httpx`` client and downstream router, running the startup
permission cross-check) happens inside the FastAPI lifespan context.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import httpx

from custos_gateway._version import __version__
from custos_gateway.errors import register_exception_handlers
from custos_gateway.health import router as health_router
from custos_gateway.middleware import CorrelationIdMiddleware
from custos_gateway.middleware.auth import AUTH_CLIENT_STATE_ATTR
from custos_gateway.middleware.idempotency import METADATA_STORE_STATE_ATTR
from custos_gateway.middleware.ratelimit import RATE_LIMITER_STATE_ATTR, RateLimiter
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes import (
    DEVICE_CODE_STORE_STATE_ATTR,
    DOWNSTREAM_ROUTER_STATE_ATTR,
    build_device_code_router,
    build_registry_router,
    build_webhook_router,
)
from custos_gateway.settings import Settings, load_settings
from custos_gateway.startup import validate_route_permissions

if TYPE_CHECKING:
    from fastapi import FastAPI

    from custos_gateway.clients.auth import AuthServiceClient
    from custos_gateway.middleware.idempotency import IdempotencyStore
    from custos_gateway.routes.devicecode import DeviceCodeStore

logger = logging.getLogger("custos_gateway")


def create_app(
    *,
    settings: Settings | None = None,
    auth_client: AuthServiceClient | None = None,
    downstream_router: DownstreamRouter | None = None,
    metadata_store: IdempotencyStore | None = None,
    device_code_store: DeviceCodeStore | None = None,
    rate_limiter: RateLimiter | None = None,
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
            on any undeclared permission. When ``None`` the gateway becomes ready
            without the cross-check; tests inject a fake.
        downstream_router: Lifespan-owned :class:`DownstreamRouter`. When ``None``
            (production) the factory builds an owned :class:`httpx.AsyncClient`
            and a Dapr-targeted router during startup and closes the client on
            shutdown. Tests inject a fake router so no socket is opened.
        metadata_store: SPL metadata-store provider backing write-path
            idempotency. ``None`` (the M1 default) skips idempotency reservation;
            tests inject a fake store to exercise the replay/complete paths.
        device_code_store: Backing store for the device-code flow. ``None`` (the
            M1 default) leaves the device-code routes returning ``503`` until the
            store is wired.
        rate_limiter: Lifespan-owned :class:`RateLimiter`. When ``None`` the
            factory builds one from ``settings`` during startup; tests may inject
            a pre-seeded limiter.

    The factory is import-safe: no DSN lookups, no socket connections. All
    side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    effective_settings = settings if settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned_client: httpx.AsyncClient | None = None
        if downstream_router is not None:
            router = downstream_router
        else:
            owned_client = httpx.AsyncClient()
            router = DownstreamRouter(
                http_client=owned_client,
                host=effective_settings.dapr_http_host,
                http_port=effective_settings.dapr_http_port,
            )
        setattr(app.state, DOWNSTREAM_ROUTER_STATE_ATTR, router)
        setattr(
            app.state,
            RATE_LIMITER_STATE_ATTR,
            rate_limiter
            if rate_limiter is not None
            else RateLimiter.from_settings(effective_settings),
        )
        setattr(app.state, METADATA_STORE_STATE_ATTR, metadata_store)
        setattr(app.state, DEVICE_CODE_STORE_STATE_ATTR, device_code_store)

        # When an Auth Service client is available, the gateway validates that
        # every route declares a permission the Auth Service knows about and
        # refuses to become ready otherwise.
        if auth_client is not None:
            setattr(app.state, AUTH_CLIENT_STATE_ATTR, auth_client)
            await validate_route_permissions(app=app, client=auth_client)
        app.state.ready = True
        logger.info("api-gateway is ready")
        try:
            yield
        finally:
            if owned_client is not None:
                await owned_client.aclose()

    app = FastAPI(
        title="Custos API Gateway",
        version=__version__,
        description="Single uniform HTTPS entrypoint for Custos (COMP-001).",
        lifespan=lifespan,
    )
    app.state.settings = effective_settings
    app.state.ready = False

    # The correlation middleware is added first and the CORS middleware second so
    # CORS sits outermost: every response — including Problem+JSON errors and the
    # preflight short-circuit — carries both the CORS headers and the
    # x-correlation-id stamped by the inner correlation middleware.
    app.add_middleware(CorrelationIdMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(effective_settings.cors_allowed_origins),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    register_exception_handlers(app)

    app.include_router(health_router)
    app.include_router(build_registry_router())
    app.include_router(build_webhook_router())
    app.include_router(build_device_code_router())
    return app


__all__ = ["create_app"]
