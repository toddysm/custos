"""API Gateway FastAPI application factory (AGW-IMPL-002, AGW-IMPL-016, AGW-IMPL-017).

:func:`create_app` builds the FastAPI application: the ``/healthz`` + ``/readyz``
probes, the full ingress pipeline (declarative route registry, anonymous webhook
pass-through, auth-bootstrap device-code routes), the cross-cutting CORS and
correlation middleware, the OpenAPI 3.1 document at ``/openapi.json``, and a
lifespan hook that wires the downstream router, rate limiter, idempotency
metadata store and Auth Service client onto ``app.state`` before flipping
readiness.

The factory is import-safe — no socket connections, no env-dependent failures at
construction time beyond resolving :class:`Settings`. All side-effecting work
(building the owned ``httpx`` client and downstream router, running the startup
permission cross-check) happens inside the FastAPI lifespan context.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

import httpx

from custos_gateway._version import __version__
from custos_gateway.clients.auth import DaprAuthServiceClient, read_dapr_endpoint
from custos_gateway.errors import register_exception_handlers
from custos_gateway.health import router as health_router
from custos_gateway.middleware import CorrelationIdMiddleware
from custos_gateway.middleware.auth import AUTH_CLIENT_STATE_ATTR
from custos_gateway.middleware.idempotency import METADATA_STORE_STATE_ATTR
from custos_gateway.middleware.ratelimit import RATE_LIMITER_STATE_ATTR, RateLimiter
from custos_gateway.openapi import install_openapi
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes import (
    DEVICE_CODE_STORE_STATE_ATTR,
    DOWNSTREAM_ROUTER_STATE_ATTR,
    build_device_code_router,
    build_registry_router,
    build_webhook_router,
)
from custos_gateway.settings import Settings, load_settings
from custos_gateway.startup import (
    StartupCheckOutcome,
    converge_route_permissions,
    run_startup_permission_check,
)

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
        auth_client: Lifespan-owned Auth Service client (the test-injection
            seam). When ``None`` (production) the lifespan builds a Dapr-backed
            :class:`DaprAuthServiceClient` over the owned ``httpx`` client.
            Either way the bound client drives the startup permission check
            (AGW-IMPL-008): every route's declared permission is validated
            against the Auth Service registry before readiness flips, refusing
            to boot on any undeclared permission. Tests inject a fake.
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
        converge_task: asyncio.Task[None] | None = None

        def owned_http_client() -> httpx.AsyncClient:
            # Lazily build a single httpx client shared by the downstream router
            # and the Dapr-backed Auth client (both talk to the local sidecar),
            # closed once on shutdown.
            nonlocal owned_client
            if owned_client is None:
                owned_client = httpx.AsyncClient()
            return owned_client

        if downstream_router is not None:
            router = downstream_router
        else:
            router = DownstreamRouter(
                http_client=owned_http_client(),
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

        # The lifespan owns the Auth Service client: tests inject a fake while
        # production builds a Dapr-backed client over the owned httpx client. The
        # startup cross-check then refuses to become ready unless every route's
        # declared permission is known to the Auth Service.
        effective_auth_client = (
            auth_client
            if auth_client is not None
            else DaprAuthServiceClient(
                http_client=owned_http_client(),
                endpoint=read_dapr_endpoint(os.environ),
            )
        )
        setattr(app.state, AUTH_CLIENT_STATE_ATTR, effective_auth_client)
        # Resilient to dependency start order (issue #815): a transient
        # Auth Service / Dapr-sidecar outage at boot must not crash the process.
        # Attempt the cross-check once; on a transient failure stay not-ready and
        # converge in the background until the registry becomes reachable.
        outcome = await run_startup_permission_check(app=app, client=effective_auth_client)
        if outcome is StartupCheckOutcome.TRANSIENT_FAILURE:
            converge_task = asyncio.create_task(
                converge_route_permissions(
                    app=app,
                    client=effective_auth_client,
                    initial_backoff_seconds=(
                        effective_settings.startup_permission_check_initial_backoff_seconds
                    ),
                    max_backoff_seconds=(
                        effective_settings.startup_permission_check_max_backoff_seconds
                    ),
                )
            )
        try:
            yield
        finally:
            if converge_task is not None:
                converge_task.cancel()
                with suppress(asyncio.CancelledError):
                    await converge_task
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
    app.state.ready_detail = "api-gateway has not finished startup"

    # The correlation middleware is added first and the CORS middleware second so
    # CORS sits outermost: every response that reaches the app — normal replies
    # and Problem+JSON errors alike — carries both the CORS headers and the
    # x-correlation-id stamped by the inner correlation middleware. (CORS
    # preflight ``OPTIONS`` requests are short-circuited by CORSMiddleware itself
    # and never reach the correlation middleware.)
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
    install_openapi(app)
    return app


__all__ = ["create_app"]
