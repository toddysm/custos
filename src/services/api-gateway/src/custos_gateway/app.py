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
from contextlib import AsyncExitStack, asynccontextmanager, suppress
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
        auth_client: Caller-owned Auth Service client override. When ``None``
            (production) the lifespan builds a Dapr-backed
            :class:`DaprAuthServiceClient` over the owned ``httpx`` client.
            Either way the bound client drives the startup permission check
            (AGW-IMPL-008): every route's declared permission is validated
            against the Auth Service registry before readiness flips, refusing
            to boot on any undeclared permission. Tests inject a fake.
        downstream_router: Caller-owned :class:`DownstreamRouter` override. When ``None``
            (production) the factory builds an owned :class:`httpx.AsyncClient`
            and a Dapr-targeted router during startup and closes the client on
            shutdown. Tests inject a fake router so no socket is opened.
        metadata_store: SPL metadata-store provider backing write-path
            idempotency. Injected stores remain caller-owned. Otherwise a configured
            metadata DSN creates a lifespan-owned PostgreSQL provider and checks
            its schema before readiness. Without either, ordinary writes retain
            legacy behavior and recovery writes fail closed.
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
        app.state.ready = False
        async with AsyncExitStack() as stack:

            def owned_http_client() -> httpx.AsyncClient:
                """Share one owned transport, including cleanup after partial startup."""
                nonlocal owned_client
                if owned_client is None:
                    owned_client = httpx.AsyncClient()
                    stack.push_async_callback(owned_client.aclose)
                return owned_client

            try:
                router = (
                    downstream_router
                    if downstream_router is not None
                    else DownstreamRouter(
                        http_client=owned_http_client(),
                        host=effective_settings.dapr_http_host,
                        http_port=effective_settings.dapr_http_port,
                    )
                )
                setattr(app.state, DOWNSTREAM_ROUTER_STATE_ATTR, router)
                setattr(
                    app.state,
                    RATE_LIMITER_STATE_ATTR,
                    rate_limiter
                    if rate_limiter is not None
                    else RateLimiter.from_settings(effective_settings),
                )
                store = metadata_store
                if store is None and effective_settings.metadata_store_dsn:
                    from custos_pg import PgMetadataAdapter
                    from custos_pg.pool import LazyPool
                    from custos_spl.errors import MigrationRequired
                    from custos_spl.interfaces.metadata_store import MetadataStoreProvider

                    pool = LazyPool(effective_settings.metadata_store_dsn)
                    stack.push_async_callback(pool.aclose)
                    adapter = PgMetadataAdapter(lazy=pool)
                    await adapter.refresh_declared()
                    interface, revision = (
                        MetadataStoreProvider.__name__,
                        MetadataStoreProvider.SCHEMA_REVISION,
                    )
                    if revision not in adapter.declared_revisions.get(interface, frozenset()):
                        raise MigrationRequired([(interface, revision)])
                    store = adapter
                setattr(app.state, METADATA_STORE_STATE_ATTR, store)
                setattr(app.state, DEVICE_CODE_STORE_STATE_ATTR, device_code_store)
                effective_auth_client = (
                    auth_client
                    if auth_client is not None
                    else DaprAuthServiceClient(
                        http_client=owned_http_client(),
                        endpoint=read_dapr_endpoint(os.environ),
                    )
                )
                setattr(app.state, AUTH_CLIENT_STATE_ATTR, effective_auth_client)
                outcome = await run_startup_permission_check(app=app, client=effective_auth_client)
                if outcome is StartupCheckOutcome.TRANSIENT_FAILURE:
                    task = asyncio.create_task(
                        converge_route_permissions(
                            app=app,
                            client=effective_auth_client,
                            initial_backoff_seconds=effective_settings.startup_permission_check_initial_backoff_seconds,
                            max_backoff_seconds=effective_settings.startup_permission_check_max_backoff_seconds,
                        )
                    )
                    stack.push_async_callback(_stop_convergence, task)
                yield
            finally:
                app.state.ready = False

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


async def _stop_convergence(task: asyncio.Task[None]) -> None:
    """Cancel the owned startup worker before its HTTP client is closed."""
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
