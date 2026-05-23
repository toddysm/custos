"""Custos Catalog Service (COMP-007).

This package hosts the Catalog Service runtime: workflow + template
definition lifecycle, the activity-type and connector-type read-side index,
and the publish-time validation gate (schema, CEL syntactic + name-binding,
reference resolution, digest pinning).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/catalog-service/design.md

Phase B (CS-IMPL-003 + CS-IMPL-004) ships the SPL provider wiring, the
schema-revision startup gate, the call-context middleware (with dev shim),
and the ``/healthz`` + ``/readyz`` probes. REST routes (CS-IMPL-017) and
the remaining managers (CS-IMPL-009/010/011/012/013/...) land later.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_catalog.api import (
    all_routers,
    register_exception_handlers,
)
from custos_catalog.health import router as health_router
from custos_catalog.middleware import (
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
)
from custos_catalog.providers import (
    MigrationRequired,
    Providers,
    load_providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from custos_catalog.settings import Settings, load_settings

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"

logger = logging.getLogger("custos_catalog")


def create_app(
    *,
    settings: Settings | None = None,
    providers: Providers | None = None,
) -> FastAPI:
    """Build and return the Catalog Service FastAPI application.

    Args:
        settings: Pre-parsed :class:`Settings`. Defaults to
            :func:`custos_catalog.settings.load_settings` reading from
            the process environment.
        providers: Pre-built :class:`Providers` (used by tests to inject
            in-memory fakes). When ``None``, the lifespan hook constructs
            the real Postgres adapters from the settings DSNs.

    The factory is import-safe: no DSN lookups, no socket connections.
    All side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI

    effective_settings = settings if settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = effective_settings
        local_providers = (
            providers
            if providers is not None
            else load_providers(
                effective_settings,
            )
        )
        app.state.providers = local_providers
        app.state.ready = False
        app.state.schema_gate_error = None
        try:
            await verify_schema_revisions(local_providers)
            app.state.ready = True
            logger.info("schema-revision gate passed; catalog-service is ready")
        except MigrationRequired as exc:
            app.state.schema_gate_error = exc
            logger.error("%s", schema_gate_explainer(exc))
        yield

    app = FastAPI(
        title="Custos Catalog Service",
        version=__version__,
        lifespan=lifespan,
    )

    # CallContextMiddleware is instantiated lazily on the first request,
    # so a DevShimDisabledInProductionError surfaces during startup
    # (visible to the operator the same way any startup exception is)
    # rather than at create_app() time. This keeps the factory side-effect
    # free and trivially testable.
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=effective_settings.authz_endpoint,
        environment=effective_settings.environment,
    )
    # Pair the middleware with its exception handler so the dependency-side
    # 4xx responses (get_call_context / require_permission) emit the same
    # `{"error": {"code", "detail"}}` envelope as the middleware itself.
    app.add_exception_handler(CallContextError, call_context_error_handler)

    app.include_router(health_router)
    for r in all_routers:
        app.include_router(r)
    register_exception_handlers(app)
    return app
