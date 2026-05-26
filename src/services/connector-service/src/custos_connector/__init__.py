"""Custos Connector Service (COMP-005).

This package hosts the Connector Service runtime: the connector type
registry, the connector instance lifecycle (configure / validate / activate
/ disable), capability matching at workflow publish, context issuance for
running activities, and the trigger listen / pull streams that feed the
Trigger Service.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/connector-service/design.md

Phase A (CONN-IMPL-001, #284) shipped the package skeleton plus the
``/healthz`` + ``/readyz`` probes so the Phase A Helm chart could deploy.
Phase B (CONN-IMPL-003 + CONN-IMPL-004) ships the SPL provider wiring,
the schema-revision startup gate, and the call-context middleware (with
dev shim). REST routes land in CONN-IMPL-026; the secret-bridge sidecar
lands in Phase H (CONN-IMPL-019..021).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse

from custos_connector.binding import binding_router
from custos_connector.health import router as health_router
from custos_connector.lease.router import router as lease_router
from custos_connector.middleware import (
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
)
from custos_connector.providers import (
    MigrationRequired,
    Providers,
    load_providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from custos_connector.settings import Settings, load_settings

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]
__version__ = "0.1.0"

logger = logging.getLogger("custos_connector")


def create_app(
    *,
    settings: Settings | None = None,
    providers: Providers | None = None,
) -> FastAPI:
    """Build and return the Connector Service FastAPI application.

    Args:
        settings: Pre-parsed :class:`Settings`. Defaults to
            :func:`custos_connector.settings.load_settings` reading from
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
        local_providers = providers if providers is not None else load_providers(effective_settings)
        app.state.providers = local_providers
        app.state.ready = False
        app.state.schema_gate_error = None
        try:
            await verify_schema_revisions(local_providers)
            app.state.ready = True
            logger.info("schema-revision gate passed; connector-service is ready")
        except MigrationRequired as exc:
            app.state.schema_gate_error = exc
            logger.error("%s", schema_gate_explainer(exc))
        try:
            yield
        finally:
            # Release any HTTP transport the IdentityResolverRegistry owns
            # (CONN-IMPL-015). The registry's aclose() is a no-op when no
            # transport was injected (the unit-test path), so this is safe
            # to call unconditionally.
            try:
                await local_providers.identity_registry.aclose()
            except Exception:
                logger.exception("identity registry aclose failed during shutdown")

    app = FastAPI(
        title="Custos Connector Service",
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
    # ``{"error": {"code", "detail"}}`` envelope as the middleware itself.
    app.add_exception_handler(CallContextError, call_context_error_handler)
    # FastAPI's default RequestValidationError handler returns a body of
    # shape ``{"detail": [...]}``, which does not match the call-context
    # envelope. Register our own handler so wire-shape failures from the
    # binding (and any future) routers emit the canonical
    # ``{"error": {"code", "detail"}}`` envelope.
    app.add_exception_handler(
        RequestValidationError,
        _request_validation_error_handler,
    )

    app.include_router(health_router)
    app.include_router(binding_router)
    app.include_router(lease_router)
    return app


async def _request_validation_error_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Render :class:`RequestValidationError` as the service envelope.

    The ``code`` is fixed to ``invalid-request`` to match the
    :class:`~custos_connector.binding.errors.BindErrorCode` taxonomy;
    the ``detail`` is a compact summary of the first validation error
    (full pydantic context is preserved in the structured logs via
    FastAPI's default logging — we don't echo a list to clients because
    that surface is not stable across pydantic releases).

    The signature accepts :class:`Exception` so it matches
    :meth:`FastAPI.add_exception_handler` without an explicit ``# type:
    ignore``; the implementation narrows back to
    :class:`RequestValidationError` for the attribute access.
    """
    assert isinstance(exc, RequestValidationError)
    errors = exc.errors()
    if errors:
        first = errors[0]
        loc = ".".join(str(part) for part in first.get("loc", ()) if part != "body")
        msg = first.get("msg", "validation failed")
        detail = f"{loc}: {msg}" if loc else msg
    else:
        detail = "request validation failed"
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "invalid-request", "detail": detail}},
    )
