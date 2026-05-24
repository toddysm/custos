"""Custos Auth Service (COMP-002).

This package hosts the Auth Service runtime: identity issuance, identity
verification, authorization decisions, and the internal signed call-context
contract.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/auth-service/design.md

Phase A (AS-IMPL-001 / AS-IMPL-002) shipped the FastAPI scaffold + Helm
subchart. Phase B (AS-IMPL-003 / AS-IMPL-004) wires the SPL provider
bundle (``AuthStoreProvider`` + ``MetadataStoreProvider``) into the app
factory via a FastAPI lifespan hook and runs the schema-revision startup
gate before serving traffic. Tenancy + principal model, permission/role
registry, authorization engine, service tokens, call-context signing,
and the OIDC verifier all land in subsequent AS-IMPL-* phases.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_auth.health import router as health_router
from custos_auth.providers import (
    MigrationRequired,
    Providers,
    load_providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from custos_auth.settings import Settings, load_settings

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"

logger = logging.getLogger("custos_auth")


def create_app(
    *,
    settings: Settings | None = None,
    providers: Providers | None = None,
) -> FastAPI:
    """Build and return the Auth Service FastAPI application.

    Args:
        settings: Pre-parsed :class:`Settings`. Defaults to
            :func:`custos_auth.settings.load_settings` reading from
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
            logger.info("schema-revision gate passed; auth-service is ready")
        except MigrationRequired as exc:
            app.state.schema_gate_error = exc
            logger.error("%s", schema_gate_explainer(exc))
        yield

    app = FastAPI(
        title="Custos Auth Service",
        version=__version__,
        description=(
            "Identity issuance, identity verification, authorization "
            "decisions, and the internal signed call-context contract."
        ),
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app
