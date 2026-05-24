"""Custos Auth Service (COMP-002).

This package hosts the Auth Service runtime: identity issuance, identity
verification, authorization decisions, and the internal signed call-context
contract.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/auth-service/design.md

AS-IMPL-001 (Phase A) ships the scaffold only: a FastAPI application with
``/healthz`` and ``/readyz`` returning 200 unconditionally. Persistence
(``AuthStoreProvider``), the tenancy + principal model, the permission/role
registry, the authorization engine, service tokens, the signed call-context,
and the OIDC verifier all land in subsequent AS-IMPL-* phases.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from custos_auth.health import router as health_router

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"

logger = logging.getLogger("custos_auth")


def create_app() -> FastAPI:
    """Build and return the Auth Service FastAPI application.

    The factory is import-safe: no DSN lookups, no socket connections, no
    secret-store calls. Subsequent phases (AS-IMPL-004, AS-IMPL-017) will
    wire startup-time work into a FastAPI lifespan hook.
    """
    from fastapi import FastAPI

    app = FastAPI(
        title="Custos Auth Service",
        version=__version__,
        description=(
            "Identity issuance, identity verification, authorization "
            "decisions, and the internal signed call-context contract."
        ),
    )
    app.include_router(health_router)
    return app
