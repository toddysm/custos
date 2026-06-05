"""Trigger Service FastAPI application factory (TS-IMPL-003).

:func:`create_app` builds the FastAPI application: the ``/healthz`` +
``/readyz`` probes, the call-context middleware (with dev shim), and a
lifespan hook that marks the app ready once startup completes.

The factory is import-safe — no socket connections, no env-dependent failures
at construction time. All side-effecting work belongs in the FastAPI lifespan
context. The SPL provider wiring + schema-revision gate land in TS-IMPL-008,
the typed :class:`Settings` loader in TS-IMPL-004, and the REST/RPC surface
across TS-IMPL-015..018; this module grows those in place without changing
the factory's import-safety contract.

Configuration today is read directly from the process environment because the
typed settings loader is a later phase (TS-IMPL-004). Two knobs steer the
call-context middleware:

- ``TRIGGER_AUTHZ_ENDPOINT`` (default empty) — when empty the dev shim is
  active; when set, production call-context validation against the Auth
  Service (COMP-002) is required and currently surfaces a
  :class:`NotImplementedError` until that integration lands.
- ``ENVIRONMENT`` (default ``development``) — ``production`` with an empty
  authz endpoint refuses to boot (the dev shim is forbidden in production).
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_trigger._version import __version__
from custos_trigger.health import router as health_router
from custos_trigger.middleware import (
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger("custos_trigger")


def create_app(
    *,
    authz_endpoint: str | None = None,
    environment: str | None = None,
) -> FastAPI:
    """Build and return the Trigger Service FastAPI application.

    Args:
        authz_endpoint: Auth Service URL steering the call-context
            middleware. Defaults to the ``TRIGGER_AUTHZ_ENDPOINT`` env var
            (empty enables the dev shim). The typed settings loader
            (TS-IMPL-004) will supply this from :class:`Settings`.
        environment: Deployment environment. Defaults to the ``ENVIRONMENT``
            env var (``development`` when unset). ``production`` with an
            empty ``authz_endpoint`` refuses to boot.

    The factory is import-safe: no DSN lookups, no socket connections. All
    side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI

    effective_authz = (
        authz_endpoint
        if authz_endpoint is not None
        else os.environ.get("TRIGGER_AUTHZ_ENDPOINT", "")
    )
    effective_env = (
        environment if environment is not None else os.environ.get("ENVIRONMENT", "development")
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # No schema-revision gate yet — the SPL provider wiring lands in
        # TS-IMPL-008, at which point this hook grows the readiness check.
        # For now the app is ready as soon as the process has booted.
        app.state.ready = True
        logger.info("trigger-service is ready")
        yield

    app = FastAPI(
        title="Custos Trigger Service",
        version=__version__,
        lifespan=lifespan,
    )

    # CallContextMiddleware is instantiated lazily by Starlette when the
    # middleware stack is first built, so a DevShimDisabledInProductionError
    # surfaces either at startup or on the first request (whichever builds the
    # stack first) — visible to the operator the same way any startup
    # exception is, rather than at create_app() time. This keeps the factory
    # side-effect free and trivially testable.
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=effective_authz,
        environment=effective_env,
    )
    # Pair the middleware with its exception handler so the dependency-side
    # 4xx responses (get_call_context / require_permission) emit the same
    # `{"error": {"code", "detail"}}` envelope as the middleware itself.
    app.add_exception_handler(CallContextError, call_context_error_handler)

    app.include_router(health_router)
    return app


__all__ = ["create_app"]
