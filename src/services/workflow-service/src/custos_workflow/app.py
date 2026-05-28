"""FastAPI application factory for the Workflow Service (WF-IMPL-015).

The factory is import-safe: no environment lookups beyond the
``WF_REQUIRE_CALL_CONTEXT`` flag, no socket connections, no Postgres
DSN parsing. All side-effecting work (compiler bootstrap, Catalog
client warm-up, Dapr Workflow client construction) lands in later
WF-IMPL-* tasks and runs inside the FastAPI lifespan.

The minimal Phase-A surface is:

- ``GET /healthz`` — flat liveness probe.
- ``GET /readyz`` — readiness gate (503 until lifespan flips
  ``app.state.ready``).
- :class:`custos_workflow.call_context.CallContextMiddleware` —
  extracts ``X-Custos-Workspace`` / ``X-Custos-Principal`` headers
  onto ``request.state.call_context`` (dev shim by default;
  production mode opts in via ``WF_REQUIRE_CALL_CONTEXT=1``).

The lifespan hook flips ``app.state.ready`` to ``True`` after startup
because WF-IMPL-015 has no real gate yet. WF-IMPL-016+ will gate readiness
on the Definition Compiler bootstrap and Catalog warm-up.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_workflow._version import __version__
from custos_workflow.call_context import CallContextMiddleware
from custos_workflow.healthz import router as health_router

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["create_app"]

logger = logging.getLogger("custos_workflow")


def _read_require_call_context_flag() -> bool:
    """Resolve ``WF_REQUIRE_CALL_CONTEXT`` to a bool.

    Truthy only on the exact string ``"1"`` so accidental values like
    ``"true"`` / ``"yes"`` do not silently flip production-mode
    behaviour. The strict comparison matches the design.md § Configuration
    convention used by sibling services.
    """
    return os.environ.get("WF_REQUIRE_CALL_CONTEXT", "") == "1"


def create_app(*, require_call_context: bool | None = None) -> FastAPI:
    """Build and return the Workflow Service FastAPI application.

    Args:
        require_call_context: Override for the ``WF_REQUIRE_CALL_CONTEXT``
            env-driven default. Tests pass this explicitly; production
            and CI rely on the env-var path. The flag is only consulted
            at app construction time so a running app never silently
            flips identity-enforcement behaviour.

    Returns:
        A configured :class:`FastAPI` instance with the call-context
        middleware mounted and the ``/healthz`` / ``/readyz`` router
        included.
    """
    from fastapi import FastAPI

    effective_require = (
        require_call_context
        if require_call_context is not None
        else _read_require_call_context_flag()
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ready = False
        app.state.ready_detail = "workflow-service has not finished startup"
        # WF-IMPL-016+ will gate this on the Definition Compiler bootstrap
        # and Catalog client warm-up. For Phase A there are no startup
        # dependencies, so the gate flips immediately.
        app.state.ready = True
        app.state.ready_detail = None
        logger.info(
            "workflow-service ready (call-context dev shim %s)",
            "disabled" if effective_require else "active",
        )
        try:
            yield
        finally:
            # No resources to release in Phase A. Later phases will close
            # the Catalog client and the Dapr Workflow client here.
            pass

    app = FastAPI(
        title="Custos Workflow Service",
        version=__version__,
        lifespan=lifespan,
    )
    app.add_middleware(
        CallContextMiddleware,
        require_call_context=effective_require,
    )
    app.include_router(health_router)
    return app
