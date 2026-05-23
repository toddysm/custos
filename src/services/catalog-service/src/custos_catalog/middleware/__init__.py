"""FastAPI/Starlette middleware and dependencies for HTTP scoping."""

from custos_catalog.middleware.callctx import (
    CALLCTX_HEADER,
    CallContext,
    CallContextMiddleware,
    DevShimDisabledInProductionError,
    get_call_context,
    require_permission,
)

__all__ = [
    "CALLCTX_HEADER",
    "CallContext",
    "CallContextMiddleware",
    "DevShimDisabledInProductionError",
    "get_call_context",
    "require_permission",
]
