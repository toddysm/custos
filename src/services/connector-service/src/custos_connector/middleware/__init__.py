"""FastAPI/Starlette middleware and dependencies for HTTP scoping."""

from custos_connector.middleware.callctx import (
    CALLCTX_HEADER,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    DevShimDisabledInProductionError,
    call_context_error_handler,
    get_call_context,
    require_permission,
)

__all__ = [
    "CALLCTX_HEADER",
    "CallContext",
    "CallContextError",
    "CallContextMiddleware",
    "DevShimDisabledInProductionError",
    "call_context_error_handler",
    "get_call_context",
    "require_permission",
]
