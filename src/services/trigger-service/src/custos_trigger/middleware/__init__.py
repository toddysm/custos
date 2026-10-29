"""FastAPI/Starlette middleware and dependencies for HTTP scoping."""

from custos_trigger.middleware.callctx import (
    CALLCTX_HEADER,
    TRIGGER_ADMIN_PERMISSION,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    DaprWorkflowCallerVerifier,
    DevShimDisabledInProductionError,
    call_context_error_handler,
    ensure_workspace,
    get_call_context,
    require_permission,
)

__all__ = [
    "CALLCTX_HEADER",
    "TRIGGER_ADMIN_PERMISSION",
    "CallContext",
    "CallContextError",
    "CallContextMiddleware",
    "DaprWorkflowCallerVerifier",
    "DevShimDisabledInProductionError",
    "call_context_error_handler",
    "ensure_workspace",
    "get_call_context",
    "require_permission",
]
