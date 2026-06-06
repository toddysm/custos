"""Cross-cutting ASGI middleware for the Custos API Gateway.

The middleware in this package implement the gateway's request-pipeline
concerns documented in ``design/components/api-gateway/design.md``. Each is wired
into the application by :func:`custos_gateway.app.create_app`.
"""

from __future__ import annotations

from custos_gateway.middleware.auth import (
    AuthorizedCaller,
    get_auth_client,
    is_auth_bypass_path,
    require_permission,
)
from custos_gateway.middleware.correlation import (
    CorrelationIdMiddleware,
    new_correlation_id,
)
from custos_gateway.middleware.workspace import (
    ResolvedWorkspace,
    resolve_workspace,
)

__all__ = [
    "AuthorizedCaller",
    "CorrelationIdMiddleware",
    "ResolvedWorkspace",
    "get_auth_client",
    "is_auth_bypass_path",
    "new_correlation_id",
    "require_permission",
    "resolve_workspace",
]
