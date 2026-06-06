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
from custos_gateway.middleware.callctx_mint import (
    MintedCallContext,
    mint_call_context,
)
from custos_gateway.middleware.correlation import (
    CorrelationIdMiddleware,
    new_correlation_id,
)
from custos_gateway.middleware.idempotency import (
    IdempotencyCoordinator,
    IdempotencyKey,
    ProceedReservation,
    ReplayReservation,
    compute_request_hash,
    is_idempotent_method,
    resolve_idempotency_key,
)
from custos_gateway.middleware.ratelimit import (
    Allow,
    BucketConfig,
    Deny,
    RateLimiter,
    is_rate_limited_method,
    rate_limit_denied_error,
    rate_limit_headers,
)
from custos_gateway.middleware.workspace import (
    ResolvedWorkspace,
    resolve_workspace,
)

__all__ = [
    "Allow",
    "AuthorizedCaller",
    "BucketConfig",
    "CorrelationIdMiddleware",
    "Deny",
    "IdempotencyCoordinator",
    "IdempotencyKey",
    "MintedCallContext",
    "ProceedReservation",
    "RateLimiter",
    "ReplayReservation",
    "ResolvedWorkspace",
    "compute_request_hash",
    "get_auth_client",
    "is_auth_bypass_path",
    "is_idempotent_method",
    "is_rate_limited_method",
    "mint_call_context",
    "new_correlation_id",
    "rate_limit_denied_error",
    "rate_limit_headers",
    "require_permission",
    "resolve_idempotency_key",
    "resolve_workspace",
]
