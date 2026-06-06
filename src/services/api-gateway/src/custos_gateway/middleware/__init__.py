"""Cross-cutting ASGI middleware for the Custos API Gateway.

The middleware in this package implement the gateway's request-pipeline
concerns documented in ``design/components/api-gateway/design.md``. Each is wired
into the application by :func:`custos_gateway.app.create_app`.
"""

from __future__ import annotations

from custos_gateway.middleware.correlation import (
    CorrelationIdMiddleware,
    new_correlation_id,
)

__all__ = ["CorrelationIdMiddleware", "new_correlation_id"]
