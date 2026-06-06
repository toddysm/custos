"""Custos API Gateway (COMP-001).

The single uniform HTTPS entrypoint for every external Custos caller. The
gateway contains no domain logic: it terminates TLS, validates request shape,
delegates every authn/authz decision to the Auth Service, mints the signed
call context internal RPCs travel on, deduplicates idempotent writes, applies
coarse rate limits, normalizes errors into one RFC 7807 envelope, and routes
each request to the owning downstream component via Dapr service invocation.

Design: ``design/components/api-gateway/design.md``.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
