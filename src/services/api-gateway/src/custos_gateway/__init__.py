"""Custos API Gateway (COMP-001).

The single uniform HTTPS entrypoint for every external Custos caller. The
gateway contains no domain logic: it terminates TLS, validates request shape,
delegates every authn/authz decision to the Auth Service, mints the signed
call context internal RPCs travel on, deduplicates idempotent writes, applies
coarse rate limits, normalizes errors into one RFC 7807 envelope, and routes
each request to the owning downstream component via Dapr service invocation.

Design: ``design/components/api-gateway/design.md``.

The FastAPI application factory :func:`create_app` (AGW-IMPL-002) grows the
settings, health probes, middleware stack, and downstream route registry across
the subsequent AGW-IMPL phases.
"""

from __future__ import annotations

from custos_gateway._version import __version__
from custos_gateway.app import create_app

__all__ = ["__version__", "create_app"]
