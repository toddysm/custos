"""FastAPI HTTP surface for auth-service (AS-IMPL-005/006/007).

Phase C ships the M1 admin and identity-introspection endpoints:

* :mod:`custos_auth.api.routes.tenants` — tenant + workspace create/list.
* :mod:`custos_auth.api.routes.workspaces` — workspace list/read with
  cross-tenant 404 semantics.
* :mod:`custos_auth.api.routes.principals` — ``GET /v1/principals/me``
  and the admin disable endpoint.
* :mod:`custos_auth.api.routes.service_accounts` — service-account
  create endpoint.

:mod:`custos_auth.api.errors` maps every manager-level exception to the
shared ``{"error": {"code", "detail"}}`` envelope so HTTP clients see
one shape regardless of which layer produced the response.
"""

from __future__ import annotations

from custos_auth.api.errors import register_exception_handlers
from custos_auth.api.routes import all_routers

__all__ = [
    "all_routers",
    "register_exception_handlers",
]
