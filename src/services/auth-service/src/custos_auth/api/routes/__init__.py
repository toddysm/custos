"""Routes that ship with :mod:`custos_auth.api`."""

from __future__ import annotations

from custos_auth.api.routes import (
    principals,
    service_accounts,
    tenants,
    workspaces,
)

#: Ordered list of routers wired into the FastAPI app by
#: :func:`custos_auth.create_app`. Order is purely cosmetic (the
#: rendered OpenAPI doc reads from this list).
all_routers = [
    tenants.router,
    workspaces.router,
    principals.router,
    service_accounts.router,
]


__all__ = ["all_routers"]
