"""Shared FastAPI dependencies for auth-service routes.

* :func:`get_providers` pulls the SPL provider bundle stored on
  ``app.state`` by the lifespan.
* :func:`get_auth_store` / :func:`get_metadata_store` are typed
  shortcuts that callers can list as router-level deps to make the
  per-route signatures shorter.
* :func:`require_permission` re-exports the middleware dependency so
  routers only import from one module.
* :func:`require_workspace_membership` adds the workspace scope check
  on top of a permission requirement (the natural shape of any
  ``/v1/workspaces/{id}/...`` route).

The pattern mirrors catalog-service's ``api/dependencies.py``; the
shape is identical so any future shared package can hoist both.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from fastapi import Depends, Request

from custos_auth.middleware.callctx import (
    CallContext,
    get_call_context,
)
from custos_auth.middleware.callctx import (
    require_permission as _require_permission_inner,
)
from custos_auth.providers import Providers


def get_providers(request: Request) -> Providers:
    """Return the SPL provider bundle attached by the lifespan.

    Raises :class:`RuntimeError` when called before the lifespan has
    populated ``app.state.providers`` — that only happens in
    pathological tests; production traffic is gated by ``/readyz``.
    """
    providers = getattr(request.app.state, "providers", None)
    if providers is None:  # pragma: no cover - defensive
        raise RuntimeError("Providers bundle is not attached to app.state. Did the lifespan run?")
    assert isinstance(providers, Providers)
    return providers


def get_auth_store(
    providers: Annotated[Providers, Depends(get_providers)],
) -> AuthStoreProvider:
    return providers.auth_store


def get_metadata_store(
    providers: Annotated[Providers, Depends(get_providers)],
) -> MetadataStoreProvider:
    return providers.metadata_store


def require_permission(
    *names: str,
) -> Callable[[Request], Awaitable[CallContext]]:
    """Re-export of :func:`custos_auth.middleware.callctx.require_permission`.

    Routes import from one place; the middleware module stays the
    canonical home of the implementation.
    """
    return _require_permission_inner(*names)


__all__ = [
    "get_auth_store",
    "get_call_context",
    "get_metadata_store",
    "get_providers",
    "require_permission",
]
