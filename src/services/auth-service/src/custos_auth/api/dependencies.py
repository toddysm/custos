"""Shared FastAPI dependencies for auth-service routes.

* :func:`get_providers` pulls the SPL provider bundle stored on
  ``app.state`` by the lifespan.
* :func:`get_auth_store` / :func:`get_metadata_store` are typed
  shortcuts that callers can list as router-level deps to make the
  per-route signatures shorter.
* :func:`require_permission` re-exports the middleware dependency so
  routers only import from one module.

The pattern mirrors the shared dependency modules used by sibling
services so any future common package can hoist these helpers.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from fastapi import Depends, Request

from custos_auth.authn_cache import AuthnCache
from custos_auth.binding_events import BindingChangedPublisher
from custos_auth.middleware.callctx import (
    CallContext,
    get_call_context,
)
from custos_auth.middleware.callctx import (
    require_permission as _require_permission_inner,
)
from custos_auth.providers import Providers
from custos_auth.settings import Settings
from custos_auth.token_revoked_events import TokenRevokedPublisher


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


def get_settings(request: Request) -> Settings:
    """Return the parsed :class:`Settings` attached by the lifespan.

    Used by route handlers that need to read configuration knobs
    (service-token default TTL, cache TTLs, …) without re-parsing
    the environment.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None:  # pragma: no cover - defensive
        raise RuntimeError("Settings is not attached to app.state. Did the lifespan run?")
    assert isinstance(settings, Settings)
    return settings


def get_auth_store(
    providers: Annotated[Providers, Depends(get_providers)],
) -> AuthStoreProvider:
    return providers.auth_store


def get_metadata_store(
    providers: Annotated[Providers, Depends(get_providers)],
) -> MetadataStoreProvider:
    return providers.metadata_store


def get_binding_changed_publisher(
    providers: Annotated[Providers, Depends(get_providers)],
) -> BindingChangedPublisher:
    """Return the binding-changed event publisher from the provider bundle.

    Defaults to :class:`NoOpBindingChangedPublisher` for single-replica
    deployments; the Phase E deployment swaps in the real transport.
    """
    return providers.binding_changed_publisher


def get_token_revoked_publisher(
    providers: Annotated[Providers, Depends(get_providers)],
) -> TokenRevokedPublisher:
    """Return the token-revoked event publisher from the provider bundle.

    Defaults to :class:`LocalTokenRevokedBus` for single-replica
    deployments; multi-replica deployments swap in the real
    Dapr Pub/Sub or SPL-outbox-backed transport.
    """
    return providers.token_revoked_publisher


def get_authn_cache(
    providers: Annotated[Providers, Depends(get_providers)],
) -> AuthnCache:
    """Return the per-replica authn cache from the provider bundle."""
    return providers.authn_cache


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
    "get_authn_cache",
    "get_binding_changed_publisher",
    "get_call_context",
    "get_metadata_store",
    "get_providers",
    "get_settings",
    "get_token_revoked_publisher",
    "require_permission",
]
