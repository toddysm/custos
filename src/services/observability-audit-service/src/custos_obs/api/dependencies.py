"""FastAPI dependency factories for the read-back Query API (OBS-IMPL-012).

The :func:`custos_obs.create_app` lifespan resolves the SPL providers once and
stashes them on ``app.state.providers`` (a :class:`custos_obs.providers.Providers`
bundle). The read-back routes (OBS-IMPL-013/014) declare these dependencies so
each handler receives the long-lived provider singleton — no per-request
construction, no global lookups in the route bodies.

Resolving from ``request.app.state`` (rather than a module global) keeps the
providers swappable in tests: inject a ``Providers`` bundle of fakes via
``create_app(providers=...)`` and the same dependencies hand them to the routes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.requests import Request

if TYPE_CHECKING:
    from custos_spl.interfaces.log_query import LogQueryProvider
    from custos_spl.interfaces.metadata_store import MetadataStoreProvider
    from custos_spl.interfaces.metrics_query import MetricsQueryProvider

    from custos_obs.providers import Providers
    from custos_obs.settings import Settings


def get_providers(request: Request) -> Providers:
    """Return the resolved provider bundle stashed by the app lifespan.

    Raises:
        RuntimeError: When ``app.state.providers`` is unset — the lifespan did
            not run (a wiring bug), not a client-facing condition.
    """
    providers = getattr(request.app.state, "providers", None)
    if providers is None:
        raise RuntimeError(
            "app.state.providers is not set; the application lifespan must run "
            "before request handling (resolve providers in create_app)."
        )
    from custos_obs.providers import Providers

    assert isinstance(providers, Providers)
    return providers


def get_log_query_provider(request: Request) -> LogQueryProvider:
    """Dependency yielding the shared :class:`LogQueryProvider` singleton."""
    return get_providers(request).log_query


def get_metrics_query_provider(request: Request) -> MetricsQueryProvider:
    """Dependency yielding the shared :class:`MetricsQueryProvider` singleton."""
    return get_providers(request).metrics_query


def get_metadata_store(request: Request) -> MetadataStoreProvider:
    """Dependency yielding the shared :class:`MetadataStoreProvider` singleton."""
    return get_providers(request).metadata_store


def get_settings(request: Request) -> Settings:
    """Return the resolved :class:`Settings` stashed by the app lifespan.

    Routes read the ``*_external_url`` pointers from here to populate the
    ``503`` Problem Details extension when a read-back provider is unavailable.

    Raises:
        RuntimeError: When ``app.state.settings`` is unset — the lifespan did
            not run (a wiring bug), not a client-facing condition.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError(
            "app.state.settings is not set; the application lifespan must run "
            "before request handling (load settings in create_app)."
        )
    from custos_obs.settings import Settings

    assert isinstance(settings, Settings)
    return settings


__all__ = [
    "get_log_query_provider",
    "get_metadata_store",
    "get_metrics_query_provider",
    "get_providers",
    "get_settings",
]
