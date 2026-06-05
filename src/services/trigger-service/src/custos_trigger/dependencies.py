"""FastAPI dependency helpers for the SPL provider bundle (TS-IMPL-008).

The lifespan in :mod:`custos_trigger.app` stashes the :class:`Providers`
bundle on ``app.state.providers``; these helpers surface it (and the
metadata store within) to request handlers introduced by the REST/RPC
phases (TS-IMPL-015..018), mirroring the auth-service convention.
"""

from __future__ import annotations

from typing import Annotated, cast

from fastapi import Depends, Request

from custos_trigger.providers import Providers
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = ["get_metadata_store", "get_providers"]


def get_providers(request: Request) -> Providers:
    """Return the :class:`Providers` bundle attached during startup."""
    providers = getattr(request.app.state, "providers", None)
    if providers is None:
        raise RuntimeError("Providers bundle is not attached to app.state; did the lifespan run?")
    return cast(Providers, providers)


def get_metadata_store(
    providers: Annotated[Providers, Depends(get_providers)],
) -> TriggerMetadataStore:
    """Return the Trigger Service metadata store from the providers bundle."""
    return providers.metadata_store
