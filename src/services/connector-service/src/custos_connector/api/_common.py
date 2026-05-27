"""Shared helpers for the public REST surface (CONN-IMPL-026).

This module is private to :mod:`custos_connector.api`. It centralizes:

* :func:`error_response` — the canonical
  ``{"error": {"code", "detail"}}`` envelope used by every route in
  the service.
* :func:`workspace_mismatch_response` — the 403
  ``connector.workspace_mismatch`` guard that every workspace-scoped
  route enforces in addition to the
  :func:`~custos_connector.middleware.require_permission` permission
  check.
* :func:`resolve_providers` and the per-collaborator resolver helpers
  that lift typed providers off ``app.state.providers``. Each
  resolver raises :class:`RuntimeError` when the corresponding
  collaborator was not wired during lifespan — that's a startup-time
  bug, not a per-request 500, so we surface it eagerly instead of
  letting an ``AttributeError`` leak into the response.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from starlette.requests import Request
from starlette.responses import JSONResponse

if TYPE_CHECKING:
    from custos_spl.interfaces.catalog_store import CatalogStoreProvider
    from custos_spl.interfaces.lease_store import LeaseStoreProvider
    from custos_spl.interfaces.metadata_store import MetadataStoreProvider

    from custos_connector.instances.service import InstanceService
    from custos_connector.lease.service import LeaseManager
    from custos_connector.middleware import CallContext
    from custos_connector.providers import Providers


#: HTTP error code returned by every workspace-scoped handler when
#: the ``{ws}`` path segment does not match the call-context's
#: ``workspace_id``. Mirrors the cursor and lease admin routers'
#: convention so cross-service operator tooling has one vocabulary.
WORKSPACE_MISMATCH_CODE: Final[str] = "connector.workspace_mismatch"


def error_response(*, status_code: int, code: str, detail: str) -> JSONResponse:
    """Build a canonical ``{"error": {"code", "detail"}}`` response."""
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


def workspace_mismatch_response(ctx: CallContext, ws: str) -> JSONResponse | None:
    """Return a 403 envelope if ``ws`` does not match the call-context."""
    if ctx.workspace_id != ws:
        return error_response(
            status_code=403,
            code=WORKSPACE_MISMATCH_CODE,
            detail=(
                f"call-context workspace {ctx.workspace_id!r} does not match path workspace {ws!r}"
            ),
        )
    return None


def resolve_providers(request: Request) -> Providers:
    """Pull the typed :class:`Providers` bundle off ``app.state``."""
    providers = getattr(request.app.state, "providers", None)
    if providers is None:
        raise RuntimeError(
            "connector-service: app.state.providers is unset; "
            "lifespan did not run or providers wiring failed",
        )
    return providers  # type: ignore[no-any-return]


def resolve_instance_service(request: Request) -> InstanceService:
    providers = resolve_providers(request)
    service = providers.instance_service
    if service is None:
        raise RuntimeError(
            "connector-service: InstanceService is not wired on Providers; "
            "this route requires CONN-IMPL-014 instance management to be "
            "constructed in load_providers()",
        )
    return service


def resolve_lease_manager(request: Request) -> LeaseManager:
    providers = resolve_providers(request)
    return providers.lease_manager


def resolve_lease_store(request: Request) -> LeaseStoreProvider:
    providers = resolve_providers(request)
    return providers.lease_store


def resolve_catalog_store(request: Request) -> CatalogStoreProvider:
    providers = resolve_providers(request)
    return providers.catalog_store


def resolve_metadata_store(request: Request) -> MetadataStoreProvider:
    providers = resolve_providers(request)
    return providers.metadata_store


def page_query_params(cursor: str | None, limit: int | None) -> dict[str, Any]:
    """Shared `cursor`/`limit` clean-up for list endpoints.

    Empty-string ``cursor`` is treated as ``None`` (FastAPI's default
    parser surfaces missing query params as ``None`` already, but a
    client that sends ``?cursor=`` would otherwise reach the store
    with an empty opaque cursor and crash deep in the adapter).
    """
    cleaned: dict[str, Any] = {}
    if cursor:
        cleaned["cursor"] = cursor
    if limit is not None:
        cleaned["limit"] = limit
    return cleaned


__all__ = [
    "WORKSPACE_MISMATCH_CODE",
    "error_response",
    "page_query_params",
    "resolve_catalog_store",
    "resolve_instance_service",
    "resolve_lease_manager",
    "resolve_lease_store",
    "resolve_metadata_store",
    "resolve_providers",
    "workspace_mismatch_response",
]
