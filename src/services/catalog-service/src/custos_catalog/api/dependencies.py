"""FastAPI dependency factories for the Catalog Service API.

The factories pull the SPL :class:`Providers` off ``app.state`` and
build the manager objects per-request. Managers are stateless and
cheap to construct, so we re-create them on every call rather than
keep singletons on app.state. This also makes test injection trivial:
override ``get_providers`` and every manager dependency follows.

Workspace authorization is enforced by :func:`require_workspace_access` —
a dependency factory that pulls the call-context off the request state
(populated by :class:`CallContextMiddleware`, CS-IMPL-004) and enforces
that the path workspace matches the context workspace, plus an optional
named permission.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import Depends, Path, Request

from custos_catalog.managers.activity_registry import ActivityTypeRegistry
from custos_catalog.managers.connector_registry import ConnectorTypeRegistry
from custos_catalog.managers.definition import DefinitionManager
from custos_catalog.managers.template import TemplateManager
from custos_catalog.middleware.callctx import (
    CallContext,
    CallContextError,
    get_call_context,
)
from custos_catalog.providers import Providers
from custos_catalog.resolve import ConnectorClient, StubConnectorClient
from custos_catalog.versioning import VersioningManager

# ---------------------------------------------------------------------------
# State accessors
# ---------------------------------------------------------------------------


def get_providers(request: Request) -> Providers:
    """Return the :class:`Providers` bundle held on ``app.state``.

    The lifespan hook in :func:`custos_catalog.create_app` populates
    ``app.state.providers`` before any request is dispatched.
    """
    providers = getattr(request.app.state, "providers", None)
    if providers is None:  # pragma: no cover - lifespan misconfigured
        raise RuntimeError("Providers not initialized on app.state; create_app() lifespan failed.")
    assert isinstance(providers, Providers)
    return providers


def get_connector_client(request: Request) -> ConnectorClient:
    """Return the connector-client used by the resolver pipeline.

    Falls back to a per-request :class:`StubConnectorClient` until
    CS-IMPL-023 lands. Tests can override the fallback by assigning
    ``app.state.connector_client``.
    """
    client = getattr(request.app.state, "connector_client", None)
    if client is None:
        client = StubConnectorClient()
    return client


def get_activity_registry(
    request: Request,
    providers: Providers = Depends(get_providers),
) -> ActivityTypeRegistry:
    """Build a per-request :class:`ActivityTypeRegistry`.

    Picks up optional ``platform_admins`` / ``vendor_grants`` from
    ``app.state`` so tests can extend authorisation without rewiring
    the whole factory.
    """
    state = request.app.state
    return ActivityTypeRegistry(
        catalog_store=providers.catalog_store,
        platform_admins=getattr(state, "platform_admins", None),
        vendor_grants=getattr(state, "vendor_grants", None),
    )


def get_connector_registry(
    providers: Providers = Depends(get_providers),
) -> ConnectorTypeRegistry:
    """Build a per-request :class:`ConnectorTypeRegistry`."""
    return ConnectorTypeRegistry(catalog_store=providers.catalog_store)


def get_versioning_manager(
    providers: Providers = Depends(get_providers),
) -> VersioningManager:
    return VersioningManager(store=providers.definition_store)


def get_definition_manager(
    providers: Providers = Depends(get_providers),
    activity_registry: ActivityTypeRegistry = Depends(get_activity_registry),
    connector_client: ConnectorClient = Depends(get_connector_client),
    versioning: VersioningManager = Depends(get_versioning_manager),
) -> DefinitionManager:
    """Build a per-request :class:`DefinitionManager`."""
    return DefinitionManager(
        definition_store=providers.definition_store,
        activity_registry=activity_registry,
        connector_client=connector_client,
        versioning=versioning,
    )


def get_template_manager(
    providers: Providers = Depends(get_providers),
    activity_registry: ActivityTypeRegistry = Depends(get_activity_registry),
    connector_client: ConnectorClient = Depends(get_connector_client),
    versioning: VersioningManager = Depends(get_versioning_manager),
    definition_manager: DefinitionManager = Depends(get_definition_manager),
) -> TemplateManager:
    """Build a per-request :class:`TemplateManager`."""
    return TemplateManager(
        definition_store=providers.definition_store,
        activity_registry=activity_registry,
        connector_client=connector_client,
        versioning=versioning,
        definition_manager=definition_manager,
    )


# ---------------------------------------------------------------------------
# Workspace + permission enforcement
# ---------------------------------------------------------------------------


def require_workspace_access(
    permission: str,
) -> Callable[[str, CallContext], Awaitable[CallContext]]:
    """Build a dependency that enforces the path workspace + a permission.

    The returned coroutine:
    1. Calls :func:`get_call_context` (which raises 401 on a missing /
       malformed header via the call-context handler).
    2. Verifies the URL ``{ws}`` matches the context's ``workspace_id``
       (403 ``catalog.workspace_mismatch`` otherwise).
    3. Verifies the context carries ``permission`` (403
       ``permission_denied``).
    """

    async def _dep(
        ws: str = Path(..., description="Workspace id from the URL path."),
        ctx: CallContext = Depends(get_call_context),
    ) -> CallContext:
        if ctx.workspace_id != ws:
            raise CallContextError(
                403,
                "catalog.workspace_mismatch",
                f"call context workspace {ctx.workspace_id!r} does not match URL workspace {ws!r}",
            )
        if not ctx.has_permission(permission):
            raise CallContextError(
                403,
                "permission_denied",
                f"missing required permission: {permission}",
            )
        return ctx

    _dep.__name__ = f"require_workspace_access[{permission}]"
    return _dep


def require_permission_only(
    permission: str,
) -> Callable[[CallContext], Awaitable[CallContext]]:
    """Permission-only dependency for endpoints without a ``{ws}`` segment.

    Used by ``/v1/catalog/connector-types`` and ``/v1/workflows/...``
    (the by-ID lookup), both of which are not workspace-scoped at the
    URL level.
    """

    async def _dep(ctx: CallContext = Depends(get_call_context)) -> CallContext:
        if not ctx.has_permission(permission):
            raise CallContextError(
                403,
                "permission_denied",
                f"missing required permission: {permission}",
            )
        return ctx

    _dep.__name__ = f"require_permission_only[{permission}]"
    return _dep


__all__ = [
    "get_activity_registry",
    "get_connector_client",
    "get_connector_registry",
    "get_definition_manager",
    "get_providers",
    "get_template_manager",
    "get_versioning_manager",
    "require_permission_only",
    "require_workspace_access",
]
