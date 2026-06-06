"""Startup permission validation for the Custos API Gateway (AGW-IMPL-008).

The gateway never invents permission names: every route declares its
``requiredPermission`` at registration time via
:func:`custos_gateway.middleware.auth.require_permission`, and those names must
exist in the Auth Service permission registry (``GET /v1/permissions``). This
module performs that cross-check once, inside the FastAPI lifespan *before*
readiness flips, and refuses to boot
(:class:`GatewayStartupError`, code ``gateway-startup-permission-missing``) when
a route references an undeclared permission. A drifted permission name is
therefore a loud fail-fast at startup rather than a per-request authorization
surprise (see ``design/components/api-gateway/design.md`` § Failure Modes).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from custos_gateway.errors import GatewayErrorCode
from custos_gateway.middleware.auth import route_required_permission

if TYPE_CHECKING:
    from fastapi import FastAPI
    from starlette.routing import BaseRoute

    from custos_gateway.clients.auth import AuthServiceClient

logger = logging.getLogger("custos_gateway")

__all__ = [
    "GatewayStartupError",
    "collect_required_permissions",
    "validate_route_permissions",
]


class GatewayStartupError(RuntimeError):
    """A fatal startup precondition failed; the gateway refuses to boot.

    Carries the locked taxonomy ``code`` so logs and operators can correlate the
    panic with the documented failure mode. Unlike
    :class:`~custos_gateway.errors.GatewayError`, it never reaches the wire — it
    aborts the lifespan before the app serves traffic.
    """

    def __init__(self, message: str, *, code: GatewayErrorCode) -> None:
        super().__init__(message)
        self.code = code


def collect_required_permissions(app: FastAPI) -> set[str]:
    """Return every permission name the app's routes declare.

    Walks each route's dependency tree (a route may declare the permission
    dependency directly or inherit it from a mounted router) and collects the
    name stamped on every :func:`require_permission` dependency.
    """
    permissions: set[str] = set()
    for route in app.routes:
        permissions.update(_route_permissions(route))
    return permissions


def _route_permissions(route: BaseRoute) -> set[str]:
    """Collect declared permissions from a single route's dependency tree."""
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return set()
    return _dependant_permissions(dependant)


def _dependant_permissions(dependant: object) -> set[str]:
    """Recursively collect permissions from a FastAPI ``Dependant`` tree."""
    found: set[str] = set()
    permission = route_required_permission(getattr(dependant, "call", None))
    if permission is not None:
        found.add(permission)
    for sub in getattr(dependant, "dependencies", []):
        found.update(_dependant_permissions(sub))
    return found


async def validate_route_permissions(*, app: FastAPI, client: AuthServiceClient) -> None:
    """Refuse to start if any route references an undeclared permission.

    Fetches the Auth Service permission registry and cross-checks it against the
    permissions the registered routes declare.

    Raises:
        GatewayStartupError: When one or more declared route permissions are
            absent from the registry (code ``gateway-startup-permission-missing``).
    """
    required = collect_required_permissions(app)
    if not required:
        logger.info("startup permission check: no route permissions to validate")
        return

    declared = {permission.name for permission in await client.get_permissions()}
    missing = sorted(required - declared)
    if missing:
        raise GatewayStartupError(
            "Routes reference permissions absent from the Auth Service registry: "
            + ", ".join(missing),
            code=GatewayErrorCode.GATEWAY_STARTUP_PERMISSION_MISSING,
        )
    logger.info("startup permission check: %d route permission(s) validated", len(required))
