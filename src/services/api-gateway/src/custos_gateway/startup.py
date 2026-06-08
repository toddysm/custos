"""Startup permission validation for the Custos API Gateway (AGW-IMPL-008).

The gateway never invents permission names: every route declares its
``requiredPermission`` at registration time via
:func:`custos_gateway.middleware.auth.require_permission`, and those names must
exist in the Auth Service permission registry (``GET /v1/permissions``). This
module performs that cross-check inside the FastAPI lifespan and only flips
readiness once it passes.

The check is resilient to dependency start order (issue #815): the Auth Service
and the Dapr sidecar may not be reachable yet on a cold cluster, so a *transient*
transport / ``5xx`` failure does not crash the process. Instead the gateway stays
up but not-ready and :func:`converge_route_permissions` keeps retrying in the
background until the registry becomes reachable. A *permanent* failure — a
permission drift (:class:`GatewayStartupError`, code
``gateway-startup-permission-missing``) or a non-retryable Auth Service contract
error — leaves the gateway permanently not-ready with an operator-actionable
``/readyz`` detail rather than crash-looping (see
``design/components/api-gateway/design.md`` § Failure Modes).
"""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from custos_gateway.clients.auth import AuthServiceClientError
from custos_gateway.errors import GatewayErrorCode
from custos_gateway.middleware.auth import route_required_permission

if TYPE_CHECKING:
    from fastapi import FastAPI
    from starlette.routing import BaseRoute

    from custos_gateway.clients.auth import AuthServiceClient

logger = logging.getLogger("custos_gateway")

__all__ = [
    "GatewayStartupError",
    "StartupCheckOutcome",
    "collect_required_permissions",
    "converge_route_permissions",
    "run_startup_permission_check",
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


class StartupCheckOutcome(enum.Enum):
    """The result of a single startup permission cross-check attempt.

    ``READY`` flips the gateway ready; ``PERMANENT_FAILURE`` leaves it up but
    permanently not-ready (a permission drift or a non-retryable Auth Service
    contract error — retrying cannot help); ``TRANSIENT_FAILURE`` signals the
    Auth Service was unreachable and the caller should retry with backoff.
    """

    READY = "ready"
    PERMANENT_FAILURE = "permanent_failure"
    TRANSIENT_FAILURE = "transient_failure"


async def run_startup_permission_check(
    *, app: FastAPI, client: AuthServiceClient
) -> StartupCheckOutcome:
    """Run the permission cross-check once and record readiness on ``app.state``.

    Unlike :func:`validate_route_permissions` this never raises: it classifies
    the result so the lifespan can keep the process alive and converge in the
    background. ``app.state.ready`` is set to ``True`` only on success; on any
    failure it is forced ``False`` and ``app.state.ready_detail`` is set to an
    operator-actionable reason surfaced by the ``/readyz`` probe.
    """
    try:
        await validate_route_permissions(app=app, client=client)
    except GatewayStartupError as exc:
        app.state.ready = False
        app.state.ready_detail = f"startup permission check failed: {exc}"
        logger.error("startup permission check failed; staying not-ready: %s", exc)
        return StartupCheckOutcome.PERMANENT_FAILURE
    except AuthServiceClientError as exc:
        if not exc.retryable:
            app.state.ready = False
            app.state.ready_detail = f"auth-service contract error during startup: {exc}"
            logger.error(
                "startup permission check hit a non-retryable auth-service error; "
                "staying not-ready: %s",
                exc,
            )
            return StartupCheckOutcome.PERMANENT_FAILURE
        app.state.ready = False
        app.state.ready_detail = f"auth-service unreachable during startup: {exc}"
        logger.warning("startup permission check transient failure; will retry: %s", exc)
        return StartupCheckOutcome.TRANSIENT_FAILURE
    app.state.ready = True
    app.state.ready_detail = None
    logger.info("api-gateway is ready")
    return StartupCheckOutcome.READY


async def converge_route_permissions(
    *,
    app: FastAPI,
    client: AuthServiceClient,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Retry the startup permission check until it settles (success or permanent).

    Call this after :func:`run_startup_permission_check` has returned
    :attr:`StartupCheckOutcome.TRANSIENT_FAILURE`: it sleeps (the first attempt
    already failed transiently) then re-runs the check on an exponential backoff,
    flipping the gateway ready as soon as the Auth Service becomes reachable and
    the cross-check passes. It returns once the outcome is no longer transient —
    either ready or a permanent failure (no infinite retry on permission drift).
    Designed to run as a lifespan-owned background task that is cancelled on
    shutdown.
    """
    backoff = initial_backoff_seconds
    while True:
        await sleep(backoff)
        outcome = await run_startup_permission_check(app=app, client=client)
        if outcome is not StartupCheckOutcome.TRANSIENT_FAILURE:
            return
        backoff = min(backoff * 2, max_backoff_seconds)
