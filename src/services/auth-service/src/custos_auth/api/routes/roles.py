"""Role + permission registry read endpoints (AS-IMPL-009, GH-#244).

Phase D ships only the **read** surface for the built-in role catalogue
and the declared permission registry:

* ``GET /v1/roles``           — list every role known to this build of
                                auth-service. Currently the six v1
                                built-ins seeded at startup; M2+ custom
                                roles will appear here once a
                                ``POST /v1/roles`` lands.
* ``GET /v1/permissions``     — list every declared permission, with
                                the loader-side multi-declarer
                                attribution (``declared_by``).
* ``POST /v1/roles``          — explicit 501 ``not_implemented``.
                                Reserved for the M2+ custom-role surface.

Both read endpoints require only an authenticated call context. The
role / permission catalogue is operator metadata that any logged-in
principal can read; access-control on **using** a role is enforced at
the role-binding endpoints (AS-IMPL-010).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request, status

from custos_auth import _telemetry as telemetry
from custos_auth.api.dependencies import get_call_context
from custos_auth.api.errors import AuthApiError
from custos_auth.api.models import (
    PermissionListResponse,
    PermissionResponse,
    RoleListResponse,
    RoleResponse,
)
from custos_auth.middleware.callctx import CallContext
from custos_auth.permission_registry import DeclaredPermission
from custos_auth.roles import BUILTIN_ROLES

router = APIRouter(prefix="/v1", tags=["roles"])


def get_declared_permissions(request: Request) -> dict[str, DeclaredPermission]:
    """Return the permission registry materialised by the startup loader.

    Cached on ``app.state.declared_permissions`` by the auth-service
    lifespan immediately after the schema-revision gate passes. The
    dependency reads it through the request rather than re-loading
    the YAML so a single startup-time validation step covers every
    serving request.
    """
    declared = getattr(request.app.state, "declared_permissions", None)
    if declared is None:  # pragma: no cover - defensive
        raise RuntimeError(
            "Declared permissions registry is not attached to app.state. Did the lifespan run?"
        )
    assert isinstance(declared, dict)
    return declared


@router.get(
    "/roles",
    response_model=RoleListResponse,
)
async def list_roles(
    _ctx: Annotated[CallContext, Depends(get_call_context)],
) -> RoleListResponse:
    """List every role known to this build of auth-service."""
    with telemetry.observe_operation(telemetry.OP_ROLE_LIST):
        return RoleListResponse(
            roles=[
                RoleResponse(
                    role_id=str(role.role_id),
                    name=role.name,
                    description=role.description,
                    permission_names=list(role.permission_names),
                    allowed_scopes=sorted(role.allowed_scopes),
                )
                for role in BUILTIN_ROLES
            ],
        )


class _NotImplementedError(AuthApiError):
    """Marker 501 error rendered through the shared error envelope."""

    status_code = status.HTTP_501_NOT_IMPLEMENTED
    code = "not_implemented"


@router.post("/roles", include_in_schema=False)
async def create_role(
    _ctx: Annotated[CallContext, Depends(get_call_context)],
) -> None:
    """Reserved for M2+ custom roles.

    Returns 501 with the shared error envelope so clients distinguish
    "not yet implemented" from "permission denied".
    """
    raise _NotImplementedError(
        "Custom role authoring is not part of M1. Built-in roles are "
        "seeded at startup and exposed via GET /v1/roles."
    )


@router.get(
    "/permissions",
    response_model=PermissionListResponse,
)
async def list_permissions(
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    declared: Annotated[
        dict[str, DeclaredPermission],
        Depends(get_declared_permissions),
    ],
) -> PermissionListResponse:
    """List every declared permission with multi-declarer attribution.

    Reads the registry materialised by the startup loader; does **not**
    round-trip through ``AuthStoreProvider.list_permissions`` because
    the SPL ``Permission`` row does not carry the ``declared_by``
    attribution.
    """
    with telemetry.observe_operation(telemetry.OP_PERMISSION_LIST):
        return PermissionListResponse(
            permissions=[
                PermissionResponse(
                    name=perm.name,
                    description=perm.description,
                    declared_by=perm.declared_by,
                )
                for perm in sorted(declared.values(), key=lambda p: p.name)
            ],
        )


__all__ = ["get_declared_permissions", "router"]
