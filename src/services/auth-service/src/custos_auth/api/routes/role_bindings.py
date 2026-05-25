"""Workspace-scope role-binding endpoints (AS-IMPL-010, GH-#245).

Phase D ships two role-binding endpoints, both ``admin:role-binding``:

* ``POST   /v1/workspaces/{workspace_id}/role-bindings``
* ``DELETE /v1/workspaces/{workspace_id}/role-bindings/{binding_id}``

Tenant- and platform-scope binding endpoints are out of #245's listed
scope; they will land alongside the full public surface in AS-IMPL-024
(Phase I) and share the same audit + binding-changed plumbing
implemented here.

Atomicity
---------

The SPL ``with_transaction`` primitive is intra-provider — a handle
issued by ``AuthStoreProvider`` cannot legally be passed to
``MetadataStoreProvider.append_audit`` because the two providers run
in independent transaction domains. The handlers therefore commit the
binding write first and follow up with a **best-effort** audit
emission and a **best-effort** binding-changed publish, in that
order. The OTel counter
:data:`custos_auth.audit.EMIT_FAILURES_TOTAL` alerts on dropped audit
rows; the per-pod authorize cache (Phase E / AS-IMPL-012) falls back
to its TTL when a publish drops.

Anti-leak semantics
-------------------

* Cross-tenant workspace targeting collapses to 404 ``not_found``
  rather than 403 (matching ``POST /v1/tenants/.../workspaces``).
* DELETE on a binding that exists in a **different** workspace
  collapses to 404 — the operator must know the (workspace, binding)
  pair, the binding identifier alone is not enough.
* Scope-rule violations (binding ``role:tenant.admin`` through the
  workspace endpoint) surface as 400 ``invalid_role_scope`` with the
  role and the violated scope kind in the diagnostic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import uuid4

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import PrincipalId, RoleBindingId, RoleId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    RoleBinding,
    RoleBindingFilter,
    WorkspaceScope,
)
from fastapi import APIRouter, Depends, status

from custos_auth import _telemetry as telemetry
from custos_auth.api.dependencies import (
    get_auth_store,
    get_binding_changed_publisher,
    get_metadata_store,
    require_permission,
)
from custos_auth.api.errors import InvalidRoleScope, NotFound
from custos_auth.api.models import (
    RoleBindingCreateRequest,
    RoleBindingResponse,
)
from custos_auth.audit import (
    audit_role_binding_granted,
    audit_role_binding_revoked,
)
from custos_auth.binding_events import (
    BindingChangedEvent,
    BindingChangedPublisher,
)
from custos_auth.middleware.callctx import CallContext
from custos_auth.roles import is_scope_allowed, scope_kind

router = APIRouter(prefix="/v1", tags=["role-bindings"])

_REVOKE_REASON: str = "revoked"


def _binding_to_response(binding: RoleBinding) -> RoleBindingResponse:
    """Project a SPL :class:`RoleBinding` to the wire envelope."""
    kind = scope_kind(binding.scope)
    scope_id: str | None
    if isinstance(binding.scope, WorkspaceScope):
        scope_id = str(binding.scope.workspace_id)
    elif kind == "tenant":
        # ``binding.scope`` is a :class:`TenantScope` — mypy --strict
        # cannot narrow the union via :func:`scope_kind` so the cast is
        # via attribute access guarded by the kind tag.
        scope_id = str(binding.scope.tenant_id)  # type: ignore[union-attr]
    else:
        scope_id = None
    return RoleBindingResponse(
        binding_id=str(binding.binding_id),
        principal_id=str(binding.principal_id),
        role_id=str(binding.role_id),
        scope_kind=kind,
        scope_id=scope_id,
        bound_at=binding.bound_at,
        bound_by=str(binding.bound_by),
    )


async def _resolve_workspace(
    auth_store: AuthStoreProvider,
    *,
    ctx: CallContext,
    workspace_id: str,
) -> WorkspaceId:
    """Resolve and authorize a workspace target for the calling context.

    Returns the typed :class:`WorkspaceId` when the workspace exists
    and the caller's tenant context can target it; otherwise raises
    :class:`NotFound`. Platform-admin callers bypass the cross-tenant
    check.
    """
    typed = WorkspaceId(workspace_id)
    workspace = await auth_store.get_workspace(typed)
    if workspace is None:
        raise NotFound(f"workspace '{workspace_id}' not found")
    # Tenant scoping: a workspace-scoped admin who is not platform.admin
    # may only target workspaces in their own tenant. A missing
    # ``ctx.tenant_id`` is treated as a mismatch — we deny by default
    # rather than skipping the check, otherwise a tenant-less
    # ``admin:role-binding`` caller would silently target every
    # workspace. Cross-tenant attempts (and the tenant-less case)
    # collapse to 404 to avoid leaking existence.
    if not ctx.has_permission("platform.admin") and ctx.tenant_id != str(workspace.tenant_id):
        raise NotFound(f"workspace '{workspace_id}' not found")
    return typed


@router.post(
    "/workspaces/{workspace_id}/role-bindings",
    status_code=status.HTTP_201_CREATED,
    response_model=RoleBindingResponse,
)
async def create_workspace_role_binding(
    workspace_id: str,
    body: RoleBindingCreateRequest,
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:role-binding")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    publisher: Annotated[
        BindingChangedPublisher,
        Depends(get_binding_changed_publisher),
    ],
) -> RoleBindingResponse:
    """Bind a principal to a role at workspace scope.

    Workflow:

    1. Verify the workspace exists and the caller can target it
       (cross-tenant collapses to 404).
    2. Look up the role; reject with 400 ``invalid_role_scope`` if the
       role is not allowed at workspace scope (e.g. binding
       ``role:tenant.admin`` here).
    3. Generate a server-side ``binding_id`` and write the binding.
    4. Best-effort emit ``role-binding.granted``.
    5. Best-effort publish the binding-changed event.
    """
    with telemetry.observe_operation(
        telemetry.OP_ROLE_BINDING_GRANT,
        outcomes={
            NotFound: "not_found",
            InvalidRoleScope: "invalid_role_scope",
        },
    ):
        workspace_typed = await _resolve_workspace(
            auth_store,
            ctx=ctx,
            workspace_id=workspace_id,
        )

        role_id_typed = RoleId(body.role_id)
        role = await auth_store.get_role(role_id_typed)
        if role is None:
            # Unknown role ⇒ scope rule cannot be evaluated; reject
            # with the same machine-readable code so client UX is one path.
            raise InvalidRoleScope(
                f"role '{body.role_id}' is not defined; declare it before binding"
            )
        scope = WorkspaceScope(workspace_id=workspace_typed)
        if not is_scope_allowed(role_id_typed, scope):
            raise InvalidRoleScope(
                f"role '{body.role_id}' may not be bound at workspace scope; "
                f"check GET /v1/roles for the allowed_scopes registry"
            )

        binding = RoleBinding(
            binding_id=RoleBindingId(str(uuid4())),
            principal_id=PrincipalId(body.principal_id),
            role_id=role_id_typed,
            scope=scope,
            bound_at=datetime.now(UTC),
            bound_by=PrincipalId(ctx.principal_id),
        )
        await auth_store.put_role_binding(binding)

        # Best-effort audit emission — see module docstring "Atomicity".
        await audit_role_binding_granted(
            metadata_store,
            actor=ctx.principal_id,
            binding_id=str(binding.binding_id),
            principal_id=str(binding.principal_id),
            role_id=str(binding.role_id),
            scope_kind="workspace",
            scope_id=workspace_id,
        )
        # Best-effort cache-invalidation publish.
        await publisher.publish(
            BindingChangedEvent(
                principal_id=str(binding.principal_id),
                role_id=str(binding.role_id),
                scope=scope,
                action="granted",
                binding_id=str(binding.binding_id),
            ),
        )
        return _binding_to_response(binding)


@router.delete(
    "/workspaces/{workspace_id}/role-bindings/{binding_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workspace_role_binding(
    workspace_id: str,
    binding_id: str,
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:role-binding")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    publisher: Annotated[
        BindingChangedPublisher,
        Depends(get_binding_changed_publisher),
    ],
) -> None:
    """Revoke a role binding at workspace scope.

    Workflow:

    1. Verify the workspace exists and the caller can target it
       (cross-tenant collapses to 404).
    2. Scan ``list_role_bindings_for_scope(WorkspaceScope(...))`` for
       the binding. If the binding does not exist at this workspace
       (missing entirely OR existing at a different workspace), 404.
    3. Delete the binding.
    4. Best-effort emit ``role-binding.revoked``.
    5. Best-effort publish the binding-changed event.

    Idempotency: a repeat DELETE on an already-revoked binding returns
    404, matching the rest of auth-service.
    """
    with telemetry.observe_operation(
        telemetry.OP_ROLE_BINDING_REVOKE,
        outcomes={NotFound: "not_found"},
    ):
        workspace_typed = await _resolve_workspace(
            auth_store,
            ctx=ctx,
            workspace_id=workspace_id,
        )
        scope = WorkspaceScope(workspace_id=workspace_typed)
        bindings = await auth_store.list_role_bindings_for_scope(
            scope,
            RoleBindingFilter(),
        )
        target: RoleBinding | None = None
        for b in bindings:
            if str(b.binding_id) == binding_id:
                target = b
                break
        if target is None:
            raise NotFound(f"role binding '{binding_id}' not found in workspace '{workspace_id}'")

        await auth_store.delete_role_binding(
            target.binding_id,
            actor=PrincipalId(ctx.principal_id),
            reason=_REVOKE_REASON,
        )

        await audit_role_binding_revoked(
            metadata_store,
            actor=ctx.principal_id,
            binding_id=str(target.binding_id),
            principal_id=str(target.principal_id),
            role_id=str(target.role_id),
            scope_kind="workspace",
            scope_id=workspace_id,
            reason=_REVOKE_REASON,
        )
        await publisher.publish(
            BindingChangedEvent(
                principal_id=str(target.principal_id),
                role_id=str(target.role_id),
                scope=target.scope,
                action="revoked",
                binding_id=str(target.binding_id),
            ),
        )
        return None


__all__ = ["router"]
