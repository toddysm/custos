"""Connector-owned real credential issuance for a durably accepted activity attempt.

Only the authenticated runtime control-plane caller may invoke this surface.
Slot authority comes from the immutable accepted snapshot, never instance IDs,
grants or TTL hints supplied by the sandbox. Upstream material stays opaque;
lease expiry is deliberately separate from actual or unknown provider expiry.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from custos_callctx.execution import ExecutionIdentity
from custos_callctx.mesh import MeshAuthenticationError, require_service_caller
from custos_spl.errors import SPLError
from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.activity_execution import ActivityExecutionStoreProvider
from custos_spl.interfaces.atomic_lease import AtomicLeaseStoreProvider
from custos_spl.interfaces.lease_store import Lease
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator
from starlette.responses import JSONResponse

from custos_connector.audit import build_audit_event
from custos_connector.binding.errors import BindError
from custos_connector.binding.models import BindSlotRequest
from custos_connector.identity.errors import IdentityResolverError
from custos_connector.lease.errors import LeaseError, http_status_for
from custos_connector.lease.router import _lease_to_wire
from custos_connector.lease.service import LeaseManager
from custos_connector.middleware import CallContext, CallContextError, get_call_context
from custos_connector.providers import Providers

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/internal/v1", tags=["credentials"])


class CredentialRequest(BaseModel):
    """Closed per-attempt request: authority and lifetime are exclusively server-owned."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)
    operation: Literal["issue", "refresh", "release"]
    run_id: str = Field(alias="runId", min_length=1, max_length=4096)
    step_id: str = Field(alias="stepId", min_length=1, max_length=4096)
    attempt: int = Field(ge=1, le=2_147_483_647)
    slot: str | None = Field(default=None, min_length=1, max_length=120)
    capability: str | None = Field(default=None, min_length=1, max_length=120)
    lease_id: str | None = Field(default=None, alias="leaseId", min_length=1, max_length=120)

    @model_validator(mode="after")
    def operation_shape(self) -> CredentialRequest:
        """Do not allow refresh/release to change slot authority or issue to reuse a lease."""
        if self.operation == "issue":
            if self.slot is None or self.capability is None or self.lease_id is not None:
                raise ValueError("issue requires only slot and capability")
        elif self.slot is not None or self.capability is not None or self.lease_id is None:
            raise ValueError("refresh/release requires only leaseId")
        return self


class CredentialDenied(Exception):
    """A closed rejection with no upstream material or credential-bearing cause."""


class CredentialIssuer:
    """Resolve each issuance against live durable scope, instance policy and provider material."""

    def __init__(self, providers: Providers) -> None:
        """Require durable attempt reads and atomic capacity instead of memory fallbacks."""
        self.providers = providers
        if not isinstance(providers.metadata_store, ActivityExecutionStoreProvider):
            raise CredentialDenied("durable activity authority is unavailable")
        if not isinstance(providers.lease_store, AtomicLeaseStoreProvider):
            raise CredentialDenied("atomic credential lease admission is unavailable")
        if providers.bind_for_step_service is None:
            raise CredentialDenied("credential binding service is unavailable")
        self.attempts = providers.metadata_store
        self.binder = providers.bind_for_step_service

    async def authority(self, ctx: CallContext, body: CredentialRequest) -> dict[str, Any]:
        """Read only a live exact attempt belonging to the freshly authenticated principal."""
        await self.attempts.check_activity_execution_ready()
        row = await self.attempts.read_activity_execution(
            ctx.workspace_id,
            body.run_id,
            body.step_id,
            body.attempt,
        )
        if row is None or row.completed or row.authority_revoked:
            raise CredentialDenied("activity attempt is not live")
        value = json.loads(row.snapshot_json)
        if not isinstance(value, dict):
            raise CredentialDenied("accepted execution authority is invalid")
        try:
            identity = ExecutionIdentity.from_dict(value["execution_identity"])
            deadline = datetime.fromisoformat(value["deadline"])
        except (KeyError, TypeError, ValueError):
            raise CredentialDenied("accepted execution authority is unavailable") from None
        if (
            identity.workspace_id != ctx.workspace_id
            or identity.principal_id != ctx.principal_id
            or value.get("state") != "running"
            or deadline.utcoffset() is None
            or deadline <= datetime.now(UTC)
        ):
            raise CredentialDenied("activity authority is inactive or mismatched")
        return value

    async def operate(self, ctx: CallContext, body: CredentialRequest) -> dict[str, Any]:
        """Issue/refresh opaque material, or release a scoped lease without upstream resolution."""
        accepted = await self.authority(ctx, body)
        workspace = WorkspaceId(ctx.workspace_id)
        manager = LeaseManager(
            lease_store=self.providers.lease_store,
            metadata_store=self.providers.metadata_store,
            default_ttl_sec=300,
            max_concurrent_leases=16,
            actor=ctx.principal_id,
            require_atomic=True,
        )
        lease = None
        slot, capability = body.slot, body.capability
        if body.lease_id is not None:
            lease = await self.providers.lease_store.get_lease(workspace, body.lease_id)
            self._live_lease(lease, body)
            assert lease is not None
            slot, capability = lease.slot, lease.capability
        scopes = accepted.get("credential_scopes")
        scope = scopes.get(slot) if isinstance(scopes, dict) else None
        if (
            not isinstance(scope, dict)
            or set(scope) != {"instanceId", "kind", "capabilities"}
            or not isinstance(scope["instanceId"], str)
            or not isinstance(scope["kind"], str)
            or not isinstance(scope["capabilities"], list)
            or capability not in scope["capabilities"]
        ):
            raise CredentialDenied("slot or capability was not accepted for this attempt")
        if lease is not None and str(lease.connector_instance_id) != scope["instanceId"]:
            raise CredentialDenied("lease does not match the accepted instance")
        assert slot is not None and capability is not None
        spec = BindSlotRequest(slot, scope["instanceId"], (capability,))
        if body.operation == "release":
            assert lease is not None
            await manager.release(workspace_id=workspace, lease_id=lease.lease_id)
            return {"released": True}
        instance, _ = await self.binder.credential_instance(ctx.workspace_id, spec)
        if instance.type != scope["kind"] or not instance.lease_ttl_seconds:
            raise CredentialDenied("instance kind or lease policy changed")
        deadline = datetime.fromisoformat(accepted["deadline"])
        if lease is None:
            lease = await manager.issue(
                workspace_id=workspace,
                run_id=RunId(body.run_id),
                step_id=StepId(body.step_id),
                attempt=body.attempt,
                slot=slot,
                capability=capability,
                connector_instance_id=ConnectorInstanceId(spec.instance_id),
                token_type="provider-material",
                instance_ttl_sec=instance.lease_ttl_seconds,
                step_deadline=deadline,
            )
        else:
            lease = await manager.refresh(
                workspace_id=workspace,
                lease_id=lease.lease_id,
                instance_ttl_sec=instance.lease_ttl_seconds,
                step_deadline=deadline,
            )
        delivered = False
        try:
            material = await self.binder.credential_material(
                workspace_id=ctx.workspace_id,
                actor=ctx.principal_id,
                slot=spec,
                instance=instance,
            )
            # Recheck after upstream I/O: completion/revocation must not return late credentials.
            await self.authority(ctx, body)
            current_instance, _ = await self.binder.credential_instance(ctx.workspace_id, spec)
            if (
                current_instance.type != instance.type
                or current_instance.version != instance.version
                or current_instance.credentials_authentication
                != instance.credentials_authentication
            ):
                raise CredentialDenied("instance credential authority changed during issuance")
            current = await self.providers.lease_store.get_lease(workspace, lease.lease_id)
            self._live_lease(current, body)
            if material.expires_at is not None and material.expires_at <= datetime.now(UTC):
                raise CredentialDenied("provider returned expired material")
            payload = dict(material.material)
            encoded = json.dumps(payload, allow_nan=False).encode()
            if len(encoded) > 1024 * 1024:
                raise CredentialDenied("provider material exceeds the private response limit")
            await self.providers.metadata_store.append_audit(
                workspace,
                build_audit_event(
                    workspace_id=ctx.workspace_id,
                    event_type="connector.credential.issued",
                    actor=ctx.principal_id,
                    subject={"runId": body.run_id, "stepId": body.step_id, "attempt": body.attempt},
                    payload={
                        "slot": slot,
                        "capability": capability,
                        "leaseId": lease.lease_id,
                        "connectorInstanceId": spec.instance_id,
                    },
                ),
            )
            await self.authority(ctx, body)
            self._live_lease(
                await self.providers.lease_store.get_lease(workspace, lease.lease_id),
                body,
            )
            delivered = True
            return {
                "schemaVersion": "2",
                "lease": _lease_to_wire(lease),
                "credential": {
                    "authenticationType": material.authentication_type,
                    "material": payload,
                    "issuedAt": material.issued_at.isoformat(),
                    "expiresAt": material.expires_at.isoformat() if material.expires_at else None,
                },
            }
        finally:
            if not delivered:
                await manager.release(workspace_id=workspace, lease_id=lease.lease_id)

    @staticmethod
    def _live_lease(lease: Lease | None, body: CredentialRequest) -> None:
        """Prevent cross-attempt, released, expired or revoked lease use on every operation."""
        if (
            lease is None
            or (str(lease.run_id), str(lease.step_id), lease.attempt)
            != (body.run_id, body.step_id, body.attempt)
            or lease.released_at is not None
            or lease.revoked_at is not None
            or lease.expires_at <= datetime.now(UTC)
        ):
            raise CredentialDenied("credential lease is inactive or belongs to another attempt")


@router.post("/activity-credentials")
async def activity_credentials(
    body: CredentialRequest,
    request: Request,
    ctx: Annotated[CallContext, Depends(get_call_context)],
) -> JSONResponse:
    """Authorize the runtime mesh hop separately from the durable account/attempt scope."""
    settings = request.app.state.settings
    if (
        not getattr(request.state, "signed_call_context", False)
        or ctx.execution_operation != "connector-credentials"
        or not ctx.has_permission("connector:bind")
        or ctx.caller_component != settings.runtime_app_id
    ):
        raise CallContextError(403, "execution_denied", "runtime credential authority required")
    try:
        require_service_caller(
            request.headers,
            token=settings.app_api_token,
            app_id=settings.runtime_app_id,
            namespace=settings.runtime_namespace,
        )
    except MeshAuthenticationError as exc:
        raise CallContextError(exc.status_code, exc.code, exc.detail) from None
    try:
        result = await CredentialIssuer(request.app.state.providers).operate(ctx, body)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})
    except CredentialDenied:
        logger.warning("Activity credential request denied")
        return JSONResponse(
            {"error": {"code": "credential_denied", "detail": "credential request denied"}},
            status_code=403,
        )
    except LeaseError as exc:
        logger.warning("Activity credential lease request rejected: %s", exc.code)
        return JSONResponse(
            {"error": {"code": str(exc.code), "detail": "credential lease request rejected"}},
            status_code=http_status_for(exc.code),
        )
    except (SPLError, BindError, IdentityResolverError, ValueError, TypeError):
        logger.warning("Activity credential dependency unavailable")
        return JSONResponse(
            {
                "error": {
                    "code": "credential_unavailable",
                    "detail": "credential issuance unavailable",
                }
            },
            status_code=503,
        )
