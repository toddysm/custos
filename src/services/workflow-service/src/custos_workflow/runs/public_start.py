"""Caller-bound public starts over the existing immutable durable execution path.

Gateway authorizes entry; each downstream operation obtains its own scoped
authority for the same account. Workspace defaults and incoming bearer tokens
are never execution-identity selection inputs.
"""

from __future__ import annotations

import json
import logging
import traceback
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any

from custos_callctx.execution import ExecutionBindings, ExecutionIdentity
from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
)
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.delivery import MAX_DELIVERY_RETENTION_SECONDS, WorkflowStart

from custos_workflow.call_context import CallContext
from custos_workflow.clients._errors import OutboundRpcError, OutboundRpcStatusError
from custos_workflow.runs.controller import RunRef
from custos_workflow.runs.errors import RunStateConflictError, WorkflowRuntimeUnavailableError
from custos_workflow.runs.ids import derive_run_id
from custos_workflow.runs.orchestrator import RunInput
from custos_workflow.runs.trigger_delivery import TriggerDeliveryController
from custos_workflow.validator import StartRunValidator
from custos_workflow.validator.errors import WorkspaceUnauthorizedError

logger = logging.getLogger(__name__)


class PublicRunStarter:
    """Share durable start without granting a public caller Trigger provenance."""

    def __init__(
        self,
        bindings: ExecutionBindings,
        delivery: TriggerDeliveryController,
        validator: StartRunValidator,
        ttl: timedelta,
    ) -> None:
        """Require execution-aware validation and a supported receipt-retention window."""
        validator.require_execution_catalog()
        if not 0 < ttl.total_seconds() <= MAX_DELIVERY_RETENTION_SECONDS:
            raise ValueError("public execution requires a supported idempotency TTL")
        self.bindings, self.delivery, self.validator, self.ttl = bindings, delivery, validator, ttl

    def _identity(
        self, workspace: str, principal: str, saved: WorkflowStart | None
    ) -> ExecutionIdentity:
        """Retain an accepted pin or select one exact caller binding, never a workspace default."""
        if saved is not None:
            original = RunInput.from_dict(json.loads(saved.input_json))
            identity = original.execution_identity
            if (
                identity is None
                or identity.workspace_id != workspace
                or identity.principal_id != principal
            ):
                raise RunStateConflictError(
                    "accepted run has a different or unavailable caller identity"
                )
            try:
                self.bindings.resolve(identity)
            except ValueError:
                raise WorkflowRuntimeUnavailableError(
                    "original public execution binding is unavailable"
                ) from None
            return identity
        matching = tuple(
            identity
            for identity in self.bindings.identities
            if identity.workspace_id == workspace and identity.principal_id == principal
        )
        if len(matching) != 1:
            raise WorkflowRuntimeUnavailableError(
                "public execution requires one matching caller binding"
            )
        return matching[0]

    async def start(
        self,
        workspace: str,
        version_id: str,
        inputs: Mapping[str, Any],
        key: str | None,
        context: CallContext,
    ) -> tuple[RunRef, bool]:
        """Validate fresh work or recover its original identity and result.

        Keyless calls keep a newly generated UUIDv4 per request. Keyed calls
        reuse the locked workspace/key UUIDv5 and reject divergent accepted
        identity or payload. Only non-secret references enter runtime history.
        Upstream RPC faults are unavailable, not accepted-work conflicts. Diagnostics
        contain only the run ID, exception type, source location and HTTP status.
        """
        if (
            context.workspace != workspace
            or context.principal is None
            or context.execution_verified
        ):
            raise WorkspaceUnauthorizedError("public execution requires its verified caller")
        run_id = derive_run_id(workspace, key)
        try:
            saved = (
                await self.delivery.provider.get_workflow_start(WorkspaceId(workspace), run_id)
                if key is not None
                else None
            )
            identity = self._identity(workspace, context.principal, saved)
            until = await self.delivery.provider.delivery_now() + self.ttl
            if key is not None:
                prior = await self.delivery.lookup_start(
                    workspace, version_id, inputs, key, until, execution_identity=identity
                )
                if prior is not None:
                    return prior, True
            validated = await self.validator.validate_start_run(
                workspace_id=workspace,
                workflow_version_id=version_id,
                inputs=inputs,
                idempotency_key=key,
                call_context=replace(context, execution_identity=identity),
            )
            ref = await self.delivery.start(
                workspace,
                validated.workflow_version,
                validated.inputs,
                key,
                until,
                execution_identity=identity,
                keyless_run_id=run_id if key is None else None,
            )
            return ref, validated.replayed
        except (ImmutableViolation, ValueError, RunStateConflictError) as exc:
            origin = traceback.extract_tb(exc.__traceback__)[-1]
            logger.warning(
                "PUBLIC_START_REJECTED run=%s cause=%s origin=%s:%d status=%s",
                run_id,
                type(exc).__name__,
                origin.name,
                origin.lineno,
                exc.status_code if isinstance(exc, OutboundRpcStatusError) else None,
            )
            if isinstance(exc, OutboundRpcError):
                raise WorkflowRuntimeUnavailableError(
                    "public execution Catalog is unavailable"
                ) from None
            if isinstance(exc, RunStateConflictError):
                raise
            raise RunStateConflictError("public start conflicts with accepted work") from None
        except (BackendUnavailable, LeaseBusy, LeaseExpired, MigrationRequired):
            raise WorkflowRuntimeUnavailableError(
                "durable public execution is temporarily unavailable"
            ) from None
