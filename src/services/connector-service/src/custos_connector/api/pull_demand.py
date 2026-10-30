"""Private Trigger-to-Connector reconciliation of authoritative persisted polling intent."""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

from custos_spl.errors import SPLError
from custos_spl.interfaces.dead_letters import validate_dispatch_identifier
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from starlette.responses import JSONResponse

from custos_connector.api._common import error_response, resolve_providers
from custos_connector.listen.verification import WebhookVerificationError, parse_json

logger = logging.getLogger(__name__)
router = APIRouter(tags=["pull-demand"])


class ReconcilePullDemandRequest(BaseModel):
    """Version-one command; demand itself is read from storage, never supplied by a caller."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1"] = Field(alias="schemaVersion")
    workspace_id: str = Field(alias="workspaceId", min_length=1, max_length=512)

    @field_validator("workspace_id")
    @classmethod
    def valid_workspace(cls, value: str) -> str:
        """Reject blank, embedded-NUL and oversized identifiers as request errors."""
        validate_dispatch_identifier(value)
        return value


class ReconcilePullDemandResponse(BaseModel):
    """Version-one local registration count, including operator-paused demand."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1"] = Field(default="1", alias="schemaVersion")
    registered: int = Field(ge=0)


@router.post(
    "/ReconcilePullDemand",
    response_model=ReconcilePullDemandResponse,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": ReconcilePullDemandRequest.model_json_schema(),
                }
            },
        },
    },
)
async def reconcile_pull_demand(request: Request) -> JSONResponse:
    """Refresh one workspace's registrations without clearing operator pause or starting a timer."""
    if getattr(request.state, "trigger_caller_verified", False) is not True:
        return error_response(
            status_code=403, code="mesh_caller_denied", detail="Trigger service identity required"
        )
    providers = resolve_providers(request)
    reconciler = providers.pull_demand_reconciler
    if not request.app.state.ready or reconciler is None:
        logger.warning("Pull demand reconciliation is not ready")
        return error_response(
            status_code=503,
            code="connector.pull_control_unavailable",
            detail="Durable pull reconciliation is unavailable.",
        )
    try:
        async with asyncio.timeout(60):
            raw = bytearray()
            async for chunk in request.stream():
                if len(raw) + len(chunk) > 16 * 1024:
                    logger.warning("Pull demand command exceeds its byte limit")
                    return error_response(
                        status_code=413,
                        code="connector.pull_demand_too_large",
                        detail="Pull demand command exceeds the limit.",
                    )
                raw.extend(chunk)
            body = ReconcilePullDemandRequest.model_validate(parse_json(bytes(raw)))
            count = await reconciler.reconcile(body.workspace_id)
    except (ValidationError, WebhookVerificationError) as exc:
        logger.warning("Invalid pull demand command: %s", type(exc).__name__)
        return error_response(
            status_code=400, code="connector.pull_demand_invalid", detail="Invalid demand command."
        )
    except (SPLError, ValueError, TimeoutError) as exc:
        logger.error("Pull demand reconciliation failed: %s", type(exc).__name__)
        return error_response(
            status_code=503,
            code="connector.pull_control_unavailable",
            detail="Pull reconciliation is unavailable; retry the command.",
        )
    return JSONResponse({"schemaVersion": "1", "registered": count})
