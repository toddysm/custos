"""Exchange a sidecar-only attempt proof for fresh, scoped Connector control-plane authorization."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import UTC, datetime
from typing import Literal

import httpx
from custos_callctx.execution import ExecutionIdentity
from custos_callctx.execution_client import ExecutionAuthorizationError, ExecutionContextClient
from custos_callctx.private_transport import PrivateTransportError, private_post
from custos_spl.errors import SPLError
from custos_spl.interfaces.activity_execution import ActivityExecutionStoreProvider
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)
PROXY_PATH = "/internal/v1/activity-credentials"
router = APIRouter()


class ProxyRequest(BaseModel):
    """Only attempt coordinates and operation, never account or instance authority."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)
    workspace_id: str = Field(alias="workspaceId", min_length=1, max_length=120)
    run_id: str = Field(alias="runId", min_length=1, max_length=4096)
    step_id: str = Field(alias="stepId", min_length=1, max_length=4096)
    attempt: int = Field(ge=1, le=2_147_483_647)
    operation: Literal["issue", "refresh", "release"]
    slot: str | None = Field(default=None, min_length=1, max_length=120)
    capability: str | None = Field(default=None, min_length=1, max_length=120)
    lease_id: str | None = Field(default=None, alias="leaseId", min_length=1, max_length=120)


class CredentialProxy:
    """Borrow owned lifespan resources; obtain fresh service authority for every operation."""

    def __init__(
        self,
        attempts: ActivityExecutionStoreProvider,
        execution: ExecutionContextClient,
        http: httpx.AsyncClient,
        connector_endpoint: str,
    ) -> None:
        """The destination is operator-owned and is never derived from sandbox input."""
        self.attempts, self.execution, self.http = attempts, execution, http
        self.endpoint = connector_endpoint.rstrip("/") + "/internal/v1/activity-credentials"

    async def forward(self, body: ProxyRequest, proof: str) -> dict[str, object]:
        """Validate the attempt proof before authorizing the real control-plane hop."""
        if not proof.startswith("Bearer ") or not 40 <= len(proof) <= 128:
            raise HTTPException(403, "invalid attempt credential proof")
        row = await self.attempts.read_activity_execution(
            body.workspace_id,
            body.run_id,
            body.step_id,
            body.attempt,
        )
        if row is None or row.completed or row.authority_revoked:
            raise HTTPException(403, "activity attempt is not live")
        snapshot = json.loads(row.snapshot_json)
        expected = snapshot.get("credential_grant_hash")
        if not isinstance(expected, str) or not hmac.compare_digest(
            expected,
            hashlib.sha256(proof[7:].encode()).hexdigest(),
        ):
            raise HTTPException(403, "invalid attempt credential proof")
        deadline = datetime.fromisoformat(snapshot["deadline"])
        if snapshot.get("state") != "running" or deadline <= datetime.now(UTC):
            raise HTTPException(403, "activity attempt is inactive")
        identity = ExecutionIdentity.from_dict(snapshot["execution_identity"])
        if identity.workspace_id != body.workspace_id:
            raise HTTPException(403, "activity scope does not match")
        headers = await self.execution.headers(identity, "connector-credentials")
        payload = body.model_dump(by_alias=True, exclude_none=True, exclude={"workspace_id"})
        return await private_post(self.http, self.endpoint, payload, headers)


@router.post(PROXY_PATH)
async def proxy_credentials(body: ProxyRequest, request: Request) -> JSONResponse:
    """Private proof is authenticated here instead of by the service-call-context middleware."""
    proxy = getattr(request.app.state, "credential_proxy", None)
    if not isinstance(proxy, CredentialProxy):
        raise HTTPException(503, "activity credential proxy is not ready")
    proofs = request.headers.getlist("authorization")
    if len(proofs) != 1:
        raise HTTPException(403, "one private attempt proof is required")
    try:
        response = await proxy.forward(body, proofs[0])
        return JSONResponse(response, headers={"Cache-Control": "no-store"})
    except PrivateTransportError as exc:
        logger.warning("Activity credential control-plane request failed")
        raise HTTPException(exc.status, "activity credential request failed") from None
    except ExecutionAuthorizationError as exc:
        logger.warning("Activity credential authorization failed")
        raise HTTPException(
            503 if exc.unavailable else 403, "credential authorization failed"
        ) from None
    except (SPLError, KeyError, TypeError, ValueError):
        logger.warning("Activity credential authority is unavailable")
        raise HTTPException(503, "activity credential authority is unavailable") from None
