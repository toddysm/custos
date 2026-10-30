"""Authenticated-mesh command client; Trigger owns intent, Connector owns upstream polling."""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from custos_trigger.clients.workflow import DaprEndpoint, build_invoke_url
from custos_trigger.errors import TriggerError, TriggerErrorKind

logger = logging.getLogger(__name__)


class PullDemandResult(BaseModel):
    """Version-one count of local advisory registrations, including paused demand."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1"] = Field(alias="schemaVersion")
    registered: int = Field(ge=0)


class DaprPullDemandClient:
    """Send only a trusted workspace hint; Connector reads current stored intent after locking."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        endpoint: DaprEndpoint,
        *,
        api_token: str | None = None,
    ) -> None:
        """Bind deployment-owned addressing and a caller-owned HTTP transport."""
        if "://" in endpoint.app_id or "/" in endpoint.app_id or not endpoint.app_id.strip():
            raise ValueError("pull reconciliation requires an explicit Connector app ID")
        self._client = client
        self._url = build_invoke_url(endpoint, "ReconcilePullDemand")
        self._headers = {"dapr-api-token": api_token} if api_token is not None else {}

    async def reconcile(self, workspace_id: str) -> int:
        """Await a bounded command without user context or caller-supplied demand."""
        try:
            async with (
                asyncio.timeout(70),
                self._client.stream(
                    "POST",
                    self._url,
                    json={"schemaVersion": "1", "workspaceId": workspace_id},
                    headers=self._headers,
                    timeout=70,
                ) as response,
            ):
                if response.status_code != 200:
                    raise ValueError("pull reconciliation was not acknowledged")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > 16 * 1024:
                        raise ValueError("pull reconciliation response exceeds the limit")
                    body.extend(chunk)
                return PullDemandResult.model_validate_json(body).registered
        except (httpx.HTTPError, ValueError, TimeoutError) as exc:
            logger.warning("Pull demand command unavailable: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.SOURCE_UNAVAILABLE,
                "Connector pull reconciliation is unavailable; retry the command.",
            ) from exc
