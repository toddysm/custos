"""Verify scoped raw webhooks through Connector before acknowledged broker publication."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Any, Literal, Self
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator
from starlette.responses import JSONResponse

from custos_trigger.api.errors import problem_response
from custos_trigger.clients.workflow import DaprEndpoint, build_invoke_url
from custos_trigger.dependencies import get_audit_sink
from custos_trigger.middleware.callctx import WEBHOOK_PATH
from custos_trigger.models import SourceType
from custos_trigger.receivers.sources import (
    ScopedEvent,
    SourceEventRejected,
    _unique_fields,
    adapt_connector_event,
)

logger = logging.getLogger(__name__)
MAX_RAW_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 2 * MAX_RAW_BYTES
MAX_HEADER_BYTES = 16 * 1024
MAX_BATCH_EVENTS = 1000
MAX_REQUEST_SECONDS = 60
PRIVATE_AUTHORIZATION = "x-custos-webhook-authorization"
_STRIPPED_HEADERS = frozenset(
    {
        "authorization",
        PRIVATE_AUTHORIZATION,
        "x-custos-callctx",
        "host",
        "content-length",
        "connection",
        "transfer-encoding",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "upgrade",
    }
)
RejectionCode = Literal["missing-event-id", "missing-event-type", "unknown-event-type", "malformed"]
_VERIFIER_ERRORS = {
    (400, "webhook.malformed"): "trigger.webhook.malformed",
    (401, "webhook.signature_invalid"): "trigger.webhook.invalid_signature",
    (404, "connector.instance_not_listening"): "webhook-route-not-found",
    (413, "webhook.too_large"): "trigger.webhook.too_large",
}


class WebhookIngressError(Exception):
    """A closed caller-safe error kind without raw transport or upstream exception text."""

    def __init__(self, kind: str = "trigger.webhook.unavailable") -> None:
        """Retain only a kind rendered through the registered HTTP error taxonomy."""
        super().__init__(kind)
        self.kind = kind


class VerifiedItem(BaseModel):
    """A Connector envelope and its original zero-based batch index."""

    model_config = ConfigDict(extra="forbid", strict=True)
    index: int = Field(ge=0, lt=MAX_BATCH_EVENTS)
    event: dict[str, Any]


class RejectedItem(BaseModel):
    """A sanitized indexed rejection; no raw payload or arbitrary reason is allowed."""

    model_config = ConfigDict(extra="forbid", strict=True)
    index: int = Field(ge=0, lt=MAX_BATCH_EVENTS)
    code: RejectionCode


class VerifiedBatch(BaseModel):
    """The exact version-one verifier partition, validated before any publication."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1"] = Field(alias="schemaVersion")
    received_count: int = Field(alias="receivedCount", ge=1, le=MAX_BATCH_EVENTS)
    events: list[VerifiedItem] = Field(max_length=MAX_BATCH_EVENTS)
    rejections: list[RejectedItem] = Field(max_length=MAX_BATCH_EVENTS)

    @model_validator(mode="after")
    def complete_partition(self) -> Self:
        """Require complete unique indices and preserve the producer's accepted-event order."""
        indices = [item.index for item in self.events]
        if indices != sorted(indices) or sorted(
            indices + [item.index for item in self.rejections]
        ) != list(range(self.received_count)):
            raise ValueError("invalid verifier partition")
        return self


class DaprWebhookTransport:
    """Bounded Connector invocation and normalized publication using a lifespan-owned client."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        endpoint: DaprEndpoint,
        *,
        pubsub: str,
        topic: str,
        api_token: str | None = None,
    ) -> None:
        """Bind deployment-owned addresses; external request data cannot select a destination."""
        for value in (endpoint.app_id, pubsub, topic):
            if not value.strip() or value in {".", ".."} or any(c in value for c in "/\\"):
                raise ValueError("webhook transport requires explicit Dapr resource names")
        self.client = client
        self.verify_url = build_invoke_url(endpoint, "VerifyWebhook")
        self.publish_url = (
            f"http://{endpoint.host}:{endpoint.http_port}/v1.0/publish/"
            f"{quote(pubsub, safe='')}/{quote(topic, safe='')}"
        )
        self._headers = {"dapr-api-token": api_token} if api_token is not None else {}

    async def verify(
        self, workspace: str, connector: str, body: bytes, headers: dict[str, str]
    ) -> VerifiedBatch:
        """Verify exact raw bytes and reject malformed or ambiguous upstream responses."""
        payload = {
            "schemaVersion": "1",
            "workspaceId": workspace,
            "connectorInstanceId": connector,
            "bodyBase64": base64.b64encode(body).decode("ascii"),
            "headers": headers,
        }
        try:
            async with self.client.stream(
                "POST", self.verify_url, json=payload, headers=self._headers, timeout=20
            ) as response:
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(data) + len(chunk) > MAX_RESPONSE_BYTES:
                        raise WebhookIngressError()
                    data.extend(chunk)
                decoded = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_fields)
                if response.status_code != 200:
                    error = decoded.get("error") if isinstance(decoded, dict) else None
                    code = error.get("code") if isinstance(error, dict) else None
                    kind = (
                        _VERIFIER_ERRORS.get((response.status_code, code))
                        if isinstance(code, str)
                        else None
                    )
                    raise WebhookIngressError(kind or "trigger.webhook.unavailable")
                return VerifiedBatch.model_validate(decoded)
        except (httpx.HTTPError, ValueError, UnicodeError, RecursionError) as exc:
            logger.warning("Webhook verifier unavailable: %s", type(exc).__name__)
            raise WebhookIngressError() from exc

    async def publish(self, event: ScopedEvent) -> None:
        """Acknowledge only Dapr's completed publish response, never a partial batch success."""
        try:
            async with self.client.stream(
                "POST",
                self.publish_url,
                json=event.model_dump(mode="json", by_alias=True, exclude={"event": {"raw"}}),
                headers=self._headers,
                timeout=20,
            ) as response:
                if response.status_code != 204:
                    raise WebhookIngressError()
        except httpx.HTTPError as exc:
            logger.warning("Webhook publication unavailable: %s", type(exc).__name__)
            raise WebhookIngressError() from exc


def verification_headers(request: Request) -> dict[str, str]:
    """Discard mesh/context headers and translate the private bearer into verification data."""
    headers: dict[str, str] = {}
    for name, value in request.headers.items():
        key = name.lower()
        if key in _STRIPPED_HEADERS or key.startswith("dapr-"):
            continue
        if key in headers:
            raise WebhookIngressError("trigger.webhook.malformed")
        headers[key] = value
    credentials = request.headers.getlist(PRIVATE_AUTHORIZATION)
    if len(credentials) > 1:
        raise WebhookIngressError("trigger.webhook.malformed")
    if credentials:
        headers["authorization"] = credentials[0]
    if sum(len(k.encode("utf-8")) + len(v.encode("utf-8")) + 4 for k, v in headers.items()) > (
        MAX_HEADER_BYTES
    ):
        raise WebhookIngressError("trigger.webhook.too_large")
    return headers


async def read_webhook_body(request: Request) -> bytes:
    """Apply the raw one-MiB limit while streaming, independent of Content-Length."""
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > MAX_RAW_BYTES:
            raise WebhookIngressError("trigger.webhook.too_large")
        data.extend(chunk)
    return bytes(data)


async def ingest_webhook(
    request: Request, *, workspace: str, connector: str, transport: DaprWebhookTransport
) -> dict[str, int]:
    """Combine indexed rejections and publish valid scoped events before reporting counts."""
    if any(
        not value.strip() or "\0" in value or len(value) > 256 for value in (workspace, connector)
    ):
        raise WebhookIngressError("webhook-route-not-found")
    headers = verification_headers(request)
    raw = await read_webhook_body(request)
    batch = await transport.verify(workspace, connector, raw, headers)
    events: list[ScopedEvent] = []
    rejections = list(batch.rejections)
    for item in batch.events:
        source = item.event.get("source")
        if not isinstance(source, dict) or (
            source.get("workspaceId") != workspace
            or source.get("instanceId") != connector
            or source.get("deliveryMode") != "push"
        ):
            raise WebhookIngressError()
        try:
            events.append(adapt_connector_event(item.event, source_type=SourceType.WEBHOOK))
        except SourceEventRejected:
            rejections.append(RejectedItem(index=item.index, code="malformed"))
    for rejection in sorted(rejections, key=lambda item: item.index):
        await get_audit_sink(request).emit(
            "trigger.webhook.rejected",
            workspace_id=workspace,
            attributes={
                "connectorInstanceId": connector,
                "index": rejection.index,
                "reason": rejection.code,
            },
        )
    if not events:
        raise WebhookIngressError("trigger.webhook.no_valid_events")
    for event in events:
        await transport.publish(event)
    return {"accepted": len(events), "rejected": len(rejections)}


def build_webhook_router(*, enabled: bool) -> APIRouter:
    """Mount the exact scoped route behind independently authenticated Gateway identity."""
    router = APIRouter(tags=["webhooks"])

    @router.post(WEBHOOK_PATH)
    async def receive(request: Request, workspaceId: str, connectorInstanceId: str) -> JSONResponse:
        """Return accepted/rejected only after complete verified durable handoff."""
        try:
            transport = getattr(request.app.state, "webhook_transport", None)
            if (
                not enabled
                or not getattr(request.app.state, "ready", False)
                or getattr(request.state, "webhook_caller_verified", False) is not True
                or not isinstance(transport, DaprWebhookTransport)
            ):
                raise WebhookIngressError()
            async with asyncio.timeout(MAX_REQUEST_SECONDS):
                result = await ingest_webhook(
                    request,
                    workspace=workspaceId,
                    connector=connectorInstanceId,
                    transport=transport,
                )
            return JSONResponse(result)
        except TimeoutError:
            logger.warning("Webhook handoff exceeded its bounded request deadline")
            return problem_response(
                request, kind="trigger.webhook.unavailable", detail="Webhook handoff timed out."
            )
        except WebhookIngressError as exc:
            logger.warning("Webhook ingress rejected: %s", exc.kind)
            return problem_response(request, kind=exc.kind, detail=exc.kind)

    return router
