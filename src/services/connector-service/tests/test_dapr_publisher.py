"""Tests for :class:`DaprPubSubEventPublisher` (CONN-IMPL-027, Phase J).

Drives the publisher with an :class:`httpx.MockTransport` so the assertions
verify the exact Dapr Pub/Sub HTTP wire shape without standing up a Dapr
sidecar:

* URL: ``{endpoint}/v1.0/publish/{pubsub_name}/{topic}``.
* Body: :meth:`NormalizedEvent.to_wire` (canonical JSON envelope).
* Headers: ``Content-Type: application/json``.
* Failure → :class:`EventPublishError` (the publisher contract the
  :class:`PublisherBridge` re-raises so :meth:`CursorService.tick`
  refuses to commit the cursor).
"""

from __future__ import annotations

import json

import httpx
import pytest

from custos_connector.listen.errors import EventPublishError
from custos_connector.listen.normalizer import NormalizedEvent
from custos_connector.listen.publisher import DaprPubSubEventPublisher


def _event() -> NormalizedEvent:
    return NormalizedEvent(
        schema_version="1.0",
        event_id="evt-1",
        event_type="objects.created",
        source={
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "deliveryMode": "pull",
            "receivedAt": "2026-05-27T00:00:00Z",
        },
        payload={"key": "value"},
    )


@pytest.mark.asyncio
async def test_publish_posts_to_dapr_sidecar_with_expected_body() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        captured["content_type"] = request.headers.get("content-type")
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    try:
        publisher = DaprPubSubEventPublisher(
            http_client=client,
            dapr_endpoint="http://localhost:3500",
            pubsub_name="custos-pubsub",
            topic="custos.connector.events",
        )
        await publisher.publish(_event())
    finally:
        await client.aclose()
    assert captured["url"] == (
        "http://localhost:3500/v1.0/publish/custos-pubsub/custos.connector.events"
    )
    assert captured["content_type"] == "application/json"
    assert captured["body"] == _event().to_wire()


@pytest.mark.asyncio
async def test_publish_strips_trailing_slash_from_endpoint() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    try:
        publisher = DaprPubSubEventPublisher(
            http_client=client,
            dapr_endpoint="http://localhost:3500/",
            pubsub_name="custos-pubsub",
            topic="custos.connector.events",
        )
        await publisher.publish(_event())
    finally:
        await client.aclose()
    # No leading double slash:
    assert "//v1.0" not in captured["url"]
    assert captured["url"].endswith("/v1.0/publish/custos-pubsub/custos.connector.events")


@pytest.mark.asyncio
async def test_publish_raises_on_non_2xx_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text='{"errorCode":"ERR_PUBSUB_PUBLISH"}')

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    try:
        publisher = DaprPubSubEventPublisher(
            http_client=client,
            dapr_endpoint="http://localhost:3500",
            pubsub_name="custos-pubsub",
            topic="custos.connector.events",
        )
        with pytest.raises(EventPublishError) as exc_info:
            await publisher.publish(_event())
    finally:
        await client.aclose()
    msg = str(exc_info.value)
    assert "500" in msg
    assert "custos-pubsub" in msg
    assert "custos.connector.events" in msg


@pytest.mark.asyncio
async def test_publish_wraps_transport_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sidecar unreachable")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    try:
        publisher = DaprPubSubEventPublisher(
            http_client=client,
            dapr_endpoint="http://localhost:3500",
            pubsub_name="custos-pubsub",
            topic="custos.connector.events",
        )
        with pytest.raises(EventPublishError) as exc_info:
            await publisher.publish(_event())
    finally:
        await client.aclose()
    assert "transport" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_publisher_rejects_empty_endpoint() -> None:
    client = httpx.AsyncClient()
    try:
        with pytest.raises(ValueError, match="dapr_endpoint"):
            DaprPubSubEventPublisher(
                http_client=client,
                dapr_endpoint="",
                pubsub_name="custos-pubsub",
                topic="custos.connector.events",
            )
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_publisher_rejects_empty_pubsub_name() -> None:
    client = httpx.AsyncClient()
    try:
        with pytest.raises(ValueError, match="pubsub_name"):
            DaprPubSubEventPublisher(
                http_client=client,
                dapr_endpoint="http://localhost:3500",
                pubsub_name="",
                topic="custos.connector.events",
            )
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_publisher_rejects_empty_topic() -> None:
    client = httpx.AsyncClient()
    try:
        with pytest.raises(ValueError, match="topic"):
            DaprPubSubEventPublisher(
                http_client=client,
                dapr_endpoint="http://localhost:3500",
                pubsub_name="custos-pubsub",
                topic="",
            )
    finally:
        await client.aclose()
