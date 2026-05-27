"""FastAPI router for the ``SubscribeEvents`` internal RPC (CONN-IMPL-027).

Route ``POST /internal/v1/events:subscribe`` is a Pub/Sub subscription
metadata discovery surface. Trigger Service calls this to learn the
``(pubsubName, topic)`` pair the Connector Service is publishing
normalized connector events on (so it can wire its own Dapr
subscription against the same component / topic) and to record an
optional instance-id filter the subscriber should apply locally on
``source.instanceId`` after Dapr fans the topic out.

The endpoint is **not** a subscription registration — it stores no
state. Dapr is the broker, declarative subscriptions are the binding
mechanism. This endpoint just gives the subscriber a stable place to
discover the metadata so Trigger Service does not have to read the
Connector Service's Helm values.

Gating
------

The route is gated by
:data:`custos_connector.permissions.EVENTS_SUBSCRIBE`, held only by
the Trigger Service's service identity.

Failure modes
-------------

* ``503 connector.pubsub_unconfigured`` when
  :attr:`Settings.dapr_pubsub_enabled` is false (single-node /
  in-process dev deployments that wire the
  :class:`NoOpEventPublisher` instead). The subscriber backs off
  and retries — once an operator sets ``CONN_DAPR_HTTP_ENDPOINT``
  the route flips to 200.
* ``403 connector.workspace_mismatch`` — never raised from this
  body-only route; reserved for future workspace-scoped path
  parametrisation.

Workspace scoping
-----------------

The body carries no workspace; the call context's
``workspace_id`` is the scoping authority. The subscriber is
expected to call once per workspace it cares about and receive
a workspace-shape metadata response; the wire layer surfaces the
workspace alongside the topic for traceability even though the
topic itself is global per Connector Service deployment.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_connector.api._common import error_response
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import EVENTS_SUBSCRIBE
from custos_connector.settings import Settings

router = APIRouter(prefix="/internal/v1", tags=["subscribe"])

#: Stable wire ``error.code`` returned with HTTP 503 when this
#: Connector Service deployment is not configured for Dapr Pub/Sub.
#: The subscriber treats this as "Dapr not ready yet" and backs off.
PUBSUB_UNCONFIGURED_CODE = "connector.pubsub_unconfigured"


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _SubscribeRequestWire(BaseModel):
    """Wire shape of the SubscribeEvents request body."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    connector_instance_ids: list[str] | None = Field(
        default=None,
        alias="connectorInstanceIds",
        description=(
            "Optional filter: subscriber will only act on events whose "
            "``source.instanceId`` is in this list. Empty list narrows to "
            "no instances (useful for a no-op subscription handshake); "
            "omitted means 'fan out every event the subscriber receives'."
        ),
    )


class _SubscribeResponseWire(BaseModel):
    """Wire shape of the SubscribeEvents response body.

    The ``workspaceId`` echo lets the subscriber verify it routed the
    request through the right call-context (defence-in-depth against
    a misconfigured Trigger Service principal mapping).
    """

    model_config = ConfigDict(populate_by_name=True)

    pubsub_name: str = Field(..., alias="pubsubName")
    topic: str = Field(..., alias="topic")
    workspace_id: str = Field(..., alias="workspaceId")
    instance_filter: list[str] | None = Field(default=None, alias="instanceFilter")


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


def _resolve_settings(request: Request) -> Settings:
    """Pull the typed :class:`Settings` off ``app.state``."""
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError(
            "connector-service: app.state.settings is unset; "
            "lifespan did not run or settings wiring failed",
        )
    assert isinstance(settings, Settings)
    return settings


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/events:subscribe", response_model=_SubscribeResponseWire)
async def subscribe_events(
    body: _SubscribeRequestWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(EVENTS_SUBSCRIBE))],
) -> JSONResponse:
    """Return the Pub/Sub subscription metadata for connector events.

    Idempotent and cacheable per
    ``(workspace, connectorInstanceIds)`` — the response only changes
    when an operator reconfigures the Helm values for the topic /
    pubsub component or scales the publisher down. Trigger Service
    is expected to refresh the metadata on a slow timer and on
    successive 503 responses (Dapr coming online after Connector
    Service).
    """
    settings = _resolve_settings(request)
    if not settings.dapr_pubsub_enabled:
        return error_response(
            status_code=503,
            code=PUBSUB_UNCONFIGURED_CODE,
            detail=(
                "connector-service is not configured for Dapr Pub/Sub; "
                "set CONN_DAPR_HTTP_ENDPOINT to enable event publication"
            ),
        )
    wire = _SubscribeResponseWire(
        pubsubName=settings.dapr_pubsub_name,
        topic=settings.dapr_event_topic,
        workspaceId=ctx.workspace_id,
        instanceFilter=(
            list(body.connector_instance_ids) if body.connector_instance_ids is not None else None
        ),
    )
    return JSONResponse(status_code=200, content=wire.model_dump(by_alias=True))


__all__ = ["PUBSUB_UNCONFIGURED_CODE", "router"]
