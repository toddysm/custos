"""Push receiver router (CONN-IMPL-025, issue #308).

Mounts ``POST /v1/webhooks/connectors/{instance_id}/events`` so
upstream targets that deliver events by webhook can hand off batches
to the platform. The path is deliberately under ``/v1/webhooks/`` so
the :class:`custos_connector.middleware.CallContextMiddleware` skips
its call-context check (webhooks authenticate via signature, not via
the internal-RPC bearer token); see ``_BYPASS_PREFIXES`` in
``middleware/callctx.py``.

Request flow
------------

1. Resolve the :class:`PushRegistration` for the path's
   ``instance_id`` (via the wired
   :class:`custos_connector.listen.ListenManager`). 404 with
   ``connector.instance_not_listening`` when the instance is not
   registered for push delivery (unknown instance OR pull-only
   instance OR deactivated instance — the wire shape collapses
   these so an attacker cannot probe the registry).
2. Read the raw request body bytes.
3. Verify the signature via the registration's
   :class:`SignatureVerifier`. 401
   ``webhook.signature_invalid`` on failure. The signature is checked
   *before* parsing the JSON so an attacker that finds a JSON parser
   bug cannot reach it on an unauthenticated request.
4. Parse the body as JSON. 400 ``webhook.malformed`` on bad JSON or
   on a non-object top-level value.
5. Audit ``event.received`` with the batch size.
6. Build a :class:`PublisherBridge` keyed to the registration's
   normalizer + the app-level publisher and call ``process_batch``.
   Per-event ``event.normalized`` / ``event.rejected`` rows fire from
   the bridge.
7. Return ``200 {accepted, rejected}``.

The router does not surface 5xx from publisher failures with a
custom code: an :class:`EventPublishError` raised by the wired
publisher propagates as a generic FastAPI 500 (caught at the ASGI
layer) so the upstream webhook caller knows to retry. The cursor
contract does not apply on the push path — there is no cursor to
hold back — but the publish-failure-→ caller-retry contract still
gives at-least-once.

Wire shape
----------

Request body::

    {
      "events": [
        {"eventId": "...", "eventType": "...", ...},
        ...
      ]
    }

Response body (200)::

    {"accepted": N, "rejected": M}

Error body (4xx)::

    {"error": {"code": "...", "detail": "..."}}
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, Final, cast

from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from fastapi import APIRouter, Path, Request
from starlette.responses import JSONResponse

from custos_connector.audit import audit_event_received
from custos_connector.listen.manager import ListenManager
from custos_connector.listen.normalizer import DELIVERY_MODE_PUSH
from custos_connector.listen.publisher import (
    EventPublisher,
    PublisherBridge,
)

if TYPE_CHECKING:
    from custos_connector.providers import Providers

_LOGGER: Final[logging.Logger] = logging.getLogger("custos_connector.listen.router")

#: Common path prefix. Mounted in :func:`custos_connector.create_app`.
router = APIRouter(prefix="/v1/webhooks", tags=["push-receiver"])

#: Audit ``actor`` attributed to push receives. Webhooks do not carry
#: an internal principal so we attribute every emission to a stable
#: service-account-like string. Operators reading the audit log
#: distinguish push from pull on the ``deliveryMode`` field in the
#: payload, not on the actor.
_PUSH_ACTOR: Final[str] = "connector-service:webhook"
#: Free-form provenance code on the ``event.received`` audit payload.
_PUSH_SOURCE: Final[str] = "webhook"


def _error_response(*, status_code: int, code: str, detail: str) -> JSONResponse:
    """Canonical connector-service error envelope.

    Same shape as :func:`custos_connector.cursor.router._error_response`
    so clients have one error vocabulary across the service.
    """
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


def _resolve_listen_manager(request: Request) -> ListenManager:
    """Pull :class:`ListenManager` off ``app.state.providers``.

    Surfaces a startup-wiring bug as a :class:`RuntimeError` (rather
    than a generic 500) so the test suite + operator dashboards can
    distinguish "wired wrong on startup" from "request failed at
    runtime". Mirrors :func:`custos_connector.cursor.router._resolve_cursor_service`.
    """
    providers = request.app.state.providers
    mgr = getattr(providers, "listen_manager", None)
    if mgr is None:
        raise RuntimeError(
            "listen_manager is not wired on app.state.providers; this is a startup wiring bug"
        )
    assert isinstance(mgr, ListenManager)
    return mgr


def _resolve_event_publisher(request: Request) -> EventPublisher:
    """Pull the wired :class:`EventPublisher` off ``app.state.providers``."""
    providers = request.app.state.providers
    pub = getattr(providers, "event_publisher", None)
    if pub is None:
        raise RuntimeError(
            "event_publisher is not wired on app.state.providers; this is a startup wiring bug"
        )
    return cast("EventPublisher", pub)


def _resolve_metadata_store(request: Request) -> MetadataStoreProvider:
    """Pull :class:`MetadataStoreProvider` off ``app.state.providers``."""
    providers = cast("Providers", request.app.state.providers)
    return providers.metadata_store


@router.post(
    "/connectors/{instance_id}/events",
    summary="Webhook receiver for push-mode connector events",
    status_code=200,
)
async def post_events(
    request: Request,
    instance_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=200,
            description="Connector instance identifier the webhook targets.",
        ),
    ],
) -> JSONResponse:
    """Accept a webhook batch and route it through the listen pipeline.

    See module docstring for the request / response shape and the
    error vocabulary.
    """
    manager = _resolve_listen_manager(request)
    publisher = _resolve_event_publisher(request)
    metadata_store = _resolve_metadata_store(request)

    # Step 1 — resolve the push registration. We do not have a
    # workspace path segment on the webhook URL (upstream callers do
    # not know it), so the lookup walks the registry to find the
    # instance. The registry is keyed by (ws, inst); collisions on
    # instance_id across workspaces are impossible because instance
    # ids are workspace-scoped UUIDs.
    registration = _find_push_registration(manager, instance_id)
    if registration is None:
        return _error_response(
            status_code=404,
            code="connector.instance_not_listening",
            detail=(f"connector instance {instance_id!r} is not registered for push delivery"),
        )

    if not registration.active:
        return _error_response(
            status_code=404,
            code="connector.instance_not_listening",
            detail=(f"connector instance {instance_id!r} is not registered for push delivery"),
        )

    # Step 2 — read raw bytes BEFORE signature verification so we
    # sign over the bytes that arrived on the wire (re-encoding loses
    # canonical ordering and breaks HMAC).
    body = await request.body()

    # Step 3 — verify the signature.
    headers = {key: value for key, value in request.headers.items()}
    try:
        signature_ok = await registration.signature_verifier.verify(
            body=body,
            headers=headers,
            instance_id=registration.instance_id,
        )
    except Exception:
        _LOGGER.exception(
            "signature verifier raised; treating as failed verification instance=%s",
            instance_id,
        )
        signature_ok = False
    if not signature_ok:
        return _error_response(
            status_code=401,
            code="webhook.signature_invalid",
            detail="webhook signature verification failed",
        )

    # Step 4 — parse the JSON body.
    try:
        parsed: Any = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        return _error_response(
            status_code=400,
            code="webhook.malformed",
            detail=f"request body is not valid JSON: {err}",
        )
    if not isinstance(parsed, Mapping):
        return _error_response(
            status_code=400,
            code="webhook.malformed",
            detail="request body must be a JSON object",
        )
    events_raw = parsed.get("events")
    if not isinstance(events_raw, list):
        return _error_response(
            status_code=400,
            code="webhook.malformed",
            detail="request body must contain an 'events' array",
        )

    # Step 5 — audit ``event.received`` with the pre-normalization
    # batch size. Per-event audit rows fire from the bridge below.
    await audit_event_received(
        metadata_store,
        workspace_id=registration.workspace_id,
        actor=_PUSH_ACTOR,
        instance_id=registration.instance_id,
        delivery_mode=DELIVERY_MODE_PUSH,
        event_count=len(events_raw),
        source=_PUSH_SOURCE,
    )

    # Step 6 — build a per-request bridge with the registration's
    # normalizer (so the produced-event-types catalog matches the
    # connector-type version the instance was bound to) and the
    # app-level publisher.
    bridge = PublisherBridge(
        normalizer=registration.normalizer,
        publisher=publisher,
        metadata_store=metadata_store,
        actor=_PUSH_ACTOR,
    )
    result = await bridge.process_batch(
        workspace_id=registration.workspace_id,
        instance_id=registration.instance_id,
        events=events_raw,
        delivery_mode=DELIVERY_MODE_PUSH,
        received_at=datetime.now(UTC),
    )

    return JSONResponse(
        status_code=200,
        content={"accepted": result.accepted, "rejected": result.rejected},
    )


def _find_push_registration(manager: ListenManager, instance_id: str) -> Any:
    """Walk the push registry looking for ``instance_id`` across workspaces.

    Instance ids are workspace-scoped UUIDs (see
    :class:`custos_spl.ids.ConnectorInstanceId`) so a single instance
    id has at most one matching entry. The walk is O(n) over the
    push registry which is acceptable at the M1 single-replica scale
    (registries hold tens to hundreds of push instances per pod);
    when the registry grows to thousands a follow-up will replace
    the walk with a second index keyed by instance id only.
    """
    # Touch the private state directly so we don't have to widen the
    # ListenManager public surface for the router. The router lives
    # in the same package so this is intra-module access.
    registry: Mapping[tuple[str, str], Any] = manager._push_registry
    for (_workspace_id, candidate_instance_id), reg in registry.items():
        if candidate_instance_id == instance_id:
            return reg
    return None
