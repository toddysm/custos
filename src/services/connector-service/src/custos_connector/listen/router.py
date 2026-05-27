"""Push receiver router (CONN-IMPL-025, issue #308).

Mounts ``POST /v1/webhooks/workspaces/{workspace_id}/connectors/{instance_id}/events``
so upstream targets that deliver events by webhook can hand off
batches to the platform. The workspace segment is part of the URL
because :class:`custos_spl.ids.ConnectorInstanceId` is an opaque
string, not a globally-unique identifier — two workspaces are free
to reuse the same instance id locally — so routing a webhook by
``instance_id`` alone would risk delivering an event to the wrong
tenant. With the workspace in the URL the lookup is an exact match
on ``(workspace_id, instance_id)``.

The path is deliberately under ``/v1/webhooks/`` so the
:class:`custos_connector.middleware.CallContextMiddleware` skips
its call-context check (webhooks authenticate via signature, not via
the internal-RPC bearer token); see ``_BYPASS_PREFIXES`` in
``middleware/callctx.py``.

Request flow
------------

1. Validate the request size against
   ``settings.publish_max_body_mb`` *before* buffering the body. The
   webhook path is unauthenticated until step 4 (signature check),
   so an attacker that knows the workspace + instance id can
   trivially exhaust memory if the size cap is enforced only inside
   FastAPI's JSON parser. The cap is checked twice:
   the ``Content-Length`` header is rejected early when present and
   over the cap, and the streamed body is rejected if it grows past
   the cap during reading (``Content-Length`` is advisory; clients
   can lie or send ``Transfer-Encoding: chunked``).
2. Resolve the :class:`PushRegistration` for
   ``(workspace_id, instance_id)`` from the wired
   :class:`custos_connector.listen.ListenManager`. 404 with
   ``connector.instance_not_listening`` when the instance is not
   registered for push delivery (unknown instance OR pull-only
   instance OR deactivated instance — the wire shape collapses
   these so an attacker cannot probe the registry).
3. Read the raw request body bytes (already size-capped).
4. Verify the signature via the registration's
   :class:`SignatureVerifier`. 401
   ``webhook.signature_invalid`` on failure. The signature is checked
   *before* parsing the JSON so an attacker that finds a JSON parser
   bug cannot reach it on an unauthenticated request.
5. Parse the body as JSON. 400 ``webhook.malformed`` on bad JSON or
   on a non-object top-level value.
6. Audit ``event.received`` with the batch size.
7. Build a :class:`PublisherBridge` keyed to the registration's
   normalizer + the app-level publisher and call ``process_batch``.
   Per-event ``event.normalized`` / ``event.rejected`` rows fire from
   the bridge.
8. Return ``200 {accepted, rejected}``.

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
from custos_connector.settings import Settings

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
#: Bytes per megabyte for the body-size cap. Pulled out as a module
#: constant so the conversion is one place to grep.
_BYTES_PER_MB: Final[int] = 1024 * 1024


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


def _resolve_settings(request: Request) -> Settings:
    """Pull :class:`Settings` off ``app.state.settings``.

    Required for the body-size cap; the webhook path is the only
    request path on the service that runs before call-context
    authentication and therefore has to enforce its own resource
    limits.
    """
    settings = getattr(request.app.state, "settings", None)
    if not isinstance(settings, Settings):
        raise RuntimeError(
            "Settings is not wired on app.state.settings; this is a startup wiring bug"
        )
    return settings


async def _read_body_with_cap(request: Request, *, max_bytes: int) -> bytes | None:
    """Stream the request body, refusing to buffer past ``max_bytes``.

    Returns the body bytes on success, or ``None`` when the cap was
    breached (caller surfaces a 413). The cap is checked twice:

    * **Header pre-check.** When the upstream sets
      ``Content-Length`` we refuse before reading any chunks so a
      hostile client cannot allocate up to the cap before being
      rejected.
    * **Streaming check.** The body is read chunk-by-chunk and we
      bail as soon as the buffered total crosses the cap; this
      handles ``Transfer-Encoding: chunked`` and lying
      ``Content-Length`` headers.
    """
    content_length_raw = request.headers.get("content-length")
    if content_length_raw is not None:
        try:
            declared = int(content_length_raw)
        except ValueError:
            return None
        if declared > max_bytes:
            return None

    buffered = bytearray()
    async for chunk in request.stream():
        buffered.extend(chunk)
        if len(buffered) > max_bytes:
            return None
    return bytes(buffered)


@router.post(
    "/workspaces/{workspace_id}/connectors/{instance_id}/events",
    summary="Webhook receiver for push-mode connector events",
    status_code=200,
)
async def post_events(
    request: Request,
    workspace_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=200,
            description="Workspace the connector instance is bound to.",
        ),
    ],
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
    settings = _resolve_settings(request)

    # Step 1 — enforce the body-size cap before any work. The webhook
    # path is unauthenticated until the signature check (step 4); the
    # cap is the first defence against trivial memory exhaustion.
    max_body_bytes = settings.publish_max_body_mb * _BYTES_PER_MB
    body = await _read_body_with_cap(request, max_bytes=max_body_bytes)
    if body is None:
        return _error_response(
            status_code=413,
            code="webhook.body_too_large",
            detail=(f"request body exceeds the {settings.publish_max_body_mb} MB limit"),
        )

    # Step 2 — resolve the push registration by the exact
    # (workspace, instance) tuple from the URL. Workspace-scoped
    # lookup means two workspaces using the same instance id (allowed
    # by SPL: ``ConnectorInstanceId`` is an opaque string, not
    # globally unique) cannot cross-tenant route.
    registration = manager.get_push_registration(workspace_id, instance_id)
    if registration is None:
        return _error_response(
            status_code=404,
            code="connector.instance_not_listening",
            detail=(
                f"connector instance {instance_id!r} in workspace {workspace_id!r} "
                "is not registered for push delivery"
            ),
        )

    if not registration.active:
        return _error_response(
            status_code=404,
            code="connector.instance_not_listening",
            detail=(
                f"connector instance {instance_id!r} in workspace {workspace_id!r} "
                "is not registered for push delivery"
            ),
        )

    # Step 4 — verify the signature.
    headers = {key: value for key, value in request.headers.items()}
    try:
        signature_ok = await registration.signature_verifier.verify(
            body=body,
            headers=headers,
            instance_id=registration.instance_id,
        )
    except Exception:
        _LOGGER.exception(
            "signature verifier raised; treating as failed verification ws=%s instance=%s",
            workspace_id,
            instance_id,
        )
        signature_ok = False
    if not signature_ok:
        return _error_response(
            status_code=401,
            code="webhook.signature_invalid",
            detail="webhook signature verification failed",
        )

    # Step 5 — parse the JSON body.
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

    # Step 6 — audit ``event.received`` with the pre-normalization
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

    # Step 7 — build a per-request bridge with the registration's
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
