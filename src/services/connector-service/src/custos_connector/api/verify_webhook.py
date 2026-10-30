"""Bounded private VerifyWebhook transport; vendor JSON is parsed only after verification."""

from __future__ import annotations

import base64
import binascii
import logging

from custos_spl.errors import BackendUnavailable, MigrationRequired
from fastapi import APIRouter, Request
from pydantic import ValidationError
from starlette.responses import JSONResponse

from custos_connector.api._common import error_response, resolve_providers
from custos_connector.listen.router import _read_body_with_cap
from custos_connector.listen.verification import (
    MAX_ENCODED_REQUEST_BYTES,
    MAX_RAW_BYTES,
    VerifyWebhookRequest,
    VerifyWebhookResponse,
    WebhookVerificationError,
    WebhookVerificationService,
    parse_json,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["webhook-verification"])


@router.post(
    "/VerifyWebhook",
    response_model=VerifyWebhookResponse,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": VerifyWebhookRequest.model_json_schema()}},
        },
    },
)
async def verify_webhook(request: Request) -> JSONResponse:
    """Verify one bounded request from the permitted mesh service without publishing anything."""
    try:
        if getattr(request.state, "webhook_caller_verified", False) is not True:
            raise WebhookVerificationError(
                403, "mesh_caller_denied", "permitted service identity required"
            )
        if not request.app.state.ready:
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "connector storage is not ready"
            )
        raw = await _read_body_with_cap(request, max_bytes=MAX_ENCODED_REQUEST_BYTES)
        if raw is None:
            raise WebhookVerificationError(
                413, "webhook.too_large", "encoded verification request exceeds the limit"
            )
        try:
            body = VerifyWebhookRequest.model_validate(parse_json(raw))
        except ValidationError as exc:
            raise WebhookVerificationError(
                400, "webhook.malformed", "invalid verification request"
            ) from exc
        if len(body.body_base64) > 4 * ((MAX_RAW_BYTES + 2) // 3):
            raise WebhookVerificationError(
                413, "webhook.too_large", "webhook body exceeds the limit"
            )
        try:
            decoded = base64.b64decode(body.body_base64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise WebhookVerificationError(
                400, "webhook.malformed", "invalid body encoding"
            ) from exc
        providers = resolve_providers(request)
        service = WebhookVerificationService(
            providers.instance_store, providers.catalog_store, providers.identity_registry
        )
        response = await service.verify(
            workspace_id=body.workspace_id,
            instance_id=body.connector_instance_id,
            body=decoded,
            headers=body.headers,
        )
    except WebhookVerificationError as exc:
        logger.warning("Webhook verification rejected: %s", exc.code)
        return error_response(status_code=exc.status_code, code=exc.code, detail=exc.detail)
    except (BackendUnavailable, MigrationRequired) as exc:
        logger.warning("Webhook verification storage unavailable: %s", type(exc).__name__)
        return error_response(
            status_code=503,
            code="webhook.verifier_unavailable",
            detail="connector storage is unavailable",
        )
    return JSONResponse(response.model_dump(mode="json", by_alias=True))
