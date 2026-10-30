"""Verify scoped webhook bytes with Connector-owned credentials, without publishing events."""

from __future__ import annotations

import hmac
import json
import logging
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceStoreProvider,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from custos_connector.identity import IdentityResolverError, IdentityResolverRegistry
from custos_connector.listen.errors import EventNormalizationError
from custos_connector.listen.normalizer import DELIVERY_MODE_PUSH, EventNormalizer
from custos_connector.listen.signature import SIGNATURE_HEADER, HmacSignatureVerifier

logger = logging.getLogger(__name__)
MAX_RAW_BYTES = 1024 * 1024
MAX_ENCODED_REQUEST_BYTES = 2 * 1024 * 1024
MAX_HEADER_BYTES = 16 * 1024
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_PRIVATE_HEADERS = frozenset(
    {
        "x-custos-callctx",
        "x-custos-webhook-authorization",
        "dapr-api-token",
        "dapr-caller-app-id",
        "dapr-caller-namespace",
    }
)
RejectionCode = Literal["missing-event-id", "missing-event-type", "unknown-event-type", "malformed"]


class WebhookVerificationError(Exception):
    """A stable sanitized failure, never carrying raw payloads or credential values."""

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        """Store only caller-safe error metadata."""
        super().__init__(detail)
        self.status_code, self.code, self.detail = status_code, code, detail


class VerifyWebhookRequest(BaseModel):
    """Version-one private request; credentials are verification data, never platform identity."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1"] = Field(alias="schemaVersion")
    workspace_id: str = Field(alias="workspaceId", min_length=1, max_length=256)
    connector_instance_id: str = Field(alias="connectorInstanceId", min_length=1, max_length=256)
    body_base64: str = Field(alias="bodyBase64", repr=False)
    headers: dict[str, str] = Field(repr=False)


class VerifiedWebhookEvent(BaseModel):
    """An existing canonical connector envelope retaining its original batch position."""

    index: int = Field(ge=0)
    event: dict[str, Any]


class RejectedWebhookEvent(BaseModel):
    """The entire public rejection surface: original index and a closed reason code."""

    index: int = Field(ge=0)
    code: RejectionCode


class VerifyWebhookResponse(BaseModel):
    """A complete indexed partition of a verified, structurally valid batch."""

    schema_version: Literal["1"] = Field(default="1", alias="schemaVersion")
    received_count: int = Field(alias="receivedCount", ge=1)
    events: list[VerifiedWebhookEvent]
    rejections: list[RejectedWebhookEvent]


class WebhookCredentialConfig(BaseModel):
    """Instance-owned verification policy and a reference consumed by an existing resolver."""

    model_config = ConfigDict(extra="forbid", strict=True)
    scheme: Literal["hmac-sha256", "bearer"]
    authentication_type: str = Field(alias="authenticationType", min_length=1)
    authentication: dict[str, Any] = Field(repr=False)
    material_key: str = Field(alias="materialKey", min_length=1)
    signature_header: str = Field(default="x-custos-signature", alias="signatureHeader")


def bounded_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Normalize external names while rejecting ambiguity, private identity and oversized values."""
    normalized: dict[str, str] = {}
    size = 0
    for name, value in headers.items():
        try:
            size += len(name.encode("utf-8")) + len(value.encode("utf-8")) + 4
        except UnicodeError as exc:
            raise WebhookVerificationError(
                400, "webhook.malformed", "invalid webhook headers"
            ) from exc
        if size > MAX_HEADER_BYTES:
            raise WebhookVerificationError(
                413, "webhook.too_large", "webhook headers exceed the limit"
            )
        key = name.lower()
        if (
            not _HEADER_NAME.fullmatch(name)
            or key in normalized
            or key in _PRIVATE_HEADERS
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise WebhookVerificationError(400, "webhook.malformed", "invalid webhook headers")
        normalized[key] = value
    return normalized


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys rather than silently selecting an identity or credential."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _nonfinite(_value: str) -> None:
    """Reject non-JSON numeric extensions before normalization."""
    raise ValueError("nonfinite JSON value")


def parse_json(raw: bytes) -> Any:
    """Decode bounded strict UTF-8 JSON without exposing parser excerpts on failure."""
    try:
        parsed = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_nonfinite
        )
        json.dumps(parsed, ensure_ascii=False, allow_nan=False).encode("utf-8")
        return parsed
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise WebhookVerificationError(400, "webhook.malformed", "invalid webhook JSON") from exc


class CredentialSignatureVerifier:
    """An instance-bound SignatureVerifier that resolves fresh material on every request."""

    def __init__(self, instance: ConnectorInstance, registry: IdentityResolverRegistry) -> None:
        """Read verification policy only from the persisted instance credential model."""
        self._instance, self._registry = instance, registry
        try:
            self._config = WebhookCredentialConfig.model_validate(
                instance.credentials_authentication.get("webhookVerification")
            )
            header = self._config.signature_header
            if not _HEADER_NAME.fullmatch(header) or header.lower() in _PRIVATE_HEADERS:
                raise ValueError("invalid configured signature header")
        except (ValidationError, ValueError) as exc:
            logger.warning("Webhook verification configuration unavailable: %s", type(exc).__name__)
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "webhook verification is not configured"
            ) from exc

    async def _secret(self, instance_id: str) -> bytes:
        """Resolve this instance's reference without cached pre-rotation values."""
        if instance_id != str(self._instance.instance_id):
            raise WebhookVerificationError(
                403, "webhook.instance_mismatch", "verification scope mismatch"
            )
        config = self._config
        try:
            resolved = await self._registry.resolve(
                workspace_id=str(self._instance.workspace_id),
                instance_id=instance_id,
                actor="connector-service:webhook-verifier",
                authentication_type=config.authentication_type,
                credentials_authentication=config.authentication,
                lease_ttl_seconds=0,
                force_refresh=True,
            )
        except IdentityResolverError as exc:
            logger.warning("Webhook credential resolution unavailable: %s", exc.code.value)
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "webhook credentials are unavailable"
            ) from exc
        if resolved.expires_at is not None and resolved.expires_at <= datetime.now(UTC):
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "webhook credentials are unavailable"
            )
        value = resolved.material.get(config.material_key)
        try:
            secret = value.encode("utf-8") if isinstance(value, str) else value
        except UnicodeError as exc:
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "webhook credentials are unavailable"
            ) from exc
        if not isinstance(secret, bytes) or not secret:
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "webhook credentials are unavailable"
            )
        return secret

    async def verify(self, *, body: bytes, headers: Mapping[str, str], instance_id: str) -> bool:
        """Verify raw bytes or the configured bearer without exporting either credential."""
        if instance_id != str(self._instance.instance_id):
            return False
        if self._config.scheme == "bearer":
            scheme, separator, supplied = headers.get("authorization", "").partition(" ")
            if (
                scheme.lower() != "bearer"
                or not separator
                or not supplied
                or supplied.strip() != supplied
            ):
                return False
            return hmac.compare_digest(supplied.encode("utf-8"), await self._secret(instance_id))
        value = headers.get(self._config.signature_header.lower())
        translated = {SIGNATURE_HEADER: value} if value is not None else {}
        return await HmacSignatureVerifier(secret_lookup=self._secret).verify(
            body=body, headers=translated, instance_id=instance_id
        )


class WebhookVerificationService:
    """Read an exact active pair, authenticate before parsing, and normalize without dispatch."""

    def __init__(
        self,
        instance_store: ConnectorInstanceStoreProvider,
        catalog_store: CatalogStoreProvider,
        identity_registry: IdentityResolverRegistry,
    ) -> None:
        """Bind existing Connector-owned providers; no publisher or Workflow client is accepted."""
        self._instances, self._catalog, self._identities = (
            instance_store,
            catalog_store,
            identity_registry,
        )

    async def verify(
        self, *, workspace_id: str, instance_id: str, body: bytes, headers: Mapping[str, str]
    ) -> VerifyWebhookResponse:
        """Partition all indices, or fail the whole unauthenticated or malformed batch."""
        if not workspace_id.strip() or not instance_id.strip():
            raise WebhookVerificationError(
                400, "webhook.malformed", "verification scope is required"
            )
        if len(body) > MAX_RAW_BYTES:
            raise WebhookVerificationError(
                413, "webhook.too_large", "webhook body exceeds the limit"
            )
        headers = bounded_headers(headers)
        instance = await self._instances.get_connector_instance(
            WorkspaceId(workspace_id), ConnectorInstanceId(instance_id)
        )
        if instance is None or not instance.enabled or instance.status != "active":
            raise WebhookVerificationError(
                404, "connector.instance_not_listening", "unknown or inactive connector"
            )
        version = await self._catalog.get_connector_type_version(instance.type, instance.version)
        if version is None:
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "connector event catalog unavailable"
            )
        spec = version.normalized_manifest.get("spec")
        if not isinstance(spec, Mapping):
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "connector event catalog unavailable"
            )
        events = spec.get("events")
        if events is None:
            raise WebhookVerificationError(
                404, "connector.instance_not_listening", "unknown or inactive connector"
            )
        if not isinstance(events, Mapping):
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "connector event catalog unavailable"
            )
        modes = events.get("delivery")
        if not isinstance(modes, list):
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "connector event catalog unavailable"
            )
        if DELIVERY_MODE_PUSH not in modes:
            raise WebhookVerificationError(
                404, "connector.instance_not_listening", "unknown or inactive connector"
            )
        produced = events.get("produced")
        if (
            not isinstance(produced, list)
            or not produced
            or not all(isinstance(item, str) for item in produced)
        ):
            raise WebhookVerificationError(
                503, "webhook.verifier_unavailable", "connector event catalog unavailable"
            )
        verifier = CredentialSignatureVerifier(instance, self._identities)
        if not await verifier.verify(body=body, headers=headers, instance_id=instance_id):
            raise WebhookVerificationError(
                401, "webhook.signature_invalid", "webhook verification failed"
            )
        parsed = parse_json(body)
        batch = parsed.get("events") if isinstance(parsed, dict) else None
        if not isinstance(batch, list) or not batch:
            raise WebhookVerificationError(
                400, "webhook.malformed", "a nonempty event batch is required"
            )
        normalizer = EventNormalizer(produced_event_types=produced)
        accepted: list[VerifiedWebhookEvent] = []
        rejected: list[RejectedWebhookEvent] = []
        received_at = datetime.now(UTC)
        for index, raw in enumerate(batch):
            try:
                event = normalizer.normalize(
                    raw,
                    workspace_id=workspace_id,
                    instance_id=instance_id,
                    delivery_mode=DELIVERY_MODE_PUSH,
                    received_at=received_at,
                )
            except EventNormalizationError as exc:
                rejected.append(
                    RejectedWebhookEvent.model_validate({"index": index, "code": exc.reason})
                )
            else:
                accepted.append(VerifiedWebhookEvent(index=index, event=event.to_wire()))
        return VerifyWebhookResponse(receivedCount=len(batch), events=accepted, rejections=rejected)
