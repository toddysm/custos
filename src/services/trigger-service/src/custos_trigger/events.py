"""The ``NormalizedEvent`` envelope (design § NormalizedEvent schema).

Every receiver (manual, scheduled, webhook, vendor-push, pull, internal)
normalizes its source-specific payload into a single :class:`NormalizedEvent`
before it enters the linear ``Classify → Match → Dedup → Dispatch`` pipeline.
The envelope is also the shape selectors evaluate against via the ``event``
binding root (``custos-cel``): ``event.kind``, ``event.subject``,
``event.source.{type,connectorInstanceId,subscriptionId,vendor,occurredAt}``,
``event.data.*``, ``event.raw.{headers,body}``.

``kind`` is the platform-level event taxonomy (validated via
:func:`custos_trigger.taxonomy.validate_kind`); ``data`` is the normalized,
vendor-agnostic payload; ``raw`` is retained for audit and to let
connector-aware activities re-parse the original event if needed.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, field_validator

from custos_trigger._wire import WireModel
from custos_trigger.models import SourceType
from custos_trigger.taxonomy import validate_kind


class EventSource(WireModel):
    """Provenance block — who emitted the event and when.

    ``type`` is the locked :class:`SourceType`; the remaining fields are
    optional because not every source populates every one (a manual fire has
    no ``connectorInstanceId`` or ``vendor``, for example).
    """

    type: SourceType
    connector_instance_id: str | None = Field(default=None, alias="connectorInstanceId")
    subscription_id: str | None = Field(default=None, alias="subscriptionId")
    vendor: str | None = None
    occurred_at: str = Field(..., alias="occurredAt", min_length=1)


class EventRaw(WireModel):
    """The unparsed source payload, retained for audit and re-parsing.

    ``headers`` carries the inbound transport headers (lower-cased by the
    receiver) and ``body`` the raw textual body. Both default empty so an
    internally minted event need not synthesize a transport frame.
    """

    headers: dict[str, str] = Field(default_factory=dict)
    body: str = ""


class NormalizedEvent(WireModel):
    """The uniform event envelope produced by every receiver.

    The schema mirrors the ``event`` binding root that selectors evaluate
    against, so a selector authored against this shape sees exactly these
    fields. ``kind`` is validated against the canonical taxonomy at
    construction time, rejecting malformed or platform-colliding kinds.
    """

    schema_version: Literal["1"] = Field(default="1", alias="schemaVersion")
    event_id: str = Field(..., alias="eventId", min_length=1)
    source: EventSource
    kind: str = Field(..., min_length=1)
    subject: str = ""
    data: dict[str, Any] = Field(default_factory=dict)
    raw: EventRaw = Field(default_factory=EventRaw)

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, value: str) -> str:
        """Reject any ``kind`` the taxonomy considers malformed or illegal.

        :func:`validate_kind` raises :class:`~custos_trigger.taxonomy.InvalidKindError`
        (a :class:`ValueError`), which pydantic surfaces as a validation error.
        """
        return validate_kind(value)


__all__ = ["EventRaw", "EventSource", "NormalizedEvent"]
