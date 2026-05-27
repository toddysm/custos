"""Connector-service Listen module (CONN-IMPL-025, issue #308).

Implements the inbound side of the event pipeline:

* :class:`~custos_connector.listen.normalizer.EventNormalizer` —
  translates plugin-emitted event mappings into canonical
  :class:`~custos_connector.listen.normalizer.NormalizedEvent`
  envelopes with strict ``eventId`` / ``eventType`` validation.
* :class:`~custos_connector.listen.publisher.EventPublisher` Protocol +
  :class:`~custos_connector.listen.publisher.PublisherBridge` —
  the single normalize → audit → publish pipeline used by both pull
  and push delivery, ensuring identical envelopes downstream.
* :class:`~custos_connector.listen.manager.ListenManager` —
  per-instance activation / deactivation of pull (via the
  :class:`~custos_connector.scheduler.PullLoopScheduler`) and push
  (via an in-memory registry) delivery, branching on
  ``spec.events.delivery`` from the connector type's manifest.
* :class:`~custos_connector.listen.signature.SignatureVerifier`
  Protocol + HMAC / Reject-all / Allow-all impls — webhook signature
  verification used by the push receiver.
* :mod:`custos_connector.listen.router` — the FastAPI router that
  mounts ``POST /v1/webhooks/connectors/{instance_id}/events``.

Re-exports the names callers most commonly need at the package level
so consumers can ``from custos_connector.listen import EventNormalizer,
ListenManager, ...`` instead of reaching into the submodules.
"""

from __future__ import annotations

from custos_connector.listen.errors import (
    EventNormalizationError,
    EventPublishError,
    MalformedEventError,
    MissingEventIdError,
    MissingEventTypeError,
    UnknownEventTypeError,
)
from custos_connector.listen.manager import (
    REASON_SINK_CONNECTOR,
    ActivationResult,
    ListenManager,
    ListenManagerError,
    PushRegistration,
    UnknownConnectorTypeError,
)
from custos_connector.listen.normalizer import (
    DELIVERY_MODE_PULL,
    DELIVERY_MODE_PUSH,
    SCHEMA_VERSION,
    EventNormalizer,
    NormalizedEvent,
)
from custos_connector.listen.publisher import (
    EventPublisher,
    EventPublisherHandler,
    LocalEventBus,
    NoOpEventPublisher,
    NormalizerSelector,
    PublisherBridge,
    RecordingEventPublisher,
    build_publisher_bridge,
)
from custos_connector.listen.signature import (
    SIGNATURE_HEADER,
    AllowAllSignatureVerifier,
    HmacSignatureVerifier,
    RejectAllSignatureVerifier,
    SignatureVerificationFailure,
    SignatureVerifier,
)

__all__ = [
    "DELIVERY_MODE_PULL",
    "DELIVERY_MODE_PUSH",
    "REASON_SINK_CONNECTOR",
    "SCHEMA_VERSION",
    "SIGNATURE_HEADER",
    "ActivationResult",
    "AllowAllSignatureVerifier",
    "EventNormalizationError",
    "EventNormalizer",
    "EventPublishError",
    "EventPublisher",
    "EventPublisherHandler",
    "HmacSignatureVerifier",
    "ListenManager",
    "ListenManagerError",
    "LocalEventBus",
    "MalformedEventError",
    "MissingEventIdError",
    "MissingEventTypeError",
    "NoOpEventPublisher",
    "NormalizedEvent",
    "NormalizerSelector",
    "PublisherBridge",
    "PushRegistration",
    "RecordingEventPublisher",
    "RejectAllSignatureVerifier",
    "SignatureVerificationFailure",
    "SignatureVerifier",
    "UnknownConnectorTypeError",
    "UnknownEventTypeError",
    "build_publisher_bridge",
]
