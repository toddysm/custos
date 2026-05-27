"""Event publisher seam + the shared publisher bridge (CONN-IMPL-025, #308).

Two collaborators live here:

* :class:`EventPublisher` — the Protocol the Listen Manager publishes
  normalized events through. Concrete implementations:
  :class:`NoOpEventPublisher`, :class:`RecordingEventPublisher`,
  :class:`LocalEventBus` for dev / test deployments, and
  :class:`DaprPubSubEventPublisher` for the production fan-out path
  through Dapr Pub/Sub (CONN-IMPL-027, Phase J).

* :class:`PublisherBridge` (returned by :func:`build_publisher_bridge`)
  — the callable that satisfies
  :data:`custos_connector.cursor.service.EventPublisher` and is wired
  into :class:`CursorService` as its tick-time publisher. Same callable
  is also used by the push receiver
  (:func:`custos_connector.listen.router.post_events`) so both delivery
  paths flow through one normalize → audit → publish pipeline. This is
  what makes the issue #308 acceptance criterion
  "both paths produce identical normalized envelopes" hold by
  construction: there is exactly one normalizer call site.

Bridge semantics
----------------

Per plugin event, the bridge:

1. calls :meth:`EventNormalizer.normalize` with the workspace + instance
   + delivery mode + batch-shared receive timestamp;
2. on :class:`EventNormalizationError` (missing eventId, missing
   eventType, unknown eventType, or malformed object): audits
   ``event.rejected`` with the stable ``reason`` carried on the
   exception and the ``batch_index`` of the offending event, then
   **continues** with the next event. The cursor still advances —
   poison-pill quarantine per design § 22.4
   "Push receiver and pull fan-out";
3. on success: audits ``event.normalized`` and forwards the envelope
   to the wired :class:`EventPublisher`. If the publisher raises
   :class:`EventPublishError` (or any other exception), the bridge
   re-raises so :meth:`CursorService.tick` refuses to commit the
   cursor (the "MUST raise on partial publish" contract on
   :data:`custos_connector.cursor.service.EventPublisher`).

Receive-timestamp policy: every event in one batch receives the
*same* ``source.receivedAt`` value (captured once at the top of the
bridge call). This keeps per-batch ordering observable in the audit
log even after fan-out into Pub/Sub.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, Protocol

import httpx
from custos_spl.ids import WorkspaceId

from custos_connector.audit import (
    audit_event_normalized,
    audit_event_rejected,
)
from custos_connector.listen.errors import EventNormalizationError, EventPublishError
from custos_connector.listen.normalizer import (
    DELIVERY_MODE_PULL,
    EventNormalizer,
    NormalizedEvent,
)

if TYPE_CHECKING:
    from custos_spl import MetadataStoreProvider
    from custos_spl.interfaces.connector_instance_store import ConnectorInstance

_LOGGER: Final[logging.Logger] = logging.getLogger("custos_connector.listen.publisher")

__all__ = [
    "DaprPubSubEventPublisher",
    "EventPublisher",
    "EventPublisherHandler",
    "LocalEventBus",
    "NoOpEventPublisher",
    "NormalizerSelector",
    "PublisherBridge",
    "RecordingEventPublisher",
    "build_publisher_bridge",
]


#: Callable seam the pull-path bridge calls on every tick to resolve
#: the per-instance normalizer. Typically wired to
#: :meth:`custos_connector.listen.manager.ListenManager.get_normalizer_for_instance`
#: which caches by ``(type, version)``. Defined as a module-level
#: ``TypeAlias`` so subclasses / tests can name the type explicitly
#: when stubbing.
NormalizerSelector = Callable[["ConnectorInstance"], Awaitable[EventNormalizer]]


# ---------------------------------------------------------------------------
# Publisher Protocol + canonical implementations
# ---------------------------------------------------------------------------


class EventPublisher(Protocol):
    """Downstream sink for normalized connector events.

    Implementations MUST raise on failure so the calling bridge can
    propagate the error back to :meth:`CursorService.tick` and keep
    the cursor untouched (the at-least-once contract).

    Implementations SHOULD be idempotent on retry: the connector
    service may invoke ``publish`` again with the same envelope after
    a transient failure, and downstream consumers already de-duplicate
    on :attr:`NormalizedEvent.event_id`.
    """

    async def publish(self, event: NormalizedEvent) -> None:
        """Publish exactly one normalized event."""
        ...


class NoOpEventPublisher:
    """Dev-mode publisher that logs each event and otherwise does nothing.

    Wired by :func:`custos_connector.providers.load_providers` whenever
    ``CONN_DAPR_HTTP_ENDPOINT`` is unset (single-node dev deployments and
    the test suite). Production deployments set the env var so the
    provider factory swaps in :class:`DaprPubSubEventPublisher` instead
    (CONN-IMPL-027).
    """

    async def publish(self, event: NormalizedEvent) -> None:
        _LOGGER.info(
            "event-published (no-op) event_id=%s event_type=%s instance=%s mode=%s",
            event.event_id,
            event.event_type,
            event.source.get("instanceId"),
            event.source.get("deliveryMode"),
        )


class RecordingEventPublisher:
    """Test-only publisher that captures every event in :attr:`published`.

    Mirrors the shape of
    :class:`custos_auth.token_revoked_events.RecordingTokenRevokedPublisher`
    so test code across services follows one pattern.
    """

    def __init__(self) -> None:
        self.published: list[NormalizedEvent] = []

    async def publish(self, event: NormalizedEvent) -> None:
        self.published.append(event)


#: Signature of an in-process event handler attached to
#: :class:`LocalEventBus`.
EventPublisherHandler = Callable[[NormalizedEvent], Awaitable[None]]


@dataclass(slots=True)
class LocalEventBus:
    """In-process publisher that synchronously fans out to local handlers.

    Plays two roles:

    * Implements :class:`EventPublisher` so the listen path can
      publish through the same interface a real Dapr publisher uses.
    * Holds a list of subscribed handlers that fire synchronously on
      :meth:`publish`. This is the in-process fan-out used by the
      integration test suite (and the future single-replica dev
      profile) where standing up Dapr would be over-kill.

    Handlers that raise are logged at WARNING and skipped so one
    misbehaving consumer cannot break the publish path. The bridge's
    "raise on publish failure" contract is still honored at the
    bridge boundary: a handler that raises does *not* halt cursor
    advance because the bus has already absorbed the error here.
    Operators who need per-handler at-least-once must wire a real
    Pub/Sub publisher (:class:`DaprPubSubEventPublisher`) instead.
    """

    handlers: list[EventPublisherHandler] = field(default_factory=list)

    def subscribe(self, handler: EventPublisherHandler) -> None:
        """Register ``handler`` to receive every published event."""
        self.handlers.append(handler)

    async def publish(self, event: NormalizedEvent) -> None:
        """Deliver ``event`` to every subscribed handler in order."""
        _LOGGER.info(
            "event-published (local bus) event_id=%s event_type=%s mode=%s",
            event.event_id,
            event.event_type,
            event.source.get("deliveryMode"),
        )
        for handler in self.handlers:
            try:
                await handler(event)
            except Exception:  # guard the publish path
                _LOGGER.warning(
                    "event handler raised; continuing",
                    exc_info=True,
                )


# ---------------------------------------------------------------------------
# Dapr Pub/Sub publisher — production fan-out path
# ---------------------------------------------------------------------------


#: Default per-request HTTP timeout (seconds) for the Dapr Pub/Sub
#: publish call. Picked to be well above the in-cluster Dapr round-trip
#: budget but tight enough that a stuck Dapr sidecar surfaces as a
#: publish failure quickly (so the cursor stops advancing and the pull
#: loop retries from the previous committed envelope).
_DAPR_DEFAULT_TIMEOUT_SEC: Final[float] = 10.0


@dataclass(slots=True)
class DaprPubSubEventPublisher:
    """Production :class:`EventPublisher` that fans out via Dapr Pub/Sub.

    Posts each :class:`NormalizedEvent` as ``Content-Type:
    application/json`` to the Dapr sidecar's
    ``/v1.0/publish/{pubsub_name}/{topic}`` endpoint (see Dapr
    Pub/Sub HTTP API). The Trigger Service binds its Dapr subscription
    against the same ``(pubsub_name, topic)`` pair — returned to it by
    the ``SubscribeEvents`` internal RPC — and filters delivery on
    ``source.instanceId`` locally so one topic shard fans out to N
    subscriptions per the design's per-instance ownership model.

    Failure semantics
    -----------------

    Any non-2xx HTTP response or transport error is wrapped in
    :class:`EventPublishError` so the publisher contract holds: the
    :class:`PublisherBridge` re-raises, :meth:`CursorService.tick`
    refuses to commit the cursor, and the next tick re-emits the
    batch from the prior committed envelope — preserving the at-
    least-once delivery guarantee documented at design § Cursor
    Ownership.

    Lifecycle
    ---------

    The publisher does **not** own the :class:`httpx.AsyncClient` — it
    receives an already-constructed client. The FastAPI lifespan hook
    in :func:`custos_connector.providers.load_providers` builds a
    dedicated :class:`httpx.AsyncClient` for the Dapr publisher and
    stores it on :attr:`Providers.dapr_http_client` so the lifespan
    ``finally`` block can :meth:`aclose` it on shutdown. The identity
    registry owns a separate :class:`HttpxAsyncHttpClient` for its own
    outbound calls; the two clients are intentionally not shared
    because their lifetimes, retry/timeout policies, and shutdown
    paths are independent.
    """

    http_client: httpx.AsyncClient
    dapr_endpoint: str
    pubsub_name: str
    topic: str
    request_timeout_seconds: float = _DAPR_DEFAULT_TIMEOUT_SEC
    _publish_url: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Strip trailing slash so the join doesn't produce
        # ``http://localhost:3500//v1.0/publish/...``.
        endpoint = self.dapr_endpoint.rstrip("/")
        if not endpoint:
            raise ValueError(
                "DaprPubSubEventPublisher requires a non-empty dapr_endpoint; "
                "an empty string disables Dapr publishing and the caller should "
                "wire NoOpEventPublisher instead"
            )
        if not self.pubsub_name:
            raise ValueError("DaprPubSubEventPublisher requires a non-empty pubsub_name")
        if not self.topic:
            raise ValueError("DaprPubSubEventPublisher requires a non-empty topic")
        # ``object.__setattr__`` because the dataclass is ``slots=True``
        # and we're setting an ``init=False`` field from ``__post_init__``.
        object.__setattr__(
            self,
            "_publish_url",
            f"{endpoint}/v1.0/publish/{self.pubsub_name}/{self.topic}",
        )

    async def publish(self, event: NormalizedEvent) -> None:
        """Publish exactly one normalized event through Dapr Pub/Sub."""
        wire = event.to_wire()
        try:
            response = await self.http_client.post(
                self._publish_url,
                json=wire,
                timeout=self.request_timeout_seconds,
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise EventPublishError(
                f"Dapr publish failed (transport): {exc!r} "
                f"(pubsub={self.pubsub_name!r}, topic={self.topic!r})"
            ) from exc
        if response.status_code // 100 != 2:
            # Truncate the body so log payloads stay bounded; the Dapr
            # sidecar typically returns a short JSON envelope so 200
            # chars is plenty for diagnostics.
            body_preview = response.text[:200] if response.text else ""
            raise EventPublishError(
                f"Dapr publish failed: status={response.status_code} "
                f"body={body_preview!r} "
                f"(pubsub={self.pubsub_name!r}, topic={self.topic!r})"
            )
        _LOGGER.debug(
            "event-published (dapr) event_id=%s event_type=%s pubsub=%s topic=%s",
            event.event_id,
            event.event_type,
            self.pubsub_name,
            self.topic,
        )


# ---------------------------------------------------------------------------
# Publisher bridge — the single normalize+audit+publish pipeline
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BridgeResult:
    """Summary returned by :meth:`PublisherBridge.process_batch`.

    The pull-tick path discards this (CursorService doesn't read it),
    but the push receiver uses it to build the HTTP response body
    (``{accepted, rejected}``).
    """

    accepted: int
    rejected: int


class PublisherBridge:
    """Single normalize → audit → publish pipeline used by both paths.

    Wraps an :class:`EventNormalizer`, an :class:`EventPublisher`, and
    a :class:`MetadataStoreProvider` (for audit emission). Exposes two
    entry points:

    * :meth:`__call__` — implements the
      :data:`custos_connector.cursor.service.EventPublisher` Callable
      contract so :class:`CursorService` can drive the pull-tick path
      without knowing about the listen module.
    * :meth:`process_batch` — the explicit version used by the push
      receiver, which needs the
      :class:`BridgeResult` counts for its HTTP response.

    Both entry points share one helper (:meth:`_run_batch`) so the
    pull and push paths cannot drift. This is what makes the
    "identical normalized envelopes" acceptance criterion structural.

    The ``actor`` field carries the audit-pipeline identity attributed
    to the publish (``connector-service:listen`` for pull, the
    authenticated webhook principal for push). The bridge does not
    re-authenticate; the caller is responsible for handing in an
    accurate ``actor`` value.
    """

    __slots__ = (
        "_actor",
        "_metadata_store",
        "_normalizer",
        "_normalizer_selector",
        "_publisher",
    )

    def __init__(
        self,
        *,
        normalizer: EventNormalizer | None = None,
        normalizer_selector: NormalizerSelector | None = None,
        publisher: EventPublisher,
        metadata_store: MetadataStoreProvider,
        actor: str,
    ) -> None:
        # Exactly one of ``normalizer`` (push path / tests — the
        # normalizer is pinned to the instance's connector type
        # version at activation time) or ``normalizer_selector``
        # (pull path — resolves per-instance at tick time so a single
        # bridge wired into ``CursorService`` validates every
        # connector instance against the right ``produced`` catalog)
        # must be supplied. Both modes are accepted because the push
        # receiver builds a per-request bridge with the registration's
        # frozen normalizer, while the pull path uses one shared
        # bridge across every registered instance.
        if (normalizer is None) == (normalizer_selector is None):
            raise ValueError(
                "PublisherBridge requires exactly one of normalizer / normalizer_selector"
            )
        self._normalizer = normalizer
        self._normalizer_selector = normalizer_selector
        self._publisher = publisher
        self._metadata_store = metadata_store
        self._actor = actor

    async def __call__(
        self,
        workspace_id: WorkspaceId,
        instance: ConnectorInstance,
        events: Sequence[Mapping[str, object]],
    ) -> None:
        """Pull-tick :class:`EventPublisher` callable.

        Drops the :class:`BridgeResult` because
        :class:`CursorService` does not consume it. Resolves the
        per-instance normalizer via the selector when configured so
        a single bridge can serve every instance across every
        connector type version registered on the pod.
        """
        normalizer = await self._resolve_normalizer(instance)
        await self._run_batch(
            normalizer=normalizer,
            workspace_id=str(workspace_id),
            instance_id=str(instance.instance_id),
            events=events,
            delivery_mode=DELIVERY_MODE_PULL,
            received_at=datetime.now(UTC),
        )

    async def process_batch(
        self,
        *,
        workspace_id: str,
        instance_id: str,
        events: Sequence[Mapping[str, object]],
        delivery_mode: str,
        received_at: datetime,
    ) -> BridgeResult:
        """Push-receiver entry point.

        Returns counts the HTTP layer surfaces back to the webhook
        caller so operators can see at the call site how many events
        in their POST were quarantined.

        The push path always supplies a fixed ``normalizer`` at
        construction time (the per-instance one from the
        :class:`PushRegistration`), so the bridge MUST have been
        built with ``normalizer=...`` rather than
        ``normalizer_selector=...``; calling this on a
        selector-mode bridge raises :class:`RuntimeError` because
        there is no ``ConnectorInstance`` in scope to resolve the
        normalizer for.
        """
        if self._normalizer is None:
            raise RuntimeError(
                "PublisherBridge.process_batch requires a fixed normalizer; "
                "selector-mode bridges may only be invoked via __call__"
            )
        return await self._run_batch(
            normalizer=self._normalizer,
            workspace_id=workspace_id,
            instance_id=instance_id,
            events=events,
            delivery_mode=delivery_mode,
            received_at=received_at,
        )

    async def _resolve_normalizer(self, instance: ConnectorInstance) -> EventNormalizer:
        """Return the :class:`EventNormalizer` to validate ``instance``'s events.

        Hot path on every pull tick; selector-mode bridges defer to
        the configured callable (typically
        :meth:`ListenManager.get_normalizer_for_instance` which
        caches by ``(type, version)``) so the catalog read happens
        at most once per type version.
        """
        if self._normalizer_selector is not None:
            return await self._normalizer_selector(instance)
        # _normalizer is non-None when _normalizer_selector is None
        # (the constructor enforces the XOR), but mypy can't see
        # that across the call so we narrow defensively.
        assert self._normalizer is not None
        return self._normalizer

    async def _run_batch(
        self,
        *,
        normalizer: EventNormalizer,
        workspace_id: str,
        instance_id: str,
        events: Sequence[Mapping[str, object]],
        delivery_mode: str,
        received_at: datetime,
    ) -> BridgeResult:
        accepted = 0
        rejected = 0
        for index, raw in enumerate(events):
            try:
                normalized = normalizer.normalize(
                    raw,
                    workspace_id=workspace_id,
                    instance_id=instance_id,
                    delivery_mode=delivery_mode,
                    received_at=received_at,
                )
            except EventNormalizationError as err:
                rejected += 1
                await audit_event_rejected(
                    self._metadata_store,
                    workspace_id=workspace_id,
                    actor=self._actor,
                    instance_id=instance_id,
                    delivery_mode=delivery_mode,
                    batch_index=index,
                    reason=err.reason,
                    event_id=_safe_str(raw, "eventId") if isinstance(raw, Mapping) else None,
                    event_type=_safe_str(raw, "eventType") if isinstance(raw, Mapping) else None,
                    detail=err.detail,
                )
                continue

            # Publish first, audit-on-success second: if the publish
            # raises, we have not yet emitted ``event.normalized`` so
            # the audit log doesn't lie about events that never
            # actually went out.
            await self._publisher.publish(normalized)
            accepted += 1
            await audit_event_normalized(
                self._metadata_store,
                workspace_id=workspace_id,
                actor=self._actor,
                instance_id=instance_id,
                event_id=normalized.event_id,
                event_type=normalized.event_type,
                delivery_mode=delivery_mode,
                batch_index=index,
            )
        return BridgeResult(accepted=accepted, rejected=rejected)


def build_publisher_bridge(
    *,
    normalizer: EventNormalizer | None = None,
    normalizer_selector: NormalizerSelector | None = None,
    publisher: EventPublisher,
    metadata_store: MetadataStoreProvider,
    actor: str = "connector-service:listen",
) -> PublisherBridge:
    """Convenience constructor used by the app factory + tests.

    Forwards exactly one of ``normalizer`` (push path / fixed
    catalog) or ``normalizer_selector`` (pull path / per-instance
    lookup) — the same XOR :class:`PublisherBridge` enforces in its
    own constructor. The default ``actor`` matches the pull-tick
    path; the push receiver constructs its own bridge with the
    authenticated webhook principal so audit emissions on the push
    path carry the right identity.
    """
    return PublisherBridge(
        normalizer=normalizer,
        normalizer_selector=normalizer_selector,
        publisher=publisher,
        metadata_store=metadata_store,
        actor=actor,
    )


def _safe_str(raw: Mapping[str, object], key: str) -> str | None:
    """Return ``raw[key]`` if it is a non-empty string, else ``None``.

    Used by :meth:`PublisherBridge._run_batch` when assembling the
    ``event.rejected`` audit payload: a present-but-wrong ``eventId``
    or ``eventType`` is still useful provenance for the operator,
    while an absent or malformed value collapses to ``None`` so the
    audit payload never carries garbage.
    """
    val = raw.get(key)
    if isinstance(val, str) and val:
        return val
    return None
