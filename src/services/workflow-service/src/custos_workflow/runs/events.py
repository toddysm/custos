"""Workflow lifecycle event publication (WF-IMPL-041).

Implements design.md § Dapr Pub/Sub Publications:

* :class:`DaprPubSubLifecyclePublisher` posts each
  :class:`LifecycleEvent` to the Dapr sidecar's
  ``/v1.0/publish/{pubsub}/{topic}`` HTTP endpoint, with the
  canonical envelope produced by :meth:`LifecycleEvent.to_wire`.
  Mirrors the connector-service ``DaprPubSubEventPublisher``
  shape so operators see one HTTP-publisher pattern across the
  platform.
* :class:`DedupingLifecyclePublisher` wraps any
  :class:`LifecycleEventPublisher` and absorbs producer-side
  duplicates on the documented ``(run_id, kind, occurred_at)``
  triple — Dapr Workflow replay can re-execute the
  ``await ctx.call_activity(...)`` path that triggers
  ``publish``, so the dedup gate is the layer that converts
  at-least-once execution semantics into the at-least-once
  *publication* semantics promised in design.md (subscribers
  still rely on their own dedup; the producer-side gate keeps
  the topic from flooding under replay storms).

The Protocol surface (:class:`LifecycleEventPublisher`), the
in-memory test stub (:class:`InMemoryLifecycleEventPublisher`),
and the envelope dataclass (:class:`LifecycleEvent`) continue
to live in :mod:`custos_workflow.runs.controller` so the
controller's emit sites do not need to ``import events`` and
so existing test modules keep importing them from the same
location.

Module-level Dapr SDK imports are intentionally avoided —
the publisher uses :mod:`httpx` against the Dapr Pub/Sub HTTP
API directly. Unit tests can therefore exercise the publisher
under :class:`httpx.MockTransport` without a Dapr install.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

import httpx

from custos_workflow.runs.controller import (
    LifecycleEvent,
    LifecycleEventPublisher,
)

if TYPE_CHECKING:
    pass

__all__ = [
    "DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS",
    "DEFAULT_DEDUP_CACHE_SIZE",
    "DaprPubSubLifecyclePublisher",
    "DedupingLifecyclePublisher",
    "LifecycleEventPublishError",
]


#: Default HTTP request timeout (seconds) the production publisher
#: applies to each Dapr ``/v1.0/publish`` POST. Mirrors the
#: connector-service publisher default so operators see one
#: tunable across the platform.
DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS: Final[float] = 5.0

#: Default maximum number of ``(run_id, kind, occurred_at)`` keys
#: :class:`DedupingLifecyclePublisher` will remember. The cache
#: evicts the oldest entry once the bound is hit; sized so a
#: replay storm of one run cannot push out unrelated runs'
#: dedup state.
DEFAULT_DEDUP_CACHE_SIZE: Final[int] = 10_000


class LifecycleEventPublishError(RuntimeError):
    """Raised by :class:`DaprPubSubLifecyclePublisher` on a publish
    failure (transport error or non-2xx response).

    Subclass of :class:`RuntimeError` so callers that already
    catch the broader category (Run Controller's
    ``except Exception`` around the publish call) re-raise
    without translation. Carries a human-readable message
    including the offending ``(pubsub, topic)`` pair so
    operators can correlate the failure with the sidecar
    configuration.
    """


# ---------------------------------------------------------------------------
# Producer-side dedup
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DedupingLifecyclePublisher:
    """Wraps any :class:`LifecycleEventPublisher` with
    ``(run_id, kind, occurred_at)`` dedup.

    Dapr Workflow's at-least-once activity execution means that
    the same ``await ctx.call_activity(...)`` boundary can fire
    its observable side-effect twice (or more) on replay. The
    Run Controller's lifecycle emits sit on top of that
    machinery, so a naive forward to the topic would publish
    one logical event N times per replay episode. This wrapper
    fingerprints each event on the design-documented dedup
    triple, drops duplicates silently, and forwards
    first-seen events to :attr:`inner`.

    The cache is a bounded LRU keyed on the triple. When the
    cache is at :attr:`max_seen_keys`, the oldest entry is
    evicted to make room for a new key — sized so a single run
    in a replay storm cannot evict unrelated runs' dedup
    state.
    """

    inner: LifecycleEventPublisher
    max_seen_keys: int = DEFAULT_DEDUP_CACHE_SIZE
    _seen: OrderedDict[tuple[str, str, str], None] = field(
        default_factory=OrderedDict, init=False, repr=False
    )

    async def publish(self, event: LifecycleEvent) -> None:
        """Forward *event* exactly once per
        ``(run_id, kind, occurred_at)`` triple.

        On a duplicate this returns immediately without calling
        :attr:`inner`. On a first-seen key the event is
        forwarded; the key is recorded only AFTER the inner
        publish succeeds, so a transient publish failure
        leaves the cache in a state where a retry will still
        forward the event."""
        key = (str(event.run_id), event.kind, event.occurred_at.isoformat())
        if key in self._seen:
            # Touch — keep this key as recently-seen.
            self._seen.move_to_end(key)
            return
        await self.inner.publish(event)
        self._seen[key] = None
        # Evict the oldest entry once we've exceeded the bound.
        if len(self._seen) > self.max_seen_keys:
            self._seen.popitem(last=False)


# ---------------------------------------------------------------------------
# Dapr Pub/Sub HTTP adapter
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DaprPubSubLifecyclePublisher:
    """Production :class:`LifecycleEventPublisher` for
    ``custos.workflow.events``.

    Posts each :class:`LifecycleEvent` as
    ``Content-Type: application/json`` to the Dapr sidecar's
    ``/v1.0/publish/{pubsub}/{topic}`` endpoint with the
    canonical envelope from :meth:`LifecycleEvent.to_wire`.
    The Trigger Service Internal Event Receiver (the only
    consumer in v1) binds its subscription against the same
    ``(pubsub_name, topic)`` pair per the design's REQ-080
    internal-triggering contract.

    Lifecycle
    ---------

    The publisher does **not** own the :class:`httpx.AsyncClient`
    — it receives an already-constructed instance. The
    FastAPI lifespan hook is responsible for building and
    aclose-ing the client.

    Failure semantics
    -----------------

    Any non-2xx HTTP response or transport error surfaces as
    :class:`LifecycleEventPublishError`. The Run Controller's
    ``await self._lifecycle_publisher.publish(...)`` site
    re-raises through the existing
    :class:`WorkflowRuntimeUnavailableError` translation gate,
    so callers see the same error surface they already
    handle for other Dapr-side failures.
    """

    http_client: httpx.AsyncClient
    dapr_endpoint: str
    pubsub_name: str
    topic: str
    request_timeout_seconds: float = DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS
    _publish_url: str = field(init=False, repr=False, default="")

    def __post_init__(self) -> None:
        endpoint = self.dapr_endpoint.rstrip("/")
        if not endpoint:
            raise ValueError(
                "DaprPubSubLifecyclePublisher requires a non-empty dapr_endpoint; "
                "to disable Dapr publishing wire the in-memory publisher instead"
            )
        if not self.pubsub_name:
            raise ValueError("DaprPubSubLifecyclePublisher requires a non-empty pubsub_name")
        if not self.topic:
            raise ValueError("DaprPubSubLifecyclePublisher requires a non-empty topic")
        self._publish_url = f"{endpoint}/v1.0/publish/{self.pubsub_name}/{self.topic}"

    async def publish(self, event: LifecycleEvent) -> None:
        """Publish exactly one lifecycle event through the Dapr sidecar."""
        wire = event.to_wire()
        try:
            response = await self.http_client.post(
                self._publish_url,
                json=wire,
                timeout=self.request_timeout_seconds,
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise LifecycleEventPublishError(
                f"Dapr publish failed (transport): {exc!r} "
                f"(pubsub={self.pubsub_name!r}, topic={self.topic!r})"
            ) from exc
        if response.status_code // 100 != 2:
            body_preview = response.text[:200] if response.text else ""
            raise LifecycleEventPublishError(
                f"Dapr publish failed: status={response.status_code} "
                f"body={body_preview!r} "
                f"(pubsub={self.pubsub_name!r}, topic={self.topic!r})"
            )
