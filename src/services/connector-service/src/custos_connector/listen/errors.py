"""Typed errors for the Listen Manager + Event Normalizer (CONN-IMPL-025).

Two failure families live here:

* :class:`EventNormalizationError` (and subclasses) — raised by
  :class:`custos_connector.listen.normalizer.EventNormalizer.normalize`
  when a single plugin event fails validation. These are
  poison-pill-quarantined by the shared publisher bridge
  (:func:`custos_connector.listen.publisher.process_batch`): the
  failing event is audited via ``event.rejected`` with the stable
  ``reason`` code carried on the exception and the batch continues
  with the next event. The cursor still advances. Operators reading
  the audit log can halt the connector via ``pull-loop:pause`` if
  the reject rate spikes (per design § 22.4
  "Push receiver and pull fan-out").

* :class:`EventPublishError` — raised by an
  :class:`custos_connector.listen.publisher.EventPublisher`
  implementation when the downstream publish (Dapr Pub/Sub today,
  internal-RPC fallback tomorrow) fails. This is **not** a
  poison-pill: the cursor must not advance past unacknowledged
  events. The exception propagates back through
  :func:`process_batch` into :meth:`CursorService.tick`, which
  surfaces it through the existing tick error envelope and leaves
  the cursor untouched (at-least-once semantics per design § Pull
  Cursor Model → "Publisher delegate" contract).
"""

from __future__ import annotations

from typing import Final

#: Stable reason code carried on :class:`MissingEventIdError`. The
#: Trigger Service alerting consumes this code verbatim, so it is part
#: of the audit-log contract; do not localise.
REASON_MISSING_EVENT_ID: Final[str] = "missing-event-id"
#: Stable reason code carried on :class:`MissingEventTypeError`.
REASON_MISSING_EVENT_TYPE: Final[str] = "missing-event-type"
#: Stable reason code carried on :class:`UnknownEventTypeError`. The
#: ``event_type`` value supplied by the plugin is not present in the
#: connector type's ``events.produced`` catalog.
REASON_UNKNOWN_EVENT_TYPE: Final[str] = "unknown-event-type"
#: Stable reason code carried on :class:`MalformedEventError`. The
#: plugin event was not a JSON object, or required scalar fields
#: were of the wrong type. The exception detail carries the
#: normalizer-side error description (never raw payload bytes).
REASON_MALFORMED: Final[str] = "malformed"


class EventNormalizationError(Exception):
    """Base class for per-event normalization failures.

    Carries the stable machine-readable :attr:`reason` code that the
    publisher bridge attaches to the ``event.rejected`` audit event.
    Subclasses fix :attr:`reason` so call-sites can ``raise
    MissingEventIdError(detail=...)`` without re-specifying the code.
    """

    #: Stable reason code (one of the ``REASON_*`` constants in this
    #: module). Subclasses override this as a class attribute.
    reason: str = REASON_MALFORMED

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class MissingEventIdError(EventNormalizationError):
    """The plugin event lacked a non-empty ``eventId`` string."""

    reason = REASON_MISSING_EVENT_ID


class MissingEventTypeError(EventNormalizationError):
    """The plugin event lacked a non-empty ``eventType`` string."""

    reason = REASON_MISSING_EVENT_TYPE


class UnknownEventTypeError(EventNormalizationError):
    """The plugin event's ``eventType`` is not in ``events.produced``.

    Carries the rejected :attr:`event_type` so the publisher bridge
    can include it in the ``event.rejected`` audit payload.
    """

    reason = REASON_UNKNOWN_EVENT_TYPE

    def __init__(self, detail: str, *, event_type: str) -> None:
        super().__init__(detail)
        self.event_type = event_type


class MalformedEventError(EventNormalizationError):
    """The plugin event was not a JSON object or had wrongly-typed fields."""

    reason = REASON_MALFORMED


class EventPublishError(Exception):
    """The downstream publisher rejected the event.

    Distinct from :class:`EventNormalizationError`: a publish failure
    is **not** poison-pill-quarantined. The exception propagates so
    the calling tick refuses to commit the cursor, preserving the
    "publisher MUST raise on partial publish" contract documented on
    :data:`custos_connector.cursor.service.EventPublisher`.
    """
