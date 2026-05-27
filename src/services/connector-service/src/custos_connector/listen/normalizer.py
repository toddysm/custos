"""Event Normalizer (CONN-IMPL-025, issue #308).

Translates an opaque plugin-emitted event mapping into the canonical
:class:`NormalizedEvent` envelope the rest of the platform consumes.

The wire-form envelope is fixed by issue #308's acceptance criteria
and design § Push receiver and pull fan-out::

    {
      "schemaVersion": "1",
      "eventId": "<plugin-supplied stable id>",
      "eventType": "<connector-type catalog token>",
      "source": {
        "workspaceId":  "...",
        "instanceId":   "...",
        "deliveryMode": "pull" | "push",
        "receivedAt":   "ISO-8601 UTC"
      },
      "payload": { <opaque plugin body> }
    }

The Trigger Service's richer envelope (``kind``/``subject``/``data``)
is a downstream normalization step; for the v1 connector-service
boundary the four fields above are sufficient.

Validation contract
-------------------

Each event MUST be a JSON object with:

* ``eventId``  — non-empty string. Missing/empty → :class:`MissingEventIdError`
  with stable reason code ``"missing-event-id"`` (issue acceptance criterion).
* ``eventType`` — non-empty string. Missing/empty →
  :class:`MissingEventTypeError`.
* ``eventType`` MUST appear in the connector type's manifest
  ``spec.events.produced`` catalog passed in via the
  ``produced_event_types`` constructor argument; otherwise
  :class:`UnknownEventTypeError`.

Everything else on the plugin event lands in ``payload``. The two
known top-level meta fields (``eventId`` and ``eventType``) are
**not** duplicated into ``payload`` — they live only on the envelope
so consumers do not have to look in two places.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Final

from custos_connector.listen.errors import (
    MalformedEventError,
    MissingEventIdError,
    MissingEventTypeError,
    UnknownEventTypeError,
)

__all__ = [
    "DELIVERY_MODE_PULL",
    "DELIVERY_MODE_PUSH",
    "SCHEMA_VERSION",
    "EventNormalizer",
    "NormalizedEvent",
]

#: Canonical wire-form schema version. Bump only on a breaking
#: envelope change; additive fields land on the same version. The
#: Trigger Service uses this string to route across envelope shapes.
SCHEMA_VERSION: Final[str] = "1"

#: Discriminator for events that originated from a pull tick.
DELIVERY_MODE_PULL: Final[str] = "pull"
#: Discriminator for events that originated from a push webhook POST.
DELIVERY_MODE_PUSH: Final[str] = "push"

_VALID_DELIVERY_MODES: Final[frozenset[str]] = frozenset({DELIVERY_MODE_PULL, DELIVERY_MODE_PUSH})

#: Top-level keys that live on the envelope (not duplicated into
#: ``payload``). Keeping this as a class-level constant lets the
#: normalizer hold the contract in one place; the payload-extraction
#: loop iterates everything else verbatim so plugins are free to add
#: fields without normalizer changes.
_ENVELOPE_TOP_KEYS: Final[frozenset[str]] = frozenset({"eventId", "eventType"})


@dataclass(frozen=True, slots=True)
class NormalizedEvent:
    """The canonical envelope produced by :meth:`EventNormalizer.normalize`.

    All fields except :attr:`source` and :attr:`payload` are JSON
    primitives. :attr:`source` and :attr:`payload` are immutable
    mappings (wrapped in :class:`types.MappingProxyType`) so consumers
    cannot accidentally mutate normalized output and so the dataclass
    stays ``frozen=True``-compatible.
    """

    #: Always :data:`SCHEMA_VERSION` for now.
    schema_version: str
    #: Plugin-supplied stable identifier. Trigger Service uses this for
    #: duplicate suppression across the at-least-once delivery surface.
    event_id: str
    #: Connector-type catalog token (member of ``events.produced``).
    event_type: str
    #: Provenance: ``{workspaceId, instanceId, deliveryMode, receivedAt}``.
    source: Mapping[str, Any]
    #: All non-envelope plugin-emitted fields. Opaque to the platform.
    payload: Mapping[str, Any]

    def to_wire(self) -> dict[str, Any]:
        """Serialize to the canonical JSON wire form.

        Returns a plain ``dict[str, Any]`` (not a frozen mapping) so
        callers can pass the result directly to :func:`json.dumps`
        and to publisher SDKs that mutate before send (e.g. adding
        headers / partitioning hints).
        """
        return {
            "schemaVersion": self.schema_version,
            "eventId": self.event_id,
            "eventType": self.event_type,
            "source": dict(self.source),
            "payload": dict(self.payload),
        }


class EventNormalizer:
    """Validates and translates plugin events into :class:`NormalizedEvent`.

    Stateless apart from the immutable ``produced_event_types``
    catalog the constructor stores. Safe to share across coroutines.

    Construction
    ------------

    Pass the connector type's ``spec.events.produced`` catalog as
    ``produced_event_types`` (any iterable of strings). The normalizer
    snapshots it into a :class:`frozenset` so subsequent manifest
    reloads do not silently mutate validation behavior mid-batch.
    """

    __slots__ = ("_now", "_produced_event_types")

    def __init__(
        self,
        *,
        produced_event_types: Iterable[str],
    ) -> None:
        self._produced_event_types: frozenset[str] = frozenset(produced_event_types)
        # Indirection on ``datetime.now`` keeps tests deterministic
        # without monkey-patching the global. Production callers use
        # the default which closes over UTC.
        self._now = _default_now

    def normalize(
        self,
        raw: Any,
        *,
        workspace_id: str,
        instance_id: str,
        delivery_mode: str,
        received_at: datetime | None = None,
    ) -> NormalizedEvent:
        """Translate a single raw plugin event into a :class:`NormalizedEvent`.

        Args:
            raw: The plugin-emitted event object. Expected to be a
                :class:`Mapping` (typically the
                :func:`types.MappingProxyType`-frozen dict the
                :mod:`custos_connector.runtime` layer produces). Any
                non-mapping → :class:`MalformedEventError`.
            workspace_id: Workspace owning the instance.
            instance_id: Connector instance the event was produced by.
            delivery_mode: Either :data:`DELIVERY_MODE_PULL` or
                :data:`DELIVERY_MODE_PUSH`. Any other value →
                :class:`ValueError` (programmer error, not a plugin
                fault → not poison-pill-quarantined).
            received_at: Override for ``source.receivedAt``. Defaults
                to ``datetime.now(UTC)`` at the moment of the call.
                Pass an explicit timestamp when stamping a whole batch
                with the same receive time (the push router does
                this so every event in one POST shares a receivedAt).

        Raises:
            MissingEventIdError: ``eventId`` absent or non-string-or-empty.
            MissingEventTypeError: ``eventType`` absent or non-string-or-empty.
            UnknownEventTypeError: ``eventType`` not in
                ``produced_event_types``.
            MalformedEventError: ``raw`` is not a JSON object.
            ValueError: ``delivery_mode`` is not one of the two valid
                discriminators (caller bug, not plugin-input fault).
        """
        if delivery_mode not in _VALID_DELIVERY_MODES:
            raise ValueError(
                f"delivery_mode must be one of {sorted(_VALID_DELIVERY_MODES)!r}, "
                f"got {delivery_mode!r}"
            )
        if not isinstance(raw, Mapping):
            raise MalformedEventError(
                f"plugin event must be a JSON object, got {type(raw).__name__}"
            )

        event_id_raw = raw.get("eventId")
        if not isinstance(event_id_raw, str) or not event_id_raw:
            raise MissingEventIdError("plugin event is missing a non-empty string 'eventId'")

        event_type_raw = raw.get("eventType")
        if not isinstance(event_type_raw, str) or not event_type_raw:
            raise MissingEventTypeError("plugin event is missing a non-empty string 'eventType'")

        if event_type_raw not in self._produced_event_types:
            raise UnknownEventTypeError(
                (
                    f"eventType {event_type_raw!r} is not in the connector type's "
                    "events.produced catalog"
                ),
                event_type=event_type_raw,
            )

        ts = received_at if received_at is not None else self._now()
        # Always serialize to a UTC ISO-8601 string. The plugin / push
        # router timestamps go through this same path so downstream
        # consumers can rely on the string form being canonical
        # (offset-aware, microsecond-precision, ``+00:00`` suffix).
        source = MappingProxyType(
            {
                "workspaceId": workspace_id,
                "instanceId": instance_id,
                "deliveryMode": delivery_mode,
                "receivedAt": _format_iso8601(ts),
            }
        )

        payload = MappingProxyType({k: v for k, v in raw.items() if k not in _ENVELOPE_TOP_KEYS})

        return NormalizedEvent(
            schema_version=SCHEMA_VERSION,
            event_id=event_id_raw,
            event_type=event_type_raw,
            source=source,
            payload=payload,
        )


def _default_now() -> datetime:
    """Default ``received_at`` clock — UTC-aware ``datetime.now()``.

    Pulled out as a module-level function (not a lambda) so test
    suites that need a frozen clock can monkey-patch
    ``EventNormalizer._now`` cleanly on a per-instance basis.
    """
    return datetime.now(UTC)


def _format_iso8601(ts: datetime) -> str:
    """Canonical ISO-8601 UTC string for ``source.receivedAt``.

    Naive datetimes are interpreted as UTC (the platform contract
    everywhere is "always UTC-aware in flight, naïve = UTC at the
    boundary") so a test that passes ``datetime(2026,1,1,0,0,0)``
    still produces ``"2026-01-01T00:00:00+00:00"`` instead of
    crashing the audit pipeline downstream.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.isoformat()
