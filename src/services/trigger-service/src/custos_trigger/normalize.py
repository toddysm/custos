"""Event normalization (TS-IMPL-010).

Receivers hand the pipeline raw, source-shaped payloads; this module converts
the two source shapes that mint events *inside* the platform — a manual fire
and an internal ``custos.workflow.events`` lifecycle envelope — into the
canonical :class:`~custos_trigger.events.NormalizedEvent` the
``Classify → Match → Dedup → Dispatch`` pipeline expects (design
``§ NormalizedEvent schema``).

Connector-sourced events (registry/SCM/scan, webhooks, pull) arrive already
shaped as ``NormalizedEvent`` from the Connector Runtime and do not pass
through here.

Two guarantees the pipeline downstream relies on:

* **Canonical kinds.** Every emitted ``kind`` is validated against the locked
  taxonomy (:func:`custos_trigger.taxonomy.validate_kind`); an internal status
  with no canonical mapping surfaces as a taxonomy error rather than a
  malformed event.
* **Deterministic ``eventId``.** When the source omits an id, one is derived
  via :func:`generate_event_id` from stable identity fields, so an
  at-least-once redelivery of the same logical event yields the *same* id and
  the dedup store (keyed on ``hash(subscriptionId, eventId)``) collapses it.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any, Final

from custos_trigger.events import EventRaw, EventSource, NormalizedEvent
from custos_trigger.models import SourceType
from custos_trigger.taxonomy import validate_kind

__all__ = [
    "MANUAL_FIRE_KIND",
    "TRIGGER_EVENT_NAMESPACE",
    "WORKFLOW_EVENT_DOMAIN",
    "EventNormalizationError",
    "generate_event_id",
    "normalize_manual_fire",
    "normalize_workflow_event",
    "workflow_kind_from_status",
]

#: Stable UUIDv5 namespace for every generated ``eventId``. Fixed so the id a
#: replay derives matches the original byte-for-byte.
TRIGGER_EVENT_NAMESPACE: Final[uuid.UUID] = uuid.uuid5(
    uuid.NAMESPACE_URL, "custos.trigger/normalized-event-id"
)

#: Canonical kind a manual fire emits (design § Event Taxonomy, ``manual`` domain).
MANUAL_FIRE_KIND: Final[str] = "manual.fire"

#: Default platform domain a ``custos.workflow.events`` status maps into.
WORKFLOW_EVENT_DOMAIN: Final[str] = "workflow"

#: Maps the Workflow Service wire ``status`` vocabulary (the ``RunStatus``
#: terminal/transition values it stamps onto the envelope) onto the canonical
#: taxonomy *suffix*. ``succeeded`` → ``completed`` and the in-flight
#: ``queued``/``running`` → ``started`` are the two renames; the rest are
#: pass-through. A status absent here is left verbatim so
#: :func:`validate_kind` can reject it as a taxonomy error.
_STATUS_KIND_SUFFIX: Final[Mapping[str, str]] = {
    "queued": "started",
    "running": "started",
    "started": "started",
    "succeeded": "completed",
    "completed": "completed",
    "failed": "failed",
    "cancelled": "cancelled",
}


class EventNormalizationError(ValueError):
    """Raised when an inbound payload cannot be normalized.

    Carries a human-readable reason for the audit trail. A taxonomy violation
    surfaces as :class:`~custos_trigger.taxonomy.InvalidKindError` (also a
    :class:`ValueError`) rather than this type, so a caller catching
    :class:`ValueError` handles both.
    """


def generate_event_id(*parts: str) -> str:
    """Derive a deterministic ``eventId`` from stable identity ``parts``.

    The parts are NUL-joined (a byte that cannot appear in the id fields) and
    hashed into a UUIDv5 under :data:`TRIGGER_EVENT_NAMESPACE`, so the same
    inputs always yield the same id and distinct inputs effectively never
    collide.
    """
    return str(uuid.uuid5(TRIGGER_EVENT_NAMESPACE, "\x00".join(parts)))


def workflow_kind_from_status(status: str, *, domain: str = WORKFLOW_EVENT_DOMAIN) -> str:
    """Map an internal lifecycle ``status`` onto a canonical ``<domain>.<kind>``.

    ``domain`` selects the taxonomy family — ``workflow`` (default) or ``run``.
    The status is normalized through :data:`_STATUS_KIND_SUFFIX` and the result
    validated; a status with no canonical mapping (e.g. ``paused``) raises
    :class:`~custos_trigger.taxonomy.InvalidKindError`.
    """
    suffix = _STATUS_KIND_SUFFIX.get(status, status)
    return validate_kind(f"{domain}.{suffix}")


def normalize_manual_fire(
    *,
    occurred_at: str,
    subscription_id: str,
    inputs: Mapping[str, Any] | None = None,
    event_id: str | None = None,
) -> NormalizedEvent:
    """Normalize a manual ``:fire`` request into a ``NormalizedEvent``.

    The fire's ``inputs`` become the event ``data``; ``kind`` is the canonical
    ``manual.fire``. A manual fire is subscription-scoped
    (``POST …/triggers/{id}:fire``), so ``subscription_id`` is required and
    non-empty — it is the event subject and part of the deterministic
    ``event_id``, which keeps dedup keys from colliding across subscriptions.
    When ``event_id`` is omitted it is derived deterministically from the
    subscription, fire time, and inputs, so a redelivered fire collapses in
    dedup.
    """
    if not occurred_at:
        raise EventNormalizationError("manual fire requires a non-empty occurred_at")
    if not subscription_id:
        raise EventNormalizationError("manual fire requires a non-empty subscription_id")

    data: dict[str, Any] = dict(inputs) if inputs else {}
    resolved_event_id = event_id or generate_event_id(
        "manual",
        subscription_id,
        occurred_at,
        json.dumps(data, sort_keys=True, default=str),
    )
    return NormalizedEvent(
        event_id=resolved_event_id,
        source=EventSource(
            type=SourceType.MANUAL,
            subscription_id=subscription_id,
            occurred_at=occurred_at,
        ),
        kind=validate_kind(MANUAL_FIRE_KIND),
        subject=subscription_id,
        data=data,
        raw=EventRaw(body=json.dumps(data, sort_keys=True, default=str)),
    )


def normalize_workflow_event(
    envelope: Mapping[str, Any],
    *,
    domain: str = WORKFLOW_EVENT_DOMAIN,
    event_id: str | None = None,
) -> NormalizedEvent:
    """Normalize a ``custos.workflow.events`` envelope into a ``NormalizedEvent``.

    The envelope is the wire shape the Workflow Service publishes
    (``{ kind?, workflowVersionId, runId, workspace?, status?, outputs?, stepId?, occurredAt }``).
    ``kind`` is taken from an explicit, taxonomy-valid envelope ``kind`` when
    present; otherwise it is derived from ``status`` via
    :func:`workflow_kind_from_status`. The ``runId`` becomes the event subject,
    and ``eventId`` (omitted by the producer) is derived deterministically from
    the producer's ``(runId, kind, occurredAt)`` dedup triple — ``kind`` already
    embeds the domain — so Dapr's at-least-once redelivery collapses in the
    dedup store.

    Raises:
        EventNormalizationError: If a required field is missing, or neither
            ``kind`` nor ``status`` is present.
        InvalidKindError: If the resolved ``kind`` is not canonical.
    """
    occurred_at = _require_str(envelope, "occurredAt")
    run_id = _require_str(envelope, "runId")
    workflow_version_id = _require_str(envelope, "workflowVersionId")

    explicit_kind = envelope.get("kind")
    status = envelope.get("status")
    if explicit_kind is not None:
        kind = validate_kind(str(explicit_kind))
    elif status is not None:
        kind = workflow_kind_from_status(str(status), domain=domain)
    else:
        raise EventNormalizationError("workflow event envelope must carry 'kind' or 'status'")

    data: dict[str, Any] = {
        "workflowVersionId": workflow_version_id,
        "runId": run_id,
    }
    workspace = envelope.get("workspace")
    if workspace is not None:
        data["workspace"] = workspace
    if status is not None:
        data["status"] = status
    outputs = envelope.get("outputs")
    if outputs is not None:
        data["outputs"] = outputs
    step_id = envelope.get("stepId")
    if step_id is not None:
        data["stepId"] = step_id

    resolved_event_id = event_id or generate_event_id(run_id, kind, occurred_at)
    return NormalizedEvent(
        event_id=resolved_event_id,
        source=EventSource(
            type=SourceType.INTERNAL,
            occurred_at=occurred_at,
        ),
        kind=kind,
        subject=run_id,
        data=data,
        raw=EventRaw(body=json.dumps(dict(envelope), sort_keys=True, default=str)),
    )


def _require_str(envelope: Mapping[str, Any], key: str) -> str:
    """Return ``envelope[key]`` as a non-empty string or raise."""
    value = envelope.get(key)
    if value is None or str(value) == "":
        raise EventNormalizationError(f"workflow event envelope missing required {key!r}")
    return str(value)
