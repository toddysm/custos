"""Locked ``obs.*`` operational audit-event taxonomy + ``AuditEvent``-shaped models.

The Observability/Audit Service consumes every other component's audit trail, but
it also emits a small, stable set of *operational* audit events about itself
(design ``§ Audit``). The set is *locked*: every name is pinned on
:class:`ObsEventName` and enumerated in :data:`LOCKED_OBS_EVENT_NAMES`, and a
closed-set test guards both against accidental drift.

Each event is modelled as a frozen, typed payload (``RetentionApplied``,
``OutboxLagging``, ...) carrying its ``EVENT_NAME`` and a :meth:`ObsEvent.to_audit_event`
builder that produces a :class:`custos_spl.AuditEvent` in the exact shape every
other Custos service emits (see ``custos_auth.audit``). Later ``OBS-IMPL-*`` tasks
(the audit pipeline, retention worker, exporter loader, and alerting dispatcher)
construct these and hand the resulting :class:`~custos_spl.AuditEvent` to the SPL
outbox.

The events mirror design ``§ Audit``:

* ``obs.retention.applied`` — the retention worker ran; carries deleted-row counts.
* ``obs.outbox.lagging`` — drainer lag for a pipeline crossed its threshold.
* ``obs.exporter.config.rejected`` — a customer-supplied exporter block was invalid.
* ``obs.exporter.config.applied`` — an exporter ConfigMap was merged into the Collector.
* ``obs.alert.dispatched`` — an alert was delivered to a sink.
* ``obs.alert.failed`` — an alert failed delivery (dead-lettered after retries).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar, Final
from uuid import uuid4

from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId

#: Logical component name recorded on every ``obs.*`` event's ``subject``.
COMPONENT: Final[str] = "observability-audit-service"

#: Actor recorded on self-emitted operational events. These are minted by the
#: service itself, not by a human or workspace principal.
SYSTEM_ACTOR: Final[str] = "system:observability-audit-service"

#: Sentinel workspace id for platform-scope audit rows. Matches the convention
#: used by ``custos_auth.audit`` so the Observability audit pipeline sees one
#: consistent "control plane" bucket for component-level events.
PLATFORM_WORKSPACE_ID: Final[str] = "__platform__"


class ObsEventName(StrEnum):
    """Stable, dot-namespaced names for the service's own operational events."""

    RETENTION_APPLIED = "obs.retention.applied"
    OUTBOX_LAGGING = "obs.outbox.lagging"
    EXPORTER_CONFIG_REJECTED = "obs.exporter.config.rejected"
    EXPORTER_CONFIG_APPLIED = "obs.exporter.config.applied"
    ALERT_DISPATCHED = "obs.alert.dispatched"
    ALERT_FAILED = "obs.alert.failed"


#: The locked set of ``obs.*`` event-name strings. Adding or removing a member of
#: :class:`ObsEventName` is a deliberate, test-guarded taxonomy change.
LOCKED_OBS_EVENT_NAMES: Final[frozenset[str]] = frozenset(member.value for member in ObsEventName)


class ObsEvent(ABC):
    """Base for the service's own operational ``obs.*`` audit events.

    Subclasses are frozen dataclasses carrying the event's typed payload; each
    pins :attr:`EVENT_NAME` and implements :meth:`subject` and :meth:`payload`.
    :meth:`to_audit_event` assembles the SPL :class:`~custos_spl.AuditEvent`
    envelope shared by every Custos service.
    """

    __slots__ = ()

    #: The locked event name for this model.
    EVENT_NAME: ClassVar[ObsEventName]

    @abstractmethod
    def subject(self) -> dict[str, Any]:
        """Return the JSON-safe ``subject`` block (what the event is about)."""
        raise NotImplementedError

    @abstractmethod
    def payload(self) -> dict[str, Any]:
        """Return the JSON-safe ``payload`` block (the event's detail)."""
        raise NotImplementedError

    def to_audit_event(
        self,
        *,
        actor: str = SYSTEM_ACTOR,
        workspace_id: str = PLATFORM_WORKSPACE_ID,
        event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> AuditEvent:
        """Build the SPL :class:`~custos_spl.AuditEvent` for this operational event.

        ``event_id`` defaults to a fresh UUID and ``occurred_at`` to the current
        UTC time; both may be supplied for deterministic construction.
        """
        return AuditEvent(
            workspace_id=WorkspaceId(workspace_id),
            event_id=event_id if event_id is not None else str(uuid4()),
            event_type=self.EVENT_NAME.value,
            actor=actor,
            subject=self.subject(),
            payload=self.payload(),
            occurred_at=occurred_at if occurred_at is not None else datetime.now(UTC),
        )


@dataclass(frozen=True, slots=True)
class RetentionApplied(ObsEvent):
    """``obs.retention.applied`` — the retention worker completed a sweep."""

    EVENT_NAME: ClassVar[ObsEventName] = ObsEventName.RETENTION_APPLIED

    audit_rows_deleted: int
    outbox_rows_deleted: int

    def subject(self) -> dict[str, Any]:
        return {"component": COMPONENT}

    def payload(self) -> dict[str, Any]:
        return {
            "audit_rows_deleted": self.audit_rows_deleted,
            "outbox_rows_deleted": self.outbox_rows_deleted,
        }


@dataclass(frozen=True, slots=True)
class OutboxLagging(ObsEvent):
    """``obs.outbox.lagging`` — a drain pipeline's lag crossed its threshold."""

    EVENT_NAME: ClassVar[ObsEventName] = ObsEventName.OUTBOX_LAGGING

    pipeline_id: str
    lag_rows: int
    threshold_rows: int

    def subject(self) -> dict[str, Any]:
        return {"component": COMPONENT, "pipeline_id": self.pipeline_id}

    def payload(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "lag_rows": self.lag_rows,
            "threshold_rows": self.threshold_rows,
        }


@dataclass(frozen=True, slots=True)
class ExporterConfigRejected(ObsEvent):
    """``obs.exporter.config.rejected`` — a customer exporter block was invalid."""

    EVENT_NAME: ClassVar[ObsEventName] = ObsEventName.EXPORTER_CONFIG_REJECTED

    configmap: str
    reason: str

    def subject(self) -> dict[str, Any]:
        return {"component": COMPONENT, "configmap": self.configmap}

    def payload(self) -> dict[str, Any]:
        return {"configmap": self.configmap, "reason": self.reason}


@dataclass(frozen=True, slots=True)
class ExporterConfigApplied(ObsEvent):
    """``obs.exporter.config.applied`` — an exporter ConfigMap was merged in."""

    EVENT_NAME: ClassVar[ObsEventName] = ObsEventName.EXPORTER_CONFIG_APPLIED

    configmap: str
    exporter_names: tuple[str, ...]

    def subject(self) -> dict[str, Any]:
        return {"component": COMPONENT, "configmap": self.configmap}

    def payload(self) -> dict[str, Any]:
        return {
            "configmap": self.configmap,
            "exporter_names": list(self.exporter_names),
        }


@dataclass(frozen=True, slots=True)
class AlertDispatched(ObsEvent):
    """``obs.alert.dispatched`` — an alert was delivered to a sink."""

    EVENT_NAME: ClassVar[ObsEventName] = ObsEventName.ALERT_DISPATCHED

    rule_name: str
    sink: str
    audit_event_id: str

    def subject(self) -> dict[str, Any]:
        return {"component": COMPONENT, "rule_name": self.rule_name, "sink": self.sink}

    def payload(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "sink": self.sink,
            "audit_event_id": self.audit_event_id,
        }


@dataclass(frozen=True, slots=True)
class AlertFailed(ObsEvent):
    """``obs.alert.failed`` — an alert failed delivery and was dead-lettered."""

    EVENT_NAME: ClassVar[ObsEventName] = ObsEventName.ALERT_FAILED

    rule_name: str
    sink: str
    audit_event_id: str
    reason: str

    def subject(self) -> dict[str, Any]:
        return {"component": COMPONENT, "rule_name": self.rule_name, "sink": self.sink}

    def payload(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "sink": self.sink,
            "audit_event_id": self.audit_event_id,
            "reason": self.reason,
        }


__all__ = [
    "COMPONENT",
    "LOCKED_OBS_EVENT_NAMES",
    "PLATFORM_WORKSPACE_ID",
    "SYSTEM_ACTOR",
    "AlertDispatched",
    "AlertFailed",
    "ExporterConfigApplied",
    "ExporterConfigRejected",
    "ObsEvent",
    "ObsEventName",
    "OutboxLagging",
    "RetentionApplied",
]
