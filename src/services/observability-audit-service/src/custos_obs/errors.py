"""Locked Observability/Audit Service error taxonomy (RFC 7807 Problem Details).

A small, stable set of named failure modes the service surfaces to callers and
operators. The set is *locked*: every name is pinned on :class:`ObsErrorKind`
and enumerated in :data:`LOCKED_OBS_ERROR_KINDS`, and a closed-set test guards
both against accidental drift. Later ``OBS-IMPL-*`` tasks raise the matching
:class:`ObsError` subclass; the Query API surfaces :meth:`ObsError.to_dict` as an
``application/problem+json`` body (:data:`PROBLEM_CONTENT_TYPE`).

The kinds mirror design ``§ Failure Modes``:

* ``LogQueryUnavailable`` (503) — ``LogQueryProvider=noop`` or the log backend is
  unreachable. The UI shows the configured ``CUSTOS_LOGS_EXTERNAL_URL`` pointer.
* ``MetricsQueryUnavailable`` (503) — ``MetricsQueryProvider=noop`` or the metrics
  backend is unreachable. Same external-URL pattern.
* ``AuditQueryUnavailable`` (503) — the metadata store backing the audit
  read-back (``MetadataStoreProvider.query_audit``) is unreachable or declines
  the query. Surfaced by the audit search + single-event lookup routes.
* ``AuditDrainLagging`` (500) — the outbox drainer fell behind and the outbox row
  count crossed a configurable threshold. Does not block writes; emits an audit
  alert and a Prometheus metric.
* ``AlertSinkUnavailable`` (502) — a webhook/SMTP target was unreachable after
  retries. The event lands in the dead-letter table and surfaces via audit alert.
* ``ExporterConfigInvalid`` (422) — a customer-supplied exporter block failed
  Collector validation. The merge is rejected, last-good config is kept, and an
  audit alert is emitted.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar, Final

#: Media type for the RFC 7807 Problem Details body returned by
#: :meth:`ObsError.to_dict`. The Query API sets this as the ``Content-Type`` of
#: error responses.
PROBLEM_CONTENT_TYPE: Final[str] = "application/problem+json"

#: URN namespace for the ``type`` member of a Problem Details body. URNs are
#: stable identifiers and need not resolve; the human-facing description lives in
#: ``title`` and ``detail``.
PROBLEM_TYPE_PREFIX: Final[str] = "urn:custos:obs:problem:"


class ObsErrorKind(StrEnum):
    """Stable names for Observability/Audit Service failure modes."""

    LOG_QUERY_UNAVAILABLE = "LogQueryUnavailable"
    METRICS_QUERY_UNAVAILABLE = "MetricsQueryUnavailable"
    AUDIT_QUERY_UNAVAILABLE = "AuditQueryUnavailable"
    AUDIT_DRAIN_LAGGING = "AuditDrainLagging"
    ALERT_SINK_UNAVAILABLE = "AlertSinkUnavailable"
    EXPORTER_CONFIG_INVALID = "ExporterConfigInvalid"


#: The locked set of error-kind strings. Adding or removing a member of
#: :class:`ObsErrorKind` is a deliberate, test-guarded taxonomy change.
LOCKED_OBS_ERROR_KINDS: Final[frozenset[str]] = frozenset(member.value for member in ObsErrorKind)

#: RFC 7807 members that callers MUST NOT shadow via ``extensions``; an attempt
#: to do so raises :class:`ValueError` at construction time.
_RESERVED_MEMBERS: Final[frozenset[str]] = frozenset(
    {"type", "title", "status", "detail", "instance"}
)


class ObsError(Exception):
    """A typed Observability/Audit Service failure carrying a Problem Details body.

    Each concrete subclass pins a locked :class:`ObsErrorKind`, a human-readable
    ``title``, and an HTTP ``status``. Instances carry a free-form ``detail``
    string, an optional ``instance`` URI reference, and optional JSON-safe
    ``extensions`` (RFC 7807 extension members, e.g. an external-system URL).
    Callers switch on :attr:`kind` for control flow and serialize
    :meth:`to_dict` as the ``application/problem+json`` body.
    """

    kind: ClassVar[ObsErrorKind]
    title: ClassVar[str]
    status: ClassVar[int]

    def __init__(
        self,
        detail: str,
        *,
        instance: str | None = None,
        extensions: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{self.kind.value}: {detail}")
        self.detail = detail
        self.instance = instance
        extensions = dict(extensions) if extensions else {}
        clashes = _RESERVED_MEMBERS & extensions.keys()
        if clashes:
            raise ValueError(
                f"extensions may not shadow reserved Problem Details members: {sorted(clashes)}"
            )
        self.extensions: dict[str, Any] = extensions

    @property
    def type_uri(self) -> str:
        """The RFC 7807 ``type`` URN for this error kind."""
        return f"{PROBLEM_TYPE_PREFIX}{self.kind.value}"

    def to_dict(self) -> dict[str, Any]:
        """Return the RFC 7807 Problem Details body as a JSON-safe mapping.

        Always includes ``type``, ``title``, ``status``, and ``detail``;
        ``instance`` and any extension members are included only when present so
        the common case stays compact.
        """
        problem: dict[str, Any] = {
            "type": self.type_uri,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
        }
        if self.instance is not None:
            problem["instance"] = self.instance
        problem.update(self.extensions)
        return problem


class LogQueryUnavailable(ObsError):
    """The log read-back backend is unavailable (provider ``noop`` or unreachable)."""

    kind = ObsErrorKind.LOG_QUERY_UNAVAILABLE
    title = "Log query backend unavailable"
    status = 503


class MetricsQueryUnavailable(ObsError):
    """The metrics read-back backend is unavailable (provider ``noop`` or unreachable)."""

    kind = ObsErrorKind.METRICS_QUERY_UNAVAILABLE
    title = "Metrics query backend unavailable"
    status = 503


class AuditQueryUnavailable(ObsError):
    """The audit metadata store is unreachable or declines the read-back query."""

    kind = ObsErrorKind.AUDIT_QUERY_UNAVAILABLE
    title = "Audit query backend unavailable"
    status = 503


class AuditDrainLagging(ObsError):
    """The audit outbox drainer fell behind a configurable lag threshold."""

    kind = ObsErrorKind.AUDIT_DRAIN_LAGGING
    title = "Audit outbox drainer lagging"
    status = 500


class AlertSinkUnavailable(ObsError):
    """A webhook/SMTP alert sink was unreachable after the configured retries."""

    kind = ObsErrorKind.ALERT_SINK_UNAVAILABLE
    title = "Alert sink unavailable"
    status = 502


class ExporterConfigInvalid(ObsError):
    """A customer-supplied exporter block failed Collector validation."""

    kind = ObsErrorKind.EXPORTER_CONFIG_INVALID
    title = "Exporter configuration invalid"
    status = 422


__all__ = [
    "LOCKED_OBS_ERROR_KINDS",
    "PROBLEM_CONTENT_TYPE",
    "PROBLEM_TYPE_PREFIX",
    "AlertSinkUnavailable",
    "AuditDrainLagging",
    "AuditQueryUnavailable",
    "ExporterConfigInvalid",
    "LogQueryUnavailable",
    "MetricsQueryUnavailable",
    "ObsError",
    "ObsErrorKind",
]
