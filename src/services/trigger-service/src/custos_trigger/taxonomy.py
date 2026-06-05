"""Canonical platform event taxonomy (resolves design TODO-001 / INCON-013).

``kind`` strings are **dot-namespaced** ``<domain>.<event>`` values that
selectors match on and that the Normalizer stamps onto every
:class:`NormalizedEvent`. This module is the authoritative source for the
unified ``kind`` namespace shared by the Trigger Service, the Activity Runtime
Manager (ARM TODO-009), and Observability/Audit (INCON-013) — those services
consume the same strings rather than re-inventing them.

Two tiers exist (design ``§ Event Taxonomy``):

* **Platform-owned domains** form a *closed* registry. Their kind lists are
  enumerated in :data:`PLATFORM_DOMAINS` and validated for **exact membership**
  via :func:`is_canonical_kind`. Adding a platform kind is a deliberate,
  enum-grid-guarded registry edit.
* **Connector-authored (vendor) domains** let plugin authors emit kinds under a
  vendor-reserved domain declared in the connector manifest (e.g. ``ghcr.*``,
  ``github.*``, ``acr.*``). These are validated for **shape only**, not
  membership; a vendor domain MUST NOT collide with a platform-owned domain.

See change record
``design/components/trigger-service/changes/2026-06-04-007-event-taxonomy.md``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

__all__ = [
    "CANONICAL_EVENT_KINDS",
    "KIND_PATTERN",
    "PLATFORM_DOMAINS",
    "InvalidKindError",
    "is_canonical_kind",
    "is_platform_domain",
    "kind_domain",
    "validate_kind",
]

#: Shape rule for every ``kind`` string: lowercase, at least one dot, the first
#: segment is the **domain**. Domain segments are ``[a-z][a-z0-9]*``; event
#: segments additionally allow underscores (e.g. ``step.retry_scheduled``).
KIND_PATTERN: Final[str] = r"^[a-z][a-z0-9]*(\.[a-z][a-z0-9_]*)+$"

_KIND_RE: Final[re.Pattern[str]] = re.compile(KIND_PATTERN)


def _domains() -> Mapping[str, frozenset[str]]:
    """Build the closed platform-owned domain → canonical-kind registry.

    Every entry is enumerated verbatim from design ``§ Event Taxonomy``; the
    enum-grid test in ``test_taxonomy.py`` guards this table against drift.
    """
    raw: dict[str, tuple[str, ...]] = {
        "manual": ("manual.fire",),
        "cron": ("cron.tick",),
        "webhook": ("webhook.received",),
        "workflow": (
            "workflow.started",
            "workflow.completed",
            "workflow.failed",
            "workflow.cancelled",
        ),
        "run": (
            "run.started",
            "run.completed",
            "run.failed",
            "run.cancelled",
        ),
        "step": (
            "step.started",
            "step.succeeded",
            "step.failed",
            "step.retry_scheduled",
            "step.waiting",
            "step.resumed",
            "step.timed_out",
        ),
        "activity": (
            "activity.scheduled",
            "activity.started",
            "activity.succeeded",
            "activity.failed",
            "activity.timed_out",
            "activity.cancelled",
        ),
        "registry": (
            "registry.push",
            "registry.tag",
            "registry.delete",
        ),
        "pr": (
            "pr.opened",
            "pr.merged",
            "pr.closed",
            "pr.review_requested",
            "pr.synchronized",
        ),
        "scan": (
            "scan.started",
            "scan.completed",
            "scan.failed",
            "scan.vulnerable",
        ),
    }
    return MappingProxyType({domain: frozenset(kinds) for domain, kinds in raw.items()})


#: The closed registry of platform-owned domains mapped to their canonical kind
#: sets. Read-only — every value is a ``frozenset`` behind a ``MappingProxyType``.
PLATFORM_DOMAINS: Final[Mapping[str, frozenset[str]]] = _domains()

#: The flat set of every canonical platform ``kind`` string across all domains.
CANONICAL_EVENT_KINDS: Final[frozenset[str]] = frozenset(
    kind for kinds in PLATFORM_DOMAINS.values() for kind in kinds
)


class InvalidKindError(ValueError):
    """Raised by :func:`validate_kind` when a ``kind`` is malformed or illegal.

    Subclasses :class:`ValueError` so callers may catch either. Carries the
    offending ``kind`` for diagnostics.
    """

    def __init__(self, kind: str, reason: str) -> None:
        self.kind = kind
        self.reason = reason
        super().__init__(f"invalid event kind {kind!r}: {reason}")


def kind_domain(kind: str) -> str:
    """Return the domain (first dot-separated segment) of ``kind``.

    Does not validate shape — call :func:`validate_kind` for that. A ``kind``
    with no dot yields the whole string.
    """
    return kind.split(".", 1)[0]


def is_platform_domain(domain: str) -> bool:
    """Return ``True`` if ``domain`` is a closed platform-owned domain."""
    return domain in PLATFORM_DOMAINS


def is_canonical_kind(kind: str) -> bool:
    """Return ``True`` if ``kind`` is an enumerated platform-owned kind.

    Vendor (connector-authored) kinds are *not* canonical even when
    well-formed — they are validated for shape only by :func:`validate_kind`.
    """
    return kind in CANONICAL_EVENT_KINDS


def validate_kind(kind: str) -> str:
    """Validate a ``kind`` string and return it unchanged on success.

    Enforces, in order:

    1. **Shape** — ``kind`` must match :data:`KIND_PATTERN`.
    2. **Platform-collision guard** — if the domain is platform-owned, ``kind``
       must be an exact canonical member of that domain. A vendor cannot mint
       new kinds under (or collide with) a platform-owned domain.
    3. **Vendor shape** — any other (vendor) domain is accepted on shape alone.

    Args:
        kind: The dot-namespaced event kind to validate.

    Returns:
        The validated ``kind`` (unchanged), so the call can be inlined.

    Raises:
        InvalidKindError: If ``kind`` is malformed or violates the
            platform-collision guard.
    """
    if not _KIND_RE.match(kind):
        raise InvalidKindError(kind, f"does not match {KIND_PATTERN}")

    domain = kind_domain(kind)
    if is_platform_domain(domain) and kind not in PLATFORM_DOMAINS[domain]:
        raise InvalidKindError(
            kind,
            f"domain {domain!r} is platform-owned; {kind!r} is not a canonical kind for it",
        )
    return kind
