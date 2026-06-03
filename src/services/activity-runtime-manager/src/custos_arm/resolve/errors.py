"""Activity resolution errors (ARM-IMPL-007).

The Activity Resolver turns a fully-qualified ``activityRef`` into a pinned
:class:`~custos_arm.resolve.models.ActivityTypeVersion` by reading the Catalog
Service. Two failure shapes matter to the Scheduler:

* an **unknown** ref is a *permanent* failure — retrying will never succeed,
  so it maps to the ``activity.unresolved`` error envelope; and
* a Catalog that is *unreachable* or returns an unexpected status is a
  *transient* failure — the attempt may be retried.

These are surfaced as typed exceptions rather than :class:`ErrorEnvelope`
instances so the Scheduler owns the single point where domain errors are
translated into the orchestrator-facing envelope.
"""

from __future__ import annotations

from custos_arm.contract import ErrorClass

__all__ = [
    "ActivityUnresolvedError",
    "CatalogUnavailableError",
    "ResolveError",
]


class ResolveError(Exception):
    """Base class for every Activity Resolver failure."""


class ActivityUnresolvedError(ResolveError):
    """The ``activityRef`` does not resolve to a published activity type.

    Permanent: the Catalog has no row for the ref (HTTP 404) or the ref is
    syntactically invalid. Carries the canonical ``activity.unresolved``
    error code and the :class:`~custos_arm.contract.ErrorClass.PERMANENT`
    class so the Scheduler can synthesize the result envelope verbatim.
    """

    #: Canonical error code for the synthesized envelope.
    code: str = "activity.unresolved"

    #: Orchestrator-facing failure class — never retried.
    error_class: ErrorClass = ErrorClass.PERMANENT

    def __init__(self, activity_ref: str, message: str | None = None) -> None:
        self.activity_ref = activity_ref
        super().__init__(message or f"activity {activity_ref!r} does not resolve")


class CatalogUnavailableError(ResolveError):
    """The Catalog could not be reached or returned an unexpected status.

    Transient: a network error, a timeout, or any non-404 non-2xx response.
    The attempt may be retried — this is distinct from
    :class:`ActivityUnresolvedError`, which is permanent.
    """

    def __init__(self, activity_ref: str, message: str) -> None:
        self.activity_ref = activity_ref
        super().__init__(message)
