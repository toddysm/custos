"""Error taxonomy for the Lease Manager.

A small, stable set of typed reason codes that the Lease Manager
surfaces to its callers (the sidecar HTTP layer in Phase H, today's
direct-call unit tests). Each code maps onto an HTTP shape the
sidecar will use in CONN-IMPL-019; surfacing them as enum members
here keeps the taxonomy in one place and lets the type checker prove
exhaustiveness.

* :attr:`LeaseErrorCode.CAPACITY_EXCEEDED` \u2014 the
  ``(workspace_id, run_id, step_id, attempt)`` tuple already has the
  cap (default 16) of non-released, non-expired leases. The caller
  must release one before retrying.
* :attr:`LeaseErrorCode.NOT_FOUND` \u2014 ``refresh`` or ``release``
  called with a ``lease_id`` that has no row in the SPL store for
  this workspace (or, equivalently, an id from a different workspace).
* :attr:`LeaseErrorCode.ALREADY_RELEASED` \u2014 ``refresh`` called on
  a lease whose ``released_at`` is set. Refresh-after-release is
  refused so the caller cannot resurrect a returned token.
* :attr:`LeaseErrorCode.INVALID_REQUEST` \u2014 a generic catch-all
  for arg-level validation failures (e.g. non-positive
  ``requested_ttl_sec``). The Lease Manager keeps the catalogue
  small so any new case must be added here deliberately.
"""

from __future__ import annotations

from enum import StrEnum


class LeaseErrorCode(StrEnum):
    """Stable reason codes returned from the Lease Manager."""

    CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED"
    NOT_FOUND = "NOT_FOUND"
    ALREADY_RELEASED = "ALREADY_RELEASED"
    INVALID_REQUEST = "INVALID_REQUEST"


class LeaseError(Exception):
    """A typed Lease Manager failure.

    Carries the :class:`LeaseErrorCode` plus a free-form human-readable
    detail. Callers should switch on :attr:`code` for control flow and
    surface :attr:`detail` as the user-facing message.
    """

    def __init__(self, code: LeaseErrorCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail


__all__ = ["LeaseError", "LeaseErrorCode"]
