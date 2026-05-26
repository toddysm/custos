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
from typing import Final


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


#: Map each :class:`LeaseErrorCode` to the HTTP status the internal
#: lease RPC router (CONN-IMPL-019) surfaces. The same map is consumed
#: by :class:`~custos_connector.lease.service.LeaseManager` so the
#: ``lease.denied`` audit event carries the status the wire-level
#: caller will see, and by the Phase H sidecar's ``LeaseGateway`` so a
#: non-2xx response from Connector Service can be decoded back into a
#: :class:`LeaseError` with the same code semantics direct-call test
#: clients observe.
_STATUS_BY_CODE: Final[dict[LeaseErrorCode, int]] = {
    LeaseErrorCode.CAPACITY_EXCEEDED: 429,
    LeaseErrorCode.NOT_FOUND: 404,
    LeaseErrorCode.ALREADY_RELEASED: 410,
    LeaseErrorCode.INVALID_REQUEST: 400,
}


def http_status_for(code: LeaseErrorCode) -> int:
    """Return the HTTP status code the internal lease RPC emits for ``code``."""
    return _STATUS_BY_CODE[code]


__all__ = ["LeaseError", "LeaseErrorCode", "http_status_for"]
