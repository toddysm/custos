"""Resource Limiter error taxonomy (ARM-IMPL-008).

The limiter runs before any sandbox is created and produces two kinds of
*permanent* failures (re-running the same attempt would fail identically):

* :class:`ResourceLimitError` — a layer tried to *loosen* (exceed) the layer
  above it (e.g. a step override asking for more memory than the platform
  default permits), or a quantity string was malformed. This is a contract
  violation in the manifest / step request.
* :class:`RuntimeUnavailableError` — the selected isolation tier has no
  ``RuntimeClass`` configured on the cluster, so the attempt cannot run at the
  required isolation level. Downgrading would silently violate the activity's
  isolation floor, so the attempt fails instead.

Both carry the ``permanent`` semantics ARM surfaces to the orchestrator; the
Scheduler translates them into the documented error envelopes
(``system.runtime_unavailable`` etc.) — the limiter itself stays envelope-free.
"""

from __future__ import annotations

from custos_arm.contract import ErrorClass

__all__ = [
    "LimitError",
    "ResourceLimitError",
    "RuntimeUnavailableError",
]


class LimitError(Exception):
    """Base class for every Resource Limiter failure."""


class ResourceLimitError(LimitError):
    """A resource layer tried to loosen the layer above it, or a value was malformed.

    Permanent: the manifest or step request is invalid and will fail
    identically on retry.
    """

    code: str = "resource.limit_violation"
    error_class: ErrorClass = ErrorClass.PERMANENT

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RuntimeUnavailableError(LimitError):
    """The selected isolation tier has no ``RuntimeClass`` configured on the cluster.

    Permanent: ARM never downgrades isolation automatically, so the attempt
    fails before any sandbox is created.
    """

    code: str = "system.runtime_unavailable"
    error_class: ErrorClass = ErrorClass.PERMANENT

    def __init__(self, tier: str, message: str | None = None) -> None:
        self.tier = tier
        super().__init__(
            message or f"isolation tier {tier!r} has no RuntimeClass configured on the cluster"
        )
