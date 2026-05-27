"""In-memory revocation registry (CONN-IMPL-020).

Holds the leaseIds the control channel has confirmed revoked. The UDS
router consults the registry on every ``refresh`` / ``release`` so a
revoked lease serves a 410 ``lease-revoked`` problem document on
subsequent requests, regardless of what Connector Service's lease store
currently says (the registry is the authoritative local view from the
moment the control endpoint acknowledged the revoke).

The registry is process-local and re-created on sidecar restart — the
design relies on Connector Service to keep the canonical revocation
state in Postgres + audit; this local copy exists only to keep the
UDS path's hot loop independent of a CS round-trip on every refresh.

Concurrency: both servers (UDS + control HTTPS) run on the same
``asyncio`` event loop, but the registry's read paths come from
arbitrary coroutines. The mutating ``mark_revoked`` is serialised with
an :class:`asyncio.Lock` so the per-lease "first writer wins" outcome
is deterministic; the read paths are lock-free dict reads (atomic in
CPython for hashable keys).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final


class RevokeMarkStatus(StrEnum):
    """Outcome of :meth:`RevocationRegistry.mark_revoked`.

    Limited to the two states the registry alone can decide:

    * :attr:`REVOKED` — the leaseId was not in the registry before
      this call and is now recorded as revoked.
    * :attr:`ALREADY_REVOKED` — the leaseId was already present; the
      stored reason is preserved (forensics expect the first reason
      to be stable).
    """

    REVOKED = "revoked"
    ALREADY_REVOKED = "already-revoked"


@dataclass(frozen=True, slots=True)
class RevocationRecord:
    """A single revocation entry stored in :class:`RevocationRegistry`."""

    lease_id: str
    reason: str
    revoked_at: datetime


class RevocationRegistry:
    """Process-local registry of revoked lease ids.

    Thread-safe under :class:`asyncio` concurrency: mutations are
    serialised by an :class:`asyncio.Lock` and reads consult a
    plain ``dict`` (single-attribute reads are atomic in CPython).
    """

    def __init__(self) -> None:
        self._lock: Final[asyncio.Lock] = asyncio.Lock()
        self._revoked: dict[str, RevocationRecord] = {}

    async def mark_revoked(
        self,
        lease_id: str,
        reason: str,
        *,
        now: datetime | None = None,
    ) -> RevokeMarkStatus:
        """Record ``lease_id`` as revoked. Idempotent.

        On first call returns :attr:`RevokeMarkStatus.REVOKED` and
        stores the ``(reason, now)`` pair; on subsequent calls returns
        :attr:`RevokeMarkStatus.ALREADY_REVOKED` and the originally
        stored reason is preserved (the second reason is dropped).
        """
        timestamp = now if now is not None else datetime.now(UTC)
        async with self._lock:
            existing = self._revoked.get(lease_id)
            if existing is not None:
                return RevokeMarkStatus.ALREADY_REVOKED
            self._revoked[lease_id] = RevocationRecord(
                lease_id=lease_id,
                reason=reason,
                revoked_at=timestamp,
            )
            return RevokeMarkStatus.REVOKED

    def is_revoked(self, lease_id: str) -> bool:
        """Return ``True`` if ``lease_id`` has been marked revoked."""
        return lease_id in self._revoked

    def reason_for(self, lease_id: str) -> str | None:
        """Return the stored reason for ``lease_id`` or ``None``."""
        record = self._revoked.get(lease_id)
        return record.reason if record is not None else None

    def record(self, lease_id: str) -> RevocationRecord | None:
        """Return the full :class:`RevocationRecord` or ``None``."""
        return self._revoked.get(lease_id)

    def __contains__(self, lease_id: object) -> bool:
        """Convenience: ``lease_id in registry``."""
        return isinstance(lease_id, str) and lease_id in self._revoked

    def __len__(self) -> int:
        """Number of revoked lease ids currently tracked."""
        return len(self._revoked)


__all__ = [
    "RevocationRecord",
    "RevocationRegistry",
    "RevokeMarkStatus",
]
