"""``ResumeSubscriptionStore`` — domain ↔ SPL adapter for resume tokens.

Wraps the SPL ``put_resume_subscription`` / ``delete_resume_subscription``
writes, mapping the :class:`ResumeRegistration` domain model onto the SPL
``ResumeSubscription`` row (the ``eventKey`` + optional CEL ``selector`` ride
in the row's free-form ``payload`` — see :mod:`custos_trigger.models`).
"""

from __future__ import annotations

from datetime import datetime
from typing import NamedTuple

from custos_spl.ids import WorkspaceId

from custos_trigger.models import (
    ResumeRegistration,
    resume_registration_from_spl,
    to_spl_resume_subscription,
)
from custos_trigger.stores.base import ResumeReadable, TriggerMetadataStore

__all__ = [
    "ResumeReadUnsupportedError",
    "ResumeSubscriptionStore",
    "StoredResumeRegistration",
]


class ResumeReadUnsupportedError(RuntimeError):
    """Raised when the bound backend exposes no resume read surface.

    The locked SPL write Protocol has no resume read method; only a backend
    that also satisfies
    :class:`~custos_trigger.stores.base.ResumeReadable` can serve
    :meth:`ResumeSubscriptionStore.get`. The in-process backend does; the
    Postgres adapter gains the capability in a later task.
    """


class StoredResumeRegistration(NamedTuple):
    """A resume registration read back from the store, with its TTL anchor.

    ``expires_at`` lets the ``RegisterResumeSubscription`` RPC tell a still-live
    registration (idempotent replay) apart from one that has lapsed past its
    TTL (treated as a fresh registration).
    """

    registration: ResumeRegistration
    expires_at: datetime


class ResumeSubscriptionStore:
    """Register + cancel step-resume waits through the SPL provider."""

    def __init__(self, store: TriggerMetadataStore) -> None:
        self._store = store

    async def get(self, workspace_id: str, resume_id: str) -> StoredResumeRegistration | None:
        """Read one resume token back by id, or ``None`` when absent.

        Raises :class:`ResumeReadUnsupportedError` when the bound backend has
        no :class:`~custos_trigger.stores.base.ResumeReadable` surface.
        """
        store = self._store
        if not isinstance(store, ResumeReadable):
            raise ResumeReadUnsupportedError(
                "the bound metadata store exposes no resume read surface"
            )
        row = store.resume_subscription(workspace_id, resume_id)
        if row is None:
            return None
        return StoredResumeRegistration(
            registration=resume_registration_from_spl(row),
            expires_at=row.expires_at,
        )

    async def register(
        self,
        registration: ResumeRegistration,
        *,
        workspace_id: str,
        resume_id: str,
        expires_at: datetime,
    ) -> ResumeRegistration:
        """Persist a pending resume token; returns the round-tripped record."""
        row = await self._store.put_resume_subscription(
            WorkspaceId(workspace_id),
            to_spl_resume_subscription(
                registration,
                workspace_id=workspace_id,
                resume_id=resume_id,
                expires_at=expires_at,
            ),
        )
        return resume_registration_from_spl(row)

    async def cancel(self, workspace_id: str, resume_id: str) -> None:
        """Delete a resume token. Idempotent — deleting an absent id is a no-op."""
        await self._store.delete_resume_subscription(
            WorkspaceId(workspace_id),
            resume_id,
        )
