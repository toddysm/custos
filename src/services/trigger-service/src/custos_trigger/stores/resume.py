"""``ResumeSubscriptionStore`` — domain ↔ SPL adapter for resume tokens.

Wraps the SPL ``put_resume_subscription`` / ``delete_resume_subscription``
writes, mapping the :class:`ResumeRegistration` domain model onto the SPL
``ResumeSubscription`` row (the ``eventKey`` + optional CEL ``selector`` ride
in the row's free-form ``payload`` — see :mod:`custos_trigger.models`).
"""

from __future__ import annotations

from datetime import datetime

from custos_spl.ids import WorkspaceId

from custos_trigger.models import (
    ResumeRegistration,
    resume_registration_from_spl,
    to_spl_resume_subscription,
)
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = ["ResumeSubscriptionStore"]


class ResumeSubscriptionStore:
    """Register + cancel step-resume waits through the SPL provider."""

    def __init__(self, store: TriggerMetadataStore) -> None:
        self._store = store

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
