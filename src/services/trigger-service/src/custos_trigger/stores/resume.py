"""``ResumeSubscriptionStore`` — domain ↔ SPL adapter for resume tokens.

Wraps the SPL ``put_resume_subscription`` / ``delete_resume_subscription``
writes, mapping the :class:`ResumeRegistration` domain model onto the SPL
``ResumeSubscription`` row (the ``eventKey`` + optional CEL ``selector`` ride
in the row's free-form ``payload`` — see :mod:`custos_trigger.models`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, NamedTuple

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import ResumeSubscription
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider
from custos_spl.pagination import Cursor, Page

from custos_trigger.models import (
    ResumeRegistration,
    resume_registration_from_spl,
    to_spl_resume_subscription,
)
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = [
    "ResumeReadUnsupportedError",
    "ResumeSubscriptionStore",
    "StoredResumeRegistration",
]


class ResumeReadUnsupportedError(RuntimeError):
    """Raised when the bound backend exposes no resume read surface.

    The backend must implement the asynchronous ``TriggerQueryProvider``
    capability supplied by the in-memory and PostgreSQL providers.
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
        no asynchronous ``TriggerQueryProvider`` capability.
        """
        store = self._store
        if not isinstance(store, TriggerQueryProvider):
            raise ResumeReadUnsupportedError(
                "the bound metadata store exposes no resume read surface"
            )
        row = await store.get_resume_subscription(WorkspaceId(workspace_id), resume_id)
        if row is None:
            return None
        return StoredResumeRegistration(
            registration=resume_registration_from_spl(row),
            expires_at=row.expires_at,
        )

    async def query_for_source(
        self,
        workspace_id: str,
        *,
        connector_instance_id: str,
        delivery_mode: Literal["push", "pull"],
        as_of: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ResumeSubscription]:
        """Read source-scoped waits, retaining IDs and payload for external correlation."""
        store = self._store
        if not isinstance(store, TriggerQueryProvider):
            raise ResumeReadUnsupportedError(
                "the bound metadata store exposes no asynchronous resume query surface"
            )
        return await store.query_resume_subscriptions(
            WorkspaceId(workspace_id),
            connector_instance_id=connector_instance_id,
            delivery_mode=delivery_mode,
            as_of=as_of,
            cursor=cursor,
            limit=limit,
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
