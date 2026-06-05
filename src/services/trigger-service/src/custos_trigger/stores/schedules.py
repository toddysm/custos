"""``ScheduleStore`` — domain ↔ SPL adapter for scheduled triggers.

Wraps the SPL ``put_schedule`` write. Trigger Service has no richer schedule
wire model in v1, so the adapter accepts the schedule's primitive parts and
returns the persisted SPL ``Schedule`` row.
"""

from __future__ import annotations

from datetime import datetime

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import Schedule as SplSchedule

from custos_trigger.models import to_spl_schedule
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = ["ScheduleStore"]


class ScheduleStore:
    """Persist scheduled triggers through the SPL provider."""

    def __init__(self, store: TriggerMetadataStore) -> None:
        self._store = store

    async def put(
        self,
        *,
        workspace_id: str,
        schedule_id: str,
        workflow_id: str,
        cron: str,
        next_fire_at: datetime,
        enabled: bool = True,
    ) -> SplSchedule:
        """Persist a schedule row; returns the persisted SPL ``Schedule``."""
        return await self._store.put_schedule(
            WorkspaceId(workspace_id),
            to_spl_schedule(
                workspace_id=workspace_id,
                schedule_id=schedule_id,
                workflow_id=workflow_id,
                cron=cron,
                next_fire_at=next_fire_at,
                enabled=enabled,
            ),
        )
