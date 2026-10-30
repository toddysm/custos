"""Reconcile persisted Trigger/native demand into the existing single Connector polling owner."""

from __future__ import annotations

import asyncio

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstanceFilter,
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.pull_control import ConnectorPullControlProvider, pull_interval

from custos_connector.scheduler.pull_loop import PullLoopScheduler


class PullDemandReconciler:
    """Rebuild registrations on restart or intent change without clearing operator pause."""

    def __init__(
        self,
        *,
        controls: ConnectorPullControlProvider,
        instances: ConnectorInstanceStoreProvider,
        scheduler: PullLoopScheduler,
        default_interval_seconds: int = 60,
    ) -> None:
        """Require one shared authority and validate interval defaults before accepting work."""
        if scheduler.pull_control_provider is not controls:
            raise ValueError("demand reconciliation must share the scheduler's pull authority")
        self._controls, self._instances, self._scheduler = controls, instances, scheduler
        self._default_interval = pull_interval(default_interval_seconds)
        self._lock = asyncio.Lock()

    async def reconcile(self, workspace_id: str) -> int:
        """Apply current intent, preserve native listeners, and remove absent local demand."""
        async with self._lock:
            workspace = WorkspaceId(workspace_id)
            before = self._scheduler.registration_snapshot(workspace_id)
            await self._controls.check_pull_control_ready()
            await self._controls.reconcile_pull_demand(
                workspace,
                default_interval_seconds=self._default_interval,
                minimum_interval_seconds=self._scheduler.minimum_interval_seconds,
            )
            eligible: set[str] = set()
            instance_cursor = None
            while True:
                instances = await self._instances.list_connector_instances(
                    workspace,
                    filter=ConnectorInstanceFilter(enabled=True),
                    cursor=instance_cursor,
                    limit=100,
                )
                eligible.update(
                    str(instance.instance_id)
                    for instance in instances.items
                    if instance.status == "active"
                )
                instance_cursor = instances.next_cursor
                if instance_cursor is None:
                    break
            seen: set[str] = set()
            cursor = None
            while True:
                page = await self._controls.query_pull_controls(workspace, cursor=cursor, limit=100)
                for control in page.items:
                    if str(control.instance_id) not in eligible:
                        continue
                    seen.add(str(control.instance_id))
                    self._scheduler.apply_pull_control(control, clear_halt=True)
                cursor = page.next_cursor
                if cursor is None:
                    break
            for identifier, generation in before.items():
                if identifier not in seen:
                    self._scheduler.unregister(
                        workspace_id, identifier, expected_generation=generation
                    )
            return len(seen)
