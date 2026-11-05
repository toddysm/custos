"""Reconcile persisted Trigger/native demand into the existing single Connector polling owner."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstanceFilter,
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.pull_control import ConnectorPullControlProvider, pull_interval
from custos_spl.pagination import Cursor

from custos_connector.scheduler.pull_loop import PullLoopScheduler


@dataclass
class _RegistrationScan:
    """Advisory rebuild position; deletion candidates carry original registration generations."""

    remaining: dict[str, int]
    cursor: Cursor | None = None
    seen: int = 0


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
        self._scans: dict[str, _RegistrationScan] = {}

    async def check_ready(self) -> None:
        """Verify durable control storage before starting the lifespan's poll driver."""
        await self._controls.check_pull_control_ready()

    async def reconcile(self, workspace_id: str) -> int:
        """Resume a paged advisory rebuild after deadlines; only complete scans remove old entries.

        Persisted demand is refreshed on each call. Partial registration never
        authorizes an effect: CursorService still checks current instance and
        control state. Process restart rebuilds this advisory registry afresh.
        """
        async with self._lock:
            workspace = WorkspaceId(workspace_id)
            await self._controls.check_pull_control_ready()
            await self._controls.reconcile_pull_demand(
                workspace,
                default_interval_seconds=self._default_interval,
                minimum_interval_seconds=self._scheduler.minimum_interval_seconds,
            )
            progress = self._scans.get(workspace_id)
            if progress is None:
                progress = _RegistrationScan(self._scheduler.registration_snapshot(workspace_id))
                self._scans[workspace_id] = progress
            while True:
                instances = await self._instances.list_connector_instances(
                    workspace,
                    filter=ConnectorInstanceFilter(enabled=True),
                    cursor=progress.cursor,
                    limit=100,
                )
                if len(instances.items) > 100 or any(
                    instance.workspace_id != workspace for instance in instances.items
                ):
                    raise ValueError("instance page violated scope or size bound")
                identifiers = [
                    instance.instance_id
                    for instance in instances.items
                    if instance.status == "active"
                ]
                controls = (
                    await self._controls.read_pull_controls(workspace, identifiers)
                    if identifiers
                    else ()
                )
                returned = {control.instance_id for control in controls}
                if (
                    len(returned) != len(controls)
                    or not returned.issubset(identifiers)
                    or any(control.workspace_id != workspace for control in controls)
                ):
                    raise ValueError("pull-control batch violated scope or key identity")
                for control in controls:
                    self._scheduler.apply_pull_control(control, clear_halt=True)
                    progress.remaining.pop(str(control.instance_id), None)
                progress.seen += len(controls)
                progress.cursor = instances.next_cursor
                if progress.cursor is None:
                    break
            for identifier, generation in progress.remaining.items():
                self._scheduler.unregister(workspace_id, identifier, expected_generation=generation)
            del self._scans[workspace_id]
            return progress.seen
