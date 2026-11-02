"""Lifespan ownership for the existing poll scheduler and bounded restart discovery.

Shutdown stops admission, drains current work, then cancels and joins overdue
tasks. Hook cancellation retains its durable invocation fence; transports must
remain open until this runtime has finished closing.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections import deque
from collections.abc import Awaitable, Callable

from custos_spl.errors import SPLError
from custos_spl.interfaces.receiver_workspaces import ReceiverWorkspaceProvider
from custos_spl.pagination import Cursor

from custos_connector.runtime import _finish_owned_task
from custos_connector.scheduler.demand import PullDemandReconciler
from custos_connector.scheduler.pull_loop import PullLoopScheduler

logger = logging.getLogger(__name__)


class PullRuntime:
    """Own two bounded drivers, not another polling authority or an unfencing policy."""

    def __init__(
        self,
        workspaces: ReceiverWorkspaceProvider,
        scheduler: PullLoopScheduler,
        demand: PullDemandReconciler,
        *,
        interval_seconds: float = 1,
        drain_seconds: float = 330,
    ) -> None:
        """Bind existing collaborators and validate timing without launching work."""
        for value in (interval_seconds, drain_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("receiver timing must be finite and positive")
        self.workspaces, self.scheduler, self.demand = workspaces, scheduler, demand
        self.interval, self.drain_seconds = interval_seconds, drain_seconds
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._failures = {"poll": False, "discovery": False}
        self._cursor: Cursor | None = None
        self._workspaces: deque[str] = deque()
        self._close_task: asyncio.Task[None] | None = None

    @property
    def healthy(self) -> bool:
        """Readiness excludes stopping, failed or unexpectedly exited drivers."""
        return (
            bool(self._tasks)
            and not self._stop.is_set()
            and not any(self._failures.values())
            and all(not task.done() for task in self._tasks)
        )

    async def start(self) -> None:
        """Check storage before creating tasks; a runtime instance starts at most once."""
        if self._tasks or self._stop.is_set():
            raise RuntimeError("pull runtime cannot be restarted")
        async with asyncio.timeout(15):
            await self.workspaces.check_receiver_workspaces_ready()
            await self.demand.check_ready()
        self._tasks = [
            asyncio.create_task(self._drive("poll", self._poll), name="connector-poll"),
            asyncio.create_task(
                self._drive("discovery", self._discover), name="connector-pull-discovery"
            ),
        ]
        for task in self._tasks:
            task.add_done_callback(self._completed)

    def _completed(self, task: asyncio.Task[None]) -> None:
        """Observe fatal exits immediately without logging exception payloads."""
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("Receiver task %s failed: %s", task.get_name(), type(error).__name__)

    async def _poll(self) -> None:
        """Delegate all upstream effects to the already-configured scheduler."""
        await self.scheduler.tick_once()

    async def _discover(self) -> None:
        """Bound discovery and advance past a failed workspace so other tenants can recover."""
        if not self._workspaces:
            async with asyncio.timeout(15):
                page = await self.workspaces.query_receiver_workspaces(
                    cursor=self._cursor, limit=50
                )
            self._workspaces.extend(str(ws) for ws in page.items)
            self._cursor = page.next_cursor
        if self._workspaces and not self._stop.is_set():
            workspace = self._workspaces.popleft()
            async with asyncio.timeout(60):
                await self.demand.reconcile(workspace)

    async def _drive(self, name: str, operation: Callable[[], Awaitable[None]]) -> None:
        """Retry concrete provider failures after a delay; unexpected failures remain fatal."""
        while not self._stop.is_set():
            try:
                await operation()
            except (SPLError, TimeoutError) as exc:
                self._failures[name] = True
                logger.warning("Receiver %s unavailable: %s", name, type(exc).__name__)
            else:
                self._failures[name] = False
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except TimeoutError:
                continue

    async def _drain(self) -> None:
        """Wait for admitted work, then join cancellation and expose non-cancellation failures."""
        if not self._tasks:
            return
        _, pending = await asyncio.wait(self._tasks, timeout=self.drain_seconds)
        if pending:
            logger.warning("Receiver drain deadline reached; joining owned cancellation")
            for task in pending:
                task.cancel()
        outcomes = await asyncio.gather(*self._tasks, return_exceptions=True)
        if any(isinstance(result, Exception) for result in outcomes):
            raise RuntimeError("connector receiver task failed; inspect safe worker diagnostics")

    async def aclose(self) -> None:
        """Repeated caller cancellation cannot detach draining plugin/cleanup tasks."""
        self._stop.set()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._drain())
        _, interrupted = await _finish_owned_task(self._close_task, "connector-receivers")
        if interrupted:
            raise asyncio.CancelledError
