"""Protected dead-letter administration over immutable targets and audited storage transitions."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from custos_spl.errors import ImmutableViolation, LeaseBusy, SPLError
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.bulk_replay import (
    BulkReplayCommand,
    BulkReplayExpired,
    BulkReplayItem,
    BulkReplayStoreProvider,
    require_bulk_entry,
)
from custos_spl.interfaces.dead_letters import (
    DeadLetterEntry,
    DeadLetterFilter,
    validate_dispatch_identifier,
)
from custos_spl.interfaces.delivery import delivery_identity
from custos_spl.pagination import Cursor, Page

from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.pipeline.reliable import ReliableDispatcher

logger = logging.getLogger(__name__)
ADMIN_TIMEOUT_SECONDS = 60
BULK_CONCURRENCY = 8
MAX_ADMIN_PAGE_SIZE = 100


class DeadLetterAdminError(Exception):
    """A closed, safe administration error rendered by the standard problem-details handler."""

    def __init__(
        self,
        outcome: Literal[
            "not_found", "conflict", "invalid", "unavailable", "in_progress", "unsafe"
        ],
        detail: str,
    ) -> None:
        """Retain the public error category without exposing backend or plugin text."""
        self.code = (
            "trigger.replay_" + outcome
            if outcome in ("in_progress", "unsafe")
            else "trigger.deadletter." + outcome
        )
        self.detail = detail
        super().__init__(detail)


@asynccontextmanager
async def _operation() -> AsyncIterator[None]:
    """Bound each operation and classify storage failures explicitly."""
    try:
        async with asyncio.timeout(ADMIN_TIMEOUT_SECONDS):
            yield
    except LeaseBusy as exc:
        logger.info("Dead-letter replay already in progress")
        raise DeadLetterAdminError(
            "in_progress", "Another worker owns this replay. Retry after the replay lease expires."
        ) from exc
    except ImmutableViolation as exc:
        logger.info("Dead-letter mutation conflicted: %s", type(exc).__name__)
        raise DeadLetterAdminError("conflict", "Entry changed or is actively replaying.") from exc
    except (SPLError, TimeoutError) as exc:
        logger.error("Dead-letter administration unavailable: %s", type(exc).__name__)
        raise DeadLetterAdminError(
            "unavailable", "Operation could not complete safely; inspect the entry before retrying."
        ) from exc


def _validate_query(workspace: str, filters: DeadLetterFilter, limit: int) -> None:
    """Reject malformed scopes and resource limits before touching persistence."""
    try:
        filters.scope(WorkspaceId(workspace))
        if type(limit) is not int or not 1 <= limit <= MAX_ADMIN_PAGE_SIZE:
            raise ValueError("limit must be 1..100")
    except ValueError as exc:
        logger.info("Invalid dead-letter query")
        raise DeadLetterAdminError("invalid", "Invalid filter or page limit.") from exc


@dataclass(frozen=True)
class RetentionPage:
    """One bounded sweep result; callers retain the cursor for fair workspace iteration."""

    examined: int
    expired: int
    next_cursor: Cursor | None


class DeadLetterAdmin:
    """Use one durable dispatch authority for reads, replay, discard and retention."""

    def __init__(self, dispatcher: ReliableDispatcher) -> None:
        """Bind existing reliable dispatch instead of introducing a second replay implementation."""
        self.dispatcher = dispatcher
        self.store = dispatcher.store

    async def _entry(self, workspace: str, entry_id: str) -> DeadLetterEntry:
        """Require one scoped entry without revealing another workspace's data."""
        try:
            validate_dispatch_identifier(workspace)
            validate_dispatch_identifier(entry_id)
        except ValueError as exc:
            raise DeadLetterAdminError("invalid", "Invalid workspace or entry identifier.") from exc
        entry = await self.store.get_dead_letter(WorkspaceId(workspace), entry_id)
        if entry is None:
            raise DeadLetterAdminError("not_found", "Dead-letter entry not found.")
        return entry

    async def inspect(self, workspace: str, entry_id: str) -> DeadLetterEntry:
        """Read the retained snapshot, including expired entries awaiting audited cleanup."""
        async with _operation():
            return await self._entry(workspace, entry_id)

    async def list(
        self,
        workspace: str,
        filters: DeadLetterFilter,
        *,
        cursor: str | None = None,
        limit: int = 50,
    ) -> Page[DeadLetterEntry]:
        """Page by stable identifiers with every filter and workspace bound into the cursor."""
        _validate_query(workspace, filters, limit)
        async with _operation():
            try:
                return await self.store.query_dead_letters(
                    WorkspaceId(workspace),
                    filters,
                    cursor=Cursor(cursor) if cursor is not None else None,
                    limit=limit,
                )
            except ValueError as exc:
                logger.info("Invalid dead-letter pagination cursor")
                raise DeadLetterAdminError("invalid", "Invalid pagination cursor.") from exc

    async def _replay(
        self,
        workspace: str,
        entry_id: str,
        actor: str,
        bulk: BulkReplayCommand | None = None,
    ) -> Literal["pending", "discarded", "replayed"]:
        """Replay the original target, reconciling successful concurrent/repeated requests."""
        entry = await self._entry(workspace, entry_id)
        if bulk is not None:
            try:
                require_bulk_entry(bulk, entry, actor)
            except ImmutableViolation as exc:
                raise DeadLetterAdminError(
                    "unsafe", "Selected entry changed or was replaced."
                ) from exc
        if entry.status == "replayed":
            return "replayed"
        try:
            outcome = await self.dispatcher.replay(
                workspace,
                entry_id,
                actor,
                bulk_id=bulk.command_id if bulk else None,
                bulk_token=bulk.token if bulk else None,
            )
        except TriggerError as exc:
            if exc.kind == TriggerErrorKind.SUBSCRIPTION_NOT_FOUND:
                raise DeadLetterAdminError("not_found", "Dead-letter entry not found.") from exc
            if exc.kind == TriggerErrorKind.REPLAY_IN_PROGRESS:
                raise DeadLetterAdminError(
                    "in_progress",
                    "Another worker owns this replay. Retry after the replay lease expires.",
                ) from exc
            if exc.kind == TriggerErrorKind.REPLAY_UNSAFE:
                current = await self._entry(workspace, entry_id)
                if current.status == "replayed":
                    return "replayed"
                raise DeadLetterAdminError("unsafe", "Entry cannot be safely replayed.") from exc
            if exc.kind == TriggerErrorKind.DISPATCH_UNAVAILABLE:
                raise DeadLetterAdminError(
                    "unavailable", "Replay could not complete safely; inspect before retrying."
                ) from exc
            raise
        if outcome.replay_status is None:
            raise RuntimeError("replay completed without its committed attempt status")
        return outcome.replay_status

    async def replay(
        self,
        workspace: str,
        entry_id: str,
        actor: str,
    ) -> Literal["pending", "discarded", "replayed"]:
        """Bound a single replay, acknowledging success only after durable completion."""
        async with _operation():
            return await self._replay(workspace, entry_id, actor)

    async def discard(
        self, workspace: str, entry_id: str, actor: str, reason: str | None = None
    ) -> None:
        """Atomically audit discard; repeated discard is idempotent and active replay conflicts."""
        async with _operation():
            await self._entry(workspace, entry_id)
            try:
                await self.store.discard_dead_letter(
                    WorkspaceId(workspace), entry_id, actor=actor, reason=reason
                )
            except ImmutableViolation:
                await self._entry(workspace, entry_id)
                raise

    async def bulk_replay(
        self,
        workspace: str,
        filters: DeadLetterFilter,
        actor: str,
        *,
        cursor: str | None = None,
        limit: int = 50,
        request_key: str | None = None,
    ) -> tuple[int, int, Cursor | None]:
        """Recover one persisted selection/progress record, never rerun filters for a retry."""
        _validate_query(workspace, filters, limit)
        key = request_key if request_key is not None else uuid4().hex
        try:
            validate_dispatch_identifier(key)
        except ValueError as exc:
            raise DeadLetterAdminError("invalid", "Invalid bulk idempotency key.") from exc
        store = self.store
        if not isinstance(store, BulkReplayStoreProvider):
            raise DeadLetterAdminError(
                "unavailable", "Durable bulk command storage is unavailable."
            )
        async with _operation():
            try:
                command = await store.begin_bulk_replay(
                    WorkspaceId(workspace),
                    delivery_identity(workspace, actor, key),
                    actor,
                    filters,
                    cursor=Cursor(cursor) if cursor is not None else None,
                    limit=limit,
                    retention_seconds=self.dispatcher.retention,
                )
            except BulkReplayExpired as exc:
                raise DeadLetterAdminError(
                    "unsafe", "Bulk command has expired; use a new idempotency key."
                ) from exc
            except ValueError as exc:
                raise DeadLetterAdminError("invalid", "Invalid bulk selection or cursor.") from exc
            assert command.items is not None
            semaphore = asyncio.Semaphore(BULK_CONCURRENCY)

            async def replay_one(item: BulkReplayItem) -> None:
                """Complete selected unfinished work; replay finish commits progress atomically."""
                if item.accepted is not None:
                    return
                async with semaphore:
                    try:
                        accepted = (
                            await self._replay(workspace, item.entry_id, actor, command)
                            == "replayed"
                        )
                    except DeadLetterAdminError as exc:
                        if exc.code not in (
                            "trigger.deadletter.conflict",
                            "trigger.deadletter.not_found",
                            "trigger.replay_in_progress",
                            "trigger.replay_unsafe",
                        ):
                            raise
                        logger.info("Bulk replay rejected entry: %s", exc.code)
                        accepted = False
                    await store.record_bulk_replay_result(command, item.entry_id, accepted)

            tasks = [asyncio.create_task(replay_one(item)) for item in command.items]
            try:
                await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            completed = await store.get_bulk_replay(command.workspace_id, command.command_id)
            if completed is None or not completed.completed or completed.items is None:
                raise RuntimeError("bulk command finished without durable per-entry results")
            accepted = sum(item.accepted is True for item in completed.items)
            return (
                accepted,
                len(completed.items) - accepted,
                Cursor(completed.next_cursor) if completed.next_cursor is not None else None,
            )

    async def sweep(
        self, workspace: str, *, cursor: str | None = None, limit: int = 100
    ) -> RetentionPage:
        """Sweep every status in one page, auditing expiry atomically under the entry lock."""
        async with _operation():
            page = await self.list(workspace, DeadLetterFilter(), cursor=cursor, limit=limit)
            expired = 0
            for entry in page.items:
                expired += await self.store.delete_expired_dead_letter(
                    WorkspaceId(workspace), entry.data.entry_id
                )
            if isinstance(self.store, BulkReplayStoreProvider):
                await self.store.purge_bulk_replay_selections(WorkspaceId(workspace), limit=limit)
            return RetentionPage(len(page.items), expired, page.next_cursor)
