"""Durable bulk-replay selection, ownership and per-entry completion contracts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable
from uuid import uuid4

from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import (
    DeadLetterEntry,
    DeadLetterFilter,
    json_value,
    validate_dispatch_identifier,
)
from custos_spl.interfaces.delivery import delivery_fingerprint, delivery_instant
from custos_spl.pagination import Cursor, Page


class BulkReplayExpired(ImmutableViolation):
    """A retained command identity cannot be rebound after its selection horizon ends."""


@dataclass(frozen=True)
class BulkReplayItem:
    """One selected immutable entry generation, never a mutable filter reevaluation."""

    entry_id: str
    identity: str
    accepted: bool | None = None

    def __post_init__(self) -> None:
        """Validate stored identity and outcomes instead of interpreting corrupt results."""
        validate_dispatch_identifier(self.entry_id)
        if re.fullmatch(r"[a-f0-9]{64}", self.identity) is None:
            raise ValueError("bulk entry identity must be SHA-256")
        if self.accepted is not None and type(self.accepted) is not bool:
            raise ValueError("bulk entry result must be boolean or pending")


@dataclass(frozen=True)
class BulkReplayCommand:
    """Frozen selection plus independently committed progress and a bounded coordinator lease."""

    workspace_id: WorkspaceId
    command_id: str
    request_hash: str
    actor: str
    expires_at: datetime
    items: tuple[BulkReplayItem, ...] | None
    next_cursor: str | None
    token: str | None
    lease_until: datetime | None

    def __post_init__(self) -> None:
        """Keep scope, bounded selection and ownership unambiguous at provider boundaries."""
        for value in (self.workspace_id, self.command_id, self.actor):
            validate_dispatch_identifier(value)
        if re.fullmatch(r"[a-f0-9]{64}", self.request_hash) is None:
            raise ValueError("bulk request identity must be SHA-256")
        delivery_instant(self.expires_at)
        if (self.token is None) != (self.lease_until is None):
            raise ValueError("bulk ownership token and deadline must be paired")
        if self.lease_until is not None:
            delivery_instant(self.lease_until)
        if self.items is not None and (
            len(self.items) > 100 or len({item.entry_id for item in self.items}) != len(self.items)
        ):
            raise ValueError("bulk selection requires at most 100 unique entries")

    @property
    def completed(self) -> bool:
        """Only explicit per-entry results, including an empty selection, mean completion."""
        return self.items is not None and all(item.accepted is not None for item in self.items)


def bulk_entry_identity(entry: DeadLetterEntry) -> str:
    """Bind immutable input and creation time so an erased/recreated ID cannot replace selection."""
    return delivery_fingerprint(
        {
            "entryId": entry.data.entry_id,
            "createdAt": entry.created_at.isoformat(),
            "key": entry.data.idempotency_key,
            "event": json_value(entry.data.event),
            "target": json_value(entry.data.target),
        }
    )


def bulk_request_hash(
    workspace_id: WorkspaceId,
    filters: DeadLetterFilter,
    cursor: Cursor | None,
    limit: int,
) -> str:
    """Validate and bind every selection dimension before entering the provider transaction."""
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("bulk replay limit must be 1..100")
    return delivery_fingerprint(
        {
            "scope": filters.scope(workspace_id),
            "cursor": cursor.token if cursor else None,
            "limit": limit,
        }
    )


def reserve_bulk_replay(
    current: BulkReplayCommand | None,
    workspace_id: WorkspaceId,
    command_id: str,
    actor: str,
    request_hash: str,
    now: datetime,
    retention_seconds: int,
    selection: Page[DeadLetterEntry] | None = None,
) -> BulkReplayCommand:
    """Recover the same selection, never silently replace an expired or divergent command."""
    for value in (workspace_id, command_id, actor):
        validate_dispatch_identifier(value)
    if type(retention_seconds) is not int or not 1 <= retention_seconds <= 31_536_000:
        raise ValueError("bulk command retention must be 1..31536000 seconds")
    now = delivery_instant(now)
    if current is None:
        if selection is None:
            raise ValueError("new bulk command requires a selection")
        current = BulkReplayCommand(
            workspace_id,
            command_id,
            request_hash,
            actor,
            now + timedelta(seconds=retention_seconds),
            tuple(
                BulkReplayItem(entry.data.entry_id, bulk_entry_identity(entry))
                for entry in selection.items
            ),
            selection.next_cursor.token if selection.next_cursor else None,
            None,
            None,
        )
    if (current.workspace_id, current.command_id, current.actor, current.request_hash) != (
        workspace_id,
        command_id,
        actor,
        request_hash,
    ):
        raise ImmutableViolation("bulk command key is bound to another request")
    if current.items is None or current.expires_at <= now:
        raise BulkReplayExpired("bulk command expired; use a new idempotency key")
    if current.completed:
        return current
    if current.lease_until is not None and current.lease_until > now:
        raise LeaseBusy("bulk command is already in progress")
    return replace(
        current,
        token=uuid4().hex,
        lease_until=min(current.expires_at, now + timedelta(seconds=300)),
    )


def require_bulk_owner(command: BulkReplayCommand | None, token: str | None, now: datetime) -> None:
    """Fence stale coordinators before they begin new per-entry effects or record rejections."""
    if (
        command is None
        or token is None
        or command.token != token
        or command.items is None
        or command.lease_until is None
        or command.lease_until <= delivery_instant(now)
    ):
        raise LeaseExpired("bulk command ownership expired or changed")


def require_bulk_entry(command: BulkReplayCommand, entry: DeadLetterEntry, actor: str) -> None:
    """Validate exact selection and original actor while the entry is locked for claiming."""
    if (
        command.items is None
        or command.actor != actor
        or command.workspace_id != entry.data.workspace_id
    ):
        raise ImmutableViolation("bulk command scope or actor differs")
    selected = next((item for item in command.items if item.entry_id == entry.data.entry_id), None)
    if (
        selected is None
        or selected.identity != bulk_entry_identity(entry)
        or selected.accepted is not None
    ):
        raise ImmutableViolation("bulk entry changed or already has a command result")


def finish_bulk_item(
    command: BulkReplayCommand, entry_id: str, accepted: bool
) -> BulkReplayCommand:
    """Retain the first terminal result; a late retry cannot overwrite committed progress."""
    if type(accepted) is not bool:
        raise ValueError("bulk result must be boolean")
    if command.items is None:
        raise ImmutableViolation("bulk selection was erased")
    if entry_id not in {item.entry_id for item in command.items}:
        raise ImmutableViolation("entry does not belong to this bulk command")
    items = tuple(
        replace(item, accepted=accepted)
        if item.entry_id == entry_id and item.accepted is None
        else item
        for item in command.items
    )
    completed = all(item.accepted is not None for item in items)
    return replace(
        command,
        items=items,
        token=None if completed else command.token,
        lease_until=None if completed else command.lease_until,
    )


def encode_bulk_selection(command: BulkReplayCommand) -> str | None:
    """Serialize bounded IDs, fingerprints and outcomes, excluding original event payloads."""
    if command.items is None:
        return None
    return json.dumps(
        {
            "items": [
                {"entryId": item.entry_id, "identity": item.identity, "accepted": item.accepted}
                for item in command.items
            ],
            "nextCursor": command.next_cursor,
        },
        allow_nan=False,
    )


@runtime_checkable
class BulkReplayStoreProvider(Protocol):
    """Optional durable administration capability; per-entry finish updates progress atomically."""

    async def begin_bulk_replay(
        self,
        workspace_id: WorkspaceId,
        command_id: str,
        actor: str,
        filters: DeadLetterFilter,
        *,
        cursor: Cursor | None = None,
        limit: int = 50,
        retention_seconds: int = 2_592_000,
    ) -> BulkReplayCommand:
        """Persist the first selection or recover its original progress under a fresh lease."""
        ...

    async def get_bulk_replay(
        self, workspace_id: WorkspaceId, command_id: str
    ) -> BulkReplayCommand | None:
        """Read a scoped immutable command snapshot."""
        ...

    async def record_bulk_replay_result(
        self,
        claim: BulkReplayCommand,
        entry_id: str,
        accepted: bool,
    ) -> None:
        """Fence and record results not already committed by replay finish."""
        ...

    async def purge_bulk_replay_selections(
        self, workspace_id: WorkspaceId, *, limit: int = 100
    ) -> int:
        """Erase expired selections without live claims, retaining identity tombstones."""
        ...
