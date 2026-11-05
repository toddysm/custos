"""Bounded internal discovery of workspaces retaining receiver intent or recovery state.

This is not a user-facing workspace directory or an authorization decision.
Workers must still scope every subsequent operation to the returned workspace.
History without an active subscription remains discoverable for recovery/retention.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, runtime_checkable

from custos_spl.ids import WorkspaceId
from custos_spl.pagination import (
    Cursor,
    Page,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
)

RECEIVER_WORKSPACE_SCHEMA_REVISION = 13
RECEIVER_WORKSPACE_SCOPE = ("receiver-workspaces",)


def receiver_workspace_page(
    workspaces: Iterable[str], *, cursor: Cursor | None = None, limit: int | None = None
) -> Page[WorkspaceId]:
    """Apply production keyset/page semantics to an in-memory provider's workspace keys."""
    size = query_page_limit(limit)
    after = decode_query_cursor(cursor, RECEIVER_WORKSPACE_SCOPE)
    selected = sorted({ws for ws in workspaces if after is None or ws > after})[: size + 1]
    return Page(
        (WorkspaceId(ws) for ws in selected[:size]),
        encode_query_cursor(RECEIVER_WORKSPACE_SCOPE, selected[size - 1])
        if len(selected) > size
        else None,
    )


@runtime_checkable
class ReceiverWorkspaceProvider(Protocol):
    """Optional runtime discovery capability; queries return IDs, never retained payloads."""

    async def check_receiver_workspaces_ready(self) -> None:
        """Require the receiver/recovery schema before admitting a background worker."""
        ...

    async def query_receiver_workspaces(
        self, *, cursor: Cursor | None = None, limit: int | None = None
    ) -> Page[WorkspaceId]:
        """Return at most 200 distinct IDs in keyset order, including orphaned recovery state."""
        ...
