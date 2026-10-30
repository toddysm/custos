"""Immutable source-event fan-out selections retained independently of mutable subscriptions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import validate_dispatch_identifier
from custos_spl.interfaces.delivery import decode_delivery_request, delivery_instant
from custos_spl.pagination import Cursor, Page

MAX_SOURCE_TARGETS = 1000
MAX_SOURCE_SELECTION_BYTES = 8 * 1024 * 1024


class SourceSelectionLimitError(ValueError):
    """The complete event fan-out exceeds its bounded target or snapshot budget."""


@dataclass(frozen=True)
class SourceDeliverySelection:
    """First accepted targets, or a payload-free tombstone proving all targets completed."""

    workspace_id: WorkspaceId
    key: str
    request_hash: str
    expires_at: datetime
    requests: tuple[str, ...] | None

    def __post_init__(self) -> None:
        """Bound payloads and validate every immutable request before any effect is possible."""
        validate_dispatch_identifier(self.workspace_id)
        validate_dispatch_identifier(self.key)
        delivery_instant(self.expires_at)
        if re.fullmatch(r"[a-f0-9]{64}", self.request_hash) is None:
            raise ValueError("source request identity must be SHA-256")
        if self.requests is None:
            return
        if len(self.requests) > MAX_SOURCE_TARGETS:
            raise SourceSelectionLimitError("source selection exceeds target limit")
        if sum(len(raw.encode("utf-8")) for raw in self.requests) > MAX_SOURCE_SELECTION_BYTES:
            raise SourceSelectionLimitError("source selection exceeds payload limit")
        keys: set[str] = set()
        for raw in self.requests:
            data = decode_delivery_request(raw, self.expires_at)
            if data.workspace_id != self.workspace_id or data.idempotency_key in keys:
                raise ValueError("source selection has foreign or duplicate delivery identities")
            keys.add(data.idempotency_key)


def validate_source_batch(selections: tuple[SourceDeliverySelection, ...]) -> None:
    """Keep mode-specific and shared-resume selections within one atomic event budget."""
    if not 1 <= len(selections) <= 2:
        raise ValueError("source batch requires one or two selections")
    if len({item.workspace_id for item in selections}) != 1:
        raise ValueError("source batch must belong to one workspace")
    if len({item.key for item in selections}) != len(selections):
        raise ValueError("source batch keys must be distinct")
    requests = [raw for item in selections for raw in item.requests or ()]
    if len(requests) > MAX_SOURCE_TARGETS:
        raise SourceSelectionLimitError("source batch exceeds target limit")
    if sum(len(raw.encode("utf-8")) for raw in requests) > MAX_SOURCE_SELECTION_BYTES:
        raise SourceSelectionLimitError("source batch exceeds payload limit")


@runtime_checkable
class SourceDeliveryStoreProvider(Protocol):
    """Persist selection before dispatch; concurrent planners return the same first selection."""

    async def get_source_delivery(
        self, workspace_id: WorkspaceId, key: str
    ) -> SourceDeliverySelection | None:
        """Read accepted targets before consulting current subscriptions or waits."""
        ...

    async def put_source_delivery(
        self, selection: SourceDeliverySelection
    ) -> SourceDeliverySelection:
        """Install once or return equal-request existing intent; never replace selected targets."""
        ...

    async def put_source_deliveries(
        self, selections: tuple[SourceDeliverySelection, ...]
    ) -> tuple[SourceDeliverySelection, ...]:
        """Bind every routing arm atomically before either arm may produce effects."""
        ...

    async def query_pending_source_deliveries(
        self, workspace_id: WorkspaceId, *, cursor: Cursor | None = None, limit: int | None = None
    ) -> Page[SourceDeliverySelection]:
        """Page unfinished selections, including expired plans and targets not yet started."""
        ...

    async def purge_source_delivery_selections(
        self, workspace_id: WorkspaceId, *, limit: int = 100
    ) -> int:
        """Erase expired payloads only after every selected producer receipt proves completion."""
        ...
