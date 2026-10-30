"""Immutable source-event fan-out selections retained independently of mutable subscriptions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import validate_dispatch_identifier
from custos_spl.interfaces.delivery import decode_delivery_request, delivery_instant

MAX_SOURCE_TARGETS = 1000
MAX_SOURCE_SELECTION_BYTES = 8 * 1024 * 1024


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
            raise ValueError("source selection exceeds target limit")
        if sum(len(raw.encode("utf-8")) for raw in self.requests) > MAX_SOURCE_SELECTION_BYTES:
            raise ValueError("source selection exceeds payload limit")
        keys: set[str] = set()
        for raw in self.requests:
            data = decode_delivery_request(raw, self.expires_at)
            if data.workspace_id != self.workspace_id or data.idempotency_key in keys:
                raise ValueError("source selection has foreign or duplicate delivery identities")
            keys.add(data.idempotency_key)


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

    async def purge_source_delivery_selections(
        self, workspace_id: WorkspaceId, *, limit: int = 100
    ) -> int:
        """Erase expired payloads only after every selected producer receipt proves completion."""
        ...
