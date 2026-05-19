"""ArtifactStoreProvider — content-addressed blob storage.

Owns the blob bytes plus a minimal `{workspace_id, artifact_id, digest,
media_type, size}` row. User-facing metadata (`run_id`, `step_id`,
`name`) lives on `MetadataStoreProvider.append_artifact_use` — the blob
store itself never sees those.

Content-addressed: identical bytes produce identical `digest` and
identical `artifact_id`, so `put` is idempotent on the digest. Streams
are the only read shape — callers that need to load a whole blob
implement that on top, with a size cap of their choosing.

See `design/components/storage-provider-layer/design.md` § ArtifactStoreProvider.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable

from custos_spl.ids import ArtifactId, WorkspaceId


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    """Lightweight metadata for an artifact row.

    Returned by `put` and `head`. The blob bytes themselves are reached
    via `get`, which streams them. `artifact_id` is derived from the
    content digest by the adapter — two `put` calls with identical
    bytes return the same `artifact_id`.
    """

    workspace_id: WorkspaceId
    artifact_id: ArtifactId
    digest: str
    media_type: str | None
    size: int


@runtime_checkable
class ArtifactStoreProvider(Protocol):
    """Content-addressed blob storage, workspace-scoped.

    `workspace_id` is the first arg on every method. Cross-workspace
    reads are not expressible — `get`/`head` on an artifact owned by a
    different workspace raise `WorkspaceMismatch` or return `None`
    (per-method docstrings), never disclose existence.

    Failure surface:
      - `ArtifactNotFound` — `get` on an absent ID (raised, not returned).
      - `WorkspaceMismatch` — `get` on an ID owned by a different workspace.
      - `BackendUnavailable` — transient backend failure.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    async def put(
        self,
        workspace_id: WorkspaceId,
        content: AsyncIterator[bytes],
        media_type: str | None = None,
    ) -> ArtifactDescriptor:
        """Stream-write a blob.

        The adapter computes `digest` and `size` while consuming
        `content`; `artifact_id` is derived from the digest. Identical
        bytes return the existing descriptor (idempotent).
        """
        ...

    def get(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> AsyncIterator[bytes]:
        """Stream-read a blob.

        Returns an `AsyncIterator[bytes]` directly (not a coroutine) so
        adapters implement it as an async generator. Raises
        `ArtifactNotFound` if no such row exists, and `WorkspaceMismatch`
        if the row exists in a different workspace — callers MUST map
        both to HTTP 404 to avoid leaking cross-workspace existence.
        """
        ...

    async def head(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> ArtifactDescriptor | None:
        """Lightweight existence + metadata check.

        Returns `None` for both absent rows and rows owned by a
        different workspace — collapsing the two cases is the
        cross-workspace existence-leak rule applied at the API surface.
        """
        ...

    async def delete(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> None:
        """Sweeper-only delete.

        Reserved for the retention sweeper; adapters MAY refuse if
        invoked from non-sweeper contexts. SPL exposes a single entry
        point and leaves caller-side gating to the operator.
        """
        ...


__all__ = [
    "ArtifactDescriptor",
    "ArtifactStoreProvider",
]
