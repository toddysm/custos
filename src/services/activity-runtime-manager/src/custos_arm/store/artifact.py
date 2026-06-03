"""ArtifactRecord + Artifact Store Client over the SPL ``ArtifactStoreProvider``.

ARM uploads producer artifacts during two-phase output finalization and fetches
upstream artifacts when materializing a downstream activity's inputs (design
§ Artifact upload and downstream ``ArtifactRef`` materialization). The client
wraps the content-addressed :class:`ArtifactStoreProvider`, enforcing the
``ARM_ARTIFACT_MAX_BYTES`` per-artifact ceiling while streaming so an oversized
upload aborts mid-transfer rather than after the whole blob lands.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactStoreProvider
from pydantic import BaseModel, ConfigDict


class ArtifactStoreError(Exception):
    """Base class for artifact-store-client failures."""


class ArtifactTooLargeError(ArtifactStoreError):
    """Raised when an artifact exceeds the ``ARM_ARTIFACT_MAX_BYTES`` ceiling."""


class ArtifactRecord(BaseModel):
    """One uploaded artifact, keyed by the store-assigned ``id``.

    Links the content-addressed blob (``id``/``digest``/``media_type``/``size``
    from the ``ArtifactStoreProvider``) to the producing attempt and its
    manifest-declared ``name``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    name: str
    media_type: str | None
    digest: str
    size: int
    produced_by_run_id: str
    produced_by_step_id: str
    produced_by_attempt: int


class ArtifactStoreClient:
    """Upload/fetch artifacts via the SPL ``ArtifactStoreProvider``.

    ``max_bytes`` is the ``ARM_ARTIFACT_MAX_BYTES`` ceiling applied to both the
    upload stream (pre-transfer abort) and the fetch buffer.
    """

    def __init__(self, store: ArtifactStoreProvider, *, max_bytes: int) -> None:
        self._store = store
        self._max_bytes = max_bytes

    async def upload(
        self,
        *,
        workspace_id: str,
        name: str,
        content: AsyncIterator[bytes],
        produced_by_run_id: str,
        produced_by_step_id: str,
        produced_by_attempt: int,
        media_type: str | None = None,
    ) -> ArtifactRecord:
        """Stream ``content`` to the store and return its :class:`ArtifactRecord`.

        Raises :class:`ArtifactTooLargeError` — before the transfer completes —
        when the cumulative byte count exceeds ``max_bytes``.
        """
        descriptor = await self._store.put(
            WorkspaceId(workspace_id), self._capped(content), media_type
        )
        return ArtifactRecord(
            id=descriptor.artifact_id,
            name=name,
            media_type=descriptor.media_type,
            digest=descriptor.digest,
            size=descriptor.size,
            produced_by_run_id=produced_by_run_id,
            produced_by_step_id=produced_by_step_id,
            produced_by_attempt=produced_by_attempt,
        )

    async def fetch(self, workspace_id: str, artifact_id: str) -> bytes:
        """Materialize the blob bytes for ``artifact_id`` for downstream use.

        Raises :class:`ArtifactTooLargeError` when the blob exceeds ``max_bytes``.
        """
        chunks: list[bytes] = []
        total = 0
        async for chunk in self._store.get(WorkspaceId(workspace_id), ArtifactId(artifact_id)):
            total += len(chunk)
            if total > self._max_bytes:
                raise ArtifactTooLargeError(
                    f"artifact {artifact_id} exceeds {self._max_bytes} bytes"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    async def _capped(self, content: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        total = 0
        async for chunk in content:
            total += len(chunk)
            if total > self._max_bytes:
                raise ArtifactTooLargeError(f"artifact exceeds {self._max_bytes} bytes")
            yield chunk


__all__ = [
    "ArtifactRecord",
    "ArtifactStoreClient",
    "ArtifactStoreError",
    "ArtifactTooLargeError",
]
