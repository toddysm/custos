"""CSI/PVC-backed ArtifactStore adapter.

Content-addressed blob storage on a Kubernetes PVC mounted via CSI driver.
Layout: {workspace_id}/{digest-prefix-2}/{digest}
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from functools import partial
from pathlib import Path
from typing import ClassVar

import aiofiles
import aiofiles.os
from custos_spl.errors import ArtifactNotFound, BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor


async def _finish_mutation(operation: Callable[[], None]) -> None:
    """Keep the caller's lock until a filesystem mutation finishes, even on cancellation."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    cancelled = False
    while True:
        try:
            await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
        except OSError as exc:
            if cancelled:
                raise asyncio.CancelledError from exc
            raise
    if cancelled:
        raise asyncio.CancelledError


async def _publish_file(source: Path, destination: Path) -> None:
    """Replace a complete file while the caller owns the publication lock."""
    await _finish_mutation(partial(os.replace, source, destination))


@asynccontextmanager
async def _artifact_lock(path: Path, *, exclusive: bool) -> AsyncIterator[None]:
    """Coordinate blob/metadata operations across processes without blocking the loop.

    One stable lock inode is retained per workspace/digest-prefix directory.
    Never unlink it: replacing a lock inode can split concurrent lock owners.
    Process exit releases the OS lock, permitting recovery of interrupted writes.
    """
    await aiofiles.os.makedirs(path.parent, exist_ok=True)
    async with aiofiles.open(path.parent / ".lock", "a+b") as handle:
        descriptor = handle.fileno()
        mode = (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB
        while True:
            try:
                fcntl.flock(descriptor, mode)
                break
            except BlockingIOError:
                await asyncio.sleep(0.01)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)


class CsiArtifactAdapter:
    """Filesystem adapter for `ArtifactStoreProvider`.

    Stores artifacts on a mounted PVC, content-addressed by SHA256 digest.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    def __init__(self, pvc_mount: Path) -> None:
        """Initialize with PVC mount path.

        Args:
            pvc_mount: Absolute path to the PVC mount point.

        Raises:
            ValueError: if pvc_mount doesn't exist or isn't a directory.
        """
        self.pvc_mount = pvc_mount
        if not pvc_mount.exists():
            raise ValueError(f"PVC mount does not exist: {pvc_mount}")
        if not pvc_mount.is_dir():
            raise ValueError(f"PVC mount is not a directory: {pvc_mount}")

    def _artifact_path(self, workspace_id: WorkspaceId, digest: str) -> Path:
        """Compute storage path for an artifact.

        Path: {pvc_mount}/{workspace_id}/{digest-prefix-2}/{digest}
        """
        digest_prefix = digest[:2]
        return self.pvc_mount / str(workspace_id) / digest_prefix / digest

    async def _verify_workspace(self, workspace_id: WorkspaceId, artifact_id: ArtifactId) -> str:
        """Verify artifact belongs to workspace and return digest.

        Returns the digest embedded in artifact_id.

        Raises:
            WorkspaceMismatch: if artifact exists but belongs to different workspace.
            ArtifactNotFound: if artifact doesn't exist.
        """
        # artifact_id format: {workspace_id}:{digest}
        parts = str(artifact_id).split(":", 1)
        if len(parts) != 2:
            raise ArtifactNotFound(f"invalid artifact_id format: {artifact_id}")

        stored_workspace, digest = parts
        if stored_workspace != str(workspace_id):
            raise WorkspaceMismatch(f"artifact belongs to {stored_workspace}, not {workspace_id}")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ArtifactNotFound("invalid artifact digest")
        return digest

    async def put(
        self,
        workspace_id: WorkspaceId,
        content: AsyncIterator[bytes],
        media_type: str | None = None,
    ) -> ArtifactDescriptor:
        """Stream-write a blob with streaming SHA256 digest computation.

        Computes the digest while streaming to a unique temporary file. Persist
        optional media metadata beside the blob so head() survives process restart.
        Existing artifacts retain their original descriptor. Publication and
        deletion share a process-safe lock; retries replace orphan metadata
        left by a failed or cancelled writer whose blob was never published.
        """
        try:
            sha = hashlib.sha256()
            temp_path = self.pvc_mount / ".tmp" / uuid.uuid4().hex
            metadata_temp = temp_path.with_suffix(".json")
            await aiofiles.os.makedirs(temp_path.parent, exist_ok=True)

            try:
                async with aiofiles.open(temp_path, "xb") as f:
                    async for chunk in content:
                        sha.update(chunk)
                        await f.write(chunk)

                digest = sha.hexdigest()
                artifact_id = ArtifactId(f"{workspace_id}:{digest}")
                final_path = self._artifact_path(workspace_id, digest)
                async with _artifact_lock(final_path, exclusive=True):
                    existing = await self._head_unlocked(workspace_id, artifact_id, digest)
                    if existing is not None:
                        return existing
                    async with aiofiles.open(metadata_temp, "w", encoding="utf-8") as metadata:
                        await metadata.write(json.dumps({"media_type": media_type}))
                    await _publish_file(metadata_temp, final_path.with_suffix(".json"))
                    await _publish_file(temp_path, final_path)
                    descriptor = await self._head_unlocked(workspace_id, artifact_id, digest)
                    if descriptor is None:
                        raise BackendUnavailable(
                            f"PVC artifact disappeared after publication: {artifact_id}"
                        )
                    return descriptor
            finally:
                for temporary in (temp_path, metadata_temp):
                    with suppress(FileNotFoundError):
                        await aiofiles.os.remove(temporary)
        except OSError as exc:
            raise BackendUnavailable(f"PVC write failed: {exc}") from exc

    def get(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> AsyncIterator[bytes]:
        """Stream-read a blob.

        Returns an async generator yielding chunks.
        """
        return self._get_impl(workspace_id, artifact_id)

    async def _get_impl(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> AsyncIterator[bytes]:
        """Implementation of get as async generator."""
        try:
            digest = await self._verify_workspace(workspace_id, artifact_id)
            path = self._artifact_path(workspace_id, digest)

            async with _artifact_lock(path, exclusive=False):
                if not await aiofiles.os.path.exists(path):
                    raise ArtifactNotFound(f"artifact not found: {artifact_id}")
                async with aiofiles.open(path, "rb") as f:
                    while True:
                        chunk = await f.read(65536)  # 64KB chunks
                        if not chunk:
                            break
                        yield chunk
        except (ArtifactNotFound, WorkspaceMismatch):
            raise
        except OSError as exc:
            raise BackendUnavailable(f"PVC read failed: {exc}") from exc

    async def head(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> ArtifactDescriptor | None:
        """Lightweight existence + metadata check.

        Returns None for absent/cross-workspace blobs. Legacy blobs without a
        metadata sidecar retain a nullable media type; corrupt metadata fails.
        """
        try:
            digest = await self._verify_workspace(workspace_id, artifact_id)
            path = self._artifact_path(workspace_id, digest)
            async with _artifact_lock(path, exclusive=False):
                return await self._head_unlocked(workspace_id, artifact_id, digest)
        except (WorkspaceMismatch, ArtifactNotFound):
            return None
        except OSError as exc:
            raise BackendUnavailable(f"PVC stat failed: {exc}") from exc

    async def _head_unlocked(
        self, workspace_id: WorkspaceId, artifact_id: ArtifactId, digest: str
    ) -> ArtifactDescriptor | None:
        """Read a blob/metadata pair while the caller holds its stable prefix lock."""
        path = self._artifact_path(workspace_id, digest)
        if not await aiofiles.os.path.exists(path):
            return None
        stat = await aiofiles.os.stat(path)
        media_type = None
        try:
            async with aiofiles.open(path.with_suffix(".json"), encoding="utf-8") as metadata:
                stored = json.loads(await metadata.read())
            if not isinstance(stored, dict) or "media_type" not in stored:
                raise ValueError("invalid artifact media metadata")
            media_type = stored["media_type"]
            if media_type is not None and not isinstance(media_type, str):
                raise ValueError("artifact media_type must be a string or null")
        except FileNotFoundError:
            pass
        except (ValueError, UnicodeError) as exc:
            raise BackendUnavailable(f"PVC metadata is invalid for {artifact_id}") from exc
        return ArtifactDescriptor(
            workspace_id=workspace_id,
            artifact_id=artifact_id,
            digest=digest,
            media_type=media_type,
            size=stat.st_size,
        )

    async def delete(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
        is_sweeper: bool = False,
    ) -> None:
        """Sweeper-only delete.

        Raises ValueError if invoked from non-sweeper context.
        """
        if not is_sweeper:
            raise ValueError("delete is sweeper-only; pass is_sweeper=True")

        try:
            digest = await self._verify_workspace(workspace_id, artifact_id)
            path = self._artifact_path(workspace_id, digest)

            async with _artifact_lock(path, exclusive=True):
                with suppress(FileNotFoundError):
                    await _finish_mutation(partial(os.remove, path))
                with suppress(FileNotFoundError):
                    await _finish_mutation(partial(os.remove, path.with_suffix(".json")))
        except (WorkspaceMismatch, ArtifactNotFound):
            # No-op: already absent or doesn't belong to workspace
            pass
        except OSError as exc:
            raise BackendUnavailable(f"PVC delete failed: {exc}") from exc


def make_adapter() -> CsiArtifactAdapter:
    """Factory: create adapter from environment CUSTOS_CSI_PVC_MOUNT."""
    mount_path = os.getenv("CUSTOS_CSI_PVC_MOUNT")
    if not mount_path:
        raise RuntimeError(
            "CUSTOS_CSI_PVC_MOUNT environment variable not set; required for CSI adapter"
        )
    return CsiArtifactAdapter(Path(mount_path))
