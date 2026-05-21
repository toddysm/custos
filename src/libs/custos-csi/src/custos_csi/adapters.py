"""CSI/PVC-backed ArtifactStore adapter.

Content-addressed blob storage on a Kubernetes PVC mounted via CSI driver.
Layout: {workspace_id}/{digest-prefix-2}/{digest}
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from pathlib import Path

import aiofiles
import aiofiles.os

from custos_spl.errors import ArtifactNotFound, BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor, ArtifactStoreProvider


class CsiArtifactAdapter:
    """Postgres adapter for `ArtifactStoreProvider`.

    Stores artifacts on a mounted PVC, content-addressed by SHA256 digest.
    """

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
        return digest

    async def put(
        self,
        workspace_id: WorkspaceId,
        content: AsyncIterator[bytes],
        media_type: str | None = None,
    ) -> ArtifactDescriptor:
        """Stream-write a blob with streaming SHA256 digest computation.

        Computes digest incrementally as bytes arrive. If an identical digest
        exists, returns the existing descriptor (idempotent).
        """
        try:
            sha = hashlib.sha256()
            size = 0
            temp_path = self.pvc_mount / ".tmp" / "incoming"
            await aiofiles.os.makedirs(temp_path.parent, exist_ok=True)

            # Stream content, computing digest on-the-fly
            async with aiofiles.open(temp_path, "wb") as f:
                async for chunk in content:
                    sha.update(chunk)
                    size += len(chunk)
                    await f.write(chunk)

            digest = sha.hexdigest()
            artifact_id = ArtifactId(f"{workspace_id}:{digest}")

            # Final path
            final_path = self._artifact_path(workspace_id, digest)
            await aiofiles.os.makedirs(final_path.parent, exist_ok=True)

            # Move (or skip if already exists — idempotent)
            try:
                await aiofiles.os.rename(temp_path, final_path)
            except FileExistsError:
                # Already stored with this digest — idempotent
                await aiofiles.os.remove(temp_path)

            return ArtifactDescriptor(
                workspace_id=workspace_id,
                artifact_id=artifact_id,
                digest=digest,
                media_type=media_type,
                size=size,
            )
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

        Returns None for both absent rows and rows owned by different workspace.
        """
        try:
            digest = await self._verify_workspace(workspace_id, artifact_id)
            path = self._artifact_path(workspace_id, digest)

            if not await aiofiles.os.path.exists(path):
                return None

            stat = await aiofiles.os.stat(path)
            return ArtifactDescriptor(
                workspace_id=workspace_id,
                artifact_id=artifact_id,
                digest=digest,
                media_type=None,
                size=stat.st_size,
            )
        except (WorkspaceMismatch, ArtifactNotFound):
            return None
        except OSError as exc:
            raise BackendUnavailable(f"PVC stat failed: {exc}") from exc

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

            if await aiofiles.os.path.exists(path):
                await aiofiles.os.remove(path)
        except (WorkspaceMismatch, ArtifactNotFound):
            # No-op: already absent or doesn't belong to workspace
            pass
        except OSError as exc:
            raise BackendUnavailable(f"PVC delete failed: {exc}") from exc


def make_adapter() -> CsiArtifactAdapter:
    """Factory: create adapter from environment CUSTOS_CSI_PVC_MOUNT."""
    import os

    mount_path = os.getenv("CUSTOS_CSI_PVC_MOUNT")
    if not mount_path:
        raise RuntimeError(
            "CUSTOS_CSI_PVC_MOUNT environment variable not set; required for CSI adapter"
        )
    return CsiArtifactAdapter(Path(mount_path))
