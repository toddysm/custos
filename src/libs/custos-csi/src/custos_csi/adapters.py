"""CSI/PVC-backed ArtifactStore adapter.

Content-addressed blob storage on a Kubernetes PVC mounted via CSI driver.
Layout: {workspace_id}/{digest-prefix-2}/{digest}
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path
from typing import ClassVar

import aiofiles
import aiofiles.os
from custos_spl.errors import ArtifactNotFound, BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor


async def _publish_file(source: Path, destination: Path) -> None:
    """Atomically publish a complete file without replacing another writer's result."""
    with suppress(FileExistsError):
        await asyncio.to_thread(os.link, source, destination)


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
        Existing artifacts retain their original descriptor.
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
                existing = await self.head(workspace_id, artifact_id)
                if existing is not None:
                    return existing
                final_path = self._artifact_path(workspace_id, digest)
                await aiofiles.os.makedirs(final_path.parent, exist_ok=True)
                async with aiofiles.open(metadata_temp, "w", encoding="utf-8") as metadata:
                    await metadata.write(json.dumps({"media_type": media_type}))
                await _publish_file(metadata_temp, final_path.with_suffix(".json"))
                await _publish_file(temp_path, final_path)
                descriptor = await self.head(workspace_id, artifact_id)
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
            return ArtifactDescriptor(
                workspace_id=workspace_id,
                artifact_id=artifact_id,
                digest=digest,
                media_type=media_type,
                size=stat.st_size,
            )
        except (WorkspaceMismatch, ArtifactNotFound):
            return None
        except OSError as exc:
            raise BackendUnavailable(f"PVC stat failed: {exc}") from exc
        except (ValueError, UnicodeError) as exc:
            raise BackendUnavailable(f"PVC metadata is invalid for {artifact_id}") from exc

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
            with suppress(FileNotFoundError):
                await aiofiles.os.remove(path.with_suffix(".json"))
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
