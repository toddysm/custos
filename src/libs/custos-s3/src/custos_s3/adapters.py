"""S3-compatible ArtifactStore adapter.

Content-addressed blob storage on S3-compatible object storage (AWS S3, MinIO, etc).
Layout: {bucket}/{workspace_id}/{digest-prefix-2}/{digest}
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator

import aioboto3

from custos_spl.errors import ArtifactNotFound, BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor


class S3ArtifactAdapter:
    """S3-compatible adapter for `ArtifactStoreProvider`.

    Stores artifacts in S3, content-addressed by SHA256 digest.
    """

    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """Initialize with S3 bucket configuration.

        Args:
            bucket: S3 bucket name.
            region: AWS region (optional, uses default if omitted).
            endpoint_url: Custom S3 endpoint for MinIO or local stacks (optional).

        Raises:
            ValueError: if bucket is empty.
        """
        if not bucket:
            raise ValueError("bucket cannot be empty")
        self.bucket = bucket
        self.region = region
        self.endpoint_url = endpoint_url
        self.session = aioboto3.Session()

    def _artifact_key(self, workspace_id: WorkspaceId, digest: str) -> str:
        """Compute S3 object key for an artifact.

        Key: {workspace_id}/{digest-prefix-2}/{digest}
        """
        digest_prefix = digest[:2]
        return f"{workspace_id}/{digest_prefix}/{digest}"

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
        exists, this is idempotent (overwrites with identical content).
        """
        try:
            sha = hashlib.sha256()
            size = 0
            chunks: list[bytes] = []

            # Stream content, computing digest on-the-fly
            async for chunk in content:
                sha.update(chunk)
                size += len(chunk)
                chunks.append(chunk)

            digest = sha.hexdigest()
            artifact_id = ArtifactId(f"{workspace_id}:{digest}")

            # Upload to S3
            key = self._artifact_key(workspace_id, digest)
            body = b"".join(chunks)

            async with self.session.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
            ) as s3:
                await s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType=media_type or "application/octet-stream",
                )

            return ArtifactDescriptor(
                workspace_id=workspace_id,
                artifact_id=artifact_id,
                digest=digest,
                media_type=media_type,
                size=size,
            )
        except ArtifactNotFound:
            raise
        except Exception as exc:
            raise BackendUnavailable(f"S3 write failed: {exc}") from exc

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
            key = self._artifact_key(workspace_id, digest)

            async with self.session.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
            ) as s3:
                try:
                    response = await s3.get_object(Bucket=self.bucket, Key=key)
                    async for chunk in response["Body"].iter_chunks(65536):  # 64KB chunks
                        yield chunk
                except s3.exceptions.NoSuchKey as exc:
                    raise ArtifactNotFound(f"artifact not found: {artifact_id}") from exc
        except (ArtifactNotFound, WorkspaceMismatch):
            raise
        except Exception as exc:
            raise BackendUnavailable(f"S3 read failed: {exc}") from exc

    async def head(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
    ) -> ArtifactDescriptor | None:
        """Lightweight existence + metadata check.

        Returns None for both absent objects and objects owned by different workspace.
        """
        try:
            digest = await self._verify_workspace(workspace_id, artifact_id)
            key = self._artifact_key(workspace_id, digest)

            async with self.session.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
            ) as s3:
                try:
                    response = await s3.head_object(Bucket=self.bucket, Key=key)
                    return ArtifactDescriptor(
                        workspace_id=workspace_id,
                        artifact_id=artifact_id,
                        digest=digest,
                        media_type=response.get("ContentType"),
                        size=response.get("ContentLength", 0),
                    )
                except s3.exceptions.NoSuchKey:
                    return None
        except (WorkspaceMismatch, ArtifactNotFound):
            return None
        except Exception as exc:
            raise BackendUnavailable(f"S3 head failed: {exc}") from exc

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
            key = self._artifact_key(workspace_id, digest)

            async with self.session.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
            ) as s3:
                try:
                    await s3.delete_object(Bucket=self.bucket, Key=key)
                except s3.exceptions.NoSuchKey:
                    # No-op: already absent
                    pass
        except (WorkspaceMismatch, ArtifactNotFound):
            # No-op: already absent or doesn't belong to workspace
            pass
        except Exception as exc:
            raise BackendUnavailable(f"S3 delete failed: {exc}") from exc


def make_adapter() -> S3ArtifactAdapter:
    """Factory: create adapter from environment variables.

    Required:
        CUSTOS_S3_BUCKET: S3 bucket name

    Optional:
        CUSTOS_S3_REGION: AWS region (uses default if omitted)
        CUSTOS_S3_ENDPOINT: Custom endpoint for MinIO or local stacks
    """
    bucket = os.getenv("CUSTOS_S3_BUCKET")
    if not bucket:
        raise RuntimeError(
            "CUSTOS_S3_BUCKET environment variable not set; required for S3 adapter"
        )
    region = os.getenv("CUSTOS_S3_REGION")
    endpoint = os.getenv("CUSTOS_S3_ENDPOINT")
    return S3ArtifactAdapter(bucket, region=region, endpoint_url=endpoint)
