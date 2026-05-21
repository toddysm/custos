"""Conformance tests for ArtifactStoreProvider adapters.

Tests that any ArtifactStore implementation must pass:
- Workspace scoping enforcement
- Content-addressability (digest-based keying)
- Idempotency (identical content → same digest)
- Sweeper-only deletion
- Streaming semantics (put/get)
- Error classification
"""

from __future__ import annotations

import pytest

from custos_spl.errors import BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactStoreProvider

from .base import AdapterConformanceBase


class ArtifactStoreConformanceTests(AdapterConformanceBase):
    """Base conformance tests for ArtifactStoreProvider adapters.

    Subclasses MUST provide these pytest fixtures:
    - `adapter` → ArtifactStoreProvider instance, configured and ready
    - `workspace_id` → WorkspaceId for testing
    - `other_workspace_id` → different WorkspaceId for cross-workspace tests
    - `sample_content` → async iterator of bytes for put() testing

    Tests will skip if required fixtures are not provided.

    Example:
        class TestMyS3Adapter(ArtifactStoreConformanceTests):
            @pytest.fixture
            def adapter(self):
                return MyS3Adapter(bucket="conformance-test")

            @pytest.fixture
            def workspace_id(self):
                return WorkspaceId("ws-test-primary")
    """

    @pytest.fixture
    def adapter(self) -> ArtifactStoreProvider:
        """Adapter fixture (must be overridden by subclass)."""
        pytest.skip("adapter fixture not provided by subclass")

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Primary workspace ID fixture (must be overridden by subclass)."""
        pytest.skip("workspace_id fixture not provided by subclass")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Secondary workspace ID for cross-workspace tests (must be overridden by subclass)."""
        pytest.skip("other_workspace_id fixture not provided by subclass")

    @pytest.fixture
    async def sample_content(self) -> bytes:
        """Sample content for testing (must be overridden by subclass)."""
        pytest.skip("sample_content fixture not provided by subclass")

    @pytest.mark.asyncio
    async def test_sweeper_only_deletion_requires_flag(
        self,
        adapter: ArtifactStoreProvider,
        workspace_id: WorkspaceId,
        sample_content: bytes,
    ) -> None:
        """delete() requires is_sweeper=True flag.

        Prevents accidental deletion; only sweeper process can garbage-collect.
        Calling delete() without is_sweeper=True MUST raise ValueError.
        """
        # Create a sample artifact
        async def content_iter():
            yield sample_content

        descriptor = await adapter.put(workspace_id, content_iter())

        # Attempt delete without flag should raise ValueError
        with pytest.raises(ValueError, match="sweeper"):
            await adapter.delete(workspace_id, descriptor.artifact_id, is_sweeper=False)

        # Delete with flag should succeed
        await adapter.delete(workspace_id, descriptor.artifact_id, is_sweeper=True)

    @pytest.mark.asyncio
    async def test_deletion_idempotency_no_error_if_missing(
        self,
        adapter: ArtifactStoreProvider,
        workspace_id: WorkspaceId,
    ) -> None:
        """delete() succeeds even if artifact already absent.

        No error raised for missing artifact (safe for retry-able sweeper).
        """
        fake_artifact_id = ArtifactId(f"{workspace_id}:nonexistent")

        # Should not raise even though artifact doesn't exist
        await adapter.delete(workspace_id, fake_artifact_id, is_sweeper=True)

    @pytest.mark.asyncio
    async def test_workspace_scoping_get_blocks_cross_workspace(
        self,
        adapter: ArtifactStoreProvider,
        workspace_id: WorkspaceId,
        other_workspace_id: WorkspaceId,
        sample_content: bytes,
    ) -> None:
        """get() rejects cross-workspace access.

        Attempting to retrieve artifact from different workspace
        raises WorkspaceMismatch (caller maps to 404).
        """
        # Put artifact in workspace A
        async def content_iter():
            yield sample_content

        descriptor = await adapter.put(workspace_id, content_iter())

        # Attempt to get from workspace B should raise WorkspaceMismatch
        with pytest.raises(WorkspaceMismatch):
            async for _ in adapter.get(other_workspace_id, descriptor.artifact_id):
                pass

    @pytest.mark.asyncio
    async def test_workspace_scoping_head_returns_none_for_cross_workspace(
        self,
        adapter: ArtifactStoreProvider,
        workspace_id: WorkspaceId,
        other_workspace_id: WorkspaceId,
        sample_content: bytes,
    ) -> None:
        """head() returns None for cross-workspace artifacts.

        Doesn't disclose cross-workspace existence (returns None, not error).
        """
        # Put artifact in workspace A
        async def content_iter():
            yield sample_content

        descriptor = await adapter.put(workspace_id, content_iter())

        # head() from workspace B should return None, not error
        result = await adapter.head(other_workspace_id, descriptor.artifact_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_content_addressability_identical_content_same_digest(
        self,
        adapter: ArtifactStoreProvider,
        workspace_id: WorkspaceId,
        sample_content: bytes,
    ) -> None:
        """Identical content produces identical digest and artifact ID.

        Writing the same content twice must produce the same artifact_id
        and digest (idempotency guarantee).
        """
        # Put same content twice
        async def content_iter1():
            yield sample_content

        async def content_iter2():
            yield sample_content

        descriptor1 = await adapter.put(workspace_id, content_iter1())
        descriptor2 = await adapter.put(workspace_id, content_iter2())

        # Should have same artifact_id and digest
        assert descriptor1.artifact_id == descriptor2.artifact_id
        assert descriptor1.digest == descriptor2.digest

    @pytest.mark.asyncio
    async def test_media_type_consistency_stored_vs_returned(
        self,
        adapter: ArtifactStoreProvider,
        workspace_id: WorkspaceId,
        sample_content: bytes,
    ) -> None:
        """put() stores and returns consistent media_type.

        Returned ArtifactDescriptor.media_type must match what was stored,
        not just the input argument (which may be None).
        """
        # Put with media_type=None
        async def content_iter():
            yield sample_content

        descriptor = await adapter.put(workspace_id, content_iter(), media_type=None)

        # media_type in descriptor should be effective type, not None
        assert descriptor.media_type is not None
        assert isinstance(descriptor.media_type, str)
        assert len(descriptor.media_type) > 0

        # head() should return same media_type
        head_result = await adapter.head(workspace_id, descriptor.artifact_id)
        assert head_result is not None
        assert head_result.media_type == descriptor.media_type
