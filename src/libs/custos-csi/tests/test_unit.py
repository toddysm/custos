"""Unit tests for custos-csi adapter — no live PVC required."""

import tempfile
from pathlib import Path

import pytest

from custos_csi.adapters import CsiArtifactAdapter
from custos_spl.errors import ArtifactNotFound, WorkspaceMismatch
from custos_spl.ids import ArtifactId, WorkspaceId


@pytest.fixture
def temp_mount() -> Path:
    """Create a temporary directory for PVC mount."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def adapter(temp_mount: Path) -> CsiArtifactAdapter:
    """Create adapter with temp mount."""
    return CsiArtifactAdapter(temp_mount)


def test_adapter_requires_existing_directory() -> None:
    """Adapter must be initialized with existing directory."""
    with pytest.raises(ValueError, match="does not exist"):
        CsiArtifactAdapter(Path("/nonexistent/path"))


def test_adapter_requires_directory_not_file(temp_mount: Path) -> None:
    """Adapter must be initialized with a directory, not a file."""
    file_path = temp_mount / "file.txt"
    file_path.write_text("test")
    with pytest.raises(ValueError, match="not a directory"):
        CsiArtifactAdapter(file_path)


@pytest.mark.asyncio
async def test_put_and_get_simple(adapter: CsiArtifactAdapter) -> None:
    """Put and get a simple blob."""
    workspace_id = WorkspaceId("ws-123")
    content_bytes = b"hello world"

    async def content_stream():
        yield content_bytes

    # Put
    descriptor = await adapter.put(workspace_id, content_stream(), media_type="text/plain")

    assert descriptor.workspace_id == workspace_id
    assert descriptor.media_type == "text/plain"
    assert descriptor.size == len(content_bytes)
    assert len(descriptor.digest) == 64  # SHA256 hex is 64 chars
    assert descriptor.artifact_id == ArtifactId(f"{workspace_id}:{descriptor.digest}")

    # Get
    chunks = []
    async for chunk in adapter.get(workspace_id, descriptor.artifact_id):
        chunks.append(chunk)
    assert b"".join(chunks) == content_bytes


@pytest.mark.asyncio
async def test_put_idempotent_same_digest(adapter: CsiArtifactAdapter) -> None:
    """Put is idempotent: identical bytes return same artifact_id."""
    workspace_id = WorkspaceId("ws-123")
    content_bytes = b"identical content"

    async def content_stream():
        yield content_bytes

    # First put
    descriptor1 = await adapter.put(workspace_id, content_stream())

    # Second put with identical content
    async def content_stream2():
        yield content_bytes

    descriptor2 = await adapter.put(workspace_id, content_stream2())

    assert descriptor1.artifact_id == descriptor2.artifact_id
    assert descriptor1.digest == descriptor2.digest


@pytest.mark.asyncio
async def test_put_streaming_large_blob(adapter: CsiArtifactAdapter) -> None:
    """Put handles large blobs via streaming without buffering all in memory."""
    workspace_id = WorkspaceId("ws-123")
    chunk_size = 1024 * 1024  # 1MB
    num_chunks = 5
    total_size = chunk_size * num_chunks

    async def content_stream():
        for _ in range(num_chunks):
            yield b"x" * chunk_size

    descriptor = await adapter.put(workspace_id, content_stream())
    assert descriptor.size == total_size


@pytest.mark.asyncio
async def test_head_exists(adapter: CsiArtifactAdapter) -> None:
    """Head returns descriptor for existing artifact."""
    workspace_id = WorkspaceId("ws-123")
    content_bytes = b"test content"

    async def content_stream():
        yield content_bytes

    descriptor = await adapter.put(workspace_id, content_stream())

    # Head should return same descriptor (or subset)
    head_result = await adapter.head(workspace_id, descriptor.artifact_id)
    assert head_result is not None
    assert head_result.artifact_id == descriptor.artifact_id
    assert head_result.digest == descriptor.digest
    assert head_result.size == len(content_bytes)


@pytest.mark.asyncio
async def test_head_not_found() -> None:
    """Head returns None for non-existent artifact."""
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = CsiArtifactAdapter(Path(tmpdir))
        workspace_id = WorkspaceId("ws-123")
        artifact_id = ArtifactId("ws-123:abcdef1234567890")

        result = await adapter.head(workspace_id, artifact_id)
        assert result is None


@pytest.mark.asyncio
async def test_get_not_found(adapter: CsiArtifactAdapter) -> None:
    """Get raises ArtifactNotFound for non-existent artifact."""
    workspace_id = WorkspaceId("ws-123")
    artifact_id = ArtifactId("ws-123:abcdef1234567890")

    with pytest.raises(ArtifactNotFound):
        async for _ in adapter.get(workspace_id, artifact_id):
            pass


@pytest.mark.asyncio
async def test_cross_workspace_get_blocked(adapter: CsiArtifactAdapter) -> None:
    """Get blocks cross-workspace reads."""
    workspace_id_1 = WorkspaceId("ws-1")
    workspace_id_2 = WorkspaceId("ws-2")
    content_bytes = b"secret data"

    async def content_stream():
        yield content_bytes

    descriptor = await adapter.put(workspace_id_1, content_stream())

    # Try to read from different workspace
    with pytest.raises(WorkspaceMismatch):
        async for _ in adapter.get(workspace_id_2, descriptor.artifact_id):
            pass


@pytest.mark.asyncio
async def test_cross_workspace_head_returns_none(adapter: CsiArtifactAdapter) -> None:
    """Head collapses WorkspaceMismatch to None (indistinguishable from not found)."""
    workspace_id_1 = WorkspaceId("ws-1")
    workspace_id_2 = WorkspaceId("ws-2")
    content_bytes = b"data"

    async def content_stream():
        yield content_bytes

    descriptor = await adapter.put(workspace_id_1, content_stream())

    # Head from different workspace returns None (not WorkspaceMismatch)
    result = await adapter.head(workspace_id_2, descriptor.artifact_id)
    assert result is None


@pytest.mark.asyncio
async def test_delete_requires_sweeper_flag(adapter: CsiArtifactAdapter) -> None:
    """Delete raises error if not invoked as sweeper."""
    workspace_id = WorkspaceId("ws-123")
    content_bytes = b"data"

    async def content_stream():
        yield content_bytes

    descriptor = await adapter.put(workspace_id, content_stream())

    # Try delete without sweeper flag
    with pytest.raises(ValueError, match="sweeper-only"):
        await adapter.delete(workspace_id, descriptor.artifact_id, is_sweeper=False)


@pytest.mark.asyncio
async def test_delete_sweeper_removes_artifact(adapter: CsiArtifactAdapter) -> None:
    """Delete as sweeper removes artifact."""
    workspace_id = WorkspaceId("ws-123")
    content_bytes = b"data"

    async def content_stream():
        yield content_bytes

    descriptor = await adapter.put(workspace_id, content_stream())

    # Verify it exists
    head_result = await adapter.head(workspace_id, descriptor.artifact_id)
    assert head_result is not None

    # Delete as sweeper
    await adapter.delete(workspace_id, descriptor.artifact_id, is_sweeper=True)

    # Verify it's gone
    head_result = await adapter.head(workspace_id, descriptor.artifact_id)
    assert head_result is None


@pytest.mark.asyncio
async def test_delete_idempotent(adapter: CsiArtifactAdapter) -> None:
    """Delete is idempotent: can delete non-existent artifact."""
    workspace_id = WorkspaceId("ws-123")
    artifact_id = ArtifactId("ws-123:abcdef1234567890")

    # Should not raise, even though artifact doesn't exist
    await adapter.delete(workspace_id, artifact_id, is_sweeper=True)


def test_factory_requires_env_var() -> None:
    """Factory raises error if CUSTOS_CSI_PVC_MOUNT not set."""
    import os

    old_value = os.environ.pop("CUSTOS_CSI_PVC_MOUNT", None)
    try:
        from custos_csi.adapters import make_adapter

        with pytest.raises(RuntimeError, match="CUSTOS_CSI_PVC_MOUNT"):
            make_adapter()
    finally:
        if old_value:
            os.environ["CUSTOS_CSI_PVC_MOUNT"] = old_value


def test_factory_returns_adapter(temp_mount: Path) -> None:
    """Factory creates adapter from environment."""
    import os

    old_value = os.environ.get("CUSTOS_CSI_PVC_MOUNT")
    try:
        os.environ["CUSTOS_CSI_PVC_MOUNT"] = str(temp_mount)
        from custos_csi.adapters import make_adapter

        adapter = make_adapter()
        assert isinstance(adapter, CsiArtifactAdapter)
    finally:
        if old_value:
            os.environ["CUSTOS_CSI_PVC_MOUNT"] = old_value
        else:
            os.environ.pop("CUSTOS_CSI_PVC_MOUNT", None)
