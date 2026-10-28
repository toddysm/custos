"""Persisted media-type and legacy-blob regressions for the filesystem adapter."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import WorkspaceId

from custos_csi.adapters import CsiArtifactAdapter


async def _content() -> AsyncIterator[bytes]:
    """Provide identical bytes for metadata and idempotency checks."""
    yield b"persistent artifact metadata"


async def test_media_type_survives_adapter_restart(tmp_path: Path) -> None:
    """A fresh adapter returns the exact descriptor written by put()."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    descriptor = await adapter.put(workspace, _content(), media_type="text/plain")
    fresh = CsiArtifactAdapter(tmp_path)
    assert await fresh.head(workspace, descriptor.artifact_id) == descriptor
    assert await fresh.put(workspace, _content(), media_type="application/json") == descriptor
    await fresh.delete(workspace, descriptor.artifact_id, is_sweeper=True)
    assert await fresh.head(workspace, descriptor.artifact_id) is None
    assert not list((tmp_path / workspace).rglob("*.json"))


async def test_legacy_blob_without_media_sidecar_remains_readable(tmp_path: Path) -> None:
    """Existing blob layouts remain valid and report unknown media type as None."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    descriptor = await adapter.put(workspace, _content(), media_type="text/plain")
    path = tmp_path / workspace / descriptor.digest[:2] / descriptor.digest
    path.with_suffix(".json").unlink()
    legacy = await adapter.head(workspace, descriptor.artifact_id)
    assert legacy is not None and legacy.media_type is None
    assert b"".join([chunk async for chunk in adapter.get(workspace, descriptor.artifact_id)])


async def test_corrupt_media_metadata_is_not_silently_discarded(tmp_path: Path) -> None:
    """Invalid retained metadata is an explicit backend error, not a null fallback."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    descriptor = await adapter.put(workspace, _content(), media_type="text/plain")
    path = tmp_path / workspace / descriptor.digest[:2] / descriptor.digest
    path.with_suffix(".json").write_text('{"media_type": 123}', encoding="utf-8")
    with pytest.raises(BackendUnavailable, match="metadata is invalid"):
        await adapter.head(workspace, descriptor.artifact_id)


async def test_concurrent_identical_puts_share_the_persisted_descriptor(tmp_path: Path) -> None:
    """Concurrent media-type declarations must agree with the first published metadata."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    first, second = await asyncio.gather(
        adapter.put(workspace, _content(), media_type="text/plain"),
        adapter.put(workspace, _content(), media_type="application/json"),
    )
    assert first == second
    assert await CsiArtifactAdapter(tmp_path).head(workspace, first.artifact_id) == first
    assert not list((tmp_path / ".tmp").iterdir())
