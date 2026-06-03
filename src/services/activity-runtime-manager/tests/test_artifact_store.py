"""Tests for the Artifact Store Client (ARM-IMPL-006)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator

import pytest
from custos_spl.errors import ArtifactNotFound
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor, ArtifactStoreProvider

from custos_arm.store.artifact import (
    ArtifactRecord,
    ArtifactStoreClient,
    ArtifactTooLargeError,
)


class _FakeArtifactStore:
    """Content-addressed in-memory stand-in for ``ArtifactStoreProvider``."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}

    async def put(
        self,
        workspace_id: WorkspaceId,
        content: AsyncIterator[bytes],
        media_type: str | None = None,
    ) -> ArtifactDescriptor:
        data = b""
        async for chunk in content:
            data += chunk
        # Mirror the SPL adapter contract: ``digest`` is the raw hexdigest and
        # ``artifact_id`` embeds the workspace as ``{workspace_id}:{digest}``.
        digest = hashlib.sha256(data).hexdigest()
        artifact_id = f"{workspace_id}:{digest}"
        # Only commit the blob once the whole stream is consumed.
        self.blobs[artifact_id] = data
        return ArtifactDescriptor(
            workspace_id=workspace_id,
            artifact_id=ArtifactId(artifact_id),
            digest=digest,
            media_type=media_type,
            size=len(data),
        )

    def get(self, workspace_id: WorkspaceId, artifact_id: ArtifactId) -> AsyncIterator[bytes]:
        return self._stream(artifact_id)

    async def _stream(self, artifact_id: str) -> AsyncIterator[bytes]:
        if artifact_id not in self.blobs:
            raise ArtifactNotFound(f"no such artifact {artifact_id}")
        data = self.blobs[artifact_id]
        for i in range(0, len(data), 4):
            yield data[i : i + 4]


def _store() -> ArtifactStoreProvider:
    return _FakeArtifactStore()  # type: ignore[return-value]


def _client(
    store: ArtifactStoreProvider | None = None, *, max_bytes: int = 1024
) -> ArtifactStoreClient:
    return ArtifactStoreClient(store or _store(), max_bytes=max_bytes)


async def _aiter(data: bytes, *, chunk: int = 4) -> AsyncIterator[bytes]:
    for i in range(0, len(data), chunk):
        yield data[i : i + chunk]


async def test_upload_returns_store_assigned_metadata() -> None:
    payload = b"hello sbom payload"
    client = _client()
    record = await client.upload(
        workspace_id="ws-1",
        name="sbom.json",
        content=_aiter(payload),
        produced_by_run_id="run-1",
        produced_by_step_id="step-1",
        produced_by_attempt=1,
        media_type="application/json",
    )
    assert isinstance(record, ArtifactRecord)
    expected_digest = hashlib.sha256(payload).hexdigest()
    assert record.id == f"ws-1:{expected_digest}"
    assert record.digest == expected_digest
    assert record.id != record.digest
    assert record.media_type == "application/json"
    assert record.size == len(payload)
    assert record.name == "sbom.json"
    assert record.produced_by_run_id == "run-1"
    assert record.produced_by_step_id == "step-1"
    assert record.produced_by_attempt == 1


async def test_upload_over_cap_fails_before_transfer_completes() -> None:
    store = _FakeArtifactStore()
    client = ArtifactStoreClient(store, max_bytes=8)  # type: ignore[arg-type]
    with pytest.raises(ArtifactTooLargeError):
        await client.upload(
            workspace_id="ws-1",
            name="big.bin",
            content=_aiter(b"x" * 64),
            produced_by_run_id="run-1",
            produced_by_step_id="step-1",
            produced_by_attempt=1,
        )
    # The blob must never be committed when the cap trips mid-stream.
    assert store.blobs == {}


async def test_upload_at_cap_boundary_succeeds() -> None:
    payload = b"x" * 8
    client = _client(max_bytes=8)
    record = await client.upload(
        workspace_id="ws-1",
        name="exact.bin",
        content=_aiter(payload),
        produced_by_run_id="run-1",
        produced_by_step_id="step-1",
        produced_by_attempt=1,
    )
    assert record.size == 8


async def test_fetch_materializes_bytes() -> None:
    store = _store()
    client = _client(store)
    payload = b"downstream input bytes"
    record = await client.upload(
        workspace_id="ws-1",
        name="in.bin",
        content=_aiter(payload),
        produced_by_run_id="run-1",
        produced_by_step_id="step-1",
        produced_by_attempt=1,
    )
    fetched = await client.fetch("ws-1", record.id)
    assert fetched == payload


async def test_fetch_unknown_artifact_raises() -> None:
    client = _client()
    with pytest.raises(ArtifactNotFound):
        await client.fetch("ws-1", f"ws-1:{hashlib.sha256(b'missing').hexdigest()}")


async def test_fetch_over_cap_raises() -> None:
    store = _FakeArtifactStore()
    artifact_id = f"ws-1:{hashlib.sha256(b'y' * 64).hexdigest()}"
    store.blobs[artifact_id] = b"y" * 64
    client = ArtifactStoreClient(store, max_bytes=8)  # type: ignore[arg-type]
    with pytest.raises(ArtifactTooLargeError):
        await client.fetch("ws-1", artifact_id)
