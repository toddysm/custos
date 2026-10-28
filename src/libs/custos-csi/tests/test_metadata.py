"""Persisted media-type and legacy-blob regressions for the filesystem adapter."""

import asyncio
import fcntl
import multiprocessing
import os
import threading
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing, suppress
from multiprocessing.connection import Connection
from pathlib import Path

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor

from custos_csi import adapters
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


async def test_failed_publication_metadata_does_not_win_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed writer's sidecar must not determine a later successful descriptor."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    original = adapters._publish_file

    async def fail_blob(source: Path, destination: Path) -> None:
        """Fail after metadata publication but before publishing any blob."""
        if destination.suffix != ".json":
            raise OSError("injected blob publication failure")
        await original(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(adapters, "_publish_file", fail_blob)
        with pytest.raises(BackendUnavailable, match="publication failure"):
            await adapter.put(workspace, _content(), media_type="text/plain")
    retried = await adapter.put(workspace, _content(), media_type="application/json")
    assert retried.media_type == "application/json"
    assert await CsiArtifactAdapter(tmp_path).head(workspace, retried.artifact_id) == retried


@pytest.fixture
def contention(monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
    """Signal actual lock contention rather than depending on timing delays."""
    blocked = asyncio.Event()
    original = fcntl.flock

    def observe(descriptor: int, mode: int) -> None:
        """Preserve native lock behavior while observing a denied acquisition."""
        try:
            original(descriptor, mode)
        except BlockingIOError:
            blocked.set()
            raise

    monkeypatch.setattr(fcntl, "flock", observe)
    return blocked


async def test_delete_and_republication_cannot_split_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, contention: asyncio.Event
) -> None:
    """A publisher waits for both halves of a deliberately paused deletion."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    old = await adapter.put(workspace, _content(), media_type="text/plain")
    blob = tmp_path / workspace / old.digest[:2] / old.digest
    removed = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = os.remove

    def pause_after_blob(path: Path) -> None:
        """Hold deletion between blob and metadata unlink until the test releases it."""
        original(path)
        if path == blob:
            loop.call_soon_threadsafe(removed.set)
            if not release.wait(5):
                raise TimeoutError("test did not release deletion")

    monkeypatch.setattr(os, "remove", pause_after_blob)
    deletion = asyncio.create_task(adapter.delete(workspace, old.artifact_id, is_sweeper=True))
    publication: asyncio.Task[ArtifactDescriptor] | None = None
    try:
        await asyncio.wait_for(removed.wait(), 5)
        publication = asyncio.create_task(
            adapter.put(workspace, _content(), media_type="application/json")
        )
        await asyncio.wait_for(contention.wait(), 5)
        assert not publication.done()
        release.set()
        await deletion
        fresh = await publication
        assert fresh.media_type == "application/json"
        assert await CsiArtifactAdapter(tmp_path).head(workspace, old.artifact_id) == fresh
    finally:
        release.set()
        await asyncio.gather(
            deletion, *([publication] if publication else []), return_exceptions=True
        )


def _hold_prefix_lock(path: Path, ready: Connection, release: Connection) -> None:
    """Hold the real lock inode in another process until explicitly released."""
    with path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        ready.send(True)
        if not release.poll(10):
            raise TimeoutError("parent did not release child lock")
        release.recv()


async def test_readers_coordinate_with_another_process(
    tmp_path: Path, contention: asyncio.Event
) -> None:
    """A fresh process's exclusive lock blocks head() until the shared pair is safe."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    descriptor = await adapter.put(workspace, _content(), media_type="text/plain")
    lock_path = tmp_path / workspace / descriptor.digest[:2] / ".lock"
    context = multiprocessing.get_context("spawn")
    ready_recv, ready_send = context.Pipe(duplex=False)
    release_recv, release_send = context.Pipe(duplex=False)
    process = context.Process(target=_hold_prefix_lock, args=(lock_path, ready_send, release_recv))
    process.start()
    reader: asyncio.Task[ArtifactDescriptor | None] | None = None
    try:
        assert await asyncio.to_thread(ready_recv.poll, 5)
        assert ready_recv.recv() is True
        reader = asyncio.create_task(adapter.head(workspace, descriptor.artifact_id))
        await asyncio.wait_for(contention.wait(), 5)
        assert not reader.done()
        release_send.send(True)
        assert await reader == descriptor
        await asyncio.to_thread(process.join, 5)
        assert process.exitcode == 0
    finally:
        with suppress(BrokenPipeError, OSError):
            release_send.send(True)
        await asyncio.to_thread(process.join, 5)
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
        process.close()
        if reader is not None:
            await asyncio.gather(reader, return_exceptions=True)
        for connection in (ready_recv, ready_send, release_recv, release_send):
            connection.close()


async def test_cancelled_publication_allows_metadata_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancellation after metadata publication releases the lock and permits a fresh retry."""
    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    metadata_published = asyncio.Event()
    original = adapters._publish_file

    async def pause_before_blob(source: Path, destination: Path) -> None:
        """Suspend the writer after its metadata has landed."""
        if destination.suffix != ".json":
            metadata_published.set()
            await asyncio.Event().wait()
        await original(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(adapters, "_publish_file", pause_before_blob)
        writer = asyncio.create_task(adapter.put(workspace, _content(), media_type="text/plain"))
        try:
            await asyncio.wait_for(metadata_published.wait(), 5)
        finally:
            writer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await writer
    retried = await adapter.put(workspace, _content(), media_type="application/json")
    assert retried.media_type == "application/json"


@pytest.mark.parametrize("missing", [False, True])
async def test_cancellation_waits_for_inflight_filesystem_mutation(missing: bool) -> None:
    """A cancelled caller must not release its lock while a worker can still mutate files."""
    started = threading.Event()
    release = threading.Event()

    def mutation() -> None:
        """Model a filesystem syscall that is still in progress when cancellation arrives."""
        started.set()
        if not release.wait(5):
            raise TimeoutError("test did not release filesystem mutation")
        if missing:
            raise FileNotFoundError("already removed")

    task = asyncio.create_task(adapters._finish_mutation(mutation))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_paused_download_does_not_block_sweeper(tmp_path: Path) -> None:
    """Keep reading the opened inode after deletion without retaining the prefix lock."""
    payload = b"streamed content" * 10000

    async def content() -> AsyncIterator[bytes]:
        """Supply enough bytes to require more than one read chunk."""
        yield payload

    adapter = CsiArtifactAdapter(tmp_path)
    workspace = WorkspaceId("metadata-test")
    descriptor = await adapter.put(workspace, content())
    stream = adapter.get(workspace, descriptor.artifact_id)
    assert isinstance(stream, AsyncGenerator)
    async with aclosing(stream):
        first = await anext(stream)
        assert len(first) < len(payload)
        await asyncio.wait_for(
            adapter.delete(workspace, descriptor.artifact_id, is_sweeper=True), 5
        )
        assert await adapter.head(workspace, descriptor.artifact_id) is None
        remainder = b"".join([chunk async for chunk in stream])
    assert first + remainder == payload
