"""Owned deferred-pool shutdown, including unused and failed-close lifecycles."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from custos_pg.pool import LazyPool


async def test_close_unused_pool_never_connects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Teardown cannot create a resource, and a closed wrapper cannot reopen afterward."""
    factory = AsyncMock()
    monkeypatch.setattr(asyncpg, "create_pool", factory)
    lazy = LazyPool("postgresql://unused")
    await lazy.aclose()
    await lazy.aclose()
    factory.assert_not_awaited()
    with pytest.raises(RuntimeError, match="closed"):
        await lazy.get()


async def test_close_owned_pool_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wrapper closes exactly the resource its own factory constructed."""
    pool = Mock(spec=asyncpg.Pool)
    pool.close = AsyncMock()
    factory = AsyncMock(return_value=pool)
    monkeypatch.setattr(asyncpg, "create_pool", factory)
    lazy = LazyPool("postgresql://unused")
    assert await lazy.get() is pool
    await lazy.aclose()
    await lazy.aclose()
    pool.close.assert_awaited_once()
    factory.assert_awaited_once()


async def test_failed_close_remains_retryable_without_reopening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed cleanup is surfaced and can be retried, but never permits new use."""
    pool = Mock(spec=asyncpg.Pool)
    pool.close = AsyncMock(side_effect=[OSError("cleanup unavailable"), None])
    monkeypatch.setattr(asyncpg, "create_pool", AsyncMock(return_value=pool))
    lazy = LazyPool("postgresql://unused")
    await lazy.get()
    with pytest.raises(OSError):
        await lazy.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await lazy.get()
    await lazy.aclose()
    assert pool.close.await_count == 2
