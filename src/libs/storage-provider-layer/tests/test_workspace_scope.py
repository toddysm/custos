"""Tests for the workspace-scoping middleware proxy."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import ClassVar

import pytest

from custos_spl import (
    WorkspaceId,
    WorkspaceScopingViolation,
    wrap_workspace_scoped,
)


class _FakeStore:
    """Minimal workspace-scoped provider used as the inner under test."""

    SCHEMA_REVISION: ClassVar[int] = 1

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def get_thing(self, workspace_id: WorkspaceId, thing_id: str) -> str:
        self.calls.append(("get_thing", (workspace_id, thing_id), {}))
        return f"{workspace_id}:{thing_id}"

    async def list_things(self, workspace_id: WorkspaceId) -> list[str]:
        self.calls.append(("list_things", (workspace_id,), {}))
        return []

    def tail(
        self, workspace_id: WorkspaceId
    ) -> AsyncIterator[str]:
        """Plain `def` returning an AsyncIterator — like tail_run_logs."""
        self.calls.append(("tail", (workspace_id,), {}))

        async def _gen() -> AsyncIterator[str]:
            yield f"hello from {workspace_id}"

        return _gen()

    async def platform_op(self, foo: str) -> str:
        """Not workspace-scoped — must pass through untouched."""
        self.calls.append(("platform_op", (foo,), {}))
        return foo


@pytest.fixture
def inner() -> _FakeStore:
    return _FakeStore()


@pytest.fixture
def wrapped(inner: _FakeStore) -> _FakeStore:
    return wrap_workspace_scoped(inner)


# ----- forwarding -----


def test_proxy_forwards_class_attributes(
    inner: _FakeStore, wrapped: _FakeStore
) -> None:
    assert wrapped.SCHEMA_REVISION == inner.SCHEMA_REVISION


@pytest.mark.asyncio
async def test_proxy_forwards_valid_call(
    inner: _FakeStore, wrapped: _FakeStore
) -> None:
    result = await wrapped.get_thing(WorkspaceId("ws-1"), "t-1")
    assert result == "ws-1:t-1"
    assert inner.calls == [("get_thing", (WorkspaceId("ws-1"), "t-1"), {})]


@pytest.mark.asyncio
async def test_proxy_forwards_valid_call_via_kwargs(
    inner: _FakeStore, wrapped: _FakeStore
) -> None:
    result = await wrapped.get_thing(workspace_id=WorkspaceId("ws-1"), thing_id="t-1")
    assert result == "ws-1:t-1"


@pytest.mark.asyncio
async def test_proxy_passes_through_non_scoped_method(
    inner: _FakeStore, wrapped: _FakeStore
) -> None:
    """A method whose first param is not workspace_id is forwarded unchanged."""
    result = await wrapped.platform_op("hello")
    assert result == "hello"


def test_proxy_passes_through_tail_validation_sync(
    wrapped: _FakeStore,
) -> None:
    """tail is plain def → validation happens synchronously at call time."""
    it = wrapped.tail(WorkspaceId("ws-1"))
    assert hasattr(it, "__aiter__")


# ----- violations -----


@pytest.mark.asyncio
async def test_rejects_none_workspace_id(wrapped: _FakeStore) -> None:
    with pytest.raises(WorkspaceScopingViolation, match="workspace_id is required"):
        await wrapped.get_thing(None, "t-1")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rejects_empty_workspace_id(wrapped: _FakeStore) -> None:
    with pytest.raises(WorkspaceScopingViolation, match="non-empty"):
        await wrapped.get_thing(WorkspaceId(""), "t-1")


@pytest.mark.asyncio
async def test_rejects_non_string_workspace_id(wrapped: _FakeStore) -> None:
    with pytest.raises(WorkspaceScopingViolation, match="must be a WorkspaceId"):
        await wrapped.get_thing(123, "t-1")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rejects_none_via_kwargs(wrapped: _FakeStore) -> None:
    with pytest.raises(WorkspaceScopingViolation):
        await wrapped.get_thing(workspace_id=None, thing_id="t-1")  # type: ignore[arg-type]


def test_rejects_sync_iter_method_with_none(wrapped: _FakeStore) -> None:
    with pytest.raises(WorkspaceScopingViolation):
        wrapped.tail(None)  # type: ignore[arg-type]


# ----- inner is never reached on violation -----


@pytest.mark.asyncio
async def test_violation_does_not_invoke_inner(
    inner: _FakeStore, wrapped: _FakeStore
) -> None:
    with pytest.raises(WorkspaceScopingViolation):
        await wrapped.get_thing(None, "t-1")  # type: ignore[arg-type]
    assert inner.calls == []
