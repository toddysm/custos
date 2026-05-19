"""Tests for ArtifactStoreProvider Protocol and ArtifactDescriptor."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from dataclasses import FrozenInstanceError

import pytest

from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces import ArtifactDescriptor, ArtifactStoreProvider

# ----- Data shape -----


def test_artifact_descriptor_is_frozen() -> None:
    d = ArtifactDescriptor(
        workspace_id=WorkspaceId("ws-1"),
        artifact_id=ArtifactId("a-1"),
        digest="sha256:abc",
        media_type="application/octet-stream",
        size=42,
    )
    with pytest.raises(FrozenInstanceError):
        d.size = 0  # type: ignore[misc]


def test_artifact_descriptor_allows_null_media_type() -> None:
    d = ArtifactDescriptor(
        workspace_id=WorkspaceId("ws-1"),
        artifact_id=ArtifactId("a-1"),
        digest="sha256:abc",
        media_type=None,
        size=42,
    )
    assert d.media_type is None


# ----- Protocol shape -----


def test_protocol_declares_required_schema_revision() -> None:
    assert ArtifactStoreProvider.SCHEMA_REVISION == 1


REQUIRED_METHODS = ["put", "get", "head", "delete"]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(ArtifactStoreProvider, method)


# `get` returns an AsyncIterator directly (not a coroutine); every other
# method must be `async def`.
_NON_COROUTINE_METHODS = {"get"}


@pytest.mark.parametrize(
    "method", [m for m in REQUIRED_METHODS if m not in _NON_COROUTINE_METHODS]
)
def test_protocol_methods_are_async(method: str) -> None:
    fn = getattr(ArtifactStoreProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


def test_get_is_not_a_coroutine_function() -> None:
    """`get` must return an `AsyncIterator` directly, not a coroutine.

    Pinned so a future refactor that accidentally writes
    `async def get` (which would make it a coroutine returning the
    iterator after one `await`) is caught — adapters expect to
    implement it as a plain `def` returning an async generator.
    """
    assert not inspect.iscoroutinefunction(ArtifactStoreProvider.get)


# ----- Workspace-scoping rule -----


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_methods_take_workspace_id_first(method: str) -> None:
    sig = inspect.signature(getattr(ArtifactStoreProvider, method))
    params = list(sig.parameters)
    # params[0] is self
    assert params[1] == "workspace_id", (
        f"{method} must take workspace_id as the first non-self argument"
    )


# ----- runtime_checkable conformance -----


class _MinimalArtifactStore:
    """Just enough of the Protocol to satisfy isinstance() at runtime."""

    SCHEMA_REVISION = 1

    async def put(self, *a: object, **kw: object) -> None: ...

    def get(self, *a: object, **kw: object) -> AsyncIterator[bytes]:  # type: ignore[empty-body]
        ...

    async def head(self, *a: object, **kw: object) -> None: ...
    async def delete(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalArtifactStore(), ArtifactStoreProvider)


def test_runtime_checkable_rejects_partial_impl() -> None:
    class Partial:
        async def put(self, *a: object, **kw: object) -> None: ...

    assert not isinstance(Partial(), ArtifactStoreProvider)
