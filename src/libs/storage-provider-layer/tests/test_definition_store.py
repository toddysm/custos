"""Tests for DefinitionStoreProvider Protocol and its data shapes."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from custos_spl.ids import WorkflowId, WorkflowTemplateId, WorkspaceId
from custos_spl.interfaces import (
    DefinitionListFilter,
    DefinitionStoreProvider,
    WorkflowTemplateVersion,
    WorkflowVersion,
)

# ----- Data shapes -----


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


def test_workflow_version_is_frozen() -> None:
    wv = WorkflowVersion(
        workspace_id=WorkspaceId("ws-1"),
        workflow_id=WorkflowId("wf-1"),
        version="1.0.0",
        normalized_doc={"steps": []},
        derived_from_template_version_id=None,
        deprecated=False,
        published_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        wv.version = "2.0.0"  # type: ignore[misc]


def test_workflow_template_version_is_frozen() -> None:
    tv = WorkflowTemplateVersion(
        workspace_id=WorkspaceId("ws-1"),
        template_id=WorkflowTemplateId("tpl-1"),
        version="1.0.0",
        normalized_doc={},
        derived_from_workflow_version_id=None,
        deprecated=False,
        published_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        tv.deprecated = True  # type: ignore[misc]


def test_definition_list_filter_defaults_are_open() -> None:
    f = DefinitionListFilter()
    assert f.deprecated is None
    assert f.published_after is None
    assert f.published_before is None


# ----- Protocol shape -----


def test_protocol_declares_required_schema_revision() -> None:
    assert DefinitionStoreProvider.SCHEMA_REVISION == 1


REQUIRED_METHODS = [
    "put_workflow_version",
    "get_workflow_version",
    "list_workflow_versions",
    "get_latest_workflow_version",
    "set_workflow_deprecated",
    "put_workflow_template_version",
    "get_workflow_template_version",
    "list_workflow_template_versions",
    "set_workflow_template_deprecated",
]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(DefinitionStoreProvider, method)


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_methods_are_async(method: str) -> None:
    """Every provider method must be async (Postgres adapter is async-only)."""
    fn = getattr(DefinitionStoreProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


# ----- runtime_checkable conformance -----


class _MinimalDefinitionStore:
    """Just enough of the Protocol to satisfy isinstance() at runtime.

    `runtime_checkable` only checks method NAMES, not signatures. This
    is a smoke test that a duck-typed implementation is recognized.
    """

    SCHEMA_REVISION = 1

    async def put_workflow_version(self, *a: object, **kw: object) -> None: ...
    async def get_workflow_version(self, *a: object, **kw: object) -> None: ...
    async def list_workflow_versions(self, *a: object, **kw: object) -> None: ...
    async def get_latest_workflow_version(self, *a: object, **kw: object) -> None: ...
    async def set_workflow_deprecated(self, *a: object, **kw: object) -> None: ...
    async def put_workflow_template_version(self, *a: object, **kw: object) -> None: ...
    async def get_workflow_template_version(self, *a: object, **kw: object) -> None: ...
    async def list_workflow_template_versions(self, *a: object, **kw: object) -> None: ...
    async def set_workflow_template_deprecated(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalDefinitionStore(), DefinitionStoreProvider)


def test_runtime_checkable_rejects_partial_impl() -> None:
    class Partial:
        async def put_workflow_version(self, *a: object, **kw: object) -> None: ...

    assert not isinstance(Partial(), DefinitionStoreProvider)
