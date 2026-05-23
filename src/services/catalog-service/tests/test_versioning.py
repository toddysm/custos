"""Tests for :mod:`custos_catalog.versioning` (CS-IMPL-009)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import WorkflowId, WorkflowTemplateId, WorkspaceId
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    WorkflowTemplateVersion,
    WorkflowVersion,
)
from custos_spl.pagination import Cursor, Page

from custos_catalog.versioning import (
    TemplateImmutabilityError,
    VersioningManager,
    WorkflowImmutabilityError,
)

WS = WorkspaceId("ws-1")
WF = WorkflowId("my-wf")
TPL = WorkflowTemplateId("my-tpl")


@dataclass(slots=True)
class FakeDefinitionStore:
    """Hand-rolled fake satisfying :class:`DefinitionStoreProvider`.

    Tracks only what :class:`VersioningManager` needs: per-name lists
    of ``WorkflowVersion`` / ``WorkflowTemplateVersion`` rows, sorted
    newest first like the real adapter.
    """

    workflows: dict[tuple[WorkspaceId, WorkflowId], list[WorkflowVersion]] = field(
        default_factory=dict,
    )
    templates: dict[tuple[WorkspaceId, WorkflowTemplateId], list[WorkflowTemplateVersion]] = field(
        default_factory=dict
    )
    page_size_calls: list[int | None] = field(default_factory=list)

    # --- Workflow surface ---

    async def put_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersion:
        bucket = self.workflows.setdefault((workspace_id, workflow_id), [])
        if any(row.version == version for row in bucket):
            raise ImmutableViolation(
                f"workflow {workflow_id} version {version} already exists",
            )
        row = WorkflowVersion(
            workspace_id=workspace_id,
            workflow_id=workflow_id,
            version=version,
            normalized_doc=normalized_doc,
            derived_from_template_version_id=derived_from_template_version_id,
            parent_deprecated=False,
            published_at=datetime.now(tz=UTC),
        )
        bucket.insert(0, row)
        return row

    async def get_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
    ) -> WorkflowVersion | None:
        for row in self.workflows.get((workspace_id, workflow_id), []):
            if row.version == version:
                return row
        return None

    async def list_workflow_versions(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowVersion]:
        self.page_size_calls.append(limit)
        rows = self.workflows.get((workspace_id, workflow_id), [])
        start = int(cursor.token) if cursor is not None else 0
        end = start + (limit or len(rows))
        slice_ = rows[start:end]
        next_cursor = Cursor(token=str(end)) if end < len(rows) else None
        return Page(items=slice_, next_cursor=next_cursor)

    async def get_latest_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
    ) -> WorkflowVersion | None:
        rows = self.workflows.get((workspace_id, workflow_id), [])
        return rows[0] if rows else None

    async def set_workflow_deprecated(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        deprecated: bool,
    ) -> None:
        bucket = self.workflows.get((workspace_id, workflow_id), [])
        self.workflows[(workspace_id, workflow_id)] = [
            WorkflowVersion(
                workspace_id=row.workspace_id,
                workflow_id=row.workflow_id,
                version=row.version,
                normalized_doc=row.normalized_doc,
                derived_from_template_version_id=row.derived_from_template_version_id,
                parent_deprecated=deprecated,
                published_at=row.published_at,
            )
            for row in bucket
        ]

    # --- Template surface (mirror) ---

    async def put_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersion:
        bucket = self.templates.setdefault((workspace_id, template_id), [])
        if any(row.version == version for row in bucket):
            raise ImmutableViolation(
                f"template {template_id} version {version} already exists",
            )
        row = WorkflowTemplateVersion(
            workspace_id=workspace_id,
            template_id=template_id,
            version=version,
            normalized_doc=normalized_doc,
            derived_from_workflow_version_id=derived_from_workflow_version_id,
            parent_deprecated=False,
            published_at=datetime.now(tz=UTC),
        )
        bucket.insert(0, row)
        return row

    async def get_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
    ) -> WorkflowTemplateVersion | None:
        for row in self.templates.get((workspace_id, template_id), []):
            if row.version == version:
                return row
        return None

    async def list_workflow_template_versions(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowTemplateVersion]:
        rows = self.templates.get((workspace_id, template_id), [])
        start = int(cursor.token) if cursor is not None else 0
        end = start + (limit or len(rows))
        slice_ = rows[start:end]
        next_cursor = Cursor(token=str(end)) if end < len(rows) else None
        return Page(items=slice_, next_cursor=next_cursor)

    async def set_workflow_template_deprecated(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        deprecated: bool,
    ) -> None:
        # Not exercised by these tests, but keep the surface symmetric.
        bucket = self.templates.get((workspace_id, template_id), [])
        self.templates[(workspace_id, template_id)] = [
            WorkflowTemplateVersion(
                workspace_id=row.workspace_id,
                template_id=row.template_id,
                version=row.version,
                normalized_doc=row.normalized_doc,
                derived_from_workflow_version_id=row.derived_from_workflow_version_id,
                parent_deprecated=deprecated,
                published_at=row.published_at,
            )
            for row in bucket
        ]


def _wfver(version: str) -> WorkflowVersion:
    return WorkflowVersion(
        workspace_id=WS,
        workflow_id=WF,
        version=version,
        normalized_doc={"v": version},
        derived_from_template_version_id=None,
        parent_deprecated=False,
        published_at=datetime.now(tz=UTC),
    )


def _tplver(version: str) -> WorkflowTemplateVersion:
    return WorkflowTemplateVersion(
        workspace_id=WS,
        template_id=TPL,
        version=version,
        normalized_doc={"v": version},
        derived_from_workflow_version_id=None,
        parent_deprecated=False,
        published_at=datetime.now(tz=UTC),
    )


# ---------------------------------------------------------------------------
# Version minting — workflows
# ---------------------------------------------------------------------------


async def test_next_workflow_version_starts_at_one_when_empty() -> None:
    mgr = VersioningManager(store=FakeDefinitionStore())
    assert await mgr.next_workflow_version(WS, WF) == 1


async def test_next_workflow_version_is_one_plus_max() -> None:
    store = FakeDefinitionStore(workflows={(WS, WF): [_wfver("3"), _wfver("2"), _wfver("1")]})
    mgr = VersioningManager(store=store)
    assert await mgr.next_workflow_version(WS, WF) == 4


async def test_next_workflow_version_ignores_unparseable_versions() -> None:
    # A corrupt row with a non-integer version must not block minting.
    store = FakeDefinitionStore(
        workflows={(WS, WF): [_wfver("garbage"), _wfver("2"), _wfver("1")]},
    )
    mgr = VersioningManager(store=store)
    assert await mgr.next_workflow_version(WS, WF) == 3


async def test_next_workflow_version_walks_all_pages() -> None:
    # 250 rows in descending order; default page size is 100.
    rows = [_wfver(str(v)) for v in range(250, 0, -1)]
    store = FakeDefinitionStore(workflows={(WS, WF): rows})
    mgr = VersioningManager(store=store)
    assert await mgr.next_workflow_version(WS, WF) == 251
    # Verify pagination actually happened — three pages of 100 each.
    assert store.page_size_calls == [100, 100, 100]


async def test_next_workflow_version_is_workspace_scoped() -> None:
    # Two workspaces share the same workflow name but versions are independent.
    other_ws = WorkspaceId("ws-2")
    store = FakeDefinitionStore(
        workflows={
            (WS, WF): [_wfver("5")],
            (other_ws, WF): [_wfver("99")],
        },
    )
    mgr = VersioningManager(store=store)
    assert await mgr.next_workflow_version(WS, WF) == 6
    assert await mgr.next_workflow_version(other_ws, WF) == 100


async def test_next_workflow_version_is_name_scoped() -> None:
    other_wf = WorkflowId("other-wf")
    store = FakeDefinitionStore(
        workflows={
            (WS, WF): [_wfver("5")],
            (WS, other_wf): [_wfver("99")],
        },
    )
    mgr = VersioningManager(store=store)
    assert await mgr.next_workflow_version(WS, WF) == 6
    assert await mgr.next_workflow_version(WS, other_wf) == 100


# ---------------------------------------------------------------------------
# Version minting — templates
# ---------------------------------------------------------------------------


async def test_next_template_version_mirrors_workflow_behaviour() -> None:
    store = FakeDefinitionStore(templates={(WS, TPL): [_tplver("7"), _tplver("3")]})
    mgr = VersioningManager(store=store)
    assert await mgr.next_template_version(WS, TPL) == 8


async def test_next_template_version_starts_at_one_when_empty() -> None:
    mgr = VersioningManager(store=FakeDefinitionStore())
    assert await mgr.next_template_version(WS, TPL) == 1


# ---------------------------------------------------------------------------
# WorkflowImmutabilityError / TemplateImmutabilityError surface
# ---------------------------------------------------------------------------


def test_workflow_immutability_error_carries_context() -> None:
    err = WorkflowImmutabilityError(
        workspace_id="ws-1",
        workflow_name="my-wf",
        attempted_version=3,
        next_available_version=5,
        is_idempotent_match=False,
    )
    assert err.code == "catalog.workflow_immutability"
    assert err.workspace_id == "ws-1"
    assert err.workflow_name == "my-wf"
    assert err.attempted_version == 3
    assert err.next_available_version == 5
    assert err.is_idempotent_match is False
    # Message mentions the contended slot AND the next-available hint.
    assert "version 3" in str(err)
    assert "next available is 5" in str(err)


def test_workflow_immutability_error_carries_idempotent_match_flag() -> None:
    err = WorkflowImmutabilityError(
        workspace_id="ws-1",
        workflow_name="my-wf",
        attempted_version=3,
        next_available_version=4,
        is_idempotent_match=True,
    )
    assert err.is_idempotent_match is True


def test_template_immutability_error_carries_context() -> None:
    err = TemplateImmutabilityError(
        workspace_id="ws-1",
        template_name="my-tpl",
        attempted_version=2,
        next_available_version=3,
        is_idempotent_match=False,
    )
    assert err.code == "catalog.template_immutability"
    assert err.template_name == "my-tpl"


# ---------------------------------------------------------------------------
# Race / immutability behaviour through the fake
# ---------------------------------------------------------------------------


async def test_put_raises_immutable_violation_on_existing_version() -> None:
    store = FakeDefinitionStore(workflows={(WS, WF): [_wfver("1")]})
    with pytest.raises(ImmutableViolation):
        await store.put_workflow_version(WS, WF, "1", {})


async def test_two_versioning_manager_calls_can_collide() -> None:
    # Models the race described in the module docstring:
    # Two requests both ask "next version?" against an empty workflow
    # and both get 1; the first put succeeds, the second collides.
    store = FakeDefinitionStore()
    mgr = VersioningManager(store=store)
    v1 = await mgr.next_workflow_version(WS, WF)
    v2 = await mgr.next_workflow_version(WS, WF)
    assert v1 == v2 == 1
    await store.put_workflow_version(WS, WF, str(v1), {"a": 1})
    with pytest.raises(ImmutableViolation):
        await store.put_workflow_version(WS, WF, str(v2), {"a": 2})
    # The losing caller asks for a fresh number and gets 2.
    assert await mgr.next_workflow_version(WS, WF) == 2
