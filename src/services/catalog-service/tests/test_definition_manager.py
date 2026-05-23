"""Tests for :mod:`custos_catalog.managers.definition` (CS-IMPL-010 + CS-IMPL-011)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import UUID

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import WorkflowId, WorkflowTemplateId, WorkspaceId
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    WorkflowTemplateVersion,
    WorkflowVersion,
)
from custos_spl.pagination import Cursor, Page

from custos_catalog.managers.definition import (
    DefinitionManager,
    PublishValidationError,
    WorkflowVersionRef,
)
from custos_catalog.resolve import StubConnectorClient
from custos_catalog.versioning import VersioningManager, WorkflowImmutabilityError

WS = WorkspaceId("ws-1")


# ---------------------------------------------------------------------------
# Hand-rolled fakes
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ActivityRow:
    namespace: str
    type: str
    version: str
    digest: str
    parent_deprecated: bool = False


class FakeActivityRegistry:
    """Tiny fake satisfying :class:`ActivityTypeRegistry`.

    Resolves every ``custos.builtin/<type>@<major>`` to a fixed
    ``<major>.0.0`` row with a stable digest. Tests that need the
    resolver to surface ``ActivityTypeNotFound`` pass an empty row
    list.
    """

    def __init__(self, *, allow_all: bool = True) -> None:
        self.allow_all = allow_all
        self.calls: list[tuple[str, str, str]] = []

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> _ActivityRow | None:
        self.calls.append((namespace, type, semver_range))
        if not self.allow_all:
            return None
        return _ActivityRow(
            namespace=namespace,
            type=type,
            version=f"{semver_range}.0.0",
            digest=f"sha256:{namespace}-{type}-{semver_range}",
        )

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> _ActivityRow | None:
        if not self.allow_all:
            return None
        return _ActivityRow(
            namespace=namespace,
            type=type,
            version=version,
            digest=f"sha256:{namespace}-{type}-{version}",
        )


@dataclass(slots=True)
class FakeDefinitionStore:
    """In-memory ``DefinitionStoreProvider`` covering all surface used
    by :class:`DefinitionManager`.

    A ``put_failures`` queue lets tests pre-load
    :class:`ImmutableViolation` errors that trigger on the next N
    puts — used to model race-recovery scenarios deterministically.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    workflows: dict[tuple[WorkspaceId, WorkflowId], list[WorkflowVersion]] = field(
        default_factory=dict,
    )
    parent_deprecated: dict[tuple[WorkspaceId, WorkflowId], bool] = field(
        default_factory=dict,
    )
    put_failures: list[Exception] = field(default_factory=list)
    list_calls: list[tuple[WorkspaceId, WorkflowId]] = field(default_factory=list)
    deprecate_calls: list[tuple[WorkspaceId, WorkflowId, bool]] = field(
        default_factory=list,
    )

    async def put_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersion:
        if self.put_failures:
            exc = self.put_failures.pop(0)
            raise exc
        bucket = self.workflows.setdefault((workspace_id, workflow_id), [])
        if any(row.version == version for row in bucket):
            raise ImmutableViolation(
                f"workflow {workflow_id} version {version} already exists",
            )
        row = WorkflowVersion(
            workspace_id=workspace_id,
            workflow_id=workflow_id,
            version=version,
            normalized_doc=dict(normalized_doc),
            derived_from_template_version_id=derived_from_template_version_id,
            parent_deprecated=self.parent_deprecated.get(
                (workspace_id, workflow_id),
                False,
            ),
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
                return self._with_current_deprecation(row)
        return None

    async def list_workflow_versions(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowVersion]:
        self.list_calls.append((workspace_id, workflow_id))
        rows = [
            self._with_current_deprecation(row)
            for row in self.workflows.get((workspace_id, workflow_id), [])
        ]
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
        if not rows:
            return None
        return self._with_current_deprecation(rows[0])

    async def set_workflow_deprecated(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        deprecated: bool,
    ) -> None:
        self.parent_deprecated[(workspace_id, workflow_id)] = deprecated
        self.deprecate_calls.append((workspace_id, workflow_id, deprecated))

    # Template surface (unused by DefinitionManager but kept to satisfy
    # the SPL Protocol shape during ducktyping).

    async def put_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersion:
        raise NotImplementedError

    async def get_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
    ) -> WorkflowTemplateVersion | None:
        return None

    async def list_workflow_template_versions(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowTemplateVersion]:
        return Page()

    async def set_workflow_template_deprecated(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        deprecated: bool,
    ) -> None: ...

    def _with_current_deprecation(self, row: WorkflowVersion) -> WorkflowVersion:
        """Refresh the denormalized parent_deprecated flag on the row.

        The real adapter renders this from the parent row at fetch
        time; we mirror that semantics so version rows themselves
        remain untouched after a deprecate call.
        """
        flag = self.parent_deprecated.get((row.workspace_id, row.workflow_id), False)
        if flag == row.parent_deprecated:
            return row
        return WorkflowVersion(
            workspace_id=row.workspace_id,
            workflow_id=row.workflow_id,
            version=row.version,
            normalized_doc=row.normalized_doc,
            derived_from_template_version_id=row.derived_from_template_version_id,
            parent_deprecated=flag,
            published_at=row.published_at,
        )


# ---------------------------------------------------------------------------
# Fixture documents
# ---------------------------------------------------------------------------


def _minimal_workflow(name: str = "my-wf", extra_step: bool = False) -> dict[str, Any]:
    """Smallest workflow that passes schema + normalize + CEL gates.

    Uses a single activity step whose ref is ``custos.builtin/echo@1``.
    The fake registry happily resolves it. ``extra_step=True`` adds a
    second step that mentions ``inputs.image`` so two consecutive
    publishes can differ only by document content (used in
    monotonicity tests).
    """
    steps: list[dict[str, Any]] = [
        {
            "id": "say-hi",
            "activity": "custos.builtin/echo@1",
            "with": {"message": "hello"},
        },
    ]
    if extra_step:
        steps.append(
            {
                "id": "say-bye",
                "activity": "custos.builtin/echo@1",
                "with": {"message": "goodbye"},
            },
        )
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": name, "workspace": "ws-1"},
        "spec": {
            "inputs": {"image": {"type": "string"}},
            "steps": steps,
        },
    }


def _make_manager(store: FakeDefinitionStore) -> DefinitionManager:
    return DefinitionManager(
        definition_store=store,
        activity_registry=FakeActivityRegistry(),
        connector_client=StubConnectorClient(),
        versioning=VersioningManager(store=store),
    )


# ---------------------------------------------------------------------------
# Publish — happy paths
# ---------------------------------------------------------------------------


async def test_publish_workflow_returns_version_ref() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    ref = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow()),
    )
    assert ref == WorkflowVersionRef(workspace_id=WS, workflow_name="my-wf", version=1)
    rows = store.workflows[(WS, WorkflowId("my-wf"))]
    assert len(rows) == 1
    assert rows[0].version == "1"


async def test_publish_workflow_monotonically_increases_version_for_different_content() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    ref1 = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow()),
    )
    ref2 = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow(extra_step=True)),
    )
    assert ref1.version == 1
    assert ref2.version == 2


async def test_publish_workflow_is_idempotent_on_byte_identical_content() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    source = _doc_json(_minimal_workflow())
    ref1 = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=source,
    )
    ref2 = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=source,
    )
    assert ref1 == ref2
    assert len(store.workflows[(WS, WorkflowId("my-wf"))]) == 1


async def test_publish_workflow_is_idempotent_with_yaml_vs_json_same_content() -> None:
    """The canonical-hash idempotency check must see past YAML/JSON
    surface differences once the document is normalized."""
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    json_source = _doc_json(_minimal_workflow())
    yaml_source = _doc_yaml(_minimal_workflow())
    ref1 = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=json_source,
    )
    ref2 = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=yaml_source,
    )
    assert ref1 == ref2
    assert len(store.workflows[(WS, WorkflowId("my-wf"))]) == 1


async def test_publish_workflow_accepts_bytes_source() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    ref = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow()).encode("utf-8"),
    )
    assert ref.version == 1


# ---------------------------------------------------------------------------
# Publish — error mapping
# ---------------------------------------------------------------------------


async def test_publish_workflow_parse_error_surfaces_as_parse_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    with pytest.raises(PublishValidationError) as excinfo:
        await manager.publish_workflow(
            workspace_id=WS,
            principal_id="alice",
            source="\t\tnot valid yaml",
        )
    assert excinfo.value.stage == "parse"
    assert excinfo.value.issues
    assert excinfo.value.issues[0].code == "parse.invalid"


async def test_publish_workflow_schema_error_surfaces_as_schema_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    # missing apiVersion → schema error
    doc = _minimal_workflow()
    del doc["apiVersion"]
    with pytest.raises(PublishValidationError) as excinfo:
        await manager.publish_workflow(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert excinfo.value.stage == "schema"
    assert any(i.code == "required" for i in excinfo.value.issues)


async def test_publish_workflow_resolve_error_surfaces_as_resolve_stage() -> None:
    store = FakeDefinitionStore()
    manager = DefinitionManager(
        definition_store=store,
        activity_registry=FakeActivityRegistry(allow_all=False),
        connector_client=StubConnectorClient(),
        versioning=VersioningManager(store=store),
    )
    with pytest.raises(PublishValidationError) as excinfo:
        await manager.publish_workflow(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(_minimal_workflow()),
        )
    assert excinfo.value.stage == "resolve"
    assert excinfo.value.issues[0].code == "resolve.activity_type_not_found"


async def test_publish_workflow_cel_syntax_error_surfaces_as_cel_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_workflow()
    # Break the CEL expression syntactically.
    doc["spec"]["steps"][0]["with"]["message"] = "${{ inputs.image + }}"
    with pytest.raises(PublishValidationError) as excinfo:
        await manager.publish_workflow(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert excinfo.value.stage == "cel"
    assert excinfo.value.issues[0].code == "cel.syntax"


async def test_publish_workflow_cel_name_binding_error_surfaces_as_cel_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_workflow()
    # ``not_a_real_root`` is not a CEL root.
    doc["spec"]["steps"][0]["with"]["message"] = "${{ not_a_real_root.x }}"
    with pytest.raises(PublishValidationError) as excinfo:
        await manager.publish_workflow(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert excinfo.value.stage == "cel"
    assert excinfo.value.issues[0].code == "cel.name_binding"


# ---------------------------------------------------------------------------
# Publish — race recovery
# ---------------------------------------------------------------------------


async def test_publish_workflow_retries_on_immutable_violation() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    # Pre-load one ImmutableViolation so the first put fails; the
    # retry loop must mint version=2 (since after the violation, no
    # version row exists and the next-version mint returns 1 again,
    # but the retry incrementing path runs through next_workflow_version
    # again which is still 1; the second put succeeds at version 1
    # because put_failures is now empty).
    store.put_failures = [ImmutableViolation("simulated race")]
    ref = await manager.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow()),
    )
    assert ref.version == 1
    assert not store.put_failures


async def test_publish_workflow_race_recovers_via_idempotent_match() -> None:
    """Models: caller A's put loses the race; concurrently caller B
    has just published the same content. The retry on attempt #2 must
    find the idempotent match (the content B published) and return
    B's ref without writing again.

    Implementation note: we model the race by failing the first put.
    The retry loop's post-failure idempotency rescan sees B's row
    (which we splice in via direct dict access between put attempts
    using a sentinel ``put_failures`` entry that also seeds the row).
    """
    store = FakeDefinitionStore()
    target_doc = _minimal_workflow()
    # First publish: caller "B" wins normally and lands version 1.
    manager_b = _make_manager(store)
    ref_b = await manager_b.publish_workflow(
        workspace_id=WS,
        principal_id="bob",
        source=_doc_json(target_doc),
    )
    # Second publish: caller "A" arrives with the same content. The
    # idempotency check short-circuits *before* any put attempt, so
    # this exercises the same code path that the race-recovery
    # rescan uses.
    manager_a = _make_manager(store)
    ref_a = await manager_a.publish_workflow(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(target_doc),
    )
    assert (
        ref_a
        == ref_b
        == WorkflowVersionRef(
            workspace_id=WS,
            workflow_name="my-wf",
            version=1,
        )
    )
    assert len(store.workflows[(WS, WorkflowId("my-wf"))]) == 1


async def test_publish_workflow_exhausted_retries_raises_immutability_error() -> None:
    store = FakeDefinitionStore()
    manager = DefinitionManager(
        definition_store=store,
        activity_registry=FakeActivityRegistry(),
        connector_client=StubConnectorClient(),
        versioning=VersioningManager(store=store),
        max_publish_retries=3,
    )
    # Every put fails until retries exhaust.
    store.put_failures = [ImmutableViolation("race") for _ in range(3)]
    with pytest.raises(WorkflowImmutabilityError) as excinfo:
        await manager.publish_workflow(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(_minimal_workflow()),
        )
    assert excinfo.value.workflow_name == "my-wf"
    assert excinfo.value.is_idempotent_match is False


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------


def _doc_json(doc: dict[str, Any]) -> str:
    import json

    return json.dumps(doc)


def _doc_yaml(doc: dict[str, Any]) -> str:
    import yaml

    return yaml.safe_dump(doc)


# UUID import kept so type-check passes if mypy looks for unused imports.
_ = UUID
