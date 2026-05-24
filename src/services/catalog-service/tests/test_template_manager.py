"""Tests for :class:`custos_catalog.managers.template.TemplateManager` publish path.

Covers CS-IMPL-012 and CS-IMPL-013 (materialize).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

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
)
from custos_catalog.managers.template import (
    MaterializationError,
    TemplateManager,
    TemplateNotFound,
    WorkflowTemplateVersionRef,
)
from custos_catalog.resolve import StubConnectorClient
from custos_catalog.versioning import TemplateImmutabilityError, VersioningManager
from tests._fakes import FakeMetadataStore

WS = WorkspaceId("ws-1")


# ---------------------------------------------------------------------------
# Hand-rolled fakes (re-use the same approach as test_definition_manager)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ActivityRow:
    namespace: str
    type: str
    version: str
    digest: str
    parent_deprecated: bool = False


class FakeActivityRegistry:
    def __init__(self, *, allow_all: bool = True) -> None:
        self.allow_all = allow_all

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> _ActivityRow | None:
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
    """In-memory ``DefinitionStoreProvider`` covering the template + workflow paths."""

    SCHEMA_REVISION: ClassVar[int] = 1
    workflows: dict[tuple[str, str], list[WorkflowVersion]] = field(default_factory=dict)
    templates: dict[tuple[str, str], list[WorkflowTemplateVersion]] = field(
        default_factory=dict,
    )
    workflow_deprecated: dict[tuple[str, str], bool] = field(default_factory=dict)
    template_deprecated: dict[tuple[str, str], bool] = field(default_factory=dict)
    put_template_failures: list[Exception] = field(default_factory=list)

    # ---------- Workflow versions (unused here but present for symmetry) ----------

    async def put_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersion:
        key = (str(workspace_id), str(workflow_id))
        rows = self.workflows.setdefault(key, [])
        for r in rows:
            if r.version == version:
                raise ImmutableViolation(
                    f"{workflow_id!r} version {version} already exists",
                )
        new_row = WorkflowVersion(
            workspace_id=workspace_id,
            workflow_id=workflow_id,
            version=version,
            normalized_doc=dict(normalized_doc),
            derived_from_template_version_id=derived_from_template_version_id,
            parent_deprecated=self.workflow_deprecated.get(key, False),
            published_at=datetime.now(tz=UTC),
        )
        rows.append(new_row)
        return new_row

    async def get_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
    ) -> WorkflowVersion | None:
        key = (str(workspace_id), str(workflow_id))
        for r in self.workflows.get(key, []):
            if r.version == version:
                return self._with_current_workflow_dep(r, key)
        return None

    async def list_workflow_versions(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowVersion]:
        key = (str(workspace_id), str(workflow_id))
        items = list(self.workflows.get(key, []))
        items.sort(key=lambda r: r.published_at, reverse=True)
        return Page(
            items=tuple(self._with_current_workflow_dep(r, key) for r in items),
            next_cursor=None,
        )

    async def get_latest_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
    ) -> WorkflowVersion | None:
        key = (str(workspace_id), str(workflow_id))
        rows = self.workflows.get(key, [])
        if not rows:
            return None
        latest = max(rows, key=lambda r: r.published_at)
        return self._with_current_workflow_dep(latest, key)

    async def set_workflow_deprecated(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        deprecated: bool,
    ) -> None:
        self.workflow_deprecated[(str(workspace_id), str(workflow_id))] = deprecated

    def _with_current_workflow_dep(
        self,
        row: WorkflowVersion,
        key: tuple[str, str],
    ) -> WorkflowVersion:
        return WorkflowVersion(
            workspace_id=row.workspace_id,
            workflow_id=row.workflow_id,
            version=row.version,
            normalized_doc=row.normalized_doc,
            derived_from_template_version_id=row.derived_from_template_version_id,
            parent_deprecated=self.workflow_deprecated.get(key, False),
            published_at=row.published_at,
        )

    # ---------- Template versions (the path under test) ----------

    async def put_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersion:
        if self.put_template_failures:
            raise self.put_template_failures.pop(0)
        key = (str(workspace_id), str(template_id))
        rows = self.templates.setdefault(key, [])
        for r in rows:
            if r.version == version:
                raise ImmutableViolation(
                    f"{template_id!r} version {version} already exists",
                )
        new_row = WorkflowTemplateVersion(
            workspace_id=workspace_id,
            template_id=template_id,
            version=version,
            normalized_doc=dict(normalized_doc),
            derived_from_workflow_version_id=derived_from_workflow_version_id,
            parent_deprecated=self.template_deprecated.get(key, False),
            published_at=datetime.now(tz=UTC),
        )
        rows.append(new_row)
        return new_row

    async def get_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
    ) -> WorkflowTemplateVersion | None:
        key = (str(workspace_id), str(template_id))
        for r in self.templates.get(key, []):
            if r.version == version:
                return self._with_current_template_dep(r, key)
        return None

    async def list_workflow_template_versions(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowTemplateVersion]:
        key = (str(workspace_id), str(template_id))
        items = list(self.templates.get(key, []))
        items.sort(key=lambda r: r.published_at, reverse=True)
        return Page(
            items=tuple(self._with_current_template_dep(r, key) for r in items),
            next_cursor=None,
        )

    async def set_workflow_template_deprecated(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        deprecated: bool,
    ) -> None:
        self.template_deprecated[(str(workspace_id), str(template_id))] = deprecated

    def _with_current_template_dep(
        self,
        row: WorkflowTemplateVersion,
        key: tuple[str, str],
    ) -> WorkflowTemplateVersion:
        return WorkflowTemplateVersion(
            workspace_id=row.workspace_id,
            template_id=row.template_id,
            version=row.version,
            normalized_doc=row.normalized_doc,
            derived_from_workflow_version_id=row.derived_from_workflow_version_id,
            parent_deprecated=self.template_deprecated.get(key, False),
            published_at=row.published_at,
        )


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


def _minimal_template(name: str = "my-tmpl") -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": name, "workspace": "ws-1"},
        "spec": {
            "placeholders": [
                {
                    "name": "scanActivity",
                    "type": "activityRef",
                    "activityType": "vuln-scan",
                    "required": True,
                },
                {
                    "name": "topic",
                    "type": "string",
                    "required": False,
                    "default": "default-topic",
                },
            ],
            "workflow": {
                "inputs": {"image": {"type": "string"}},
                "steps": [
                    {
                        "id": "scan",
                        "activity": "${{ placeholders.scanActivity }}",
                    },
                ],
            },
        },
    }


def _doc_json(doc: dict[str, Any]) -> str:
    return json.dumps(doc)


def _doc_yaml(doc: dict[str, Any]) -> str:
    import yaml

    return yaml.safe_dump(doc)


def _make_manager(store: FakeDefinitionStore) -> TemplateManager:
    versioning = VersioningManager(store=store)
    registry = FakeActivityRegistry()
    connector = StubConnectorClient()
    metadata_store = FakeMetadataStore()
    definition_manager = DefinitionManager(
        definition_store=store,
        metadata_store=metadata_store,  # type: ignore[arg-type]
        activity_registry=registry,
        connector_client=connector,
        versioning=versioning,
    )
    return TemplateManager(
        definition_store=store,
        metadata_store=metadata_store,  # type: ignore[arg-type]
        activity_registry=registry,
        connector_client=connector,
        versioning=versioning,
        definition_manager=definition_manager,
    )


# ---------------------------------------------------------------------------
# Publish — happy paths
# ---------------------------------------------------------------------------


async def test_publish_template_returns_version_ref() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    ref = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    assert ref == WorkflowTemplateVersionRef(
        workspace_id=WS,
        template_name="my-tmpl",
        version=1,
    )
    rows = store.templates[(WS, WorkflowTemplateId("my-tmpl"))]
    assert len(rows) == 1
    assert rows[0].version == "1"


async def test_publish_template_monotonic_increment_for_distinct_content() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    # Change content to force a new version.
    doc = _minimal_template()
    doc["spec"]["workflow"]["steps"][0]["id"] = "scan2"
    ref = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(doc),
    )
    assert ref.version == 2


async def test_publish_template_idempotent_byte_identical() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    body = _doc_json(_minimal_template())
    ref1 = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=body,
    )
    ref2 = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=body,
    )
    assert ref1 == ref2
    assert len(store.templates[(WS, WorkflowTemplateId("my-tmpl"))]) == 1


async def test_publish_template_idempotent_json_vs_yaml() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_template()
    ref1 = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(doc),
    )
    ref2 = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_yaml(doc),
    )
    assert ref1 == ref2


async def test_publish_template_accepts_bytes_source() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    ref = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()).encode("utf-8"),
    )
    assert ref.version == 1


async def test_publish_template_persists_derived_from_workflow_version_id() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    ref = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
        derived_from_workflow_version_id="src-wf@3",
    )
    row = store.templates[(WS, WorkflowTemplateId("my-tmpl"))][0]
    assert row.derived_from_workflow_version_id == "src-wf@3"
    assert ref.version == 1


# ---------------------------------------------------------------------------
# Publish — error mapping
# ---------------------------------------------------------------------------


async def test_publish_template_parse_error_surfaces_as_parse_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    with pytest.raises(PublishValidationError) as exc:
        await manager.publish_template(
            workspace_id=WS,
            principal_id="alice",
            source="\t\tnot valid yaml",
        )
    assert exc.value.stage == "parse"


async def test_publish_template_schema_error_surfaces_as_schema_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_template()
    del doc["apiVersion"]
    with pytest.raises(PublishValidationError) as exc:
        await manager.publish_template(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert exc.value.stage == "schema"


async def test_publish_template_workspace_mismatch_is_schema_error() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_template()
    doc["metadata"]["workspace"] = "other-ws"
    with pytest.raises(PublishValidationError) as exc:
        await manager.publish_template(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert exc.value.stage == "schema"
    issue = exc.value.issues[0]
    assert issue.code == "workspace_mismatch"
    assert issue.path == "metadata/workspace"


async def test_publish_template_duplicate_placeholder_names_is_placeholders_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_template()
    doc["spec"]["placeholders"].append(
        {
            "name": "scanActivity",
            "type": "string",
        },
    )
    with pytest.raises(PublishValidationError) as exc:
        await manager.publish_template(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert exc.value.stage == "placeholders"
    assert exc.value.issues[0].code == "duplicate_name"


async def test_publish_template_default_type_mismatch_surfaces_as_placeholders_stage() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    doc = _minimal_template()
    # topic is type=string; set default to an integer.
    doc["spec"]["placeholders"][1]["default"] = 99
    with pytest.raises(PublishValidationError) as exc:
        await manager.publish_template(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(doc),
        )
    assert exc.value.stage == "placeholders"
    assert exc.value.issues[0].code == "default_type_mismatch"


# ---------------------------------------------------------------------------
# Race recovery
# ---------------------------------------------------------------------------


async def test_publish_template_retries_on_immutable_violation() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    store.put_template_failures = [ImmutableViolation("simulated race")]
    ref = await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    assert ref.version == 1


async def test_publish_template_race_recovers_via_idempotent_match() -> None:
    store = FakeDefinitionStore()
    a = _make_manager(store)
    b = _make_manager(store)
    body = _doc_json(_minimal_template())
    ref_b = await b.publish_template(
        workspace_id=WS,
        principal_id="bob",
        source=body,
    )
    # Caller A's second publish: same content → idempotency short-circuit
    ref_a = await a.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=body,
    )
    assert ref_a == ref_b


async def test_publish_template_exhausted_retries_raise_template_immutability() -> None:
    store = FakeDefinitionStore()
    manager = TemplateManager(
        definition_store=store,
        metadata_store=FakeMetadataStore(),  # type: ignore[arg-type]
        activity_registry=FakeActivityRegistry(),
        connector_client=StubConnectorClient(),
        versioning=VersioningManager(store=store),
        max_publish_retries=2,
    )
    store.put_template_failures = [
        ImmutableViolation("forever"),
        ImmutableViolation("forever"),
    ]
    with pytest.raises(TemplateImmutabilityError):
        await manager.publish_template(
            workspace_id=WS,
            principal_id="alice",
            source=_doc_json(_minimal_template()),
        )


# ---------------------------------------------------------------------------
# Read / lifecycle surface
# ---------------------------------------------------------------------------


async def test_list_template_versions_returns_page() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    page = await manager.list_template_versions(
        workspace_id=WS,
        template_name="my-tmpl",
    )
    assert len(page.items) == 1


async def test_list_template_versions_empty_when_absent() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    page = await manager.list_template_versions(
        workspace_id=WS,
        template_name="ghost",
    )
    assert page.items == ()


async def test_get_template_version_by_ref_returns_row() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    row = await manager.get_template_version_by_ref(
        workspace_id=WS,
        template_name="my-tmpl",
        version=1,
    )
    assert row.version == "1"


async def test_get_template_version_by_ref_raises_when_absent() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    with pytest.raises(TemplateNotFound):
        await manager.get_template_version_by_ref(
            workspace_id=WS,
            template_name="ghost",
            version=1,
        )


async def test_deprecate_template_flips_flag() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    await manager.deprecate_template(
        workspace_id=WS,
        template_name="my-tmpl",
        principal_id="alice",
        reason="superseded",
    )
    assert store.template_deprecated[(WS, "my-tmpl")] is True
    row = await manager.get_template_version_by_ref(
        workspace_id=WS,
        template_name="my-tmpl",
        version=1,
    )
    assert row.parent_deprecated is True


async def test_deprecate_template_idempotent() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    await manager.deprecate_template(
        workspace_id=WS,
        template_name="my-tmpl",
        principal_id="alice",
    )
    await manager.deprecate_template(
        workspace_id=WS,
        template_name="my-tmpl",
        principal_id="alice",
    )
    assert store.template_deprecated[(WS, "my-tmpl")] is True


async def test_deprecate_template_unknown_raises() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    with pytest.raises(TemplateNotFound):
        await manager.deprecate_template(
            workspace_id=WS,
            template_name="ghost",
            principal_id="alice",
        )


# ---------------------------------------------------------------------------
# Materialize (CS-IMPL-013)
# ---------------------------------------------------------------------------


async def test_materialize_publishes_workflow_with_substituted_activity() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    ref = await manager.materialize(
        workspace_id=WS,
        template_name="my-tmpl",
        template_version=1,
        target_workflow_name="scan-wf",
        bindings={"scanActivity": "custos.builtin/vuln-scan@2.0.0"},
        principal_id="alice",
    )
    assert ref.workflow_name == "scan-wf"
    assert ref.version == 1
    rows = store.workflows[(WS, WorkflowId("scan-wf"))]
    assert len(rows) == 1
    step = rows[0].normalized_doc["spec"]["steps"][0]
    # The placeholder has been substituted with the concrete activity ref.
    assert step["activity"].startswith("custos.builtin/vuln-scan")


async def test_materialize_records_derived_from_template_version_id() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    await manager.materialize(
        workspace_id=WS,
        template_name="my-tmpl",
        template_version=1,
        target_workflow_name="scan-wf",
        bindings={"scanActivity": "custos.builtin/vuln-scan@2.0.0"},
        principal_id="alice",
    )
    row = store.workflows[(WS, WorkflowId("scan-wf"))][0]
    assert row.derived_from_template_version_id == "my-tmpl@1"


async def test_materialize_is_idempotent_for_same_bindings() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    bindings = {"scanActivity": "custos.builtin/vuln-scan@2.0.0"}
    ref1 = await manager.materialize(
        workspace_id=WS,
        template_name="my-tmpl",
        template_version=1,
        target_workflow_name="scan-wf",
        bindings=bindings,
        principal_id="alice",
    )
    ref2 = await manager.materialize(
        workspace_id=WS,
        template_name="my-tmpl",
        template_version=1,
        target_workflow_name="scan-wf",
        bindings=bindings,
        principal_id="alice",
    )
    assert ref1 == ref2
    assert len(store.workflows[(WS, WorkflowId("scan-wf"))]) == 1


async def test_materialize_unknown_template_raises_template_not_found() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    with pytest.raises(TemplateNotFound):
        await manager.materialize(
            workspace_id=WS,
            template_name="ghost",
            template_version=1,
            target_workflow_name="scan-wf",
            bindings={"scanActivity": "custos.builtin/vuln-scan@2.0.0"},
            principal_id="alice",
        )


async def test_materialize_missing_required_binding_raises_materialization_error() -> None:
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    with pytest.raises(MaterializationError) as exc:
        await manager.materialize(
            workspace_id=WS,
            template_name="my-tmpl",
            template_version=1,
            target_workflow_name="scan-wf",
            bindings={},  # scanActivity is required
            principal_id="alice",
        )
    assert exc.value.template_name == "my-tmpl"
    assert exc.value.template_version == 1
    assert exc.value.target_workflow_name == "scan-wf"
    # cause should be a PlaceholderBindingError
    from custos_catalog.placeholders import PlaceholderBindingError

    assert isinstance(exc.value.cause, PlaceholderBindingError)


async def test_materialize_chained_publish_failure_surfaces_through_cause() -> None:
    # Build a template that's valid as a template but produces an
    # invalid workflow: the activity placeholder resolves to a value
    # that doesn't match the workflow activity-ref pattern.
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    with pytest.raises(MaterializationError) as exc:
        await manager.materialize(
            workspace_id=WS,
            template_name="my-tmpl",
            template_version=1,
            target_workflow_name="scan-wf",
            bindings={"scanActivity": "NOT A VALID ACTIVITY REF"},
            principal_id="alice",
        )
    assert isinstance(exc.value.cause, PublishValidationError)


async def test_materialize_applies_default_for_optional_placeholder() -> None:
    # The template has `topic` with a default of "default-topic"; we
    # only supply scanActivity. Ensure the materialized doc still
    # publishes successfully (the default would be substituted into
    # any topic reference in the workflow body — for this test we
    # just verify the materialize path doesn't reject the input).
    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager.publish_template(
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_template()),
    )
    ref = await manager.materialize(
        workspace_id=WS,
        template_name="my-tmpl",
        template_version=1,
        target_workflow_name="scan-wf",
        bindings={"scanActivity": "custos.builtin/vuln-scan@2.0.0"},
        principal_id="alice",
    )
    assert ref.version == 1


# ---------------------------------------------------------------------------
# Extract (CS-IMPL-014)
# ---------------------------------------------------------------------------


def _minimal_workflow_doc(name: str = "src-wf") -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": name, "workspace": "ws-1"},
        "spec": {
            "inputs": {"image": {"type": "string"}},
            "steps": [
                {
                    "id": "scan",
                    "activity": "custos.builtin/vuln-scan@2.0.0",
                },
            ],
        },
    }


async def test_extract_from_workflow_publishes_template_and_records_lineage() -> None:
    from custos_catalog.extract import Selector

    store = FakeDefinitionStore()
    manager = _make_manager(store)
    # First publish a source workflow.
    await manager._definition_manager.publish_workflow(  # type: ignore[union-attr]
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow_doc()),
    )
    ref = await manager.extract_from_workflow(
        workspace_id=WS,
        principal_id="alice",
        source_workflow_name="src-wf",
        source_workflow_version=1,
        selectors=[
            Selector(
                path="spec.steps[0].activity",
                placeholder_name="scanActivity",
                placeholder_type="activityRef",
                activity_type="vuln-scan",
            ),
        ],
        template_name="extracted-tmpl",
    )
    assert ref.template_name == "extracted-tmpl"
    row = store.templates[(WS, WorkflowTemplateId("extracted-tmpl"))][0]
    assert row.derived_from_workflow_version_id == "src-wf@1"
    # Template body has the placeholder substitution.
    rendered_step = row.normalized_doc["spec"]["workflow"]["steps"][0]
    assert rendered_step["activity"].startswith("${{")


async def test_extract_unknown_workflow_raises_workflow_not_found() -> None:
    from custos_catalog.extract import Selector
    from custos_catalog.managers.definition import WorkflowNotFound

    store = FakeDefinitionStore()
    manager = _make_manager(store)
    with pytest.raises(WorkflowNotFound):
        await manager.extract_from_workflow(
            workspace_id=WS,
            principal_id="alice",
            source_workflow_name="ghost",
            source_workflow_version=1,
            selectors=[
                Selector(path="spec.x", placeholder_name="x", placeholder_type="string"),
            ],
            template_name="t",
        )


async def test_extract_bad_selector_raises_extraction_error() -> None:
    from custos_catalog.extract import ExtractError, Selector
    from custos_catalog.managers.template import ExtractionError

    store = FakeDefinitionStore()
    manager = _make_manager(store)
    await manager._definition_manager.publish_workflow(  # type: ignore[union-attr]
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow_doc()),
    )
    with pytest.raises(ExtractionError) as exc:
        await manager.extract_from_workflow(
            workspace_id=WS,
            principal_id="alice",
            source_workflow_name="src-wf",
            source_workflow_version=1,
            selectors=[
                Selector(
                    path="spec.does.not.exist",
                    placeholder_name="ghost",
                    placeholder_type="string",
                ),
            ],
            template_name="t",
        )
    assert isinstance(exc.value.cause, ExtractError)
    assert exc.value.source_workflow_name == "src-wf"
    assert exc.value.template_name == "t"


async def test_extract_then_materialize_yields_byte_equal_workflow() -> None:
    from custos_catalog.extract import Selector

    store = FakeDefinitionStore()
    manager = _make_manager(store)
    # 1. Publish source workflow.
    await manager._definition_manager.publish_workflow(  # type: ignore[union-attr]
        workspace_id=WS,
        principal_id="alice",
        source=_doc_json(_minimal_workflow_doc()),
    )
    source_row = store.workflows[(WS, WorkflowId("src-wf"))][0]
    source_doc = dict(source_row.normalized_doc)

    # 2. Extract a template that parameterizes the activity.
    await manager.extract_from_workflow(
        workspace_id=WS,
        principal_id="alice",
        source_workflow_name="src-wf",
        source_workflow_version=1,
        selectors=[
            Selector(
                path="spec.steps[0].activity",
                placeholder_name="scanActivity",
                placeholder_type="activityRef",
                activity_type="vuln-scan",
            ),
        ],
        template_name="round-trip-tmpl",
    )

    # 3. Materialize that template with the original binding into a
    # different workflow name. The resulting workflow spec must match
    # the source workflow's spec byte-for-byte after canonicalization.
    await manager.materialize(
        workspace_id=WS,
        template_name="round-trip-tmpl",
        template_version=1,
        target_workflow_name="rebuilt-wf",
        bindings={"scanActivity": "custos.builtin/vuln-scan@2.0.0"},
        principal_id="alice",
    )
    rebuilt_row = store.workflows[(WS, WorkflowId("rebuilt-wf"))][0]
    # Compare spec bodies (metadata.name differs).
    assert rebuilt_row.normalized_doc["spec"] == source_doc["spec"]
