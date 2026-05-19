"""DefinitionStoreProvider — workflow + template version persistence.

Owns:
- `Workflow`, `WorkflowVersion`
- `WorkflowTemplate`, `WorkflowTemplateVersion`

Write-once on version rows. Any mutation of an existing version row —
including an idempotent re-put of identical content — surfaces as
`ImmutableViolation`. Deprecation toggles the parent (`Workflow` /
`WorkflowTemplate`) row only; versions themselves are immutable.

See `design/components/storage-provider-layer/design.md` § DefinitionStoreProvider.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Protocol, runtime_checkable

from custos_spl.ids import WorkflowId, WorkflowTemplateId, WorkspaceId
from custos_spl.pagination import Cursor, Page


@dataclass(frozen=True, slots=True)
class WorkflowVersion:
    """A single immutable workflow version row.

    `parent_deprecated` is a **denormalized read of the parent `Workflow`
    row's `deprecated` flag** at fetch time — it is NOT a property of
    the version itself. The version row is immutable; toggling
    deprecation via `set_workflow_deprecated` mutates the parent only,
    which means the value of `parent_deprecated` on two `WorkflowVersion`
    instances for the same `(workflow_id, version)` can differ across
    fetches. There is no version-level deprecation in v1.
    """

    workspace_id: WorkspaceId
    workflow_id: WorkflowId
    version: str
    normalized_doc: Mapping[str, Any]
    derived_from_template_version_id: str | None
    parent_deprecated: bool
    published_at: datetime


@dataclass(frozen=True, slots=True)
class WorkflowTemplateVersion:
    """A single immutable workflow-template version row.

    `parent_deprecated` denormalizes the parent `WorkflowTemplate` row's
    `deprecated` flag — see `WorkflowVersion` for the rationale.
    """

    workspace_id: WorkspaceId
    template_id: WorkflowTemplateId
    version: str
    normalized_doc: Mapping[str, Any]
    derived_from_workflow_version_id: str | None
    parent_deprecated: bool
    published_at: datetime


@dataclass(frozen=True, slots=True)
class DefinitionListFilter:
    """Optional filter for `list_*` calls.

    `published_after` / `published_before` are half-open: `>= after`,
    `< before`.

    There is intentionally no `deprecated` filter here: `list_*` is
    already scoped to a single `workflow_id` / `template_id`, and
    deprecation is parent-level (v1 has no version-level deprecation),
    so every row in a result set shares the same `parent_deprecated`
    value. Callers that want to skip a deprecated workflow inspect
    `parent_deprecated` on any returned `WorkflowVersion`.
    """

    published_after: datetime | None = None
    published_before: datetime | None = None


@runtime_checkable
class DefinitionStoreProvider(Protocol):
    """Workflow + template version store.

    All methods are workspace-scoped; `workspace_id` is the first arg
    on every call. The schema revision required by this build is
    `SCHEMA_REVISION`; adapters declare the revisions they implement
    via `declared_revisions` (see `migrations/`).
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    # ----- Workflow versions -----

    async def put_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersion:
        """Write a new workflow version.

        Raises `ImmutableViolation` if `(workflow_id, version)` already
        exists — even if the supplied content is byte-for-byte identical
        to the existing row. Callers that want idempotence MUST check
        existence first.
        """
        ...

    async def get_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
    ) -> WorkflowVersion | None:
        """Exact-version fetch. Returns `None` if absent."""
        ...

    async def list_workflow_versions(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowVersion]:
        """Paginated listing. Order is newest `published_at` first."""
        ...

    async def get_latest_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
    ) -> WorkflowVersion | None:
        """Convenience accessor for callers that don't care which version."""
        ...

    async def set_workflow_deprecated(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        deprecated: bool,
    ) -> None:
        """Mutates the parent `Workflow` row; versions remain immutable."""
        ...

    # ----- Workflow template versions (mirror) -----

    async def put_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersion:
        """Write a new template version. Same write-once semantics as workflow versions."""
        ...

    async def get_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
    ) -> WorkflowTemplateVersion | None: ...

    async def list_workflow_template_versions(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowTemplateVersion]: ...

    async def set_workflow_template_deprecated(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        deprecated: bool,
    ) -> None: ...


__all__ = [
    "DefinitionListFilter",
    "DefinitionStoreProvider",
    "WorkflowTemplateVersion",
    "WorkflowVersion",
]
