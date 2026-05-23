"""Workflow Template Manager (CS-IMPL-012 + CS-IMPL-013 + CS-IMPL-014).

Owns the WorkflowTemplate publish path and the materialize / extract
operations that bridge templates to workflows. The shape mirrors
:class:`custos_catalog.managers.definition.DefinitionManager` so the
two managers can be composed by the API surface (CS-IMPL-017).

Publish pipeline (CS-IMPL-012)
------------------------------

``TemplateManager.publish_template`` runs:

1. **parse**            — :func:`load_document`.
2. **schema**           — :func:`validate_template`.
3. **workspace-match**  — embedded ``metadata.workspace`` (when
   present) must match the URL workspace, same gate as
   :meth:`DefinitionManager.publish_workflow`.
4. **placeholders**     — :func:`validate_placeholder_declarations`
   for cross-declaration well-formedness (duplicate names, default
   type compatibility).
5. **normalize**        — :func:`normalize_template`.
6. **resolve**          — :func:`apply_template_resolutions` for any
   *concrete* references appearing in ``spec.workflow``.
7. **cel**              — :func:`validate_template_expressions`.
8. **idempotency**      — :func:`canonical_hash` scan over existing
   template versions of the same ``(workspace, name)``.
9. **mint + put**       — :class:`VersioningManager.next_template_version`
   + :meth:`DefinitionStoreProvider.put_workflow_template_version`,
   with the same ``ImmutableViolation`` retry loop the workflow path
   uses (no ``with_transaction`` on the SPL definition store).

The publish path returns a :class:`WorkflowTemplateVersionRef`. Errors
are surfaced as :class:`PublishValidationError` (re-using the
DefinitionManager envelope; new stage ``"placeholders"`` is added for
the placeholder-declaration gate).

Materialize / extract (CS-IMPL-013 / CS-IMPL-014) land in later
commits on the same module.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import WorkflowTemplateId, WorkspaceId
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    DefinitionStoreProvider,
    WorkflowTemplateVersion,
)
from custos_spl.pagination import Cursor, Page

from custos_catalog.cel_validate import (
    CelNameBindingError,
    CelSyntaxError,
    CelValidationError,
    validate_template_expressions,
)
from custos_catalog.managers.definition import (
    DefinitionManager,
    PublishValidationError,
    PublishValidationIssue,
    WorkflowVersionRef,
    _cel_issue,
    _schema_issue,
    _SubworkflowResolverAdapter,
)
from custos_catalog.normalize import (
    NormalizedTemplate,
    canonical_hash,
    normalize_template,
)
from custos_catalog.placeholders import (
    PlaceholderBindingError,
    PlaceholderDeclaration,
    PlaceholderDeclarationError,
    effective_bindings,
    parse_declarations,
    validate_placeholder_bindings,
    validate_placeholder_declarations,
)
from custos_catalog.resolve import (
    ActivityTypeRegistry,
    ConnectorClient,
    ResolveError,
    apply_template_resolutions,
)
from custos_catalog.schema.validate import (
    DocumentParseError,
    TemplateSchemaError,
    load_document,
    validate_template,
)
from custos_catalog.template_engine import (
    TemplateRenderError,
    render,
)
from custos_catalog.versioning import (
    TemplateImmutabilityError,
    VersioningManager,
)

_LOGGER = logging.getLogger(__name__)

#: Default upper bound on race-recovery retries when minted versions
#: collide. Matches :data:`DEFAULT_MAX_PUBLISH_RETRIES` on the
#: workflow manager.
DEFAULT_MAX_PUBLISH_RETRIES: Final[int] = 8


@dataclass(frozen=True, slots=True)
class WorkflowTemplateVersionRef:
    """A handle to a published workflow-template version.

    The catalog identifies a template version by the
    ``(workspace_id, template_name, version)`` triple. The SPL
    :class:`WorkflowTemplateVersion` carries no UUID column at v1, so
    the design's ``workflowTemplateVersionId`` field is constructed
    from this triple by upper layers when needed.
    """

    workspace_id: str
    template_name: str
    version: int


class TemplateNotFound(Exception):
    """Raised when a template / template version cannot be located."""

    code: str = "catalog.template_not_found"

    def __init__(
        self,
        *,
        workspace_id: str,
        template_name: str,
        version: int | None = None,
    ) -> None:
        if version is not None:
            msg = (
                f"template {template_name!r} version {version} not found "
                f"in workspace {workspace_id!r}"
            )
        else:
            msg = f"template {template_name!r} not found in workspace {workspace_id!r}"
        super().__init__(msg)
        self.workspace_id = workspace_id
        self.template_name = template_name
        self.version = version


class MaterializationError(Exception):
    """Raised when ``TemplateManager.materialize`` cannot produce a workflow.

    Carries both endpoints of the materialization — the template
    triple the caller asked for and the chained underlying error —
    so the API layer can surface a structured ``problem+json`` body
    that includes both refs.

    Attributes:
        workspace_id: The target workspace.
        template_name: Source template name.
        template_version: Source template version.
        target_workflow_name: The workflow name the caller asked the
            materialized workflow to be written under.
        cause: The underlying error. One of:

            * :class:`custos_catalog.placeholders.PlaceholderBindingError`
              — bindings did not satisfy the template's declarations.
            * :class:`TemplateRenderError` — the engine could not
              render the template body (unbound or embedded
              placeholders).
            * :class:`PublishValidationError` — the rendered document
              failed the workflow publish pipeline.
    """

    code: str = "catalog.template_materialization_failed"

    def __init__(
        self,
        *,
        workspace_id: str,
        template_name: str,
        template_version: int,
        target_workflow_name: str,
        cause: Exception,
    ) -> None:
        super().__init__(
            f"materialization of template {template_name!r}@{template_version} "
            f"-> workflow {target_workflow_name!r} failed: {cause}",
        )
        self.workspace_id = workspace_id
        self.template_name = template_name
        self.template_version = template_version
        self.target_workflow_name = target_workflow_name
        self.cause = cause


class TemplateManager:
    """Orchestrates Template publish + materialize + extract pipelines.

    One instance per process is sufficient; the manager is stateless
    and all of its collaborators are concurrency-safe by construction.
    """

    def __init__(
        self,
        *,
        definition_store: DefinitionStoreProvider,
        activity_registry: ActivityTypeRegistry,
        connector_client: ConnectorClient,
        versioning: VersioningManager,
        definition_manager: DefinitionManager | None = None,
        max_publish_retries: int = DEFAULT_MAX_PUBLISH_RETRIES,
    ) -> None:
        self._store = definition_store
        self._activity_registry = activity_registry
        self._connector_client = connector_client
        self._versioning = versioning
        if max_publish_retries < 1:
            raise ValueError("max_publish_retries must be >= 1")
        self._max_publish_retries = max_publish_retries
        self._subworkflow_adapter = _SubworkflowResolverAdapter(definition_store)
        #: Required only by :meth:`materialize`; the publish + read
        #: paths do not call into it. Late binding keeps the publish
        #: path testable without spinning up a workflow manager.
        self._definition_manager = definition_manager

    # ------------------------------------------------------------------
    # Publish pipeline (CS-IMPL-012)
    # ------------------------------------------------------------------

    async def publish_template(
        self,
        *,
        workspace_id: str,
        principal_id: str,
        source: str | bytes,
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersionRef:
        """Publish a workflow-template document and return the new version ref.

        ``source`` is the raw JSON or YAML body forwarded by the API
        gateway (or constructed in-process by the extractor in
        CS-IMPL-014). The pipeline runs schema → workspace-match →
        placeholder-declarations → normalize → resolve → CEL →
        idempotency-check → mint+put.

        Idempotency: if the canonical hash of the resolved document
        matches an existing version under the same name, the existing
        ref is returned and no new row is written.

        Race recovery: identical to
        :meth:`DefinitionManager.publish_workflow`'s optimistic put +
        :class:`ImmutableViolation` retry loop.

        Args:
            workspace_id: Target workspace.
            principal_id: Caller identity (audit only at v1).
            source: Raw template body (JSON or YAML).
            derived_from_workflow_version_id: Optional lineage link
                set by the extractor (CS-IMPL-014). Stored on the
                resulting :class:`WorkflowTemplateVersion`.

        Raises:
            PublishValidationError: If any pre-store stage fails. The
                ``stage`` field tells which gate failed.
            TemplateImmutabilityError: If race-recovery retries are
                exhausted.
        """
        _LOGGER.info(
            "publish_template start workspace=%s principal=%s source_bytes=%d "
            "derived_from_workflow_version_id=%s",
            workspace_id,
            principal_id,
            len(source) if isinstance(source, (bytes, str)) else -1,
            derived_from_workflow_version_id or "<none>",
        )

        # 1. parse + 2. schema-validate
        doc = self._parse_and_validate_schema(source)
        # 2a. enforce metadata.workspace (when present) matches the target
        self._enforce_workspace_match(doc, workspace_id=workspace_id)
        # 2b. placeholder-declaration well-formedness
        declarations = self._validate_declarations(doc)
        # 3. normalize
        normalized = normalize_template(doc)
        # 4. resolve concrete references (template typically has few/none)
        resolved = await self._resolve_refs(normalized, workspace_id=workspace_id)
        # 5. CEL validation
        self._validate_cel(resolved)
        # 6. idempotency: scan existing versions for matching canonical hash
        template_name = self._extract_template_name(resolved.document)
        idempotent = await self._find_idempotent_match(
            workspace_id=workspace_id,
            template_name=template_name,
            resolved=resolved,
        )
        if idempotent is not None:
            _LOGGER.info(
                "publish_template idempotent re-publish workspace=%s name=%s version=%s",
                workspace_id,
                template_name,
                idempotent.version,
            )
            return WorkflowTemplateVersionRef(
                workspace_id=workspace_id,
                template_name=template_name,
                version=int(idempotent.version),
            )
        # 7. mint + put with race-recovery loop
        _ = declarations  # declarations are informational at publish time
        return await self._mint_and_put(
            workspace_id=workspace_id,
            template_name=template_name,
            resolved=resolved,
            derived_from_workflow_version_id=derived_from_workflow_version_id,
        )

    # ------------------------------------------------------------------
    # Publish pipeline — internal helpers
    # ------------------------------------------------------------------

    def _parse_and_validate_schema(self, source: str | bytes) -> dict[str, Any]:
        try:
            doc = load_document(source)
        except DocumentParseError as exc:
            raise PublishValidationError(
                stage="parse",
                issues=[
                    PublishValidationIssue(
                        stage="parse",
                        path="",
                        code="parse.invalid",
                        message=str(exc),
                    ),
                ],
            ) from exc
        try:
            validate_template(doc)
        except TemplateSchemaError as exc:
            raise PublishValidationError(
                stage="schema",
                issues=[_schema_issue(i) for i in exc.issues],
            ) from exc
        return doc

    @staticmethod
    def _enforce_workspace_match(
        doc: Mapping[str, Any],
        *,
        workspace_id: str,
    ) -> None:
        """Reject documents whose ``metadata.workspace`` disagrees with the target.

        Mirrors the gate on
        :meth:`DefinitionManager.publish_workflow`. The template
        schema marks ``metadata.workspace`` optional and defers this
        cross-check to the manager layer.
        """
        metadata = doc.get("metadata", {})
        if not isinstance(metadata, Mapping):  # pragma: no cover - schema gate
            return
        doc_workspace = metadata.get("workspace")
        if doc_workspace is None:
            return
        if not isinstance(doc_workspace, str):  # pragma: no cover - schema gate
            return
        if doc_workspace == workspace_id:
            return
        raise PublishValidationError(
            stage="schema",
            issues=[
                PublishValidationIssue(
                    stage="schema",
                    path="metadata/workspace",
                    code="workspace_mismatch",
                    message=(
                        f"metadata.workspace {doc_workspace!r} does not match "
                        f"target workspace {workspace_id!r}"
                    ),
                ),
            ],
        )

    @staticmethod
    def _validate_declarations(
        doc: Mapping[str, Any],
    ) -> list[PlaceholderDeclaration]:
        """Run cross-declaration well-formedness on ``spec.placeholders[]``.

        The schema gate has already enforced per-item shape; this gate
        adds the cross-item checks (duplicate names, default type
        compatibility) that JSON Schema cannot express.

        Raises:
            PublishValidationError: With stage ``"placeholders"`` when
                any cross-declaration check fails.
        """
        spec = doc.get("spec", {})
        if not isinstance(spec, Mapping):  # pragma: no cover - schema gate
            return []
        raw = spec.get("placeholders", []) or []
        if not isinstance(raw, list):  # pragma: no cover - schema gate
            return []
        decls = parse_declarations(raw)
        try:
            validate_placeholder_declarations(decls)
        except PlaceholderDeclarationError as exc:
            raise PublishValidationError(
                stage="placeholders",
                issues=[
                    PublishValidationIssue(
                        stage="placeholders",
                        path=issue.path,
                        code=issue.code,
                        message=issue.message,
                    )
                    for issue in exc.issues
                ],
            ) from exc
        return decls

    async def _resolve_refs(
        self,
        normalized: NormalizedTemplate,
        *,
        workspace_id: str,
    ) -> NormalizedTemplate:
        try:
            return await apply_template_resolutions(
                normalized,
                activity_registry=self._activity_registry,
                definition_store=self._subworkflow_adapter,
                connector_client=self._connector_client,
                workspace_id=workspace_id,
            )
        except ResolveError as exc:
            raise PublishValidationError(
                stage="resolve",
                issues=[
                    PublishValidationIssue(
                        stage="resolve",
                        path=exc.ref or "",
                        code=exc.code,
                        message=str(exc),
                    ),
                ],
            ) from exc

    def _validate_cel(self, resolved: NormalizedTemplate) -> None:
        try:
            validate_template_expressions(resolved)
        except CelSyntaxError as exc:
            raise PublishValidationError(
                stage="cel",
                issues=[_cel_issue(i, code="cel.syntax") for i in exc.issues],
            ) from exc
        except CelNameBindingError as exc:
            raise PublishValidationError(
                stage="cel",
                issues=[_cel_issue(i, code="cel.name_binding") for i in exc.issues],
            ) from exc
        except CelValidationError as exc:  # pragma: no cover - defensive
            raise PublishValidationError(
                stage="cel",
                issues=[_cel_issue(i, code="cel.unknown") for i in exc.issues],
            ) from exc

    @staticmethod
    def _extract_template_name(document: Mapping[str, Any]) -> str:
        metadata = document.get("metadata", {})
        if not isinstance(metadata, Mapping):  # pragma: no cover - schema gate
            raise PublishValidationError(
                stage="schema",
                issues=[
                    PublishValidationIssue(
                        stage="schema",
                        path="metadata",
                        code="required",
                        message="metadata must be an object",
                    ),
                ],
            )
        name = metadata.get("name")
        if not isinstance(name, str) or not name:  # pragma: no cover - schema gate
            raise PublishValidationError(
                stage="schema",
                issues=[
                    PublishValidationIssue(
                        stage="schema",
                        path="metadata/name",
                        code="required",
                        message="metadata.name is required",
                    ),
                ],
            )
        return name

    async def _find_idempotent_match(
        self,
        *,
        workspace_id: str,
        template_name: str,
        resolved: NormalizedTemplate,
    ) -> WorkflowTemplateVersion | None:
        target_hash = canonical_hash(resolved.document)
        cursor: Cursor | None = None
        while True:
            page = await self._store.list_workflow_template_versions(
                WorkspaceId(workspace_id),
                WorkflowTemplateId(template_name),
                cursor=cursor,
                limit=100,
            )
            for row in page.items:
                if canonical_hash(dict(row.normalized_doc)) == target_hash:
                    return row
            if page.next_cursor is None:
                return None
            cursor = page.next_cursor

    async def _mint_and_put(
        self,
        *,
        workspace_id: str,
        template_name: str,
        resolved: NormalizedTemplate,
        derived_from_workflow_version_id: str | None,
    ) -> WorkflowTemplateVersionRef:
        for attempt in range(self._max_publish_retries):
            version = await self._versioning.next_template_version(
                WorkspaceId(workspace_id),
                WorkflowTemplateId(template_name),
            )
            try:
                await self._store.put_workflow_template_version(
                    WorkspaceId(workspace_id),
                    WorkflowTemplateId(template_name),
                    str(version),
                    resolved.document,
                    derived_from_workflow_version_id=derived_from_workflow_version_id,
                )
            except ImmutableViolation:
                _LOGGER.warning(
                    "publish_template race on attempt=%d workspace=%s name=%s "
                    "attempted_version=%d; rescanning for idempotency",
                    attempt,
                    workspace_id,
                    template_name,
                    version,
                )
                rematch = await self._find_idempotent_match(
                    workspace_id=workspace_id,
                    template_name=template_name,
                    resolved=resolved,
                )
                if rematch is not None:
                    return WorkflowTemplateVersionRef(
                        workspace_id=workspace_id,
                        template_name=template_name,
                        version=int(rematch.version),
                    )
                continue
            _LOGGER.info(
                "publish_template success workspace=%s name=%s version=%d",
                workspace_id,
                template_name,
                version,
            )
            return WorkflowTemplateVersionRef(
                workspace_id=workspace_id,
                template_name=template_name,
                version=version,
            )

        # All retries exhausted; surface a structured immutability error.
        final_next = await self._versioning.next_template_version(
            WorkspaceId(workspace_id),
            WorkflowTemplateId(template_name),
        )
        raise TemplateImmutabilityError(
            workspace_id=workspace_id,
            template_name=template_name,
            attempted_version=final_next - 1,
            next_available_version=final_next,
            is_idempotent_match=False,
        )

    # ------------------------------------------------------------------
    # Materialize (CS-IMPL-013)
    # ------------------------------------------------------------------

    async def materialize(
        self,
        *,
        workspace_id: str,
        template_name: str,
        template_version: int,
        target_workflow_name: str,
        bindings: Mapping[str, Any],
        principal_id: str,
    ) -> WorkflowVersionRef:
        """Materialize a template version into a Workflow version.

        The operation is the design's ``POST
        /v1/workspaces/{ws}/templates/{id}:materialize`` (see
        design/components/catalog-service § Operation: Materialize
        Workflow from Template). The pipeline:

        1. Fetch ``(workspace_id, template_name, template_version)``.
        2. Re-parse ``spec.placeholders`` from the stored doc.
        3. Validate ``bindings`` against declarations
           (:func:`validate_placeholder_bindings`); apply declared
           defaults (:func:`effective_bindings`).
        4. Render the template body with the engine
           (:func:`custos_catalog.template_engine.render`).
        5. Publish the rendered document via
           :meth:`DefinitionManager.publish_workflow` with
           ``derived_from_template_version_id`` set to
           ``"<template_name>@<template_version>"``. SPL has no UUID
           lookup at v1, so the triple-encoded reference is the
           canonical lineage handle.

        Idempotency / immutability: piggy-backs on the workflow
        publish path. Re-running the same materialization with the
        same bindings produces byte-identical output and resolves to
        the same :class:`WorkflowVersionRef`.

        Args:
            workspace_id: Target workspace (must equal the template's
                workspace).
            template_name: Source template name.
            template_version: Source template version (must already
                exist; this method does not auto-resolve "latest").
            target_workflow_name: ``metadata.name`` for the
                materialized workflow. Callers pick this; the
                template's own ``metadata.name`` is not used.
            bindings: Concrete placeholder values. Defaults from the
                declarations fill any unbound optional placeholders.
            principal_id: Caller identity (audit only at v1).

        Raises:
            RuntimeError: When the manager was constructed without a
                ``definition_manager`` (programmer error).
            TemplateNotFound: When the requested template version
                does not exist.
            MaterializationError: When binding validation, rendering,
                or the chained workflow publish fails. The ``cause``
                attribute carries the underlying exception so callers
                can surface structured issue lists.
        """
        if self._definition_manager is None:  # pragma: no cover - construction guard
            raise RuntimeError(
                "TemplateManager.materialize requires definition_manager= "
                "to be supplied at construction",
            )
        _LOGGER.info(
            "materialize start workspace=%s template=%s@%d target=%s principal=%s",
            workspace_id,
            template_name,
            template_version,
            target_workflow_name,
            principal_id,
        )

        # 1. Fetch the template version (raises TemplateNotFound).
        template_row = await self.get_template_version_by_ref(
            workspace_id=workspace_id,
            template_name=template_name,
            version=template_version,
        )
        template_doc = dict(template_row.normalized_doc)

        # 2. Re-parse declarations from the stored body.
        spec = template_doc.get("spec", {})
        raw_placeholders: list[Any] = []
        if isinstance(spec, Mapping):
            raw = spec.get("placeholders", []) or []
            if isinstance(raw, list):
                raw_placeholders = raw
        declarations = parse_declarations(raw_placeholders)

        # 3. Validate the supplied bindings + apply defaults.
        try:
            validate_placeholder_bindings(declarations, bindings)
        except PlaceholderBindingError as exc:
            raise MaterializationError(
                workspace_id=workspace_id,
                template_name=template_name,
                template_version=template_version,
                target_workflow_name=target_workflow_name,
                cause=exc,
            ) from exc
        full_bindings = effective_bindings(declarations, bindings)

        # 4. Render the template body.
        try:
            rendered = render(
                template_doc,
                full_bindings,
                target_workflow_name=target_workflow_name,
            )
        except TemplateRenderError as exc:
            raise MaterializationError(
                workspace_id=workspace_id,
                template_name=template_name,
                template_version=template_version,
                target_workflow_name=target_workflow_name,
                cause=exc,
            ) from exc

        # 5. Publish via the workflow pipeline.
        derived_id = f"{template_name}@{template_version}"
        try:
            ref = await self._definition_manager.publish_workflow(
                workspace_id=workspace_id,
                principal_id=principal_id,
                source=json.dumps(rendered),
                derived_from_template_version_id=derived_id,
            )
        except PublishValidationError as exc:
            raise MaterializationError(
                workspace_id=workspace_id,
                template_name=template_name,
                template_version=template_version,
                target_workflow_name=target_workflow_name,
                cause=exc,
            ) from exc

        _LOGGER.info(
            "materialize success workspace=%s template=%s@%d -> workflow=%s@%d",
            workspace_id,
            template_name,
            template_version,
            ref.workflow_name,
            ref.version,
        )
        return ref

    # ------------------------------------------------------------------
    # Read / lifecycle surface
    # ------------------------------------------------------------------

    async def list_template_versions(
        self,
        *,
        workspace_id: str,
        template_name: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
        filter: DefinitionListFilter | None = None,
    ) -> Page[WorkflowTemplateVersion]:
        """Page through versions of ``template_name`` in ``workspace_id``.

        Thin pass-through to
        :meth:`DefinitionStoreProvider.list_workflow_template_versions`.
        Mirrors :meth:`DefinitionManager.list_workflow_versions`.
        """
        return await self._store.list_workflow_template_versions(
            WorkspaceId(workspace_id),
            WorkflowTemplateId(template_name),
            filter=filter,
            cursor=cursor,
            limit=limit,
        )

    async def get_template_version_by_ref(
        self,
        *,
        workspace_id: str,
        template_name: str,
        version: int,
    ) -> WorkflowTemplateVersion:
        """Fetch a single template version by its triple.

        Raises:
            TemplateNotFound: When no version row matches the triple.
        """
        row = await self._store.get_workflow_template_version(
            WorkspaceId(workspace_id),
            WorkflowTemplateId(template_name),
            str(version),
        )
        if row is None:
            raise TemplateNotFound(
                workspace_id=workspace_id,
                template_name=template_name,
                version=version,
            )
        return row

    async def deprecate_template(
        self,
        *,
        workspace_id: str,
        template_name: str,
        principal_id: str,
        reason: str | None = None,
    ) -> None:
        """Flip the template's parent-row deprecation flag to ``True``.

        Same parent-row-toggle semantics as
        :meth:`DefinitionManager.deprecate_workflow`. Idempotent.

        Raises:
            TemplateNotFound: When no version row exists. The SPL
                cannot distinguish never-published from absent at v1.
        """
        # Probe for existence by listing the first page; an empty
        # listing means "no template to deprecate" since the parent
        # row is only materialised at first put.
        page = await self._store.list_workflow_template_versions(
            WorkspaceId(workspace_id),
            WorkflowTemplateId(template_name),
            limit=1,
        )
        if not page.items:
            raise TemplateNotFound(
                workspace_id=workspace_id,
                template_name=template_name,
            )
        await self._store.set_workflow_template_deprecated(
            WorkspaceId(workspace_id),
            WorkflowTemplateId(template_name),
            True,
        )
        _LOGGER.info(
            "deprecate_template workspace=%s name=%s principal=%s reason=%s",
            workspace_id,
            template_name,
            principal_id,
            reason or "<none>",
        )


__all__ = [
    "DEFAULT_MAX_PUBLISH_RETRIES",
    "MaterializationError",
    "TemplateManager",
    "TemplateNotFound",
    "WorkflowTemplateVersionRef",
]
