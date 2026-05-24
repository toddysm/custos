"""Workflow Definition Manager (CS-IMPL-010 + CS-IMPL-011).

Orchestrates the **publish pipeline** for Workflow documents and the
**read / lifecycle** surface on top of
:class:`custos_spl.interfaces.definition_store.DefinitionStoreProvider`.

Publish pipeline stages
-----------------------

``DefinitionManager.publish_workflow`` runs the canonical sequence:

1. **parse**     — :func:`load_document` (JSON or YAML → ``dict``).
2. **schema**    — :func:`validate_workflow` (Draft 2020-12 schema).
3. **normalize** — :func:`normalize_workflow` (canonicalize keys,
   discover :class:`RefResolutionSlot` slots).
4. **resolve**   — :func:`apply_resolutions` (resolve activity / sub-
   workflow / connector refs via the registries + connector client).
5. **cel**       — :func:`validate_expressions` (parse + name-bind
   every CEL slot under the resolved document).
6. **idempotency** — :func:`canonical_hash` over the resolved document;
   look for an existing version with the same hash and short-circuit.
7. **mint + put** — :class:`VersioningManager` mints the next version
   integer; :meth:`DefinitionStoreProvider.put_workflow_version`
   commits it. ``ImmutableViolation`` triggers a retry with a fresh
   version up to ``max_publish_retries`` times — this is the only
   race-safety story available since the SPL surface does not expose
   ``with_transaction`` on the definition store.

Every stage that fails surfaces as :class:`PublishValidationError`
with the failing :attr:`PublishValidationError.stage` and the list of
:class:`PublishValidationIssue` objects. Callers (the FastAPI router
in CS-IMPL-017) map these to HTTP 400 with a stable envelope.

Read / lifecycle surface (CS-IMPL-011)
--------------------------------------

* :meth:`list_workflow_versions` — paginated list of versions for a
  given ``(workspace, name)``.
* :meth:`get_workflow_version_by_ref` — single-version fetch by
  ``(workspace, name, version)`` triple.
* :meth:`deprecate_workflow` — toggles the workflow's deprecation
  flag. **Parent-row toggle only** per the design contract; the
  version rows themselves are never mutated, with the denormalized
  ``parent_deprecated`` flag surfacing on subsequent fetches.

Two surface items called out in issue #212 are **deferred** to a
follow-up because the SPL does not yet expose them:

* ``list_workflows(workspace_id)`` — workspace-wide workflow
  enumeration. The SPL has no list-workflows method (only
  list-workflow-versions for a known name). Adding this requires
  either an SPL extension or a Catalog-side index; design has no
  REST endpoint depending on it for v1.
* ``get_workflow_version_by_id(workflow_version_id)`` — UUID-keyed
  fetch. The SPL's :class:`WorkflowVersion` has no UUID field, so the
  workspaceless ``GET /v1/workflows/{workspaceId}/{workflowName}@{version}``
  REST route uses the triple wire form (delegates to
  ``get_workflow_version_by_ref`` under the hood). The bare
  UUID PK is reserved for a future SPL evolution.

Both deferrals are documented in the PR body that lands this change.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal
from uuid import UUID

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import WorkflowId, WorkspaceId
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    DefinitionStoreProvider,
    WorkflowVersion,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.pagination import Cursor, Page

from custos_catalog import _telemetry as telemetry
from custos_catalog.audit import (
    audit_workflow_deprecated,
    audit_workflow_published,
)
from custos_catalog.cel_validate import (
    CelNameBindingError,
    CelSyntaxError,
    CelValidationError,
    CelValidationIssue,
    validate_expressions,
)
from custos_catalog.normalize import (
    NormalizedWorkflow,
    canonical_hash,
    normalize_workflow,
)
from custos_catalog.resolve import (
    ActivityTypeRegistry,
    ConnectorClient,
    ResolveError,
    apply_resolutions,
)
from custos_catalog.schema.validate import (
    DocumentParseError,
    SchemaValidationIssue,
    WorkflowSchemaError,
    load_document,
    validate_workflow,
)
from custos_catalog.versioning import (
    VersioningManager,
    WorkflowImmutabilityError,
)

_LOGGER = logging.getLogger(__name__)

#: Default upper bound on race-recovery retries when minted versions
#: collide. The expected steady-state retry count is 0; 8 leaves
#: ample headroom for the unlikely case of many concurrent publishers.
DEFAULT_MAX_PUBLISH_RETRIES: Final[int] = 8


PublishStage = Literal["parse", "schema", "placeholders", "normalize", "resolve", "cel"]


@dataclass(frozen=True, slots=True)
class WorkflowVersionRef:
    """A handle to a published workflow version.

    The catalog identifies a workflow version by the
    ``(workspace_id, workflow_name, version)`` triple at every layer
    above the database. The SPL :class:`WorkflowVersion` carries no
    UUID column at v1, so the design's ``workflowVersionId`` field
    is constructed from this triple by upper layers when needed.

    Attributes:
        workspace_id: The workspace under which the workflow lives.
        workflow_name: The friendly URL slug for the workflow
            (``WorkflowId`` at the SPL layer is the slug itself).
        version: The integer version. Monotonically increases per
            ``(workspace_id, workflow_name)`` starting at 1.
    """

    workspace_id: str
    workflow_name: str
    version: int


@dataclass(frozen=True, slots=True)
class PublishValidationIssue:
    """One issue from any publish-time gate.

    Normalises across the three issue-shaped error types we collect
    (:class:`SchemaValidationIssue`, :class:`CelValidationIssue`,
    plain resolver errors) so the API surface has one envelope.

    Attributes:
        stage: Which publish-pipeline stage emitted the issue.
        path: The document path of the offending field (JSON-Pointer
            style for schema, dotted for CEL, or the raw reference
            string for resolver errors).
        code: A stable machine-readable code (e.g. ``"required"`` for
            JSON Schema, ``"resolve.activity_type_not_found"`` for
            resolver errors, ``"cel.syntax"`` for CEL parse errors).
        message: Human-readable explanation, suitable for surfacing
            verbatim in the API response.
    """

    stage: PublishStage
    path: str
    code: str
    message: str


class PublishValidationError(Exception):
    """Raised when any pre-store publish-pipeline stage fails.

    Catalog's API surface maps this to HTTP 400 with the structured
    issue list. The :attr:`stage` field tells the client *which* gate
    failed; the :attr:`issues` list carries every failure from that
    gate (schema and CEL collect-all in one pass; resolver fails on
    the first error per its design).
    """

    code: str = "catalog.publish_validation_failed"

    def __init__(
        self,
        stage: PublishStage,
        issues: list[PublishValidationIssue],
    ) -> None:
        self.stage = stage
        self.issues = issues
        rendered = "; ".join(f"{issue.path or '<root>'} -> {issue.message}" for issue in issues)
        super().__init__(
            f"publish failed at stage {stage!r}: {len(issues)} issue(s): {rendered}",
        )


class WorkflowNotFound(Exception):
    """Raised when a workflow / workflow version cannot be located."""

    code: str = "catalog.workflow_not_found"

    def __init__(
        self,
        *,
        workspace_id: str,
        workflow_name: str,
        version: int | None = None,
    ) -> None:
        if version is not None:
            msg = (
                f"workflow {workflow_name!r} version {version} not found "
                f"in workspace {workspace_id!r}"
            )
        else:
            msg = f"workflow {workflow_name!r} not found in workspace {workspace_id!r}"
        super().__init__(msg)
        self.workspace_id = workspace_id
        self.workflow_name = workflow_name
        self.version = version


class _SubworkflowResolverAdapter:
    """Adapt :class:`DefinitionStoreProvider` to the resolver's narrow Protocol.

    The :func:`custos_catalog.resolve.resolve_subworkflow_ref` helper
    expects a store with two methods:

    * ``get_workflow_version_by_id(workflow_version_id: UUID)`` — the
      UUID-keyed lookup used by the bare-UUID reference form.
    * ``get_workflow_version_by_name(workspace, name, version)`` —
      the friendly-triple form.

    The SPL exposes only the by-name form (under the slightly
    different name ``get_workflow_version``); there is no UUID column
    on :class:`WorkflowVersion` at v1, so the by-id form has no
    backing storage operation. We return ``None`` from the by-id
    adapter, which the resolver surfaces as
    :class:`SubworkflowNotFound`. Sub-workflow references written as
    bare UUIDs therefore cannot resolve at v1; the recommended form
    is the ``<workspace>/<name>@<version>`` triple.
    """

    def __init__(self, store: DefinitionStoreProvider) -> None:
        self._store = store

    async def get_workflow_version_by_id(
        self,
        workflow_version_id: UUID,
    ) -> Any:
        _LOGGER.debug(
            "_SubworkflowResolverAdapter: by-id lookup attempted for %s; "
            "SPL has no UUID-keyed surface at v1, returning None",
            workflow_version_id,
        )
        return None

    async def get_workflow_version_by_name(
        self,
        workspace: str,
        name: str,
        version: str,
    ) -> Any:
        return await self._store.get_workflow_version(
            WorkspaceId(workspace),
            WorkflowId(name),
            version,
        )


class DefinitionManager:
    """Orchestrates Workflow publish + read + deprecation pipelines.

    Holds the :class:`DefinitionStoreProvider` and the collaborators
    needed by the publish pipeline (activity type registry, connector
    client, versioning manager). One instance per process is
    sufficient — the manager is stateless and all of its
    collaborators are concurrency-safe by construction.
    """

    def __init__(
        self,
        *,
        definition_store: DefinitionStoreProvider,
        metadata_store: MetadataStoreProvider,
        activity_registry: ActivityTypeRegistry,
        connector_client: ConnectorClient,
        versioning: VersioningManager,
        max_publish_retries: int = DEFAULT_MAX_PUBLISH_RETRIES,
    ) -> None:
        self._store = definition_store
        self._metadata_store = metadata_store
        self._activity_registry = activity_registry
        self._connector_client = connector_client
        self._versioning = versioning
        if max_publish_retries < 1:
            raise ValueError("max_publish_retries must be >= 1")
        self._max_publish_retries = max_publish_retries
        self._subworkflow_adapter = _SubworkflowResolverAdapter(definition_store)

    # ------------------------------------------------------------------
    # Publish pipeline (CS-IMPL-010)
    # ------------------------------------------------------------------

    async def publish_workflow(
        self,
        *,
        workspace_id: str,
        principal_id: str,
        source: str | bytes,
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersionRef:
        """Publish a workflow document and return the new version ref.

        ``source`` is the raw JSON or YAML body forwarded by the API
        gateway. The pipeline runs schema → normalize → resolve → CEL
        → idempotency-check → mint+put.

        Idempotency: if the canonical hash of the resolved document
        matches an existing version under the same name, the existing
        ref is returned and no new row is written. This means two
        identical publish calls produce the same
        :class:`WorkflowVersionRef`.

        Race recovery: if :meth:`DefinitionStoreProvider.put_workflow_version`
        raises :class:`ImmutableViolation`, the manager re-checks
        idempotency (a winning concurrent caller may have published
        the same content) and otherwise retries with a freshly minted
        version up to ``max_publish_retries`` times.

        Args:
            workspace_id: Target workspace.
            principal_id: Caller identity (audit only at v1).
            source: Raw workflow body (JSON or YAML).
            derived_from_template_version_id: Optional lineage link
                set by :meth:`TemplateManager.materialize` (CS-IMPL-013).
                Stored on the resulting :class:`WorkflowVersion`.

        Raises:
            PublishValidationError: If any pre-store stage fails. The
                ``stage`` field tells which gate failed.
            WorkflowImmutabilityError: If race-recovery retries are
                exhausted (extremely rare; the loop bound is 8 by
                default and contention is steady-state-zero).
        """
        _LOGGER.info(
            "publish_workflow start workspace=%s principal=%s source_bytes=%d "
            "derived_from_template_version_id=%s",
            workspace_id,
            principal_id,
            len(source) if isinstance(source, (bytes, str)) else -1,
            derived_from_template_version_id or "<none>",
        )

        with telemetry.observe_operation(
            telemetry.OP_WORKFLOW_PUBLISH,
            outcomes={
                PublishValidationError: "validation_error",
                WorkflowImmutabilityError: "immutability",
            },
        ):
            # 1. parse + 2. schema-validate
            with telemetry.observe_stage(
                telemetry.STAGE_PARSE,
                outcomes={PublishValidationError: "validation_error"},
            ):
                doc = self._parse_and_validate_schema(source)
            # 2a. enforce metadata.workspace (when present) matches the
            # target workspace. The schema marks metadata.workspace
            # optional and explicitly defers this check to the manager
            # (see workflow.WORKFLOW_SCHEMA comment); without it a caller
            # could publish a document whose embedded workspace disagrees
            # with the URL workspace.
            self._enforce_workspace_match(doc, workspace_id=workspace_id)
            # 3. normalize
            with telemetry.observe_stage(telemetry.STAGE_NORMALIZE):
                normalized = normalize_workflow(doc)
            # 4. resolve references
            with telemetry.observe_stage(
                telemetry.STAGE_RESOLVE,
                outcomes={PublishValidationError: "validation_error"},
            ):
                resolved = await self._resolve_refs(normalized, workspace_id=workspace_id)
            # 5. CEL validation
            with telemetry.observe_stage(
                telemetry.STAGE_CEL,
                outcomes={PublishValidationError: "validation_error"},
            ):
                self._validate_cel(resolved)
            # 6. idempotency: scan existing versions for matching canonical hash
            workflow_name = self._extract_workflow_name(resolved.document)
            with telemetry.observe_stage(telemetry.STAGE_IDEMPOTENCY):
                idempotent = await self._find_idempotent_match(
                    workspace_id=workspace_id,
                    workflow_name=workflow_name,
                    resolved=resolved,
                )
            if idempotent is not None:
                _LOGGER.info(
                    "publish_workflow idempotent re-publish workspace=%s name=%s version=%s",
                    workspace_id,
                    workflow_name,
                    idempotent.version,
                )
                return WorkflowVersionRef(
                    workspace_id=workspace_id,
                    workflow_name=workflow_name,
                    version=int(idempotent.version),
                )
            # 7. mint + put with race-recovery loop
            with telemetry.observe_stage(
                telemetry.STAGE_MINT_PUT,
                outcomes={WorkflowImmutabilityError: "immutability"},
            ):
                ref, newly_published = await self._mint_and_put(
                    workspace_id=workspace_id,
                    workflow_name=workflow_name,
                    resolved=resolved,
                    derived_from_template_version_id=derived_from_template_version_id,
                )
        # Audit emission runs outside the instrumentation context so
        # outbox failures (best-effort, swallowed by the helper) don't
        # appear as failed publish operations on the dashboards.
        if newly_published:
            await audit_workflow_published(
                self._metadata_store,
                workspace_id=workspace_id,
                actor=principal_id,
                workflow_name=ref.workflow_name,
                version=ref.version,
                derived_from_template_version_id=derived_from_template_version_id,
            )
        return ref

    # ------------------------------------------------------------------
    # Read / lifecycle surface (CS-IMPL-011)
    # ------------------------------------------------------------------

    async def list_workflow_versions(
        self,
        *,
        workspace_id: str,
        workflow_name: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
        filter: DefinitionListFilter | None = None,
    ) -> Page[WorkflowVersion]:
        """Page through versions of ``workflow_name`` in ``workspace_id``.

        Thin pass-through to
        :meth:`DefinitionStoreProvider.list_workflow_versions`. Order
        is newest-published-first per the SPL contract. The empty
        page (``items=()``, ``next_cursor=None``) is returned when the
        workflow has no versions; callers may treat this either as
        "workflow does not exist" or "exists but is empty" — at v1
        these are indistinguishable since the parent row is only
        materialized at first put.
        """
        return await self._store.list_workflow_versions(
            WorkspaceId(workspace_id),
            WorkflowId(workflow_name),
            filter=filter,
            cursor=cursor,
            limit=limit,
        )

    async def get_workflow_version_by_ref(
        self,
        *,
        workspace_id: str,
        workflow_name: str,
        version: int,
    ) -> WorkflowVersion:
        """Fetch a single workflow version by its triple.

        Raises:
            WorkflowNotFound: When no version row matches the triple.
        """
        row = await self._store.get_workflow_version(
            WorkspaceId(workspace_id),
            WorkflowId(workflow_name),
            str(version),
        )
        if row is None:
            raise WorkflowNotFound(
                workspace_id=workspace_id,
                workflow_name=workflow_name,
                version=version,
            )
        return row

    async def deprecate_workflow(
        self,
        *,
        workspace_id: str,
        workflow_name: str,
        principal_id: str,
        reason: str | None = None,
    ) -> None:
        """Flip the workflow's parent-row deprecation flag to ``True``.

        Per the design contract:

        * Deprecation is a **parent-row toggle**; version rows are
          never mutated. The denormalized ``parent_deprecated`` flag
          surfaces on subsequent fetches via the SPL adapter.
        * Deprecating a workflow that has no versions still
          materialises the parent row (the SPL adapter handles this).
        * Repeated deprecate calls are idempotent — flipping a
          deprecated workflow to deprecated is a no-op.

        ``reason`` and ``principal_id`` are captured in the audit log
        only; the SPL surface carries no per-action audit fields, so
        the audit trail lives in the structured log and (later) in
        the audit store.

        Raises:
            WorkflowNotFound: When the workflow has no versions at
                all in ``workspace_id``. We treat "no versions" as
                "no workflow to deprecate" because the SPL has no
                way to distinguish a never-published parent row from
                an absent one.
        """
        # Probe for existence: a workflow with at least one version is
        # the only state the SPL can attest to. An empty list_workflow_versions
        # response means "no workflow to deprecate".
        with telemetry.observe_operation(
            telemetry.OP_WORKFLOW_DEPRECATE,
            outcomes={WorkflowNotFound: "not_found"},
        ):
            latest = await self._store.get_latest_workflow_version(
                WorkspaceId(workspace_id),
                WorkflowId(workflow_name),
            )
            if latest is None:
                raise WorkflowNotFound(
                    workspace_id=workspace_id,
                    workflow_name=workflow_name,
                )
            await self._store.set_workflow_deprecated(
                WorkspaceId(workspace_id),
                WorkflowId(workflow_name),
                True,
            )
        _LOGGER.info(
            "deprecate_workflow workspace=%s name=%s principal=%s reason=%s",
            workspace_id,
            workflow_name,
            principal_id,
            reason or "<none>",
        )
        await audit_workflow_deprecated(
            self._metadata_store,
            workspace_id=workspace_id,
            actor=principal_id,
            workflow_name=workflow_name,
            reason=reason,
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
            validate_workflow(doc)
        except WorkflowSchemaError as exc:
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
        """Reject a document whose ``metadata.workspace`` disagrees with the target.

        The Workflow schema marks ``metadata.workspace`` as optional
        and explicitly defers the cross-check to this layer (see the
        comment on ``WORKFLOW_SCHEMA.properties.metadata`` in
        :mod:`custos_catalog.schema.workflow`). When the field is
        absent we accept the document as-is; the URL workspace is the
        authority. When present it must match exactly, otherwise the
        stored document would carry an embedded workspace that
        disagrees with the row's ``workspace_id`` column — a recipe
        for confusing downstream behaviour.

        Schema validation has already run, so ``metadata`` is an
        object and ``metadata.workspace`` (when present) is a string
        matching the name pattern. The ``isinstance`` checks are
        retained as defence in depth.

        Raises:
            PublishValidationError: When the embedded workspace
                disagrees with ``workspace_id``. Reported under the
                ``schema`` stage with code ``"workspace_mismatch"``.
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

    async def _resolve_refs(
        self,
        normalized: NormalizedWorkflow,
        *,
        workspace_id: str,
    ) -> NormalizedWorkflow:
        try:
            return await apply_resolutions(
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

    def _validate_cel(self, resolved: NormalizedWorkflow) -> None:
        try:
            validate_expressions(resolved)
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
    def _extract_workflow_name(document: Mapping[str, Any]) -> str:
        """Pull the workflow's ``metadata.name`` from the canonical document.

        The schema validator has already enforced
        ``metadata.name`` is present and is a non-empty string.
        """
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
        workflow_name: str,
        resolved: NormalizedWorkflow,
    ) -> WorkflowVersion | None:
        target_hash = canonical_hash(resolved.document)
        cursor: Cursor | None = None
        while True:
            page = await self._store.list_workflow_versions(
                WorkspaceId(workspace_id),
                WorkflowId(workflow_name),
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
        workflow_name: str,
        resolved: NormalizedWorkflow,
        derived_from_template_version_id: str | None = None,
    ) -> tuple[WorkflowVersionRef, bool]:
        """Mint a fresh version and put it; retry on race.

        Returns the resulting ref plus a ``newly_published`` flag that
        is ``False`` when the race-recovery loop discovered an
        idempotent match (no new row was written) and ``True`` when
        the put succeeded.
        """
        for attempt in range(self._max_publish_retries):
            version = await self._versioning.next_workflow_version(
                WorkspaceId(workspace_id),
                WorkflowId(workflow_name),
            )
            try:
                await self._store.put_workflow_version(
                    WorkspaceId(workspace_id),
                    WorkflowId(workflow_name),
                    str(version),
                    resolved.document,
                    derived_from_template_version_id=derived_from_template_version_id,
                )
            except ImmutableViolation:
                # A concurrent publisher won the race. Two possibilities:
                # (a) they published our exact content (idempotency
                #     should pick that up on the next pass).
                # (b) they published different content; we ask for a
                #     fresh version slot and try again.
                _LOGGER.warning(
                    "publish_workflow race on attempt=%d workspace=%s name=%s "
                    "attempted_version=%d; rescanning for idempotency",
                    attempt,
                    workspace_id,
                    workflow_name,
                    version,
                )
                rematch = await self._find_idempotent_match(
                    workspace_id=workspace_id,
                    workflow_name=workflow_name,
                    resolved=resolved,
                )
                if rematch is not None:
                    return (
                        WorkflowVersionRef(
                            workspace_id=workspace_id,
                            workflow_name=workflow_name,
                            version=int(rematch.version),
                        ),
                        False,
                    )
                continue
            _LOGGER.info(
                "publish_workflow success workspace=%s name=%s version=%d",
                workspace_id,
                workflow_name,
                version,
            )
            return (
                WorkflowVersionRef(
                    workspace_id=workspace_id,
                    workflow_name=workflow_name,
                    version=version,
                ),
                True,
            )

        # All retries exhausted; surface a structured immutability error
        # whose next_available_version is the freshest mint we could see.
        final_next = await self._versioning.next_workflow_version(
            WorkspaceId(workspace_id),
            WorkflowId(workflow_name),
        )
        raise WorkflowImmutabilityError(
            workspace_id=workspace_id,
            workflow_name=workflow_name,
            attempted_version=final_next - 1,
            next_available_version=final_next,
            is_idempotent_match=False,
        )


# ---------------------------------------------------------------------------
# Issue adapters
# ---------------------------------------------------------------------------


def _schema_issue(src: SchemaValidationIssue) -> PublishValidationIssue:
    return PublishValidationIssue(
        stage="schema",
        path=src.path,
        code=src.validator,
        message=src.message,
    )


def _cel_issue(src: CelValidationIssue, *, code: str) -> PublishValidationIssue:
    return PublishValidationIssue(
        stage="cel",
        path=src.path,
        code=code,
        message=src.message,
    )


__all__ = [
    "DEFAULT_MAX_PUBLISH_RETRIES",
    "DefinitionManager",
    "PublishStage",
    "PublishValidationError",
    "PublishValidationIssue",
    "WorkflowNotFound",
    "WorkflowVersionRef",
]
