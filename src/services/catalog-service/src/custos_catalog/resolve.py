"""Publish-time reference resolver (CS-IMPL-008).

Fourth and final gate in the publish-time pipeline. Consumes the
slot tuple emitted by :mod:`custos_catalog.normalize` and substitutes
**fully-qualified** activity refs, sub-workflow refs, and (via the
Connector Service stub) confirms connector-instance existence.

Per design § Operation: Resolve Activity Reference at Workflow Publish
and § Operation: Sub-Workflow Reference Resolution:

* ``<ns>/<type>@<major>`` resolves to the latest non-deprecated
  ``<ns>/<type>@<MAJOR.MINOR.PATCH>`` within ``<major>``. The resolved
  ref is substituted into the document so ``WorkflowVersion.document``
  carries the exact pinned form (REQ-025 immutability).
* ``<ns>/<type>@<MAJOR.MINOR.PATCH>`` resolves to itself iff a row
  exists and the parent ``ActivityType`` is not deprecated.
* ``<ns>/<type>@<MAJOR.MINOR>`` is **rejected** with a stable error
  code — this M1 rule mirrors the design's semver table.
* Short forms (no namespace) are rejected.
* Sub-workflow refs: ``<UUID>`` resolves via
  ``definition_store.get_workflow_version_by_id``;
  ``<workspace>/<name>@<version>`` triples are resolved by
  ``definition_store.get_workflow_version_by_name``. Cross-workspace
  refs are rejected (M1).
* Connector-instance refs: the Connector Service is consulted via
  :class:`ConnectorClient`. In M1 the resolver ships with a
  :class:`StubConnectorClient` that returns ``True`` for every name
  and emits a single batched ``WARNING`` log entry — real wiring
  lands in CS-IMPL-023 (issue #224).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from custos_catalog.normalize import (
    NormalizedTemplate,
    NormalizedWorkflow,
    RefResolutionSlot,
    SlotKind,
)

_LOGGER = logging.getLogger("custos_catalog.resolve")

# ---------------------------------------------------------------------------
# Reference grammar
# ---------------------------------------------------------------------------

#: Canonical activity reference: ``<ns>/<type>@<version>``.
#:
#: ``<ns>`` and ``<type>`` follow the DNS-friendly token grammar used
#: in the JSON Schema; ``<version>`` is captured as one opaque group
#: so we can branch on its shape afterwards.
_ACTIVITY_REF_RE: re.Pattern[str] = re.compile(
    r"^(?P<ns>[a-z][a-z0-9._-]*)/"
    r"(?P<type>[a-z][a-z0-9._-]*)"
    r"@(?P<ver>.+)$",
)

#: An integer-only version is a major pin (``@2``).
_MAJOR_PIN_RE: re.Pattern[str] = re.compile(r"^[0-9]+$")

#: A full ``MAJOR.MINOR.PATCH`` triple (the only allowed exact pin).
_EXACT_VERSION_RE: re.Pattern[str] = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")

#: ``MAJOR.MINOR`` — M1-rejected (design § Operation: Resolve
#: Activity Reference at Workflow Publish, semver rules table).
_MAJOR_MINOR_RE: re.Pattern[str] = re.compile(r"^[0-9]+\.[0-9]+$")

#: Canonical sub-workflow triple: ``<workspace>/<name>@<version>``.
_SUBWORKFLOW_TRIPLE_RE: re.Pattern[str] = re.compile(
    r"^(?P<workspace>[a-z][a-z0-9-]*)/"
    r"(?P<name>[a-z][a-z0-9-]*)"
    r"@(?P<version>[0-9]+(?:\.[0-9]+){0,2})$",
)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ResolveError(ValueError):
    """Base class for resolver errors.

    Carries a stable :attr:`code` so callers (and the API gateway) can
    map errors to remediation hints without parsing the message.
    """

    code: str = "resolve.unknown"

    def __init__(self, message: str, *, ref: str | None = None) -> None:
        super().__init__(message)
        self.ref = ref


class InvalidReferenceFormat(ResolveError):
    """Raised when the reference grammar itself is malformed."""

    code = "resolve.invalid_format"


class MajorMinorRefRejected(ResolveError):
    """Raised when an ``@<MAJOR>.<MINOR>`` ref is supplied (M1)."""

    code = "resolve.major_minor_rejected"


class ShortFormRefRejected(ResolveError):
    """Raised when the namespace is missing from an activity ref (M1)."""

    code = "resolve.short_form_rejected"


class ActivityTypeNotFound(ResolveError):
    """Raised when no version satisfies the supplied range."""

    code = "resolve.activity_type_not_found"


class ActivityTypeDeprecated(ResolveError):
    """Raised when the matching activity type is deprecated."""

    code = "resolve.activity_type_deprecated"


class SubworkflowNotFound(ResolveError):
    """Raised when no sub-workflow matches the reference."""

    code = "resolve.subworkflow_not_found"


class SubworkflowDeprecated(ResolveError):
    """Raised when the matching sub-workflow is deprecated."""

    code = "resolve.subworkflow_deprecated"


class CrossWorkspaceSubworkflowRejected(ResolveError):
    """Raised when the triple's workspace differs from the publish workspace."""

    code = "resolve.cross_workspace_subworkflow"


class ConnectorInstanceMissing(ResolveError):
    """Raised when the Connector Service says no such instance exists."""

    code = "resolve.connector_instance_missing"


# ---------------------------------------------------------------------------
# Resolved-form value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ResolvedActivityRef:
    """Outcome of :func:`resolve_activity_ref`.

    Attributes:
        canonical_ref: The fully-qualified ``<ns>/<type>@<MAJOR>.<MINOR>.<PATCH>``
            form to write into the normalized document.
        namespace: Parsed namespace component.
        type: Parsed type component.
        version: Exact resolved version (``MAJOR.MINOR.PATCH``).
        digest: The activity manifest digest (content-pin per REQ-025).
    """

    canonical_ref: str
    namespace: str
    type: str
    version: str
    digest: str


@dataclass(frozen=True, slots=True)
class ResolvedSubworkflowRef:
    """Outcome of :func:`resolve_subworkflow_ref`.

    Attributes:
        canonical_ref: The triple form ``<workspace>/<name>@<version>``
            that ends up in ``WorkflowVersion.document``.
        workspace_id: Resolved workspace identifier.
        workflow_id: Resolved workflow identifier.
        version: Resolved version string.
    """

    canonical_ref: str
    workspace_id: str
    workflow_id: str
    version: str


@dataclass(frozen=True, slots=True)
class ResolvedConnectorInstance:
    """Outcome of :func:`resolve_connector_instance`.

    Attributes:
        name: The connector-instance name as authored.
        workspace_id: The workspace the existence check ran against.
    """

    name: str
    workspace_id: str


# ---------------------------------------------------------------------------
# Adapter protocols (narrow — designed for tests + SPL plug-in)
# ---------------------------------------------------------------------------


@runtime_checkable
class ActivityTypeRegistry(Protocol):
    """Subset of :class:`custos_spl.CatalogStoreProvider` used by the resolver.

    Defined locally so this module does not import the full SPL
    interface, and so unit tests can hand-roll lightweight fakes.
    Real adapters (``CatalogStoreProvider`` implementations) satisfy
    this Protocol structurally — no separate adapter is required.
    """

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> Any:
        """Latest matching version, or ``None`` if no match."""

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> Any:
        """Exact-version lookup, or ``None`` if absent."""


@runtime_checkable
class SubworkflowResolver(Protocol):
    """Narrow Protocol for sub-workflow lookups during publish.

    Implementations are typically thin wrappers around
    :class:`custos_spl.DefinitionStoreProvider`; we keep the surface
    narrow so M1 tests can mock the two methods directly without
    spinning up the full store.
    """

    async def get_workflow_version_by_id(
        self,
        workflow_version_id: UUID,
    ) -> Any:
        """Return the (workspace, workflow, version, deprecated) tuple or ``None``."""

    async def get_workflow_version_by_name(
        self,
        workspace: str,
        name: str,
        version: str,
    ) -> Any:
        """Return the same tuple by friendly-name triple."""


@runtime_checkable
class ConnectorClient(Protocol):
    """Connector Service client surface used by the resolver.

    Real client lands in CS-IMPL-023; M1 ships
    :class:`StubConnectorClient`.
    """

    async def exists_connector_instance(
        self,
        workspace_id: str,
        name: str,
    ) -> bool:
        """Return ``True`` iff a connector instance with ``name`` exists in ``workspace_id``."""


class StubConnectorClient:
    """M1 stub for the Connector Service ``ExistsConnectorInstance`` RPC.

    Returns ``True`` for every name and tracks the per-batch call
    list. The first call in a batch emits a single ``WARNING`` log
    line so operators see the stub is being used at publish time;
    subsequent calls in the same batch are silent. Call
    :meth:`reset_batch` between batches when a new publish operation
    starts.

    The real Connector Service client lands in CS-IMPL-023 (#224);
    until then this stub means publishes will succeed even if the
    connector instance does not actually exist. The deferred-work
    PR replaces the stub with the live client.
    """

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or _LOGGER
        self._calls: list[tuple[str, str]] = []
        self._warned: bool = False

    async def exists_connector_instance(self, workspace_id: str, name: str) -> bool:
        """Stub: always ``True``; logs ``WARNING`` once per batch."""
        self._calls.append((workspace_id, name))
        if not self._warned:
            self._logger.warning(
                "StubConnectorClient: connector-instance existence checks "
                "are stubbed (real Connector Service client lands in CS-IMPL-023). "
                "First call this batch was workspace=%r name=%r.",
                workspace_id,
                name,
            )
            self._warned = True
        return True

    @property
    def calls(self) -> tuple[tuple[str, str], ...]:
        """Snapshot of all calls made in the current batch."""
        return tuple(self._calls)

    def reset_batch(self) -> None:
        """Begin a new batch.

        Resets the warning latch and the call log so the next call
        produces a fresh ``WARNING``.
        """
        self._calls.clear()
        self._warned = False


# ---------------------------------------------------------------------------
# Activity ref resolution
# ---------------------------------------------------------------------------


async def resolve_activity_ref(
    ref: str,
    *,
    registry: ActivityTypeRegistry,
) -> ResolvedActivityRef:
    """Resolve one activity reference to a digest-pinned exact version.

    Args:
        ref: The author-supplied reference (e.g. ``custos.builtin/vuln-scan@2``).
        registry: The activity-type registry to query. Any
            :class:`ActivityTypeRegistry`-compatible object works
            (e.g. a ``CatalogStoreProvider`` adapter).

    Returns:
        :class:`ResolvedActivityRef` carrying the canonical fully-qualified
        reference and the digest to pin.

    Raises:
        InvalidReferenceFormat: When ``ref`` is not parseable.
        ShortFormRefRejected: When the namespace is missing.
        MajorMinorRefRejected: When the version is a ``MAJOR.MINOR``
            pin (M1 rejects this — design § Operation: Resolve
            Activity Reference, semver rules).
        ActivityTypeNotFound: When the registry returns no match.
        ActivityTypeDeprecated: When the matching type is deprecated.
    """
    match = _ACTIVITY_REF_RE.match(ref)
    if match is None:
        # If the supplied ref looks like ``name@1`` (no namespace) we
        # raise the friendlier short-form error so operators don't
        # mistakenly think the namespace separator itself is wrong.
        if "@" in ref and "/" not in ref:
            raise ShortFormRefRejected(
                f"activity ref {ref!r} is missing a namespace "
                f"(M1 requires <namespace>/<type>@<version>)",
                ref=ref,
            )
        raise InvalidReferenceFormat(
            f"activity ref {ref!r} does not match <namespace>/<type>@<version>",
            ref=ref,
        )

    namespace = match.group("ns")
    type_ = match.group("type")
    ver = match.group("ver")

    if _MAJOR_MINOR_RE.match(ver):
        raise MajorMinorRefRejected(
            f"activity ref {ref!r} uses a MAJOR.MINOR pin; M1 accepts "
            f"only @MAJOR or @MAJOR.MINOR.PATCH",
            ref=ref,
        )

    if _MAJOR_PIN_RE.match(ver):
        # SPL stores (and the v1 Postgres adapter) accept a PEP 440
        # ``SpecifierSet`` for the resolver's ``semver_range`` argument.
        # The catalog ref grammar's ``@MAJOR`` form is sugar for "the
        # latest non-deprecated version inside that major" — translate
        # to the equivalent specifier so the SPL contract receives a
        # valid input. The in-memory fakes used by manager-level unit
        # tests still accept the bare major pin, so the registry's
        # public contract is unchanged.
        spec = f">={ver},<{int(ver) + 1}"
        result = await registry.resolve(namespace, type_, spec)
        if result is None:
            raise ActivityTypeNotFound(
                f"no non-deprecated activity-type version satisfies {ref!r}",
                ref=ref,
            )
    elif _EXACT_VERSION_RE.match(ver):
        result = await registry.get_activity_type_version(namespace, type_, ver)
        if result is None:
            raise ActivityTypeNotFound(
                f"no activity-type version row exists for {ref!r}",
                ref=ref,
            )
    else:
        raise InvalidReferenceFormat(
            f"activity ref {ref!r} version component must be @MAJOR or @MAJOR.MINOR.PATCH",
            ref=ref,
        )

    if _is_deprecated(result):
        raise ActivityTypeDeprecated(
            f"activity type {namespace}/{type_} is deprecated; cannot publish",
            ref=ref,
        )

    resolved_version = _get_attr(result, "version")
    digest = _get_attr(result, "digest")
    canonical = f"{namespace}/{type_}@{resolved_version}"
    return ResolvedActivityRef(
        canonical_ref=canonical,
        namespace=namespace,
        type=type_,
        version=str(resolved_version),
        digest=str(digest),
    )


# ---------------------------------------------------------------------------
# Sub-workflow ref resolution
# ---------------------------------------------------------------------------


async def resolve_subworkflow_ref(
    ref: str,
    *,
    store: SubworkflowResolver,
    workspace_id: str,
) -> ResolvedSubworkflowRef:
    """Resolve one sub-workflow reference.

    Accepts either:

    * A UUID string (the ``workflowVersionId``).
    * A ``<workspace>/<name>@<version>`` triple.

    Cross-workspace refs are rejected at M1 — sub-workflow callers
    must reference workflows in the same workspace as the calling
    workflow.

    Raises:
        InvalidReferenceFormat: When neither form parses.
        CrossWorkspaceSubworkflowRejected: When the triple's
            workspace differs from ``workspace_id``.
        SubworkflowNotFound: When the store returns no match.
        SubworkflowDeprecated: When the parent workflow is deprecated.
    """
    # UUID path: try to parse without the dashes-only constraint, but
    # only treat it as a UUID if the parse succeeds AND there is no
    # ``@`` (which would imply the triple form).
    if "@" not in ref:
        try:
            wf_version_id = UUID(ref)
        except ValueError as exc:
            raise InvalidReferenceFormat(
                f"sub-workflow ref {ref!r} is neither a UUID nor a "
                f"<workspace>/<name>@<version> triple",
                ref=ref,
            ) from exc
        result = await store.get_workflow_version_by_id(wf_version_id)
        if result is None:
            raise SubworkflowNotFound(
                f"no workflow version with id {ref!r}",
                ref=ref,
            )
        result_workspace = str(_get_attr(result, "workspace_id"))
        if result_workspace != workspace_id:
            raise CrossWorkspaceSubworkflowRejected(
                f"sub-workflow {ref!r} lives in workspace "
                f"{result_workspace!r}; M1 forbids cross-workspace refs",
                ref=ref,
            )
        if _is_deprecated(result):
            raise SubworkflowDeprecated(
                f"sub-workflow {ref!r} is deprecated; cannot publish",
                ref=ref,
            )
        wf_id = str(_get_attr(result, "workflow_id"))
        version = str(_get_attr(result, "version"))
        return ResolvedSubworkflowRef(
            canonical_ref=f"{result_workspace}/{wf_id}@{version}",
            workspace_id=result_workspace,
            workflow_id=wf_id,
            version=version,
        )

    # Triple form.
    triple = _SUBWORKFLOW_TRIPLE_RE.match(ref)
    if triple is None:
        raise InvalidReferenceFormat(
            f"sub-workflow ref {ref!r} does not match <workspace>/<name>@<version>",
            ref=ref,
        )
    triple_workspace = triple.group("workspace")
    name = triple.group("name")
    version = triple.group("version")
    if triple_workspace != workspace_id:
        raise CrossWorkspaceSubworkflowRejected(
            f"sub-workflow {ref!r} targets workspace {triple_workspace!r}; "
            f"M1 forbids cross-workspace refs (publishing in {workspace_id!r})",
            ref=ref,
        )
    result = await store.get_workflow_version_by_name(workspace_id, name, version)
    if result is None:
        raise SubworkflowNotFound(
            f"no workflow version {ref!r} found in {workspace_id!r}",
            ref=ref,
        )
    if _is_deprecated(result):
        raise SubworkflowDeprecated(
            f"sub-workflow {ref!r} is deprecated; cannot publish",
            ref=ref,
        )
    wf_id = str(_get_attr(result, "workflow_id"))
    return ResolvedSubworkflowRef(
        canonical_ref=f"{workspace_id}/{name}@{version}",
        workspace_id=workspace_id,
        workflow_id=wf_id,
        version=version,
    )


# ---------------------------------------------------------------------------
# Connector instance resolution
# ---------------------------------------------------------------------------


async def resolve_connector_instance(
    name: str,
    *,
    client: ConnectorClient,
    workspace_id: str,
) -> ResolvedConnectorInstance:
    """Confirm a connector-instance exists in ``workspace_id``.

    The current M1 :class:`StubConnectorClient` returns ``True`` for
    every name; CS-IMPL-023 replaces it with the real Connector
    Service client. Resolution does NOT rewrite the connector name in
    the document — connector references stay in their original
    short-name form.
    """
    exists = await client.exists_connector_instance(workspace_id, name)
    if not exists:
        raise ConnectorInstanceMissing(
            f"connector instance {name!r} does not exist in workspace {workspace_id!r}",
            ref=name,
        )
    return ResolvedConnectorInstance(name=name, workspace_id=workspace_id)


# ---------------------------------------------------------------------------
# Apply resolutions to a normalized document
# ---------------------------------------------------------------------------


async def apply_resolutions(
    norm: NormalizedWorkflow,
    *,
    activity_registry: ActivityTypeRegistry,
    definition_store: SubworkflowResolver,
    connector_client: ConnectorClient,
    workspace_id: str,
) -> NormalizedWorkflow:
    """Fill every :class:`RefResolutionSlot` in ``norm`` and return the result.

    Each slot is resolved via the appropriate function above; the
    returned :class:`NormalizedWorkflow` carries:

    * a **new** ``document`` dict with the resolved canonical
      reference string substituted at each slot's ``path``.
    * an empty ``slots`` tuple — all slots have been consumed.

    The original ``norm.document`` is not mutated.

    Errors are surfaced as the first encountered :class:`ResolveError`;
    this is consistent with the rest of the publish-time pipeline
    (schema and CEL gates collect-all, the resolver stops on the
    first store-driven failure since each lookup may also be a
    network round-trip).
    """
    new_doc: dict[str, Any] = _deep_copy_dict(norm.document)
    for slot in norm.slots:
        replacement = await _resolve_slot(
            slot,
            activity_registry=activity_registry,
            definition_store=definition_store,
            connector_client=connector_client,
            workspace_id=workspace_id,
        )
        if replacement is not None:
            _set_at_path(new_doc, slot.path, replacement)
    return NormalizedWorkflow(document=new_doc, slots=())


async def apply_template_resolutions(
    norm: NormalizedTemplate,
    *,
    activity_registry: ActivityTypeRegistry,
    definition_store: SubworkflowResolver,
    connector_client: ConnectorClient,
    workspace_id: str,
) -> NormalizedTemplate:
    """Same as :func:`apply_resolutions` but for templates.

    Templates typically carry few or no slots because their activity
    and connector references are placeholder-bound; this entrypoint
    exists for symmetry and so the M1 plumbing exercises the
    template path too.
    """
    new_doc: dict[str, Any] = _deep_copy_dict(norm.document)
    for slot in norm.slots:
        replacement = await _resolve_slot(
            slot,
            activity_registry=activity_registry,
            definition_store=definition_store,
            connector_client=connector_client,
            workspace_id=workspace_id,
        )
        if replacement is not None:
            _set_at_path(new_doc, slot.path, replacement)
    return NormalizedTemplate(document=new_doc, slots=())


async def _resolve_slot(
    slot: RefResolutionSlot,
    *,
    activity_registry: ActivityTypeRegistry,
    definition_store: SubworkflowResolver,
    connector_client: ConnectorClient,
    workspace_id: str,
) -> str | None:
    """Resolve one slot. Returns the replacement string, or ``None``.

    Connector-instance slots do NOT have a substitution — the
    reference name in the document is the canonical form. The
    Connector Service call is purely an existence assertion, so this
    helper returns ``None`` for that kind to signal "no document
    rewrite".
    """
    kind: SlotKind = slot.kind
    if kind == "activity":
        resolved = await resolve_activity_ref(
            slot.original_ref,
            registry=activity_registry,
        )
        return resolved.canonical_ref
    if kind == "subworkflow":
        resolved_sub = await resolve_subworkflow_ref(
            slot.original_ref,
            store=definition_store,
            workspace_id=workspace_id,
        )
        return resolved_sub.canonical_ref
    if kind == "connector_instance":
        await resolve_connector_instance(
            slot.original_ref,
            client=connector_client,
            workspace_id=workspace_id,
        )
        return None  # no document mutation
    raise ResolveError(f"unknown slot kind {kind!r}", ref=slot.original_ref)


def collect_connector_instance_calls(slots: Iterable[RefResolutionSlot]) -> list[str]:
    """Return the unique connector-instance names referenced in ``slots``.

    The Definition Manager (CS-IMPL-010) will use this to batch-warm
    the Connector Service client; bundling here keeps the stub
    warning-emission behaviour and the real client's
    rate-limit-friendly batch path on the same code path.
    """
    seen: list[str] = []
    seen_set: set[str] = set()
    for slot in slots:
        if slot.kind != "connector_instance":
            continue
        if slot.original_ref in seen_set:
            continue
        seen.append(slot.original_ref)
        seen_set.add(slot.original_ref)
    return seen


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_deprecated(row: Any) -> bool:
    """Return True iff the store row exposes a true ``parent_deprecated``.

    Tolerates dataclass attrs and dict-shaped fakes.
    """
    if row is None:
        return False
    return bool(_get_attr(row, "parent_deprecated", default=False))


def _get_attr(row: Any, name: str, *, default: Any = None) -> Any:
    """Get ``name`` from a dataclass-or-dict-shaped row.

    Lets the resolver work with both real SPL dataclasses and the
    light dict fakes used in tests, without sprinkling
    ``isinstance(row, dict)`` checks through the body.
    """
    if isinstance(row, Mapping):
        return row.get(name, default)
    return getattr(row, name, default)


def _deep_copy_dict(node: Any) -> Any:
    """Shallow recursive copy of dict/list/scalar nodes.

    ``json.loads(json.dumps(...))`` would also work but loses dict key
    order; we walk explicitly so the input's iteration order survives.
    """
    if isinstance(node, dict):
        return {k: _deep_copy_dict(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_deep_copy_dict(v) for v in node]
    return node


def _set_at_path(doc: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
    """Walk ``path`` from the root of ``doc`` and replace the leaf.

    ``path`` mixes string keys and integer indices (the same shape the
    normalizer emits). The function mutates ``doc`` in place.
    """
    if not path:
        raise ValueError("path must not be empty")
    cursor: Any = doc
    for segment in path[:-1]:
        cursor = cursor[segment]
    cursor[path[-1]] = value
