"""Workflow / template version minting + immutability enforcement (CS-IMPL-009).

The Versioning Manager owns the question "what is the next ``version``
integer for ``(workspace_id, name)``?" for both ``Workflow`` and
``WorkflowTemplate``. Catalog uses **monotonically increasing integers
per name within a workspace**, encoded as decimal strings at the SPL
boundary (the SPL's :class:`WorkflowVersion.version` is ``str``).

Race semantics
--------------

The :class:`DefinitionStoreProvider` protocol does **not** expose
``with_transaction``; the write-once contract is enforced by the
adapter itself via a unique constraint on ``(workflow_id, version)``.
That makes the natural concurrency model **optimistic**:

1. Versioning Manager scans the existing versions for the name and
   returns the next integer.
2. Definition Manager attempts a ``put_workflow_version`` with that
   integer.
3. If two writers race, the second one's put surfaces
   :class:`ImmutableViolation`. The Definition Manager retries by
   asking for a fresh ``next_*_version`` and putting again.

There is no advisory locking and no ``SELECT … FOR UPDATE`` here on
purpose: the design's race-recovery story is "retry on
``ImmutableViolation``" and that is the only model the SPL protocol
supports. See :class:`custos_catalog.managers.definition.DefinitionManager`
for the retry loop.

The Versioning Manager itself is therefore a thin read helper. The
heavy lifting — content-identity check, retry, error wrapping — lives
in the Definition Manager (CS-IMPL-010). Keeping the responsibilities
split this way means the in-memory fake used in unit tests can model
race outcomes by failing puts deterministically without having to
reach inside this module.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from custos_spl.errors import ImmutableViolation, SPLError
from custos_spl.ids import WorkflowId, WorkflowTemplateId, WorkspaceId
from custos_spl.interfaces.definition_store import DefinitionStoreProvider

# A version string is the decimal representation of a positive integer.
# We tolerate (but ignore) any rows whose ``version`` does not match —
# real adapters only mint integer strings, and a hypothetical legacy
# row with a non-integer version should not block the next mint.
_VERSION_RE = re.compile(r"^[0-9]+$")

# When listing existing versions to discover the max, paginate in
# 100-row chunks. Most workflows have a handful of versions; the
# constant matters only as a safety bound for runaways.
_LIST_PAGE_SIZE = 100


class WorkflowImmutabilityError(SPLError):
    """Raised when a publish attempt collides with an existing version.

    Wraps the underlying :class:`ImmutableViolation` from the SPL with
    workflow-level context: the workspace, workflow name, the
    contended version, and the next-available version the caller can
    retry with. Catalog's API surface maps this to HTTP 409 with a
    stable error code.

    The :attr:`is_idempotent_match` flag distinguishes the two
    sub-cases:

    * ``True``  — the existing row's normalized document matches the
      caller's content byte-for-byte. The Definition Manager treats
      this as an idempotent re-publish and returns the existing ref
      instead of raising.
    * ``False`` — the existing row has different content. This is a
      genuine immutability violation; the API surface returns 409 with
      the suggested ``next_available_version`` so the caller can retry
      with a fresh slot.

    Attributes:
        code: Stable error code (``"catalog.workflow_immutability"``)
            for client-side mapping.
        workspace_id: The workspace where the collision happened.
        workflow_name: The friendly workflow name (``WorkflowId`` at
            the SPL layer is the slug itself in v1).
        attempted_version: The integer version the caller tried to
            write.
        next_available_version: The next integer the caller can use to
            retry. Always strictly greater than ``attempted_version``.
        is_idempotent_match: ``True`` iff the existing row's content
            equals the caller's content under canonical-JSON encoding.
    """

    code: str = "catalog.workflow_immutability"

    def __init__(
        self,
        *,
        workspace_id: str,
        workflow_name: str,
        attempted_version: int,
        next_available_version: int,
        is_idempotent_match: bool,
    ) -> None:
        super().__init__(
            f"workflow {workflow_name!r} version {attempted_version} in "
            f"workspace {workspace_id!r} already exists; next available is "
            f"{next_available_version}",
        )
        self.workspace_id = workspace_id
        self.workflow_name = workflow_name
        self.attempted_version = attempted_version
        self.next_available_version = next_available_version
        self.is_idempotent_match = is_idempotent_match


class TemplateImmutabilityError(SPLError):
    """Raised when a template publish collides with an existing version.

    Mirror of :class:`WorkflowImmutabilityError` for the template
    publish path. Templates land in Phase E (CS-IMPL-013); this class
    is defined here so the Versioning Manager surface is symmetric
    from day one.
    """

    code: str = "catalog.template_immutability"

    def __init__(
        self,
        *,
        workspace_id: str,
        template_name: str,
        attempted_version: int,
        next_available_version: int,
        is_idempotent_match: bool,
    ) -> None:
        super().__init__(
            f"template {template_name!r} version {attempted_version} in "
            f"workspace {workspace_id!r} already exists; next available is "
            f"{next_available_version}",
        )
        self.workspace_id = workspace_id
        self.template_name = template_name
        self.attempted_version = attempted_version
        self.next_available_version = next_available_version
        self.is_idempotent_match = is_idempotent_match


@dataclass(frozen=True, slots=True)
class VersioningManager:
    """Mints monotonic integer versions per ``(workspace, name)`` pair.

    A thin read helper over :class:`DefinitionStoreProvider`. Holding
    a reference to the store as a frozen attribute keeps the manager
    cheap to construct (one per request is fine) and easy to swap in
    tests via a hand-rolled fake that satisfies the protocol.
    """

    store: DefinitionStoreProvider

    async def next_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_name: WorkflowId,
    ) -> int:
        """Return the next integer to write under this workflow name.

        Walks ``list_workflow_versions`` page by page and tracks the
        max integer-shaped ``version`` seen. Returns ``max + 1``, or
        ``1`` if the workflow has no versions yet (or its only rows
        have non-integer version strings — those are tolerated but
        ignored).
        """
        max_seen = await self._max_workflow_version(workspace_id, workflow_name)
        return max_seen + 1

    async def next_template_version(
        self,
        workspace_id: WorkspaceId,
        template_name: WorkflowTemplateId,
    ) -> int:
        """Same as :meth:`next_workflow_version` but for templates."""
        max_seen = await self._max_template_version(workspace_id, template_name)
        return max_seen + 1

    async def _max_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_name: WorkflowId,
    ) -> int:
        cursor = None
        max_seen = 0
        while True:
            page = await self.store.list_workflow_versions(
                workspace_id,
                workflow_name,
                cursor=cursor,
                limit=_LIST_PAGE_SIZE,
            )
            for row in page.items:
                parsed = _parse_version(row.version)
                if parsed is not None and parsed > max_seen:
                    max_seen = parsed
            if page.next_cursor is None:
                return max_seen
            cursor = page.next_cursor

    async def _max_template_version(
        self,
        workspace_id: WorkspaceId,
        template_name: WorkflowTemplateId,
    ) -> int:
        cursor = None
        max_seen = 0
        while True:
            page = await self.store.list_workflow_template_versions(
                workspace_id,
                template_name,
                cursor=cursor,
                limit=_LIST_PAGE_SIZE,
            )
            for row in page.items:
                parsed = _parse_version(row.version)
                if parsed is not None and parsed > max_seen:
                    max_seen = parsed
            if page.next_cursor is None:
                return max_seen
            cursor = page.next_cursor


def _parse_version(raw: str) -> int | None:
    """Return ``raw`` parsed as a positive integer, or None if it isn't.

    Non-integer version strings should never appear from real adapters,
    but defensive parsing keeps the manager total — a corrupt row must
    never block a fresh publish.
    """
    if not _VERSION_RE.match(raw):
        return None
    return int(raw)


__all__ = [
    "ImmutableViolation",
    "TemplateImmutabilityError",
    "VersioningManager",
    "WorkflowImmutabilityError",
]
