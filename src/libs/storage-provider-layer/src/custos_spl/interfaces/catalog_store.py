"""CatalogStoreProvider — activity + connector type version catalog.

Owns:
- `ActivityType`, `ActivityTypeVersion`
- `ConnectorType`, `ConnectorTypeVersion`

Unlike the workspace-scoped stores, the catalog is **platform-wide**:
methods do NOT take `workspace_id`. Catalog Service performs all
capability-regression and digest-pinning checks; SPL's only integrity
rule on this interface is the digest-conflict 409.

See `design/components/storage-provider-layer/design.md` § CatalogStoreProvider.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Protocol, runtime_checkable

from custos_spl.pagination import Cursor, Page


@dataclass(frozen=True, slots=True)
class ActivityTypeVersion:
    """A single activity-type version row.

    Primary key is `(namespace, type, version)`. `digest` is the content
    address of `normalized_manifest`; mismatches on the same key surface
    as `ConflictDigest`.

    `parent_deprecated` is a **denormalized read of the parent
    `ActivityType` row's `deprecated` flag** at fetch time — it is NOT
    a property of the version. Deprecation toggles via
    `set_activity_type_deprecated` mutate the parent only; the version
    row itself is the immutable manifest+digest binding. There is no
    version-level deprecation in v1.
    """

    namespace: str
    type: str
    version: str
    digest: str
    normalized_manifest: Mapping[str, Any]
    parent_deprecated: bool
    published_at: datetime


@dataclass(frozen=True, slots=True)
class ConnectorTypeVersion:
    """A single connector-type version row.

    Primary key is `(type, version)`. Same digest semantics as
    `ActivityTypeVersion`. `parent_deprecated` denormalizes the parent
    `ConnectorType` row's `deprecated` flag — see `ActivityTypeVersion`
    for the rationale.
    """

    type: str
    version: str
    digest: str
    normalized_manifest: Mapping[str, Any]
    parent_deprecated: bool
    published_at: datetime


@runtime_checkable
class CatalogStoreProvider(Protocol):
    """Activity + connector type catalog (platform-wide, not workspace-scoped).

    Put semantics:
      - `put_*` is idempotent on identical `(key, digest)` — re-putting
        the same digest succeeds.
      - `put_*` raises `ConflictDigest` when the same key is re-put with
        a different digest.

    The schema revision required by this build is `SCHEMA_REVISION`.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    # ----- Activity types -----

    async def put_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ActivityTypeVersion:
        """Write or re-confirm an activity-type version.

        Raises `ConflictDigest` if a row with the same
        `(namespace, type, version)` exists with a different digest.
        Identical-digest re-puts are idempotent.
        """
        ...

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> ActivityTypeVersion | None:
        """Exact-version lookup. Returns `None` if absent."""
        ...

    async def list_activity_type_versions(
        self,
        namespace: str,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ActivityTypeVersion]:
        """Paginated listing for one `(namespace, type)`."""
        ...

    async def set_activity_type_deprecated(
        self,
        namespace: str,
        type: str,
        deprecated: bool,
    ) -> None:
        """Toggle deprecation on the parent `ActivityType`.

        Affects `resolve()` outcomes but not historical lookups.
        """
        ...

    # ----- Connector types -----

    async def put_connector_type_version(
        self,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ConnectorTypeVersion: ...

    async def get_connector_type_version(
        self,
        type: str,
        version: str,
    ) -> ConnectorTypeVersion | None: ...

    async def list_connector_type_versions(
        self,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]: ...

    async def set_connector_type_deprecated(
        self,
        type: str,
        deprecated: bool,
    ) -> None: ...

    # ----- Resolution -----

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> ActivityTypeVersion | None:
        """Resolve a semver range to the latest matching activity-type version.

        Returns `None` if no matching version exists or if the parent
        activity type is deprecated. The deprecation model exposed by
        this interface is parent-type only via
        `set_activity_type_deprecated`; callers should not assume
        version-level deprecation affects resolution.
        """
        ...


__all__ = [
    "ActivityTypeVersion",
    "CatalogStoreProvider",
    "ConnectorTypeVersion",
]
