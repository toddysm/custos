"""Migration runner — schema-revision negotiation and platform startup gate.

Contract (per `design/components/storage-provider-layer/design.md`
§ Migration Runner):

1. Each interface has a monotonically increasing schema revision number
   owned by SPL. The current per-build required revisions are pinned by
   each Protocol's `SCHEMA_REVISION` class variable; `required_revisions`
   reads them.
2. Each adapter declares the revisions it has applied via
   `declared_revisions` — a mapping from interface name to the set of
   revisions present in its backing store. An adapter at revision N
   declares `{1, 2, ..., N}` (every step has been applied), but the
   contract is "is the required revision in the set" so non-contiguous
   sets are permitted.
3. `check_revisions(adapters)` collects every adapter's declarations
   and raises `MigrationRequired` listing per-interface gaps. The
   platform calls this at startup and refuses to start on any gap;
   this is the **strict** policy. `permissive` is intentionally not
   implemented in v1 (would silently mask writes).
4. The platform never auto-migrates. An operator runs the
   `custos migrate up` CLI (see `custos_spl.migrations.cli`) which
   invokes `MigrationRunner.apply_pending()` on a configured adapter.

Adapters opt in by exposing two attributes:

  - `declared_revisions: Mapping[str, AbstractSet[int]]` — what's
    already applied. Stateless query facades (rev 0) need not appear.
  - `async apply_pending() -> list[str]` — apply outstanding revisions
    forward-only and return human-readable summaries. v1 has no
    down-migration path.

Both are captured by the `MigrationCapable` runtime-checkable Protocol
below so the CLI can validate adapters before invoking them.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from collections.abc import Set as AbstractSet
from typing import Protocol, runtime_checkable

from custos_spl.errors import MigrationRequired
from custos_spl.interfaces.artifact_store import ArtifactStoreProvider
from custos_spl.interfaces.auth_store import AuthStoreProvider
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.definition_store import DefinitionStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

# Stateless query facades pin SCHEMA_REVISION to 0 and own no schema —
# they are deliberately excluded from migration negotiation. The values
# are typed as `Any` only to keep mypy happy across the Protocol-class
# attribute access; runtime behavior is unchanged.
_STATEFUL_INTERFACES: tuple[type, ...] = (
    MetadataStoreProvider,
    DefinitionStoreProvider,
    CatalogStoreProvider,
    AuthStoreProvider,
    ArtifactStoreProvider,
)


def required_revisions() -> dict[str, int]:
    """Return the platform's required revision per stateful interface.

    Each interface's required revision is its Protocol's
    `SCHEMA_REVISION` class variable for the running build. Bumping a
    Protocol's `SCHEMA_REVISION` is what makes the platform require a
    fresh adapter migration before it will start.
    """
    return {
        iface.__name__: int(getattr(iface, "SCHEMA_REVISION"))  # noqa: B009
        for iface in _STATEFUL_INTERFACES
    }


@runtime_checkable
class MigrationCapable(Protocol):
    """An adapter that participates in schema-revision negotiation.

    Adapters implementing one or more stateful provider Protocols MUST
    also implement this Protocol so SPL can gate platform startup and
    drive `custos migrate up`.

    The contract is intentionally minimal: a read-side property
    reporting what is currently applied, and an async method that
    applies any pending forward migrations. There is no down-migration
    method by design — v1 forward-only.
    """

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        """Mapping from interface name to the set of revisions present
        in this adapter's backing store. Empty mapping is legal (means
        "I declare nothing", which will produce gaps for every
        interface the platform requires)."""
        ...

    async def apply_pending(self) -> list[str]:
        """Apply outstanding forward migrations. Idempotent: a
        subsequent call after success is a no-op.

        Returns:
            Human-readable summaries of revisions applied, one per
            entry. Empty list means nothing was pending.
        """
        ...


def check_revisions(adapters: Iterable[object]) -> None:
    """Validate that the running adapter set satisfies the platform's
    required schema revisions.

    Args:
        adapters: Instances providing `declared_revisions` (typically
            the same provider instances the platform will use at
            runtime). Objects that are not `MigrationCapable` are
            skipped silently — they are stateless or out of scope for
            migration.

    Raises:
        MigrationRequired: when any required revision is not present
            in the union of the adapters' declared revisions. The
            exception carries `gaps` so operator logs can list exactly
            what `custos migrate up` needs to apply.
    """
    required = required_revisions()
    declared: dict[str, set[int]] = {name: set() for name in required}
    for adapter in adapters:
        if not isinstance(adapter, MigrationCapable):
            continue
        for iface_name, revs in adapter.declared_revisions.items():
            declared.setdefault(iface_name, set()).update(revs)

    gaps: list[tuple[str, int]] = sorted(
        (iface, rev)
        for iface, rev in required.items()
        if rev not in declared[iface]
    )
    if gaps:
        raise MigrationRequired(gaps)


__all__ = [
    "MigrationCapable",
    "check_revisions",
    "required_revisions",
]
