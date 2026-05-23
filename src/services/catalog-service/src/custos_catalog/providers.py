"""SPL provider wiring and the schema-revision startup gate (CS-IMPL-003).

Catalog Service persists everything through two SPL provider Protocols:

* :class:`custos_spl.DefinitionStoreProvider` —
  ``Workflow``/``WorkflowVersion``/``WorkflowTemplate``/``WorkflowTemplateVersion``.
* :class:`custos_spl.CatalogStoreProvider` —
  ``ActivityTypeVersion``/``ConnectorTypeVersion``.

v1 binds both to the Postgres adapters from ``custos-postgres`` because
that is the only SPL backend implemented today (see
``design/components/storage-provider-layer/design.md`` § Adapters). The
factory functions here read the DSN values from
``CAT_DEFINITION_STORE`` / ``CAT_CATALOG_STORE``
and instantiate :class:`PgDefinitionAdapter` /
:class:`PgCatalogAdapter` directly via :class:`LazyPool` so pool
construction is deferred to the first async use.

A future change (tracked separately) will introduce an adapter
discriminator and route through the ``custos_spl.adapters`` entry-point
group so non-Postgres backends can plug in without editing the service.

Startup gate
------------

:func:`verify_schema_revisions` calls each adapter's
``refresh_declared()`` to read the migration ledger, then defers to
:func:`custos_spl.check_revisions` which raises
:class:`custos_spl.MigrationRequired` when the running build's required
revision is not present. The app factory wires this onto a startup hook
that flips ``app.state.ready`` to ``False`` and surfaces a 503 on
``/readyz`` until the operator runs ``custos migrate up``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from custos_spl import MigrationRequired
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.definition_store import DefinitionStoreProvider

from custos_catalog.settings import Settings

if TYPE_CHECKING:
    pass


# The interfaces catalog-service actually owns. ``custos_spl.check_revisions``
# checks the global SPL set (Definition, Catalog, Auth, Artifact, Metadata)
# which is the right thing for the platform-wide ``custos migrate up`` CLI
# but the wrong thing for a per-service startup gate — catalog-service does
# not deploy Auth/Artifact/Metadata adapters and would otherwise refuse to
# start because of revisions owned by sibling services.
_REQUIRED_INTERFACES: tuple[type, ...] = (
    DefinitionStoreProvider,
    CatalogStoreProvider,
)


class _RefreshableAdapter(Protocol):
    """Minimal capability the startup gate needs from each adapter."""

    async def refresh_declared(self) -> None: ...


@dataclass(frozen=True, slots=True)
class Providers:
    """Bundle of the SPL providers Catalog Service consumes.

    Held on ``app.state.providers`` and exposed to FastAPI handlers via
    :func:`get_definition_store` / :func:`get_catalog_store`.
    """

    definition_store: DefinitionStoreProvider
    catalog_store: CatalogStoreProvider


def load_providers(settings: Settings) -> Providers:
    """Construct the SPL providers from the parsed settings.

    Imports the Postgres adapter classes lazily so this module can be
    imported by unit tests that monkey-patch :func:`load_providers` and
    never need the asyncpg-backed implementation on the import path.
    """
    # Imported here so that test suites injecting fakes can avoid the
    # asyncpg dependency entirely.
    from custos_pg import (
        PgCatalogAdapter,
        PgDefinitionAdapter,
    )
    from custos_pg.pool import LazyPool

    definition_store = PgDefinitionAdapter(
        lazy=LazyPool(settings.definition_store_dsn),
    )
    catalog_store = PgCatalogAdapter(
        lazy=LazyPool(settings.catalog_store_dsn),
    )
    # The adapters declare SCHEMA_REVISION as a bare class attr rather
    # than a `ClassVar[int]`, so mypy can't see them as Protocol-conforming
    # at the consumer boundary. `custos-postgres` has its own strict mypy
    # job that verifies the conformance at the implementation site; the
    # cast keeps the consumer view typed.
    return Providers(
        definition_store=cast(DefinitionStoreProvider, definition_store),
        catalog_store=cast(CatalogStoreProvider, catalog_store),
    )


async def verify_schema_revisions(providers: Providers) -> None:
    """Refresh the per-adapter declared revisions and run the schema gate.

    The gate is scoped to the two interfaces catalog-service owns
    (``DefinitionStoreProvider`` and ``CatalogStoreProvider``); revisions
    owned by sibling services (Auth, Artifact, Metadata) are deliberately
    out of scope so this service can start independently.

    Raises:
        custos_spl.MigrationRequired: when any adapter's ledger is behind
            the required revision for its Protocol. The exception carries
            the per-interface gaps so the operator can run
            ``custos migrate up`` against the specific interfaces listed.
    """
    adapters: list[object] = [providers.definition_store, providers.catalog_store]
    for adapter in adapters:
        refresh = getattr(adapter, "refresh_declared", None)
        if refresh is not None:
            await refresh()

    required = {
        iface.__name__: int(getattr(iface, "SCHEMA_REVISION"))  # noqa: B009
        for iface in _REQUIRED_INTERFACES
    }
    declared: dict[str, set[int]] = {name: set() for name in required}
    for adapter in adapters:
        adapter_revs = getattr(adapter, "declared_revisions", None)
        if adapter_revs is None:
            continue
        for iface_name, revs in adapter_revs.items():
            declared.setdefault(iface_name, set()).update(revs)

    gaps = sorted(
        (iface, rev) for iface, rev in required.items() if rev not in declared.get(iface, set())
    )
    if gaps:
        raise MigrationRequired(gaps)


def schema_gate_explainer(error: MigrationRequired) -> str:
    """Render an operator-actionable log line for a ``MigrationRequired`` gap.

    Catalog Service emits this once at startup on the WARNING/ERROR
    channel and again as the body of the ``/readyz`` 503 so logs and
    probes carry the same diagnostic.
    """
    gaps = ", ".join(f"{iface}@rev{rev}" for iface, rev in error.gaps)
    return (
        "catalog-service is not ready: the SPL ledger is behind the "
        "running build's required schema revisions. "
        f"Missing: {gaps}. "
        "Resolve by running `custos migrate up` against the configured "
        "DSNs (see CAT_DEFINITION_STORE / CAT_CATALOG_STORE)."
    )


__all__ = [
    "MigrationRequired",
    "Providers",
    "load_providers",
    "schema_gate_explainer",
    "verify_schema_revisions",
]
