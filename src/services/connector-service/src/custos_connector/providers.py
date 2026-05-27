"""SPL provider wiring and the schema-revision startup gate (CONN-IMPL-003).

Connector Service persists everything through two SPL provider Protocols:

* :class:`custos_spl.CatalogStoreProvider` —
  ``ConnectorTypeVersion`` rows (the connector-type registry; the same
  table catalog-service reads from for publish-time reference resolution).
* :class:`custos_spl.MetadataStoreProvider` —
  ``ConnectorInstance`` rows, the workspace-scoped ``ConnectorCursor``
  store + single-writer lease primitive, and the audit outbox.

v1 binds both to the Postgres adapters from ``custos-postgres`` because
that is the only SPL backend implemented today (see
``design/components/storage-provider-layer/design.md`` § Adapters). The
factory functions here read the DSN values from ``CONN_CATALOG_STORE`` /
``CONN_METADATA_STORE`` and instantiate :class:`PgCatalogAdapter` /
:class:`PgMetadataAdapter` directly via :class:`LazyPool` so pool
construction is deferred to the first async use.

A future change (tracked separately) will introduce an adapter
discriminator and route through the ``custos_spl.adapters`` entry-point
group so non-Postgres backends can plug in without editing the service.

Startup gate
------------

:func:`verify_schema_revisions` calls each adapter's
``refresh_declared()`` to read the migration ledger, then compares the
declared revisions for the interfaces owned by connector-service against
the corresponding required ``SCHEMA_REVISION`` values. It raises
:class:`custos_spl.MigrationRequired` when a required revision is not
present. The app factory wires this onto a startup hook that flips
``app.state.ready`` to ``False`` and surfaces a 503 on ``/readyz`` until
the required connector-service schema revisions have been applied.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from typing import Protocol, cast

from custos_spl import MigrationRequired
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.lease_store import LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.binding import BindForStepService
from custos_connector.cursor import CursorService
from custos_connector.identity import (
    AmazonKmsResolver,
    AzureKeyVaultResolver,
    AzureManagedIdentityResolver,
    HttpxAsyncHttpClient,
    IdentityResolverRegistry,
    OidcFederatedResolver,
)
from custos_connector.lease import LeaseManager
from custos_connector.listen.manager import ListenManager
from custos_connector.listen.publisher import EventPublisher
from custos_connector.runtime import DockerCliHookRunner, PluginInvoker
from custos_connector.scheduler import PullLoopScheduler
from custos_connector.settings import Settings

# The interfaces connector-service actually owns. ``custos_spl.check_revisions``
# checks the global SPL set which is the right thing for the platform-wide
# ``custos migrate up`` CLI but the wrong thing for a per-service startup
# gate — connector-service does not deploy Auth/Artifact/Definition adapters
# and would otherwise refuse to start because of revisions owned by sibling
# services.
_REQUIRED_INTERFACES: tuple[type, ...] = (
    CatalogStoreProvider,
    ConnectorInstanceStoreProvider,
    LeaseStoreProvider,
    MetadataStoreProvider,
)


class _RefreshableAdapter(Protocol):
    """Structural type the schema-revision startup gate needs from each
    adapter: a read-side view of the migration ledger plus an async hook
    that pulls the latest ledger state from the store before the gate
    compares declared vs. required.

    ``declared_revisions`` comes from SPL's ``MigrationCapable`` Protocol;
    ``refresh_declared`` is the custos-postgres ledger-refresh convention
    that every connector-service adapter (real or fake) implements.
    """

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]: ...

    async def refresh_declared(self) -> None: ...


@dataclass(frozen=True, slots=True)
class Providers:
    """Bundle of the SPL providers Connector Service consumes.

    Held on ``app.state.providers`` and used by the application's request-
    handling layer to access the catalog store (connector type registry),
    the connector-instance store (workspace-scoped instance rows), and the
    metadata store (audit log and supporting metadata).

    The :class:`IdentityResolverRegistry` shipped in CONN-IMPL-015 (Phase
    F) is wired alongside the SPL providers so the BindForStep call site
    (CONN-IMPL-016, Phase G) has a single place to fetch resolved
    identities. The registry is constructed lazily by
    :func:`load_identity_registry` with the four built-in resolvers
    plumbed through a shared :class:`HttpxAsyncHttpClient`.

    The :class:`BindForStepService` shipped in CONN-IMPL-016 (Phase G)
    is the only piece of state the FastAPI app holds for the BindForStep
    RPC — it carries the in-memory idempotency cache that collapses
    concurrent re-binds for the same ``(workspace_id, run_id, step_id,
    attempt)`` onto a single resolve.

    The :class:`CursorService` (CONN-IMPL-022) and the
    :class:`PullLoopScheduler` (CONN-IMPL-023) entries are optional
    on the dataclass even though their bodies have shipped, because
    full lifespan wiring (passing in an :class:`EventPublisher` for
    the pull loop and managing the scheduler's lifetime) lands as a
    follow-up. CONN-IMPL-024 (this slice) wires the admin REST surface
    that reads from them when present — when missing, the admin
    handlers raise a startup-wiring :class:`RuntimeError`.

    The :class:`ListenManager` and :class:`EventPublisher` entries
    (CONN-IMPL-025) are likewise optional. The push receiver
    (:func:`custos_connector.listen.router.post_events`) raises a
    startup-wiring :class:`RuntimeError` when called against a service
    missing either field. Pull-mode connector instances continue to
    function without them as long as the CursorService is wired with
    its own publisher; the lifespan wiring of the Listen Manager and
    the production Dapr publisher lands in a follow-up
    (CONN-IMPL-027 Phase J) when the Trigger Service's
    ``SubscribeEvents`` internal RPC is in place.
    """

    catalog_store: CatalogStoreProvider
    instance_store: ConnectorInstanceStoreProvider
    lease_store: LeaseStoreProvider
    metadata_store: MetadataStoreProvider
    identity_registry: IdentityResolverRegistry
    bind_for_step_service: BindForStepService
    lease_manager: LeaseManager
    cursor_service: CursorService | None = field(default=None)
    pull_loop_scheduler: PullLoopScheduler | None = field(default=None)
    listen_manager: ListenManager | None = field(default=None)
    event_publisher: EventPublisher | None = field(default=None)


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
        PgConnectorInstanceAdapter,
        PgLeaseAdapter,
        PgMetadataAdapter,
    )
    from custos_pg.pool import LazyPool

    metadata_lazy_pool = LazyPool(settings.metadata_store_dsn)

    catalog_store = PgCatalogAdapter(
        lazy=LazyPool(settings.catalog_store_dsn),
    )
    instance_store = PgConnectorInstanceAdapter(
        lazy=metadata_lazy_pool,
    )
    metadata_store = PgMetadataAdapter(
        lazy=metadata_lazy_pool,
    )
    # The Lease store shares the metadata DB pool: lease rows are
    # workspace-scoped operational state owned by the same connector
    # service that owns the audit outbox, so co-locating them keeps
    # the deployment footprint small. Operators that want to split
    # them can swap the DSN via a follow-up wiring change.
    lease_store = PgLeaseAdapter(
        lazy=metadata_lazy_pool,
    )
    # The adapters declare SCHEMA_REVISION as a bare class attr rather
    # than a ``ClassVar[int]``, so mypy can't see them as Protocol-conforming
    # at the consumer boundary. ``custos-postgres`` has its own strict mypy
    # job that verifies the conformance at the implementation site; the
    # cast keeps the consumer view typed.
    typed_catalog = cast(CatalogStoreProvider, catalog_store)
    typed_instances = cast(ConnectorInstanceStoreProvider, instance_store)
    typed_metadata = cast(MetadataStoreProvider, metadata_store)
    typed_lease = cast(LeaseStoreProvider, lease_store)
    identity_registry = load_identity_registry(metadata_store=typed_metadata)
    return Providers(
        catalog_store=typed_catalog,
        instance_store=typed_instances,
        lease_store=typed_lease,
        metadata_store=typed_metadata,
        identity_registry=identity_registry,
        bind_for_step_service=load_bind_for_step_service(
            catalog_store=typed_catalog,
            instance_store=typed_instances,
            metadata_store=typed_metadata,
            identity_registry=identity_registry,
        ),
        lease_manager=load_lease_manager(
            lease_store=typed_lease,
            metadata_store=typed_metadata,
            settings=settings,
        ),
    )


def load_identity_registry(
    *,
    metadata_store: MetadataStoreProvider,
) -> IdentityResolverRegistry:
    """Build the default :class:`IdentityResolverRegistry`.

    Wires the four built-in resolvers (CONN-IMPL-015) through a shared
    :class:`HttpxAsyncHttpClient` so we have a single HTTP client to
    close on shutdown. Constructing an ``httpx.AsyncClient`` is side-effect
    free (no sockets are opened until the first request), so this factory
    remains safe to call during startup.

    Operators that need vendor (``x-<vendor>``) resolvers register them
    via :meth:`IdentityResolverRegistry.register_vendor_resolver` after
    the lifespan hook has bound the registry to ``app.state``.

    The default per-resolver token providers raise an
    :class:`IdentityResolverError` so a misconfigured environment
    surfaces at first bind, not at startup. CONN-IMPL-016 (Phase G)
    will swap in the workload-identity wiring during the bind flow.
    """
    # Lazy import keeps providers.py importable in unit tests that
    # never touch the identity path.
    import httpx

    http_client = httpx.AsyncClient()
    transport = HttpxAsyncHttpClient(http_client, owns_client=True)

    return IdentityResolverRegistry(
        resolvers=[
            AzureKeyVaultResolver(http=transport),
            AmazonKmsResolver(http=transport),
            AzureManagedIdentityResolver(http=transport),
            OidcFederatedResolver(http=transport),
        ],
        metadata_store=metadata_store,
        http_transport=transport,
    )


def load_bind_for_step_service(
    *,
    catalog_store: CatalogStoreProvider,
    instance_store: ConnectorInstanceStoreProvider,
    metadata_store: MetadataStoreProvider,
    identity_registry: IdentityResolverRegistry,
    plugin_binder: PluginInvoker | None = None,
) -> BindForStepService:
    """Build the default :class:`BindForStepService` (CONN-IMPL-016).

    Wires the SPL providers + identity registry + the production
    :class:`PluginInvoker` (backed by :class:`DockerCliHookRunner`) into
    a single :class:`BindForStepService` instance. The service is the
    only place that owns the in-memory idempotency cache, so a single
    instance per FastAPI app is correct.

    Tests inject a stub via ``plugin_binder`` to avoid the Docker
    runtime; production wiring constructs the default invoker.
    """
    binder = plugin_binder if plugin_binder is not None else PluginInvoker(DockerCliHookRunner())
    return BindForStepService(
        catalog_store=catalog_store,
        instance_store=instance_store,
        metadata_store=metadata_store,
        identity_registry=identity_registry,
        plugin_binder=binder,
    )


def load_lease_manager(
    *,
    lease_store: LeaseStoreProvider,
    metadata_store: MetadataStoreProvider,
    settings: Settings,
) -> LeaseManager:
    """Build the default :class:`LeaseManager` (CONN-IMPL-017).

    Threads the per-deployment sidecar TTL default and the concurrent-
    lease cap from :class:`Settings` into the manager so the operator-
    facing env vars ``CONN_SIDECAR_DEFAULT_TTL`` and
    ``CONN_LEASE_MAX_CONCURRENT`` drive the runtime behaviour without
    needing a per-call override on the BindForStep / sidecar paths.
    """
    return LeaseManager(
        lease_store=lease_store,
        metadata_store=metadata_store,
        default_ttl_sec=settings.sidecar_default_ttl_sec,
        max_concurrent_leases=settings.lease_max_concurrent,
    )


async def verify_schema_revisions(providers: Providers) -> None:
    """Refresh the per-adapter declared revisions and run the schema gate.

    The gate is scoped to the three interfaces connector-service owns:
    ``CatalogStoreProvider`` for the connector-type registry,
    ``ConnectorInstanceStoreProvider`` for workspace-scoped connector
    instance rows, and ``MetadataStoreProvider`` for cursors and the
    audit outbox. Revisions owned by sibling services are deliberately
    out of scope so this service can start independently.

    Raises:
        custos_spl.MigrationRequired: when any adapter's ledger is behind
            the required revision for its Protocol. The exception carries
            the per-interface gaps so the operator can run
            ``custos migrate up`` against the specific interfaces listed.
    """
    adapters: list[_RefreshableAdapter] = [
        cast(_RefreshableAdapter, providers.catalog_store),
        cast(_RefreshableAdapter, providers.instance_store),
        cast(_RefreshableAdapter, providers.lease_store),
        cast(_RefreshableAdapter, providers.metadata_store),
    ]
    for adapter in adapters:
        await adapter.refresh_declared()

    required = {
        iface.__name__: int(getattr(iface, "SCHEMA_REVISION"))  # noqa: B009
        for iface in _REQUIRED_INTERFACES
    }
    declared: dict[str, set[int]] = {name: set() for name in required}
    for adapter in adapters:
        for iface_name, revs in adapter.declared_revisions.items():
            declared.setdefault(iface_name, set()).update(revs)

    gaps = sorted(
        (iface, rev) for iface, rev in required.items() if rev not in declared.get(iface, set())
    )
    if gaps:
        raise MigrationRequired(gaps)


def schema_gate_explainer(error: MigrationRequired) -> str:
    """Render an operator-actionable log line for a ``MigrationRequired`` gap.

    Connector Service emits this once at startup on the WARNING/ERROR
    channel and again as the body of the ``/readyz`` 503 so logs and
    probes carry the same diagnostic.
    """
    gaps = ", ".join(f"{iface}@rev{rev}" for iface, rev in error.gaps)
    return (
        "connector-service is not ready: the SPL ledger is behind the "
        "running build's required schema revisions. "
        f"Missing: {gaps}. "
        "Resolve by running `custos migrate up` against the configured "
        "DSNs (see CONN_CATALOG_STORE / CONN_METADATA_STORE)."
    )


__all__ = [
    "MigrationRequired",
    "Providers",
    "load_bind_for_step_service",
    "load_identity_registry",
    "load_lease_manager",
    "load_providers",
    "schema_gate_explainer",
    "verify_schema_revisions",
]
