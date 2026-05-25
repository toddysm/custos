"""SPL provider wiring and the schema-revision startup gate (AS-IMPL-004).

Auth Service persists everything through two SPL provider Protocols:

* :class:`custos_spl.AuthStoreProvider` — tenants, workspaces, principals,
  OIDC identities, service tokens, permissions, roles, role bindings.
* :class:`custos_spl.MetadataStoreProvider` — the audit-outbox writer that
  carries ``authz.decision``, ``token.*``, ``principal.*``,
  ``role-binding.*``, ``oidc.identity-linked``, ``call-context.invalid``,
  etc. (design § Audit events).

v1 binds both to the Postgres adapters from ``custos-postgres`` because
that is the only SPL backend implemented today. The factory functions
here read the DSN values from ``CUSTOS_AUTH_STORE_DSN`` /
``CUSTOS_AUTH_METADATA_STORE_DSN`` and instantiate :class:`PgAuthAdapter`
/ :class:`PgMetadataAdapter` directly via :class:`LazyPool` so pool
construction is deferred to the first async use.

A future change (tracked separately) will introduce an adapter
discriminator and route through the ``custos_spl.adapters`` entry-point
group so non-Postgres backends can plug in without editing the service.

Startup gate
------------

:func:`verify_schema_revisions` calls each adapter's
``refresh_declared()`` to read the migration ledger, then compares the
declared revisions for the interfaces owned by auth-service against the
corresponding required ``SCHEMA_REVISION`` values. It raises
:class:`custos_spl.MigrationRequired` when a required revision is not
present. The app factory wires this onto a FastAPI lifespan hook that
sets ``app.state.schema_gate_error``, emits an operator-actionable
ERROR log line (:func:`schema_gate_explainer`), and re-raises the
exception so the lifespan startup fails. uvicorn surfaces this as a
non-zero exit code, which Kubernetes turns into a CrashLoopBackOff
under the default ``restartPolicy: Always`` — matching the AS-IMPL-004
acceptance criterion "service refuses to start (clear error message +
non-zero exit) when run against a Postgres ahead-of or behind-of the
bundled migrations". Resolve by running ``custos migrate up`` against
the configured DSNs and letting Kubernetes restart the pod.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Protocol, cast

from custos_spl import MigrationRequired
from custos_spl.interfaces.auth_store import AuthStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_auth.authn_cache import AuthnCache
from custos_auth.authz_cache import (
    DEFAULT_AUTHZ_CACHE_TTL_SECONDS,
    AuthzDecisionCache,
)
from custos_auth.binding_events import (
    BindingChangedPublisher,
    BindingChangedSubscriber,
    LocalBindingChangedBus,
    NoOpBindingChangedSubscriber,
)
from custos_auth.settings import (
    DEFAULT_AUTHN_CACHE_TTL_SECONDS,
    Settings,
)
from custos_auth.token_revoked_events import (
    LocalTokenRevokedBus,
    NoOpTokenRevokedSubscriber,
    TokenRevokedPublisher,
    TokenRevokedSubscriber,
)

# The interfaces auth-service actually owns. ``custos_spl.check_revisions``
# checks the global SPL set (Definition, Catalog, Auth, Artifact, Metadata)
# which is the right thing for the platform-wide ``custos migrate up`` CLI
# but the wrong thing for a per-service startup gate — auth-service does
# not deploy Definition/Catalog/Artifact adapters and would otherwise
# refuse to start because of revisions owned by sibling services.
_REQUIRED_INTERFACES: tuple[type, ...] = (
    AuthStoreProvider,
    MetadataStoreProvider,
)


class _RefreshableAdapter(Protocol):
    """Structural type the schema-revision startup gate needs from each
    adapter: a read-side view of the migration ledger plus an async hook
    that pulls the latest ledger state from the store before the gate
    compares declared vs. required.

    ``declared_revisions`` comes from SPL's ``MigrationCapable`` Protocol;
    ``refresh_declared`` is the custos-postgres ledger-refresh convention
    that every auth-service adapter (real or fake) implements.
    """

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]: ...

    async def refresh_declared(self) -> None: ...


@dataclass(frozen=True, slots=True)
class Providers:
    """Bundle of the SPL providers Auth Service consumes.

    Held on ``app.state.providers`` and exposed to FastAPI handlers via
    dependency helpers introduced in subsequent AS-IMPL-* phases.

    ``binding_changed_publisher`` defaults to the in-process
    :class:`LocalBindingChangedBus` introduced by AS-IMPL-012. The
    lifespan subscribes the local authz cache to that bus so a
    single-replica deployment delivers invalidations synchronously —
    the multi-replica deployment additionally wires
    :attr:`binding_changed_subscriber` against a real transport so
    every replica's cache sees the same events.

    ``authz_cache`` is the per-replica TTL cache that backstops the
    authorize hot path; see :class:`custos_auth.authz_cache.AuthzDecisionCache`.
    """

    auth_store: AuthStoreProvider
    metadata_store: MetadataStoreProvider
    binding_changed_publisher: BindingChangedPublisher = dc_field(
        default_factory=LocalBindingChangedBus,
    )
    binding_changed_subscriber: BindingChangedSubscriber = dc_field(
        default_factory=NoOpBindingChangedSubscriber,
    )
    authz_cache: AuthzDecisionCache = dc_field(
        default_factory=lambda: AuthzDecisionCache(
            ttl_seconds=DEFAULT_AUTHZ_CACHE_TTL_SECONDS,
        ),
    )
    token_revoked_publisher: TokenRevokedPublisher = dc_field(
        default_factory=LocalTokenRevokedBus,
    )
    token_revoked_subscriber: TokenRevokedSubscriber = dc_field(
        default_factory=NoOpTokenRevokedSubscriber,
    )
    authn_cache: AuthnCache = dc_field(
        default_factory=lambda: AuthnCache(
            ttl_seconds=DEFAULT_AUTHN_CACHE_TTL_SECONDS,
        ),
    )


def load_providers(settings: Settings) -> Providers:
    """Construct the SPL providers from the parsed settings.

    Imports the Postgres adapter classes lazily so this module can be
    imported by unit tests that monkey-patch :func:`load_providers` and
    never need the asyncpg-backed implementation on the import path.
    """
    # Imported here so that test suites injecting fakes can avoid the
    # asyncpg dependency entirely.
    import json

    from custos_pg import PgAuthAdapter, PgMetadataAdapter
    from custos_pg.pool import LazyPool

    async def _register_jsonb_codec(conn: object) -> None:
        """Register a dict <-> JSONB codec on every pooled connection.

        ``PgAuthAdapter.put_role_binding`` (and several methods on
        ``PgMetadataAdapter``) pass Python dicts directly to JSONB
        columns; asyncpg has no built-in handler so without this
        registration writes blow up with
        ``DataError: invalid input for query argument $N``. Symmetric
        decode keeps reads producing dicts so adapter ``_json_to_*``
        helpers stay unchanged.
        """
        # ``conn`` is ``asyncpg.Connection``; ``set_type_codec`` is
        # not on a Protocol so the cast is informal — annotated as
        # ``object`` here only so ``LazyPool``'s typing surface stays
        # free of the asyncpg import. Local runtime check confirms it.
        await conn.set_type_codec(  # type: ignore[attr-defined]
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )

    auth_store = PgAuthAdapter(
        lazy=LazyPool(settings.auth_store_dsn, init=_register_jsonb_codec),
    )
    metadata_store = PgMetadataAdapter(
        lazy=LazyPool(settings.metadata_store_dsn, init=_register_jsonb_codec),
    )
    # The adapters declare SCHEMA_REVISION as a bare class attr rather
    # than a `ClassVar[int]`, so mypy can't see them as Protocol-conforming
    # at the consumer boundary. `custos-postgres` has its own strict mypy
    # job that verifies the conformance at the implementation site; the
    # cast keeps the consumer view typed.
    # The authz cache TTL is read from settings so the production knob
    # ``CUSTOS_AUTH_AUTHZ_CACHE_TTL`` flows through to the per-replica
    # cache without test or factory overrides.
    return Providers(
        auth_store=cast(AuthStoreProvider, auth_store),
        metadata_store=cast(MetadataStoreProvider, metadata_store),
        authz_cache=AuthzDecisionCache(
            ttl_seconds=settings.authz_cache_ttl_seconds,
        ),
        authn_cache=AuthnCache(
            ttl_seconds=settings.authn_cache_ttl_seconds,
        ),
    )


async def verify_schema_revisions(providers: Providers) -> None:
    """Refresh the per-adapter declared revisions and run the schema gate.

    The gate is scoped to the two interfaces auth-service owns
    (``AuthStoreProvider`` and ``MetadataStoreProvider``); revisions
    owned by sibling services (Definition, Catalog, Artifact) are
    deliberately out of scope so this service can start independently.

    Raises:
        custos_spl.MigrationRequired: when any adapter's ledger is behind
            the required revision for its Protocol. The exception carries
            the per-interface gaps so the operator can run
            ``custos migrate up`` against the specific interfaces listed.
    """
    adapters: list[_RefreshableAdapter] = [
        cast(_RefreshableAdapter, providers.auth_store),
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

    Auth Service emits this once at startup on the WARNING/ERROR channel
    and again as the body of the ``/readyz`` 503 so logs and probes
    carry the same diagnostic.
    """
    gaps = ", ".join(f"{iface}@rev{rev}" for iface, rev in error.gaps)
    return (
        "auth-service is not ready: the SPL ledger is behind the "
        "running build's required schema revisions. "
        f"Missing: {gaps}. "
        "Resolve by running `custos migrate up` against the configured "
        "DSNs (see CUSTOS_AUTH_STORE_DSN / CUSTOS_AUTH_METADATA_STORE_DSN)."
    )


__all__ = [
    "MigrationRequired",
    "Providers",
    "load_providers",
    "schema_gate_explainer",
    "verify_schema_revisions",
]
