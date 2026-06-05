"""SPL provider wiring from :class:`Settings` (OBS-IMPL-004).

The Observability and Audit Service reads and writes through three SPL provider
Protocols:

* :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider` — the audit
  writer + outbox-drain store (``custos-postgres``). The drainer
  (OBS-IMPL-005/006) and retention worker (OBS-IMPL-007) operate it across all
  workspaces.
* :class:`custos_spl.interfaces.log_query.LogQueryProvider` — inbound log
  read-back for the Query API (``custos-loki`` ``loki``; a local ``noop``).
* :class:`custos_spl.interfaces.metrics_query.MetricsQueryProvider` — inbound
  metric read-back (``custos-prometheus`` ``prometheus`` / ``noop``).

:func:`load_providers` resolves each provider identifier from :class:`Settings`
to the matching adapter and bundles them in a :class:`Providers` container held
on ``app.state.providers``. An unrecognised identifier fails fast with
:class:`ProviderConfigError` — mirroring the SPL "platform refuses to start
without an active adapter" rule. Backend adapters require their URL; ``noop``
adapters wire without one.

The Postgres adapter is built over a deferred :class:`~custos_pg.pool.LazyPool`,
so constructing the providers opens no socket; the pool connects on first query
and is reclaimed at process exit (matching auth-service, which likewise leaves
the metadata pool to teardown). The log/metrics adapters issue per-request HTTP
and hold no persistent client, so the provider bundle owns no resource that
needs an explicit async close at this phase; :func:`aclose_providers` is the
forward seam where the background workers added in later phases (the audit-outbox
drainer, the retention worker, the alert dispatcher) will be stopped. The
heavyweight adapter packages (``custos_pg``, ``custos_loki``,
``custos_prometheus``, each pulling in ``asyncpg`` / ``httpx``) are imported
lazily inside the builders so this module — and the app factory that imports it —
stays import-safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, cast

from custos_spl.errors import QueryUnsupported
from custos_spl.interfaces.log_query import (
    LogFilter,
    LogPage,
    LogQueryProvider,
    LogRecord,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.interfaces.metrics_query import MetricsQueryProvider

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from custos_spl.ids import RunId, StepId, WorkspaceId
    from custos_spl.pagination import Cursor

    from custos_obs.settings import Settings


class ProviderConfigError(RuntimeError):
    """Raised when a provider identifier has no matching adapter.

    The settings loader already constrains the log/metrics identifiers to their
    closed sets, so this is a defence-in-depth guard at the wiring boundary: it
    keeps the service from starting against an identifier no adapter serves.
    """


class NoopLogQueryAdapter:
    """A :class:`LogQueryProvider` that serves no logs.

    Wired when ``CUSTOS_LOG_QUERY_PROVIDER=noop`` (custos-loki ships no noop of
    its own). Every method raises :class:`~custos_spl.errors.QueryUnsupported`,
    which the Query API maps to ``503 LogQueryUnavailable`` so the UI falls back
    to the ``CUSTOS_LOGS_EXTERNAL_URL`` pointer. Mirrors
    ``custos_prometheus.adapters.NoopMetricsAdapter``.
    """

    SCHEMA_REVISION: ClassVar[int] = 0

    async def query_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        """Raise :class:`QueryUnsupported` — logs are not configured."""
        raise QueryUnsupported("logs not configured")

    def tail_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        from_cursor: Cursor | None = None,
    ) -> AsyncIterator[LogRecord]:
        """Raise :class:`QueryUnsupported` — logs are not configured."""
        raise QueryUnsupported("logs not configured")

    async def query_step_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        """Raise :class:`QueryUnsupported` — logs are not configured."""
        raise QueryUnsupported("logs not configured")


@dataclass(frozen=True, slots=True)
class Providers:
    """The SPL providers the Observability and Audit Service consumes.

    Held on ``app.state.providers`` and handed to the read-back routes
    (OBS-IMPL-012..014), the audit-outbox drainer (OBS-IMPL-005/006), and the
    retention worker (OBS-IMPL-007). The Postgres pool backing ``metadata_store``
    is deferred and reclaimed at process exit; the log/metrics adapters hold no
    persistent client, so the bundle owns nothing that needs an explicit close at
    this phase.
    """

    metadata_store: MetadataStoreProvider
    log_query: LogQueryProvider
    metrics_query: MetricsQueryProvider


def build_log_query_provider(settings: Settings) -> LogQueryProvider:
    """Resolve the :class:`LogQueryProvider` from ``settings.log_query_provider``."""
    provider = settings.log_query_provider
    if provider == "loki":
        if settings.loki_url is None:  # pragma: no cover - settings enforces this
            raise ProviderConfigError(
                "CUSTOS_LOKI_URL is required to wire the loki LogQueryProvider"
            )
        from custos_loki.adapters import LokiLogQueryAdapter

        return cast(LogQueryProvider, LokiLogQueryAdapter(base_url=settings.loki_url))
    if provider == "noop":
        return NoopLogQueryAdapter()
    raise ProviderConfigError(
        f"unknown LogQueryProvider identifier {provider!r}; "
        "no adapter serves it (expected one of: loki, noop)"
    )


def build_metrics_query_provider(settings: Settings) -> MetricsQueryProvider:
    """Resolve the :class:`MetricsQueryProvider` from ``settings.metrics_query_provider``."""
    provider = settings.metrics_query_provider
    if provider == "prometheus":
        if settings.prometheus_url is None:  # pragma: no cover - settings enforces this
            raise ProviderConfigError(
                "CUSTOS_PROMETHEUS_URL is required to wire the prometheus MetricsQueryProvider"
            )
        from custos_prometheus.adapters import PrometheusMetricsAdapter

        return cast(
            MetricsQueryProvider,
            PrometheusMetricsAdapter(base_url=settings.prometheus_url),
        )
    if provider == "noop":
        from custos_prometheus.adapters import NoopMetricsAdapter

        return cast(MetricsQueryProvider, NoopMetricsAdapter())
    raise ProviderConfigError(
        f"unknown MetricsQueryProvider identifier {provider!r}; "
        "no adapter serves it (expected one of: prometheus, noop)"
    )


def build_metadata_store(settings: Settings) -> MetadataStoreProvider:
    """Build the Postgres :class:`MetadataStoreProvider` over a deferred pool.

    The DSN is captured synchronously; the underlying pool connects on first
    query, so this opens no socket. Mirrors
    ``custos_pg.adapters.metadata.make_adapter`` (which wires a bare
    :class:`~custos_pg.pool.LazyPool`), differing only in sourcing the DSN from
    :class:`Settings` rather than the process environment.
    """
    from custos_pg import PgMetadataAdapter
    from custos_pg.pool import LazyPool

    adapter = PgMetadataAdapter(lazy=LazyPool(settings.metadata_store_dsn))
    return cast(MetadataStoreProvider, adapter)


def load_providers(settings: Settings) -> Providers:
    """Construct all three SPL providers from ``settings``.

    Resolves each provider identifier to its adapter, failing fast with
    :class:`ProviderConfigError` on an unrecognised identifier. Opens no socket:
    the Postgres pool is deferred and the log/metrics adapters issue per-request
    HTTP, so the providers are safe to build inside a lifespan before any
    backend is reachable.
    """
    return Providers(
        metadata_store=build_metadata_store(settings),
        log_query=build_log_query_provider(settings),
        metrics_query=build_metrics_query_provider(settings),
    )


async def aclose_providers(providers: Providers) -> None:
    """Release provider-owned resources on shutdown.

    A no-op at this phase: the log/metrics adapters issue per-request HTTP and
    hold no persistent client, and the Postgres pool backing the metadata store
    is reclaimed at process exit (matching auth-service). This is the seam where
    later phases stop the background workers they attach to the app (the
    audit-outbox drainer, the retention worker, the alert dispatcher).
    """
    return None


__all__ = [
    "NoopLogQueryAdapter",
    "ProviderConfigError",
    "Providers",
    "aclose_providers",
    "build_log_query_provider",
    "build_metadata_store",
    "build_metrics_query_provider",
    "load_providers",
]
