"""Own the real runtime resource graph; no production in-memory or synthetic fallback exists."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import cast
from uuid import uuid4

import httpx
from custos_callctx.execution import load_execution_bindings
from custos_callctx.execution_client import ExecutionContextClient
from custos_csi.adapters import make_adapter
from custos_pg import PgMetadataAdapter
from custos_pg.adapters.lease import PgLeaseAdapter
from custos_pg.pool import LazyPool
from custos_spl.errors import MigrationRequired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_arm._readiness_diagnostics import (
    PROBE_ERRORS,
    Component,
    log_readiness_failure,
    probe_component,
    readiness_cycle,
)
from custos_arm.config import Settings, SettingsError
from custos_arm.credential_proxy import CredentialProxy
from custos_arm.io import IOBroker
from custos_arm.limit import ResourceLimiter
from custos_arm.resolve.resolver import ScopedCatalogActivityResolver
from custos_arm.result import ResultMapper
from custos_arm.runtime import RuntimeDriverDispatcher
from custos_arm.runtime.oci import OciContainerDriver
from custos_arm.scheduler import ActivityScheduler
from custos_arm.secrets import SecretInjector, SidecarTokenMinter
from custos_arm.store import ExecutionRepository
from custos_arm.store.artifact import ArtifactStoreClient


async def _blocking_probe(operation: Callable[[], object]) -> None:
    """Join bounded Kubernetes calls before cancellation can close their owning API client."""
    worker = asyncio.create_task(asyncio.to_thread(operation))
    cancelled = False
    while True:
        try:
            await asyncio.shield(worker)
            break
        except asyncio.CancelledError:
            if worker.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError


@dataclass(frozen=True)
class RuntimeResources:
    """Fully probed runtime collaborators whose owner must drain before closing their clients."""

    scheduler: ActivityScheduler
    credential_proxy: CredentialProxy
    probe: Callable[[], Awaitable[None]]


@asynccontextmanager
async def open_runtime(settings: Settings) -> AsyncIterator[RuntimeResources]:
    """Build/probe owned resources; log closed component evidence without changing admission."""
    settings.read_sidecar_ca_bundle()
    bindings = load_execution_bindings(os.environ)
    if bindings is None:
        raise SettingsError("runtime activation requires explicit execution-account bindings")
    if not bindings.identities:
        raise SettingsError("runtime readiness requires a configured execution-account reference")
    if settings.artifact_store != "csi":
        raise SettingsError("production runtime currently requires ARM_ARTIFACT_STORE=csi")
    root = Path(settings.staging_root)
    if not root.is_absolute() or root.is_symlink():
        raise SettingsError("runtime staging root must be an absolute private directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_mode & 0o077:
        raise SettingsError("runtime staging root must be owner-only")
    async with AsyncExitStack() as stack:
        pool = LazyPool(settings.metadata_store)
        stack.push_async_callback(pool.aclose)
        metadata = PgMetadataAdapter(lazy=pool)
        with readiness_cycle("startup"):
            await probe_component("metadata-schema", metadata.check_activity_execution_ready())
            await probe_component("execution-schema", metadata.check_execution_identity_ready())
            leases = PgLeaseAdapter(lazy=pool)
            await probe_component("lease-schema", leases.refresh_declared())
            if 2 not in leases.declared_revisions.get("LeaseStoreProvider", frozenset()):
                error = MigrationRequired([("LeaseStoreProvider", 2)])
                log_readiness_failure("lease-schema", "error", error)
                raise error
            await probe_component(
                "metadata",
                metadata.read_activity_execution("__runtime_probe__", "probe", "probe", 1),
            )
        artifacts = make_adapter()
        probe = uuid4().bytes

        async def chunks() -> AsyncIterator[bytes]:
            """Write only random non-secret readiness data through the real artifact provider."""
            yield probe

        async def probe_artifacts() -> None:
            """Check real CSI write/read/delete each cycle, reusing one non-secret probe blob."""
            async with asyncio.timeout(2):
                descriptor = await artifacts.put(
                    WorkspaceId("__runtime_probe__"),
                    chunks(),
                    "application/octet-stream",
                )
                try:
                    actual = b"".join(
                        [
                            part
                            async for part in artifacts.get(
                                WorkspaceId("__runtime_probe__"),
                                descriptor.artifact_id,
                            )
                        ]
                    )
                    if actual != probe:
                        raise RuntimeError("artifact readiness round-trip failed")
                finally:
                    await artifacts.delete(
                        WorkspaceId("__runtime_probe__"),
                        descriptor.artifact_id,
                        is_sweeper=True,
                    )

        http = await stack.enter_async_context(httpx.AsyncClient(trust_env=False))
        execution = ExecutionContextClient.from_env(http, bindings, os.environ)
        probe_headers = (
            {"dapr-api-token": os.environ["DAPR_API_TOKEN"]}
            if os.environ.get("DAPR_API_TOKEN")
            else {}
        )
        from kubernetes import client, config

        configuration = client.Configuration()
        configuration.retries = 0
        config.load_incluster_config(client_configuration=configuration)
        api = client.ApiClient(configuration)
        stack.callback(api.close)
        batch, core = client.BatchV1Api(api), client.CoreV1Api(api)
        driver = OciContainerDriver(
            batch_api=batch, core_api=core, staging_root=root, api_timeout=10
        )
        scheduler = ActivityScheduler(
            resolver=ScopedCatalogActivityResolver(
                http,
                catalog_endpoint=settings.catalog_endpoint,
                execution=execution,
            ),
            limiter=ResourceLimiter(settings),
            broker=IOBroker(
                ArtifactStoreClient(artifacts, max_bytes=settings.artifact_max_bytes),
                output_max_bytes=settings.output_max_bytes,
            ),
            injector=SecretInjector(
                token_minter=SidecarTokenMinter(
                    ttl_seconds=int(settings.max_timeout.total_seconds()),
                )
            ),
            mapper=ResultMapper(),
            dispatcher=RuntimeDriverDispatcher((driver,)),
            repository=ExecutionRepository(
                cast(MetadataStoreProvider, metadata),
                idempotency_ttl=settings.idempotency_ttl,
                durable_store=metadata,
            ),
            settings=settings,
        )

        probe_lock = asyncio.Lock()

        async def probe_service(
            component: Component, endpoint: str, headers: dict[str, str]
        ) -> None:
            """Retain component/status on failure without consuming or logging response bodies."""
            status = None
            try:
                async with http.stream(
                    "GET", endpoint.rstrip("/") + "/readyz", headers=headers, timeout=2
                ) as result:
                    status = result.status_code
                    if status != 200:
                        raise RuntimeError("runtime dependency is unavailable")
            except PROBE_ERRORS as error:
                log_readiness_failure(
                    component,
                    "http_status" if status is not None and status != 200 else "error",
                    error,
                    http_status=status,
                )
                raise

        async def probe_authorization() -> None:
            """Exercise the configured secret/Auth path using one explicit diagnostic identity."""
            headers = await probe_component(
                "execution-authorization",
                execution.headers(bindings.identities[0], "catalog-activity-read"),
            )
            await probe_service("catalog", settings.catalog_endpoint, headers)

        async def probe_dependencies() -> None:
            """Recheck collaborators, logging their causes before the unchanged group conversion."""
            async with probe_lock:
                with readiness_cycle("readiness"):
                    try:
                        async with asyncio.timeout(5), asyncio.TaskGroup() as probes:
                            probes.create_task(
                                probe_component(
                                    "metadata",
                                    metadata.read_activity_execution(
                                        "__runtime_probe__",
                                        "probe",
                                        "probe",
                                        1,
                                    ),
                                )
                            )
                            probes.create_task(probe_component("artifacts", probe_artifacts()))
                            probes.create_task(probe_authorization())
                            probes.create_task(
                                probe_service(
                                    "connector", settings.connector_endpoint, probe_headers
                                )
                            )
                            probes.create_task(
                                probe_component(
                                    "kubernetes-jobs",
                                    _blocking_probe(
                                        partial(
                                            batch.list_namespaced_job,
                                            settings.sandbox_namespace,
                                            limit=1,
                                            _request_timeout=2,
                                        )
                                    ),
                                )
                            )
                            probes.create_task(
                                probe_component(
                                    "kubernetes-pods",
                                    _blocking_probe(
                                        partial(
                                            core.list_namespaced_pod,
                                            settings.sandbox_namespace,
                                            limit=1,
                                            _request_timeout=2,
                                        )
                                    ),
                                )
                            )
                    except* PROBE_ERRORS as errors:
                        log_readiness_failure("dependencies", "group_failed", errors)
                        raise RuntimeError("required runtime dependency probe failed") from None

        try:
            with readiness_cycle("startup"):
                await probe_dependencies()
            yield RuntimeResources(
                scheduler,
                CredentialProxy(metadata, execution, http, settings.connector_endpoint),
                probe_dependencies,
            )
        finally:
            await scheduler.drain()
