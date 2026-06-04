"""Tests for :mod:`custos_workflow.providers` (WF-IMPL-043, WF-IMPL-080).

The default in-process metadata-store provider must mirror the
SPL Postgres adapter's ``updated_at`` semantics so consumers see
a fresh timestamp on every status transition. Without this the
default in-memory wiring would silently report stale ``updated_at``
values, which is a hard pitfall to debug downstream (alerts /
observability dashboards that key off the freshness of the row).

The WF-IMPL-080 tests pin :func:`load_run_components` against the
four ARM / Connector env-var combinations and assert that the
single lifespan-owned :class:`httpx.AsyncClient` is shared across
the publisher + ARM + Connector adapters (no second client, no
second socket pool). The Noop-fallback assertions guard the
sidecar-free dev / test path that WF-IMPL-043 established.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import httpx
import pytest
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkflowId as SplWorkflowId
from custos_spl.ids import WorkspaceId as SplWorkspaceId
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.interfaces.metadata_store import Run as SplRun

from custos_workflow.clients import (
    DaprActivityRuntimeClient,
    DaprConnectorClient,
    NoopActivityRuntimeClient,
    NoopConnectorClient,
)
from custos_workflow.clients.catalog import (
    DaprCatalogClient,
    FakeCatalogClient,
)
from custos_workflow.clients.trigger import (
    DaprTriggerServiceClient,
    NoopTriggerServiceClient,
)
from custos_workflow.providers import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_MS,
    ENV_APPROVAL_DEFAULT_TIMEOUT,
    ENV_ARM_APP_ID,
    ENV_CATALOG_APP_ID,
    ENV_CONNECTOR_APP_ID,
    ENV_ENVIRONMENT,
    ENV_MAX_FANOUT_WIDTH,
    ENV_METADATA_STORE,
    ENV_OUTBOUND_RPC_TIMEOUT_MS,
    ENV_REGISTER_SUB_MAX_RETRIES,
    ENV_RESUME_SUB_DEFAULT_TTL,
    ENV_RESUME_SUB_SWEEP_INTERVAL,
    ENV_TS_ENDPOINT,
    MetadataStorePool,
    _build_catalog_client,
    _build_trigger_client,
    _in_memory_metadata_store,
    _InProcessMetadataStoreProvider,
    _NotConfiguredCatalogClient,
    _redact_dsn_credentials,
    _resolve_approval_default_timeout,
    _resolve_max_fanout_width,
    _resolve_metadata_store_dsn,
    _resolve_outbound_rpc_timeout_seconds,
    _resolve_register_sub_max_retries,
    _resolve_resume_sub_default_ttl,
    _resolve_resume_sub_sweep_interval,
    load_run_components,
    open_metadata_store,
)
from custos_workflow.runs.replay import NoopReplayReconciler
from custos_workflow.runtime import FakeWorkflowRuntime
from custos_workflow.steps.resume import (
    DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS,
    ResumeSubscriptionReplayReconciler,
    WaitForStepHandler,
)
from custos_workflow.steps.resume.handler import (
    DEFAULT_REGISTER_SUB_MAX_RETRIES,
    DEFAULT_RESUME_SUB_TTL,
)
from custos_workflow.steps.sub_orchestration import (
    DEFAULT_APPROVAL_TIMEOUT,
    DEFAULT_MAX_FANOUT_WIDTH,
)


@pytest.mark.asyncio
async def test_update_run_status_refreshes_updated_at() -> None:
    """A status transition must bump ``updated_at`` to "now".

    Mirrors ``custos_pg/adapters/metadata.py``
    (``SET status = $3, reason = $4, updated_at = now()``).
    """
    provider = _InProcessMetadataStoreProvider()
    ws = SplWorkspaceId("ws-1")
    rid = SplRunId("run-1")
    initial_ts = datetime(2000, 1, 1, tzinfo=UTC)
    row = SplRun(
        workspace_id=ws,
        run_id=rid,
        workflow_id=SplWorkflowId("wf-1"),
        workflow_version="1",
        status="queued",
        reason=None,
        started_at=initial_ts,
        updated_at=initial_ts,
    )
    await provider.put_run(ws, row)

    before = datetime.now(UTC)
    updated = await provider.update_run_status(ws, rid, "running")
    after = datetime.now(UTC)

    assert updated.status == "running"
    assert updated.started_at == initial_ts  # immutable on status transitions
    assert before - timedelta(seconds=1) <= updated.updated_at <= after + timedelta(seconds=1)
    assert updated.updated_at > initial_ts


# ---------------------------------------------------------------------------
# WF-IMPL-080 — Configuration knobs + lifespan-owned HTTP client wiring
# ---------------------------------------------------------------------------


def _arm_endpoint_env() -> dict[str, str]:
    """Env map activating the ARM Dapr adapter only.

    Uses ``WF_ARM_ENDPOINT`` plus the default ``DAPR_HTTP_HOST`` /
    ``DAPR_HTTP_PORT`` fallbacks baked into
    :func:`custos_workflow.clients._dapr_invoke.read_dapr_env` so the
    test does not need to spin up a real sidecar.
    """
    return {ENV_ARM_APP_ID: "arm-app"}


def _connector_endpoint_env() -> dict[str, str]:
    """Env map activating the Connector Dapr adapter only."""
    return {ENV_CONNECTOR_APP_ID: "connector-app"}


def test_load_run_components_noop_when_endpoints_unset() -> None:
    """Both env vars unset → ``Noop`` adapters + no shared HTTP client.

    Pins the WF-IMPL-043 sidecar-free dev / test path so the
    in-process default never accidentally opens a socket pool
    against ``127.0.0.1:3500``.
    """
    components = load_run_components(env={}, workflow_runtime=FakeWorkflowRuntime())

    assert isinstance(components.outbound_activity_client, NoopActivityRuntimeClient)
    assert isinstance(components.outbound_connector_client, NoopConnectorClient)
    # The legacy sync slots also stay Noop in production by design
    # (see ``RunComponents.activity_client`` docstring).
    assert isinstance(components.activity_client, NoopActivityRuntimeClient)
    assert isinstance(components.connector_client, NoopConnectorClient)
    assert components.dapr_http_client is None


def test_load_run_components_dapr_arm_only_shares_http_client() -> None:
    """Only ARM env set → Dapr ARM + Noop Connector + shared HTTP client.

    Asserts the WF-IMPL-080 acceptance criterion that the
    lifespan-owned :class:`httpx.AsyncClient` is the *same object*
    the production adapter holds (one socket pool, never two).
    """
    components = load_run_components(
        env=_arm_endpoint_env(), workflow_runtime=FakeWorkflowRuntime()
    )

    try:
        assert isinstance(components.outbound_activity_client, DaprActivityRuntimeClient)
        assert isinstance(components.outbound_connector_client, NoopConnectorClient)
        # The sync slot the legacy Step Coordinator path consumes
        # must NEVER hold the async Dapr client — that would leak
        # un-awaited coroutines into ``handler.execute()`` if a
        # future caller invoked it with the production env in
        # place. See ``RunComponents.activity_client`` docstring.
        assert isinstance(components.activity_client, NoopActivityRuntimeClient)
        assert components.dapr_http_client is not None
        # Same instance — no second client, no second pool.
        assert components.outbound_activity_client.http_client is components.dapr_http_client
    finally:
        # Lifespan owns the client; the test must aclose() it
        # itself because no FastAPI lifespan ran here.
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


def test_load_run_components_dapr_connector_only_shares_http_client() -> None:
    """Only Connector env set → Noop ARM + Dapr Connector + shared HTTP client."""
    components = load_run_components(
        env=_connector_endpoint_env(), workflow_runtime=FakeWorkflowRuntime()
    )

    try:
        assert isinstance(components.outbound_activity_client, NoopActivityRuntimeClient)
        assert isinstance(components.outbound_connector_client, DaprConnectorClient)
        assert isinstance(components.connector_client, NoopConnectorClient)
        assert components.dapr_http_client is not None
        assert components.outbound_connector_client.http_client is components.dapr_http_client
    finally:
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


def test_load_run_components_dapr_both_share_single_http_client() -> None:
    """Both env vars set → Dapr ARM + Dapr Connector + *single* HTTP client.

    The headline WF-IMPL-080 invariant: a worker configured to talk
    to *both* upstreams must keep exactly one ``httpx.AsyncClient``
    (the lifespan-owned one on :attr:`RunComponents.dapr_http_client`)
    so production traffic shares a single socket pool.
    """
    components = load_run_components(
        env={**_arm_endpoint_env(), **_connector_endpoint_env()},
        workflow_runtime=FakeWorkflowRuntime(),
    )

    try:
        assert isinstance(components.outbound_activity_client, DaprActivityRuntimeClient)
        assert isinstance(components.outbound_connector_client, DaprConnectorClient)
        # Legacy sync slots stay Noop — see the per-test rationale
        # in ``test_load_run_components_dapr_arm_only_shares_http_client``.
        assert isinstance(components.activity_client, NoopActivityRuntimeClient)
        assert isinstance(components.connector_client, NoopConnectorClient)
        assert components.dapr_http_client is not None
        # The same instance reaches *both* adapters.
        assert (
            components.outbound_activity_client.http_client
            is components.outbound_connector_client.http_client
            is components.dapr_http_client
        )
    finally:
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


def test_load_run_components_honours_explicit_overrides() -> None:
    """Caller-supplied overrides win even when env vars are set.

    Mirrors the existing override contract for ``lifecycle_publisher``
    so tests can swap in :class:`FakeActivityRuntimeClient` /
    :class:`FakeConnectorClient` without the factory spinning up a
    real Dapr adapter (and without allocating an HTTP client only
    the production path needs).
    """
    fake_arm = NoopActivityRuntimeClient()
    fake_connector = NoopConnectorClient()
    components = load_run_components(
        env={**_arm_endpoint_env(), **_connector_endpoint_env()},
        workflow_runtime=FakeWorkflowRuntime(),
        activity_client=fake_arm,
        connector_client=fake_connector,
    )

    assert components.activity_client is fake_arm
    assert components.connector_client is fake_connector
    # Overrides feed both the legacy sync slot *and* the outbound
    # slot symmetrically: the caller-supplied Fake satisfies both
    # surfaces (the sync Protocol is what
    # :class:`FakeActivityRuntimeClient` /
    # :class:`FakeConnectorClient` implement), so tests need only
    # pass one object to drive both the Step Coordinator and the
    # bridges.
    assert components.outbound_activity_client is fake_arm
    assert components.outbound_connector_client is fake_connector
    # Overrides short-circuit the ``need_http_for_*`` predicates so
    # no socket pool is opened (the publisher env is unset here).
    assert components.dapr_http_client is None


# ---------------------------------------------------------------------------
# WF-IMPL-080 — Outbound-RPC timeout knob
# ---------------------------------------------------------------------------


def test_resolve_outbound_rpc_timeout_default_when_unset() -> None:
    """Unset ``WF_OUTBOUND_RPC_TIMEOUT_MS`` → the 10 s default."""
    timeout = _resolve_outbound_rpc_timeout_seconds({})

    assert timeout == DEFAULT_OUTBOUND_RPC_TIMEOUT_MS / 1000.0


def test_resolve_outbound_rpc_timeout_parses_positive_int() -> None:
    """Positive integer ms → seconds float."""
    timeout = _resolve_outbound_rpc_timeout_seconds({ENV_OUTBOUND_RPC_TIMEOUT_MS: "2500"})

    assert timeout == 2.5


@pytest.mark.parametrize("bad", ["abc", "1.5", "1e3"])
def test_resolve_outbound_rpc_timeout_rejects_non_integer(bad: str) -> None:
    """Non-integer values are surfaced eagerly at process start.

    Bad operator config must not be deferred to first-request
    so :func:`load_run_components` (called from the FastAPI
    lifespan) crashes the worker before /readyz flips to 200.
    """
    with pytest.raises(ValueError):
        _resolve_outbound_rpc_timeout_seconds({ENV_OUTBOUND_RPC_TIMEOUT_MS: bad})


@pytest.mark.parametrize("bad", ["0", "-1", "-100"])
def test_resolve_outbound_rpc_timeout_rejects_non_positive(bad: str) -> None:
    """Zero or negative timeouts are operator typos, never intent."""
    with pytest.raises(ValueError):
        _resolve_outbound_rpc_timeout_seconds({ENV_OUTBOUND_RPC_TIMEOUT_MS: bad})


def test_resolve_outbound_rpc_timeout_threaded_into_dapr_adapter() -> None:
    """The knob flows end-to-end into the Dapr adapter's ``timeout`` field."""
    components = load_run_components(
        env={**_arm_endpoint_env(), ENV_OUTBOUND_RPC_TIMEOUT_MS: "2500"},
        workflow_runtime=FakeWorkflowRuntime(),
    )

    try:
        assert isinstance(components.outbound_activity_client, DaprActivityRuntimeClient)
        assert components.outbound_activity_client.timeout == 2.5
    finally:
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


# ---------------------------------------------------------------------------
# WF-IMPL-094 — Sub-orchestration knobs (fan-out width + approval timeout)
# ---------------------------------------------------------------------------


def test_resolve_max_fanout_width_default_when_unset() -> None:
    assert _resolve_max_fanout_width({}) == DEFAULT_MAX_FANOUT_WIDTH


def test_resolve_max_fanout_width_parses_positive_int() -> None:
    assert _resolve_max_fanout_width({ENV_MAX_FANOUT_WIDTH: "32"}) == 32


@pytest.mark.parametrize("bad", ["abc", "1.5", "0", "-1"])
def test_resolve_max_fanout_width_rejects_bad_values(bad: str) -> None:
    with pytest.raises(ValueError):
        _resolve_max_fanout_width({ENV_MAX_FANOUT_WIDTH: bad})


def test_resolve_approval_default_timeout_default_when_unset() -> None:
    assert _resolve_approval_default_timeout({}) == DEFAULT_APPROVAL_TIMEOUT


def test_resolve_approval_default_timeout_parses_iso8601() -> None:
    assert _resolve_approval_default_timeout({ENV_APPROVAL_DEFAULT_TIMEOUT: "PT48H"}) == timedelta(
        hours=48
    )
    assert _resolve_approval_default_timeout({ENV_APPROVAL_DEFAULT_TIMEOUT: "P2W"}) == timedelta(
        weeks=2
    )


@pytest.mark.parametrize("bad", ["24h", "PT0S", "P1Y", "", "P", "PT0.5S", "PT1H30.5S"])
def test_resolve_approval_default_timeout_rejects_bad_values(bad: str) -> None:
    # ``""`` falls back to the default, so only the genuinely malformed
    # / non-positive / calendar / sub-second values raise.
    if bad == "":
        assert _resolve_approval_default_timeout({ENV_APPROVAL_DEFAULT_TIMEOUT: bad}) == (
            DEFAULT_APPROVAL_TIMEOUT
        )
        return
    with pytest.raises(ValueError):
        _resolve_approval_default_timeout({ENV_APPROVAL_DEFAULT_TIMEOUT: bad})


def test_suborchestration_knobs_thread_onto_run_components() -> None:
    components = load_run_components(
        env={ENV_MAX_FANOUT_WIDTH: "7", ENV_APPROVAL_DEFAULT_TIMEOUT: "PT12H"},
        workflow_runtime=FakeWorkflowRuntime(),
    )

    assert components.max_fanout_width == 7
    assert components.approval_default_timeout == timedelta(hours=12)


def test_suborchestration_knobs_default_onto_run_components() -> None:
    components = load_run_components(env={}, workflow_runtime=FakeWorkflowRuntime())

    assert components.max_fanout_width == DEFAULT_MAX_FANOUT_WIDTH
    assert components.approval_default_timeout == DEFAULT_APPROVAL_TIMEOUT


def test_provider_iso8601_pattern_matches_wait_module() -> None:
    """The provider's self-contained ISO grammar must track the run-time one."""
    from custos_workflow.providers import _ISO8601_DURATION_PATTERN as provider_pattern
    from custos_workflow.runs.wait import _ISO8601_DURATION_PATTERN as wait_pattern

    assert provider_pattern.pattern == wait_pattern.pattern


# ---------------------------------------------------------------------------
# WF-IMPL-108 — Resume Subscription Manager wiring (Trigger Service client,
# mirror repository, production ReplayReconciler, config knobs)
# ---------------------------------------------------------------------------


def _trigger_endpoint_env() -> dict[str, str]:
    """Env map activating the Dapr Trigger Service client only."""
    return {ENV_TS_ENDPOINT: "trigger-app"}


def test_resolve_resume_sub_default_ttl_default_when_unset() -> None:
    """Unset ``WF_RESUME_SUB_DEFAULT_TTL`` → the ``PT24H`` default string."""
    assert _resolve_resume_sub_default_ttl({}) == DEFAULT_RESUME_SUB_TTL


def test_resolve_resume_sub_default_ttl_parses_iso8601() -> None:
    """A valid ISO-8601 duration is returned verbatim (handler takes a str)."""
    assert _resolve_resume_sub_default_ttl({ENV_RESUME_SUB_DEFAULT_TTL: "PT12H"}) == "PT12H"
    assert _resolve_resume_sub_default_ttl({ENV_RESUME_SUB_DEFAULT_TTL: "P2W"}) == "P2W"


@pytest.mark.parametrize("bad", ["24h", "PT0S", "P1Y", "P", "PT0.5S", "PT1H30.5S", ""])
def test_resolve_resume_sub_default_ttl_rejects_bad_values(bad: str) -> None:
    """Malformed / non-positive / calendar / sub-second durations crash at startup.

    ``""`` falls back to the default; everything else raises so a
    misconfigured TTL fails the worker before /readyz flips to 200.
    Sub-second precision is rejected for parity with
    :func:`_resolve_approval_default_timeout`.
    """
    if bad == "":
        assert _resolve_resume_sub_default_ttl({ENV_RESUME_SUB_DEFAULT_TTL: bad}) == (
            DEFAULT_RESUME_SUB_TTL
        )
        return
    with pytest.raises(ValueError):
        _resolve_resume_sub_default_ttl({ENV_RESUME_SUB_DEFAULT_TTL: bad})


def test_resolve_register_sub_max_retries_default_when_unset() -> None:
    """Unset ``WF_REGISTER_SUB_MAX_RETRIES`` → the ``5`` default."""
    assert _resolve_register_sub_max_retries({}) == DEFAULT_REGISTER_SUB_MAX_RETRIES


def test_resolve_register_sub_max_retries_parses_positive_int() -> None:
    assert _resolve_register_sub_max_retries({ENV_REGISTER_SUB_MAX_RETRIES: "8"}) == 8


@pytest.mark.parametrize("bad", ["abc", "1.5", "0", "-1"])
def test_resolve_register_sub_max_retries_rejects_bad_values(bad: str) -> None:
    with pytest.raises(ValueError):
        _resolve_register_sub_max_retries({ENV_REGISTER_SUB_MAX_RETRIES: bad})


def test_resolve_resume_sub_sweep_interval_default_when_unset() -> None:
    """Unset ``WF_RESUME_SUB_SWEEP_INTERVAL`` → the 300s default."""
    assert _resolve_resume_sub_sweep_interval({}) == DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS


def test_resolve_resume_sub_sweep_interval_parses_positive_number() -> None:
    assert _resolve_resume_sub_sweep_interval({ENV_RESUME_SUB_SWEEP_INTERVAL: "45"}) == 45.0
    assert _resolve_resume_sub_sweep_interval({ENV_RESUME_SUB_SWEEP_INTERVAL: "2.5"}) == 2.5


@pytest.mark.parametrize("bad", ["abc", "0", "-1", "-0.5", "nan", "inf"])
def test_resolve_resume_sub_sweep_interval_rejects_bad_values(bad: str) -> None:
    with pytest.raises(ValueError):
        _resolve_resume_sub_sweep_interval({ENV_RESUME_SUB_SWEEP_INTERVAL: bad})


def test_build_trigger_client_noop_when_endpoint_unset() -> None:
    """No ``WF_TS_ENDPOINT`` → the in-process Noop client (no socket pool)."""
    client = _build_trigger_client(env={}, http_client=None, timeout_seconds=10.0)

    assert isinstance(client, NoopTriggerServiceClient)


def test_load_run_components_noop_trigger_when_endpoint_unset() -> None:
    """Sidecar-free path: Noop trigger ⇒ inert ``NoopReplayReconciler``.

    The resume handler is still wired (the orchestrator always needs
    one to park ``waitFor:`` nodes per WF-IMPL-107), but with no
    Trigger Service endpoint the reconciler must stay Noop so nothing
    reaches the Noop trigger client (whose methods raise
    ``NotImplementedError``).
    """
    components = load_run_components(env={}, workflow_runtime=FakeWorkflowRuntime())

    assert isinstance(components.resume_handler, WaitForStepHandler)
    assert isinstance(components.replay_reconciler, NoopReplayReconciler)
    assert components.dapr_http_client is None


def test_load_run_components_dapr_trigger_wires_reconciler_and_shares_http_client() -> None:
    """``WF_TS_ENDPOINT`` set → production reconciler + shared HTTP client.

    The headline WF-IMPL-108 invariant: the
    :class:`ResumeSubscriptionReplayReconciler` activates, holds the
    lifespan-owned :class:`httpx.AsyncClient` via its Dapr trigger
    client, and shares the *same* mirror repository as the
    :class:`WaitForStepHandler` so a mirror persisted by the handler
    is visible to the reconciler on replay.
    """
    components = load_run_components(
        env=_trigger_endpoint_env(), workflow_runtime=FakeWorkflowRuntime()
    )

    try:
        assert isinstance(components.replay_reconciler, ResumeSubscriptionReplayReconciler)
        assert components.dapr_http_client is not None
        trigger = components.replay_reconciler._trigger_client
        assert isinstance(trigger, DaprTriggerServiceClient)
        assert trigger.http_client is components.dapr_http_client
        # Handler + reconciler MUST share one mirror repository.
        assert components.replay_reconciler.mirror_repo is components.resume_handler.mirror_repo
    finally:
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


def test_load_run_components_explicit_reconciler_override_wins() -> None:
    """An injected ``replay_reconciler`` wins even when the trigger env is set.

    The override path supplies its own reconciler (tests inject Fakes
    / spies), so the factory must neither build the production
    reconciler nor open a socket pool just for the trigger client.
    """
    sentinel = NoopReplayReconciler()
    components = load_run_components(
        env=_trigger_endpoint_env(),
        workflow_runtime=FakeWorkflowRuntime(),
        replay_reconciler=sentinel,
    )

    assert components.replay_reconciler is sentinel
    # No trigger socket pool opened because the override short-circuits
    # ``need_http_for_trigger`` (the publisher / ARM / connector env
    # are unset here).
    assert components.dapr_http_client is None


def test_resume_knobs_thread_into_handler() -> None:
    """TTL + retry knobs flow end-to-end into the WaitForStepHandler."""
    components = load_run_components(
        env={
            ENV_RESUME_SUB_DEFAULT_TTL: "PT12H",
            ENV_REGISTER_SUB_MAX_RETRIES: "7",
        },
        workflow_runtime=FakeWorkflowRuntime(),
    )

    assert components.resume_handler._default_ttl == "PT12H"
    assert components.resume_handler._max_register_retries == 7


def test_resume_sweep_interval_defaults_onto_components() -> None:
    """Unset sweep-interval env → the 300s default lands on the bundle."""
    components = load_run_components(
        env={},
        workflow_runtime=FakeWorkflowRuntime(),
    )

    assert components.resume_sweep_interval_seconds == DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS


def test_resume_sweep_interval_threads_onto_components() -> None:
    """The resolved sweep interval is threaded onto the RunComponents bundle."""
    components = load_run_components(
        env={ENV_RESUME_SUB_SWEEP_INTERVAL: "30"},
        workflow_runtime=FakeWorkflowRuntime(),
    )

    assert components.resume_sweep_interval_seconds == 30.0


def test_resume_default_ttl_threads_into_production_reconciler() -> None:
    """The resolved default TTL also reaches the production reconciler."""
    components = load_run_components(
        env={**_trigger_endpoint_env(), ENV_RESUME_SUB_DEFAULT_TTL: "PT6H"},
        workflow_runtime=FakeWorkflowRuntime(),
    )

    try:
        assert isinstance(components.replay_reconciler, ResumeSubscriptionReplayReconciler)
        assert components.replay_reconciler._default_ttl == "PT6H"
        assert components.resume_handler._default_ttl == "PT6H"
    finally:
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


# ---------------------------------------------------------------------------
# WF-IMPL-114 — Catalog client wiring (DaprCatalogClient, production fail-fast,
# shared HTTP client, single shared client across controller + validator)
# ---------------------------------------------------------------------------


def _catalog_endpoint_env() -> dict[str, str]:
    """Env map activating the Dapr Catalog client only."""
    return {ENV_CATALOG_APP_ID: "catalog-app"}


def test_build_catalog_client_not_configured_when_endpoint_unset() -> None:
    """No ``WF_CATALOG_ENDPOINT`` (non-prod) → the not-configured stub."""
    client = _build_catalog_client(env={}, http_client=None, timeout_seconds=10.0)

    assert isinstance(client, _NotConfiguredCatalogClient)


def test_build_catalog_client_production_requires_endpoint() -> None:
    """``ENVIRONMENT=production`` + no ``WF_CATALOG_ENDPOINT`` → fail fast."""
    with pytest.raises(RuntimeError, match=ENV_CATALOG_APP_ID):
        _build_catalog_client(
            env={ENV_ENVIRONMENT: "production"},
            http_client=None,
            timeout_seconds=10.0,
        )


@pytest.mark.parametrize("env_value", ["Production", "PRODUCTION", "production"])
def test_build_catalog_client_production_match_is_case_insensitive(env_value: str) -> None:
    """The production fail-fast matches ``ENVIRONMENT`` case-insensitively."""
    with pytest.raises(RuntimeError):
        _build_catalog_client(
            env={ENV_ENVIRONMENT: env_value},
            http_client=None,
            timeout_seconds=10.0,
        )


def test_build_catalog_client_non_production_keeps_stub() -> None:
    """A non-production ``ENVIRONMENT`` keeps the not-configured fallback."""
    client = _build_catalog_client(
        env={ENV_ENVIRONMENT: "staging"},
        http_client=None,
        timeout_seconds=10.0,
    )

    assert isinstance(client, _NotConfiguredCatalogClient)


def test_build_catalog_client_dapr_when_endpoint_set() -> None:
    """``WF_CATALOG_ENDPOINT`` set → the production Dapr client over the shared pool."""
    http_client = httpx.AsyncClient()
    try:
        client = _build_catalog_client(
            env=_catalog_endpoint_env(),
            http_client=http_client,
            timeout_seconds=10.0,
        )
        assert isinstance(client, DaprCatalogClient)
        assert client.http_client is http_client
    finally:
        import asyncio

        asyncio.run(http_client.aclose())


def test_load_run_components_not_configured_catalog_when_endpoint_unset() -> None:
    """Sidecar-free path: the controller + validator share the stub, no socket pool."""
    components = load_run_components(env={}, workflow_runtime=FakeWorkflowRuntime())

    catalog = components.run_controller._catalog
    assert isinstance(catalog, _NotConfiguredCatalogClient)
    assert components.start_run_validator._catalog is catalog
    assert components.dapr_http_client is None


def test_load_run_components_dapr_catalog_wires_and_shares_http_client() -> None:
    """``WF_CATALOG_ENDPOINT`` set → DaprCatalogClient shared across both consumers.

    The headline WF-IMPL-114 invariant: a *single* Catalog client
    (holding the lifespan-owned :class:`httpx.AsyncClient`) backs both
    the :class:`RunController` and the :class:`StartRunValidator` — no
    second Catalog connection per request.
    """
    components = load_run_components(
        env=_catalog_endpoint_env(), workflow_runtime=FakeWorkflowRuntime()
    )

    try:
        catalog = components.run_controller._catalog
        assert isinstance(catalog, DaprCatalogClient)
        assert catalog.http_client is components.dapr_http_client
        # Controller + validator MUST share the one Catalog client.
        assert components.start_run_validator._catalog is catalog
    finally:
        if components.dapr_http_client is not None:
            import asyncio

            asyncio.run(components.dapr_http_client.aclose())


def test_load_run_components_catalog_override_wins() -> None:
    """An injected ``catalog`` wins even when the Catalog env is set.

    The override path supplies its own client (tests inject Fakes), so
    the factory must neither build the Dapr client nor open a socket
    pool just for the Catalog client.
    """
    fake = FakeCatalogClient()
    components = load_run_components(
        env=_catalog_endpoint_env(),
        workflow_runtime=FakeWorkflowRuntime(),
        catalog=fake,
    )

    assert components.run_controller._catalog is fake
    assert components.start_run_validator._catalog is fake
    # No Catalog socket pool opened because the override short-circuits
    # ``need_http_for_catalog`` (the publisher / ARM / connector /
    # trigger env are unset here).
    assert components.dapr_http_client is None


# ---------------------------------------------------------------------------
# WF-IMPL-115 — Durable metadata store (WF_METADATA_STORE config + lifespan
# pool + readiness). The asyncpg pool is never opened for real here: the
# ``MetadataStorePool.open`` / ``aclose`` boundary is patched so the tests
# stay sidecar-free, matching the in-memory-default invariant.
# ---------------------------------------------------------------------------


class _FakeMetadataProvider:
    """A stand-in :class:`MetadataStoreProvider` returned by a patched pool."""


def test_resolve_metadata_store_dsn_none_when_unset() -> None:
    """No DSN + non-production env → ``None`` (the in-memory fallback)."""
    assert _resolve_metadata_store_dsn({}) is None


def test_resolve_metadata_store_dsn_returns_trimmed_dsn() -> None:
    """A set DSN is returned with surrounding whitespace stripped."""
    dsn = "postgresql://user:pw@db:5432/custos"
    assert _resolve_metadata_store_dsn({ENV_METADATA_STORE: f"  {dsn}  "}) == dsn


def test_resolve_metadata_store_dsn_production_requires_dsn() -> None:
    """Production + unset DSN fails fast so a worker never boots in-memory."""
    with pytest.raises(RuntimeError, match=ENV_METADATA_STORE):
        _resolve_metadata_store_dsn({ENV_ENVIRONMENT: "production"})


@pytest.mark.parametrize("env_value", ["Production", "PRODUCTION", "production"])
def test_resolve_metadata_store_dsn_production_match_case_insensitive(env_value: str) -> None:
    """The production fail-fast check is case-insensitive."""
    with pytest.raises(RuntimeError, match=ENV_METADATA_STORE):
        _resolve_metadata_store_dsn({ENV_ENVIRONMENT: env_value})


def test_resolve_metadata_store_dsn_non_production_keeps_fallback() -> None:
    """A non-production env with no DSN keeps the in-memory fallback."""
    assert _resolve_metadata_store_dsn({ENV_ENVIRONMENT: "staging"}) is None


def test_in_memory_metadata_store_is_in_process_provider() -> None:
    """The in-memory default wraps the process-local provider."""
    provider = _in_memory_metadata_store()
    assert isinstance(provider, _InProcessMetadataStoreProvider)


@pytest.mark.asyncio
async def test_open_metadata_store_in_memory_when_dsn_unset() -> None:
    """Unset DSN → in-memory provider, no pool, no readiness detail."""
    provider, pool, detail = await open_metadata_store({})
    assert isinstance(provider, _InProcessMetadataStoreProvider)
    assert pool is None
    assert detail is None


@pytest.mark.asyncio
async def test_open_metadata_store_production_requires_dsn() -> None:
    """Production + unset DSN propagates the fail-fast ``RuntimeError``."""
    with pytest.raises(RuntimeError, match=ENV_METADATA_STORE):
        await open_metadata_store({ENV_ENVIRONMENT: "production"})


@pytest.mark.asyncio
async def test_open_metadata_store_opens_pool_when_dsn_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A set DSN opens the pool and returns the durable adapter + handle."""
    fake_provider = cast(MetadataStoreProvider, _FakeMetadataProvider())
    opened: list[str] = []

    async def fake_open(self: MetadataStorePool) -> object:
        opened.append(self.dsn)
        return fake_provider

    monkeypatch.setattr(MetadataStorePool, "open", fake_open)

    dsn = "postgresql://user:pw@db:5432/custos"
    provider, pool, detail = await open_metadata_store({ENV_METADATA_STORE: dsn})

    assert provider is fake_provider
    assert isinstance(pool, MetadataStorePool)
    assert pool.dsn == dsn
    assert detail is None
    assert opened == [dsn]


@pytest.mark.asyncio
async def test_open_metadata_store_degrades_on_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pool open failure degrades to in-memory + a readiness detail.

    The worker must not crash on a transient database outage: the pod
    stays up while ``/readyz`` reports 503 (via the returned detail) so
    traffic is not routed while persistence is unavailable. The pool is
    closed on the failure path so a half-open handle never leaks.
    """
    closed: list[bool] = []

    async def fake_open(self: MetadataStorePool) -> object:
        raise ConnectionError("boom: cannot reach database")

    async def fake_aclose(self: MetadataStorePool) -> None:
        closed.append(True)

    monkeypatch.setattr(MetadataStorePool, "open", fake_open)
    monkeypatch.setattr(MetadataStorePool, "aclose", fake_aclose)

    provider, pool, detail = await open_metadata_store(
        {ENV_METADATA_STORE: "postgresql://user:pw@db:5432/custos"}
    )

    assert isinstance(provider, _InProcessMetadataStoreProvider)
    assert pool is None
    assert detail is not None
    assert "metadata store unavailable" in detail
    assert "boom: cannot reach database" in detail
    # The half-open pool was closed on the failure path.
    assert closed == [True]


@pytest.mark.asyncio
async def test_open_metadata_store_redacts_dsn_credentials_in_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DSN echoed in the connect error is scrubbed from the readiness detail.

    asyncpg folds the DSN into some connection errors; since the detail
    is returned in the ``/readyz`` body, the embedded password must not
    leak to anyone who can reach the probe. The userinfo segment is
    replaced wholesale while the rest of the message survives.
    """

    async def fake_open(self: MetadataStorePool) -> object:
        raise ConnectionError("could not connect to postgresql://admin:s3cret@db:5432/custos")

    async def fake_aclose(self: MetadataStorePool) -> None:
        return None

    monkeypatch.setattr(MetadataStorePool, "open", fake_open)
    monkeypatch.setattr(MetadataStorePool, "aclose", fake_aclose)

    _provider, _pool, detail = await open_metadata_store(
        {ENV_METADATA_STORE: "postgresql://admin:s3cret@db:5432/custos"}
    )

    assert detail is not None
    assert "s3cret" not in detail
    assert "admin" not in detail
    assert "//<redacted>@db:5432/custos" in detail


def test_redact_dsn_credentials_strips_userinfo() -> None:
    """The userinfo (user + password) segment is replaced; rest is kept."""
    redacted = _redact_dsn_credentials("boom postgresql://admin:s3cret@db:5432/custos tail")
    assert redacted == "boom postgresql://<redacted>@db:5432/custos tail"


def test_redact_dsn_credentials_noop_without_userinfo() -> None:
    """Text with no ``user@`` userinfo is returned unchanged."""
    msg = "connection timed out after 5s (no dsn here)"
    assert _redact_dsn_credentials(msg) == msg


@pytest.mark.asyncio
async def test_metadata_store_pool_aclose_noop_when_unopened() -> None:
    """``aclose`` on a never-opened pool is a no-op (no pool to close)."""
    pool = MetadataStorePool(dsn="postgresql://user:pw@db:5432/custos")
    await pool.aclose()  # must not raise


@pytest.mark.asyncio
async def test_metadata_store_pool_open_and_aclose_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``open`` creates the asyncpg pool; ``aclose`` releases it once.

    Both the ``asyncpg.create_pool`` call and the
    ``custos_pg.PgMetadataAdapter`` construction are patched so the test
    never touches a real database, while still exercising the real
    :meth:`MetadataStorePool.open` / :meth:`aclose` bodies (pool
    ownership + idempotent close).
    """
    import asyncpg
    import custos_pg

    pool_closed: list[bool] = []

    class _FakePool:
        async def close(self) -> None:
            pool_closed.append(True)

    fake_pool = _FakePool()
    created_with: dict[str, object] = {}

    async def fake_create_pool(*, dsn: str) -> _FakePool:
        created_with["dsn"] = dsn
        return fake_pool

    def fake_adapter(*, pool: object) -> _FakeMetadataProvider:
        created_with["pool"] = pool
        return _FakeMetadataProvider()

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(custos_pg, "PgMetadataAdapter", fake_adapter)

    dsn = "postgresql://user:pw@db:5432/custos"
    pool = MetadataStorePool(dsn=dsn)
    provider = await pool.open()

    assert isinstance(provider, _FakeMetadataProvider)
    assert created_with == {"dsn": dsn, "pool": fake_pool}

    # First close releases the asyncpg pool; second is an idempotent no-op.
    await pool.aclose()
    await pool.aclose()
    assert pool_closed == [True]


def test_load_run_components_defaults_in_memory_metadata_store() -> None:
    """With no override the bundle carries the in-memory provider."""
    components = load_run_components(env={}, workflow_runtime=FakeWorkflowRuntime())
    assert isinstance(components.metadata_store, _InProcessMetadataStoreProvider)


def test_load_run_components_metadata_store_override_wins() -> None:
    """An injected ``metadata_store`` is threaded onto the bundle verbatim."""
    fake = cast(MetadataStoreProvider, _FakeMetadataProvider())
    components = load_run_components(
        env={},
        workflow_runtime=FakeWorkflowRuntime(),
        metadata_store=fake,
    )
    assert components.metadata_store is fake
