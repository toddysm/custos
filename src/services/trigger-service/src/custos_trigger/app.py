"""Trigger Service FastAPI application factory (TS-IMPL-003).

:func:`create_app` builds the FastAPI application: the ``/healthz`` +
``/readyz`` probes, the call-context middleware (with dev shim), and a
lifespan hook that marks the app ready once startup completes.

The factory is import-safe and validates authentication configuration without
opening sockets. Resource-owning work belongs in the FastAPI lifespan context.
The lifespan binds the SPL provider bundle + domain store adapters
(TS-IMPL-008) onto ``app.state``; the REST/RPC surface across TS-IMPL-015..018
grows in place atop them without changing the factory's import-safety contract.

Configuration today is read directly from the process environment because the
typed settings loader is a later phase (TS-IMPL-004). Two knobs steer the
call-context middleware:

- ``TRIGGER_AUTHZ_ENDPOINT`` (default empty) — when empty the dev shim is
  active; when set, production call-context validation against the Auth
  Service's public signing keys is required.
- ``APP_API_TOKEN`` authenticates sidecar-to-app requests independently of
  user contexts; resume routes require explicit Workflow app/namespace identity
  from Dapr >=1.18.6 with mutual TLS.
- ``ENVIRONMENT`` (default ``development``) — ``production`` with an empty
  authz endpoint refuses to boot (the dev shim is forbidden in production).
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from custos_trigger._version import __version__
from custos_trigger.api import register_exception_handlers
from custos_trigger.api.routes import resume_rpc_router, subscriptions_router
from custos_trigger.api.routes.dead_letters import dead_letter_error_handler
from custos_trigger.api.routes.dead_letters import router as dead_letters_router
from custos_trigger.dead_letters import DeadLetterAdminError
from custos_trigger.dependencies import WorkflowCallerVerifier
from custos_trigger.health import router as health_router
from custos_trigger.middleware import (
    CallContextError,
    CallContextMiddleware,
    DaprWorkflowCallerVerifier,
    call_context_error_handler,
)
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers, load_providers
from custos_trigger.receivers import build_internal_event_router
from custos_trigger.receivers.sources import (
    CONNECTOR_EVENTS_PATH,
    NORMALIZED_EVENTS_PATH,
    build_source_event_router,
)
from custos_trigger.receivers.webhook import DaprWebhookTransport, build_webhook_router
from custos_trigger.settings import (
    DEFAULT_CONNECTOR_EVENTS_TOPIC,
    DEFAULT_DISPATCH_MAX_RETRIES,
    DEFAULT_FANOUT_MAX_DEPTH,
    DEFAULT_NORMALIZED_TOPIC,
    DEFAULT_PUBSUB_COMPONENT,
    DEFAULT_RESUME_DEFAULT_TTL_SECONDS,
    DEFAULT_WORKFLOW_EVENTS_TOPIC,
    ENV_APP_API_TOKEN,
    ENV_CONNECTOR_APP_ID,
    ENV_CONNECTOR_EVENTS_TOPIC,
    ENV_DISPATCH_MAX_RETRIES,
    ENV_FANOUT_MAX_DEPTH,
    ENV_GATEWAY_APP_ID,
    ENV_GATEWAY_NAMESPACE,
    ENV_METADATA_STORE,
    ENV_NORMALIZED_TOPIC,
    ENV_PUBSUB_COMPONENT,
    ENV_RESUME_DEFAULT_TTL_SECONDS,
    ENV_WORKFLOW_APP_ID,
    ENV_WORKFLOW_ENDPOINT,
    ENV_WORKFLOW_EVENTS_TOPIC,
    ENV_WORKFLOW_NAMESPACE,
    read_receiver_configuration_enabled,
    read_source_receivers_enabled,
    read_webhook_receiver_enabled,
)
from custos_trigger.stores import (
    ResumeSubscriptionStore,
    ScheduleStore,
    SubscriptionStore,
)

if TYPE_CHECKING:
    import httpx
    from custos_callctx import CallContextVerifier
    from fastapi import FastAPI

    from custos_trigger.pipeline.dispatch import AuditSink, Dispatcher

logger = logging.getLogger("custos_trigger")

#: Dapr app-id used to reach the Workflow Service when ``TRIGGER_WF_ENDPOINT``
#: is unset (the chart always sets it; this keeps dev/test bootable).
_DEFAULT_WORKFLOW_APP_ID: str = "workflow-service"


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    """Parse a non-negative int env knob, falling back to ``default``."""
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def create_app(
    *,
    authz_endpoint: str | None = None,
    environment: str | None = None,
    providers: Providers | None = None,
    dispatcher: Dispatcher | None = None,
    audit_sink: AuditSink | None = None,
    receiver_configuration_enabled: bool | None = None,
    workflow_caller_verifier: WorkflowCallerVerifier | None = None,
    call_context_verifier: CallContextVerifier | None = None,
    app_api_token: str | None = None,
    source_receivers_enabled: bool | None = None,
    webhook_receiver_enabled: bool | None = None,
    webhook_transport: DaprWebhookTransport | None = None,
) -> FastAPI:
    """Build and return the Trigger Service FastAPI application.

    Args:
        authz_endpoint: Auth Service URL steering the call-context
            middleware. Defaults to the ``TRIGGER_AUTHZ_ENDPOINT`` env var
            (empty enables the dev shim). The typed settings loader
            (TS-IMPL-004) will supply this from :class:`Settings`.
        environment: Deployment environment. Defaults to the ``ENVIRONMENT``
            env var (``development`` when unset). ``production`` with an
            empty ``authz_endpoint`` refuses to boot.
        providers: Pre-built SPL :class:`Providers` bundle. When ``None``
            (the default) the lifespan builds one from the
            ``TRIGGER_METADATA_STORE`` env knob — empty/unset selects the
            in-process backend (see :func:`custos_trigger.providers.load_providers`).
            Tests inject an in-memory bundle here to avoid a database.
        dispatcher: Pre-built matching/dispatch
            :class:`~custos_trigger.pipeline.dispatch.Dispatcher`. When ``None``
            (the default) the lifespan builds one over an owned
            :class:`httpx.AsyncClient` reaching the Workflow Service through
            Dapr. Tests inject a dispatcher wrapping a fake Workflow client.
        audit_sink: Audit surface the resume RPCs (and the internally-built
            dispatcher) emit to. When ``None`` (the default) the lifespan wires
            a :class:`~custos_trigger._telemetry.TelemetryAuditSink`, which
            records OTel pipeline counters/spans and forwards to a
            :class:`~custos_trigger.pipeline.dispatch.NoopAuditSink`. Tests
            inject a recording sink to assert
            ``resume.subscription.divergent`` emission.
        receiver_configuration_enabled: Default-off compatibility gate, read from
            ``TRIGGER_RECEIVER_CONFIGURATION_ENABLED`` when omitted. Enable only
            after every subscription writer has been upgraded.
        workflow_caller_verifier: Authenticator for the Workflow service principal,
            including direct app-port calls. Defaults to sidecar-token verification
            plus explicit Workflow app ID/namespace when both token and namespace are configured.
        call_context_verifier: Optional shared verifier with an injected public-key source.
        app_api_token: Sidecar-to-app secret; defaults to ``APP_API_TOKEN``. Never
            sent to callers or used as an end-user identity.
        source_receivers_enabled: Default-off broker callback activation, separate from authoring.
        webhook_receiver_enabled: Default-off verified ingress, requiring source consumption
            and an explicit authenticated Gateway identity.
        webhook_transport: Injected verifier/publisher transport. Its HTTP client remains
            caller-owned; the default transport shares the lifespan-owned client.

    The factory is import-safe: no DSN lookups, no socket connections. All
    side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI

    effective_authz = (
        authz_endpoint
        if authz_endpoint is not None
        else os.environ.get("TRIGGER_AUTHZ_ENDPOINT", "")
    )
    effective_env = (
        environment if environment is not None else os.environ.get("ENVIRONMENT", "development")
    )
    effective_token = (
        app_api_token if app_api_token is not None else os.environ.get(ENV_APP_API_TOKEN)
    )
    effective_sources = (
        source_receivers_enabled
        if source_receivers_enabled is not None
        else read_source_receivers_enabled(os.environ)
    )
    effective_webhooks = (
        webhook_receiver_enabled
        if webhook_receiver_enabled is not None
        else read_webhook_receiver_enabled(os.environ)
    )
    gateway_app_id = os.environ.get(ENV_GATEWAY_APP_ID, "api-gateway")
    gateway_namespace = os.environ.get(ENV_GATEWAY_NAMESPACE)
    if effective_webhooks and (
        not effective_sources
        or not effective_token
        or not effective_token.strip()
        or not gateway_namespace
        or not gateway_namespace.strip()
        or not gateway_app_id.strip()
        or "://" in gateway_app_id
        or "/" in gateway_app_id
    ):
        raise ValueError("webhook ingress requires source receivers and authenticated Gateway")
    workflow_topic = (
        os.environ.get(ENV_WORKFLOW_EVENTS_TOPIC, "").strip() or DEFAULT_WORKFLOW_EVENTS_TOPIC
    )
    connector_topic = (
        os.environ.get(ENV_CONNECTOR_EVENTS_TOPIC, "").strip() or DEFAULT_CONNECTOR_EVENTS_TOPIC
    )
    normalized_topic = os.environ.get(ENV_NORMALIZED_TOPIC, "").strip() or DEFAULT_NORMALIZED_TOPIC
    if effective_sources and len({workflow_topic, connector_topic, normalized_topic}) != 3:
        raise ValueError("workflow, connector and normalized topics must be distinct")
    effective_workflow_verifier = workflow_caller_verifier
    workflow_namespace = os.environ.get(ENV_WORKFLOW_NAMESPACE)
    workflow_app_id = os.environ.get(ENV_WORKFLOW_APP_ID, _DEFAULT_WORKFLOW_APP_ID)
    workflow_target = os.environ.get(ENV_WORKFLOW_ENDPOINT, _DEFAULT_WORKFLOW_APP_ID)
    if ENV_WORKFLOW_APP_ID in os.environ:
        workflow_target = workflow_app_id
        if workflow_namespace:
            workflow_target += "." + workflow_namespace
    if (
        effective_workflow_verifier is None
        and effective_token is not None
        and workflow_namespace is not None
    ):
        effective_workflow_verifier = DaprWorkflowCallerVerifier(
            effective_token,
            workflow_app_id,
            namespace=workflow_namespace,
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.receiver_configuration_enabled = (
            receiver_configuration_enabled
            if receiver_configuration_enabled is not None
            else read_receiver_configuration_enabled(os.environ)
        )
        app.state.workflow_caller_verifier = effective_workflow_verifier
        app.state.source_receivers_enabled = effective_sources
        # Bind the SPL providers (in-memory by default, Postgres when
        # TRIGGER_METADATA_STORE is a DSN) and the domain store adapters
        # the REST/RPC surface (TS-IMPL-015..018) drives. The provider
        # factory never opens a socket, so binding here is cheap.
        effective_providers = (
            providers
            if providers is not None
            else load_providers(os.environ.get(ENV_METADATA_STORE, ""))
        )
        metadata_store = effective_providers.metadata_store
        app.state.providers = effective_providers
        app.state.subscription_store = SubscriptionStore(metadata_store)
        app.state.resume_subscription_store = ResumeSubscriptionStore(metadata_store)
        app.state.schedule_store = ScheduleStore(metadata_store)

        # Shared CEL selector evaluator (in-process compile cache) the REST
        # surface validates selectors with on create/patch and matches with on
        # :fire.
        from custos_trigger.selector import SelectorEvaluator

        app.state.selector_evaluator = SelectorEvaluator()

        # Audit surface the resume RPCs + dispatcher emit to. The default
        # TelemetryAuditSink (TS-IMPL-019) records OTel pipeline
        # counters/spans for every event and forwards to a Noop inner sink.
        from custos_trigger._telemetry import TelemetryAuditSink

        effective_audit: AuditSink = audit_sink if audit_sink is not None else TelemetryAuditSink()
        app.state.audit_sink = effective_audit

        # Default resume-subscription TTL applied when a RegisterResumeSubscription
        # request carries no (or an unparseable) ttl.
        app.state.resume_default_ttl_seconds = _env_int(
            os.environ, ENV_RESUME_DEFAULT_TTL_SECONDS, DEFAULT_RESUME_DEFAULT_TTL_SECONDS
        )

        # The dispatch path needs a Workflow Service client. When a dispatcher
        # is injected (tests) we use it as-is and own no transport; otherwise we
        # build one over an httpx client reaching the Workflow Service through
        # Dapr and close it at shutdown.
        if effective_sources:
            from custos_spl.interfaces.scheduler import SchedulerStateProvider
            from custos_spl.interfaces.trigger_queries import TriggerQueryProvider

            from custos_trigger.pipeline.reliable import ReliableStorage

            if effective_env.strip().lower() == "production" and isinstance(
                metadata_store, InMemoryTriggerMetadataStore
            ):
                raise ValueError("production source receivers require restart-durable storage")
            if (
                not app.state.receiver_configuration_enabled
                or not effective_token
                or not effective_token.strip()
                or effective_workflow_verifier is None
                or not isinstance(metadata_store, ReliableStorage)
                or not isinstance(metadata_store, TriggerQueryProvider)
                or not isinstance(metadata_store, SchedulerStateProvider)
            ):
                raise ValueError("source receivers require authenticated upgraded durable storage")
            if dispatcher is not None:
                if dispatcher.reliable is None:
                    raise ValueError("source receivers require durable dispatch")
            elif os.environ.get("TRIGGER_DURABLE_DISPATCH_ENABLED", "").strip().lower() != "true":
                raise ValueError("source receivers require durable dispatch")
            await metadata_store.check_delivery_ready()
            await metadata_store.check_dead_letters_ready()
            await metadata_store.check_scheduler_ready()

        owned_http_client: httpx.AsyncClient | None = None
        if dispatcher is not None:
            app.state.dispatcher = dispatcher
        else:
            import httpx

            from custos_trigger.clients import (
                DaprWorkflowServiceClient,
                read_dapr_endpoint,
            )
            from custos_trigger.dedup import Deduplicator
            from custos_trigger.pipeline.dispatch import Dispatcher as _Dispatcher

            endpoint = read_dapr_endpoint(
                os.environ,
                app_id=workflow_target,
            )
            enabled = os.environ.get("TRIGGER_DURABLE_DISPATCH_ENABLED", "false").strip().lower()
            if enabled not in {"true", "false"}:
                raise ValueError("TRIGGER_DURABLE_DISPATCH_ENABLED must be true or false")
            from custos_trigger.pipeline.reliable import (
                ReliableDispatcher,
                ReliableStorage,
                validate_dispatch_limits,
            )

            reliable_store: ReliableStorage | None = None
            retries = _env_int(os.environ, ENV_DISPATCH_MAX_RETRIES, DEFAULT_DISPATCH_MAX_RETRIES)
            depth = _env_int(os.environ, ENV_FANOUT_MAX_DEPTH, DEFAULT_FANOUT_MAX_DEPTH)
            retention, replay_limit = 2_592_000, 3
            if enabled == "true":
                if not app.state.receiver_configuration_enabled:
                    raise ValueError(
                        "durable dispatch requires the upgraded-writer configuration gate"
                    )
                if app.state.workflow_caller_verifier is None:
                    raise ValueError(
                        "durable dispatch requires an authenticated Workflow caller verifier"
                    )
                if not isinstance(metadata_store, ReliableStorage):
                    raise ValueError(
                        "durable dispatch requires receipt and dead-letter capabilities"
                    )
                retention = _env_int(os.environ, "TRIGGER_DEADLETTER_RETENTION_SECONDS", retention)
                replay_limit = _env_int(
                    os.environ, "TRIGGER_DEADLETTER_MAX_REPLAY_ATTEMPTS", replay_limit
                )
                validate_dispatch_limits(retries, depth, retention, replay_limit)
                await metadata_store.check_delivery_ready()
                await metadata_store.check_dead_letters_ready()
                reliable_store = metadata_store
            owned_http_client = httpx.AsyncClient()
            workflow_client = DaprWorkflowServiceClient(owned_http_client, endpoint)
            reliable = None
            if reliable_store is not None:
                reliable = ReliableDispatcher(
                    workflow_client,
                    reliable_store,
                    effective_audit,
                    max_retries=retries,
                    max_fanout_depth=depth,
                    retention_seconds=retention,
                    max_replay_attempts=replay_limit,
                )
            app.state.dispatcher = _Dispatcher(
                workflow_client,
                Deduplicator(metadata_store),
                max_retries=retries,
                max_fanout_depth=depth,
                audit=effective_audit,
                reliable=reliable,
            )

        try:
            if effective_webhooks:
                if webhook_transport is not None:
                    app.state.webhook_transport = webhook_transport
                else:
                    import httpx

                    from custos_trigger.clients import read_dapr_endpoint

                    if owned_http_client is None:
                        owned_http_client = httpx.AsyncClient()
                    app.state.webhook_transport = DaprWebhookTransport(
                        owned_http_client,
                        read_dapr_endpoint(
                            os.environ,
                            app_id=os.environ.get(ENV_CONNECTOR_APP_ID, "connector-service"),
                        ),
                        pubsub=os.environ.get(ENV_PUBSUB_COMPONENT, "").strip()
                        or DEFAULT_PUBSUB_COMPONENT,
                        topic=normalized_topic,
                        api_token=os.environ.get("DAPR_API_TOKEN"),
                    )
            app.state.ready = True
            logger.info("trigger-service is ready")
            yield
        finally:
            if owned_http_client is not None:
                await owned_http_client.aclose()

    app = FastAPI(
        title="Custos Trigger Service",
        version=__version__,
        lifespan=lifespan,
    )

    # CallContextMiddleware is instantiated lazily by Starlette when the
    # middleware stack is first built, so a DevShimDisabledInProductionError
    # surfaces either at startup or on the first request (whichever builds the
    # stack first) — visible to the operator the same way any startup
    # exception is, rather than at create_app() time. This keeps the factory
    # side-effect free and trivially testable.
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=effective_authz,
        environment=effective_env,
        verifier=call_context_verifier,
        app_api_token=effective_token,
        workflow_caller_verifier=effective_workflow_verifier,
        workflow_app_id=workflow_app_id,
        workflow_namespace=workflow_namespace,
        gateway_app_id=gateway_app_id,
        gateway_namespace=gateway_namespace,
    )
    # Pair the middleware with its exception handler so the dependency-side
    # 4xx responses (get_call_context / require_permission) emit the same
    # `{"error": {"code", "detail"}}` envelope as the middleware itself.
    app.add_exception_handler(CallContextError, call_context_error_handler)

    # Route-level failures (domain TriggerError + request validation) surface
    # through the RFC 7807 Problem+JSON envelope.
    register_exception_handlers(app)
    app.add_exception_handler(DeadLetterAdminError, dead_letter_error_handler)

    app.include_router(health_router)
    app.include_router(dead_letters_router)
    app.include_router(subscriptions_router)
    app.include_router(resume_rpc_router)
    app.include_router(build_source_event_router(enabled=effective_sources))
    app.include_router(build_webhook_router(enabled=effective_webhooks))
    # The internal-event receiver subscribes to the Workflow Service's lifecycle
    # topic via Dapr's programmatic subscription route (``GET /dapr/subscribe``).
    # Both the pub/sub component and topic are config-driven so air-gapped /
    # alternate broker deployments can retarget them without a code change.
    app.include_router(
        build_internal_event_router(
            pubsub_component=(
                os.environ.get(ENV_PUBSUB_COMPONENT, "").strip() or DEFAULT_PUBSUB_COMPONENT
            ),
            workflow_events_topic=workflow_topic,
            source_topics=(
                (
                    (
                        connector_topic,
                        CONNECTOR_EVENTS_PATH,
                    ),
                    (
                        normalized_topic,
                        NORMALIZED_EVENTS_PATH,
                    ),
                )
                if effective_sources
                else ()
            ),
        )
    )
    return app


__all__ = ["create_app"]
