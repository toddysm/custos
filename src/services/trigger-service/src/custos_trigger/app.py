"""Trigger Service FastAPI application factory (TS-IMPL-003).

:func:`create_app` builds the FastAPI application: the ``/healthz`` +
``/readyz`` probes, the call-context middleware (with dev shim), and a
lifespan hook that marks the app ready once startup completes.

The factory is import-safe — no socket connections, no env-dependent failures
at construction time. All side-effecting work belongs in the FastAPI lifespan
context. The lifespan binds the SPL provider bundle + domain store adapters
(TS-IMPL-008) onto ``app.state``; the REST/RPC surface across TS-IMPL-015..018
grows in place atop them without changing the factory's import-safety contract.

Configuration today is read directly from the process environment because the
typed settings loader is a later phase (TS-IMPL-004). Two knobs steer the
call-context middleware:

- ``TRIGGER_AUTHZ_ENDPOINT`` (default empty) — when empty the dev shim is
  active; when set, production call-context validation against the Auth
  Service (COMP-002) is required and currently surfaces a
  :class:`NotImplementedError` until that integration lands.
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
from custos_trigger.api.routes import subscriptions_router
from custos_trigger.health import router as health_router
from custos_trigger.middleware import (
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
)
from custos_trigger.providers import Providers, load_providers
from custos_trigger.settings import (
    DEFAULT_DISPATCH_MAX_RETRIES,
    DEFAULT_FANOUT_MAX_DEPTH,
    ENV_DISPATCH_MAX_RETRIES,
    ENV_FANOUT_MAX_DEPTH,
    ENV_METADATA_STORE,
    ENV_WORKFLOW_ENDPOINT,
)
from custos_trigger.stores import (
    ResumeSubscriptionStore,
    ScheduleStore,
    SubscriptionStore,
)

if TYPE_CHECKING:
    import httpx
    from fastapi import FastAPI

    from custos_trigger.pipeline.dispatch import Dispatcher

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

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
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

        # The dispatch path needs a Workflow Service client. When a dispatcher
        # is injected (tests) we use it as-is and own no transport; otherwise we
        # build one over an httpx client reaching the Workflow Service through
        # Dapr and close it at shutdown.
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

            owned_http_client = httpx.AsyncClient()
            endpoint = read_dapr_endpoint(
                os.environ,
                app_id=os.environ.get(ENV_WORKFLOW_ENDPOINT, _DEFAULT_WORKFLOW_APP_ID),
            )
            app.state.dispatcher = _Dispatcher(
                DaprWorkflowServiceClient(owned_http_client, endpoint),
                Deduplicator(metadata_store),
                max_retries=_env_int(
                    os.environ, ENV_DISPATCH_MAX_RETRIES, DEFAULT_DISPATCH_MAX_RETRIES
                ),
                max_fanout_depth=_env_int(
                    os.environ, ENV_FANOUT_MAX_DEPTH, DEFAULT_FANOUT_MAX_DEPTH
                ),
            )

        app.state.ready = True
        logger.info("trigger-service is ready")
        try:
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
    )
    # Pair the middleware with its exception handler so the dependency-side
    # 4xx responses (get_call_context / require_permission) emit the same
    # `{"error": {"code", "detail"}}` envelope as the middleware itself.
    app.add_exception_handler(CallContextError, call_context_error_handler)

    # Route-level failures (domain TriggerError + request validation) surface
    # through the RFC 7807 Problem+JSON envelope.
    register_exception_handlers(app)

    app.include_router(health_router)
    app.include_router(subscriptions_router)
    return app


__all__ = ["create_app"]
