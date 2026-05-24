"""Custos Auth Service (COMP-002).

This package hosts the Auth Service runtime: identity issuance, identity
verification, authorization decisions, and the internal signed call-context
contract.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/auth-service/design.md

Phase A (AS-IMPL-001 / AS-IMPL-002) shipped the FastAPI scaffold + Helm
subchart. Phase B (AS-IMPL-003 / AS-IMPL-004) wires the SPL provider
bundle (``AuthStoreProvider`` + ``MetadataStoreProvider``) into the app
factory via a FastAPI lifespan hook and runs the schema-revision startup
gate before serving traffic. Phase C (AS-IMPL-005/006/007) mounts the
:class:`CallContextMiddleware`, registers the M1 admin endpoints
(tenants / workspaces / principals / service-accounts), and ships the
``OidcIdentity`` storage helpers used by the Phase H verifier.
Permission/role registry, authorization engine, service tokens,
call-context signing, and the OIDC verifier all land in subsequent
AS-IMPL-* phases.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from custos_auth.api import all_routers, register_exception_handlers
from custos_auth.binding_events import LocalBindingChangedBus
from custos_auth.callctx_keyring import (
    KeyRing,
    install_key_age_metric,
    run_rotation_loop,
)
from custos_auth.callctx_signer import (
    CallContextSigner,
    DaprSecretsSigningKeyResolver,
    SigningKey,
    StaticSigningKeyResolver,
)
from custos_auth.health import router as health_router
from custos_auth.middleware.callctx import CallContextMiddleware
from custos_auth.permission_registry import seed_permissions_and_validate_roles
from custos_auth.providers import (
    MigrationRequired,
    Providers,
    load_providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from custos_auth.roles import BUILTIN_ROLES, seed_builtin_roles
from custos_auth.settings import Settings, load_settings
from custos_auth.sweeper import run_sweeper_loop
from custos_auth.token_revoked_events import LocalTokenRevokedBus

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"

logger = logging.getLogger("custos_auth")


async def _load_initial_signing_key(settings: Settings) -> SigningKey:
    """Build the initial :class:`SigningKey` for the lifespan.

    Production / non-development deployments must set
    :data:`custos_auth.settings.ENV_CALL_CONTEXT_KEY_REF` so the key
    is pulled from the Dapr Secrets sidecar — that is the M1 secrets
    contract for the call-context signer
    (``design/components/auth-service/design.md`` § Internal vs
    External Auth — Trust Model).

    Development convenience: when the env var is empty and
    ``CUSTOS_AUTH_ENVIRONMENT`` is set to ``"development"`` (or left
    at its default), the helper generates an ephemeral keypair so
    operators get a working signer without standing up Dapr. The
    generated key does not survive a pod restart, which is desirable
    for dev and explicitly disallowed in production.

    Production deployments without ``CUSTOS_AUTH_CALL_CONTEXT_KEY_REF``
    crash-loop with an operator-actionable diagnostic — the same
    pattern used by the call-context middleware verifier-url guard.
    """
    key_ref = settings.call_context_key_ref.strip()
    if key_ref:
        import httpx

        async def _fetch(url: str) -> dict[str, Any]:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()  # type: ignore[no-any-return]

        resolver = DaprSecretsSigningKeyResolver(
            secret_store=settings.call_context_secret_store,
            secret_name=key_ref,
            fetch_json=_fetch,
        )
        return await resolver.active_signing_key()
    if settings.environment != "development":
        raise RuntimeError(
            "CUSTOS_AUTH_CALL_CONTEXT_KEY_REF must be set outside development; "
            "the call-context signer cannot mint tokens without a stable key. "
            "Configure the Dapr secret reference in the Helm values "
            "(deploy/helm/charts/auth-service/values.yaml) and re-deploy."
        )
    logger.warning(
        "CUSTOS_AUTH_CALL_CONTEXT_KEY_REF not set; generating an ephemeral "
        "Ed25519 call-context signing key for development. Tokens minted "
        "by this replica will not survive a pod restart."
    )
    return SigningKey.generate()


def create_app(
    *,
    settings: Settings | None = None,
    providers: Providers | None = None,
) -> FastAPI:
    """Build and return the Auth Service FastAPI application.

    Args:
        settings: Pre-parsed :class:`Settings`. Defaults to
            :func:`custos_auth.settings.load_settings` reading from
            the process environment.
        providers: Pre-built :class:`Providers` (used by tests to inject
            in-memory fakes). When ``None``, the lifespan hook constructs
            the real Postgres adapters from the settings DSNs.

    The factory is import-safe: no DSN lookups, no socket connections.
    All side-effecting work happens inside the FastAPI lifespan context.
    """
    from fastapi import FastAPI

    effective_settings = settings if settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = effective_settings
        local_providers = (
            providers
            if providers is not None
            else load_providers(
                effective_settings,
            )
        )
        app.state.providers = local_providers
        app.state.ready = False
        app.state.schema_gate_error = None
        try:
            await verify_schema_revisions(local_providers)
        except MigrationRequired as exc:
            # Stash on app.state for forensic inspection in tests, log the
            # operator-actionable diagnostic, then re-raise so uvicorn
            # surfaces a non-zero exit. Kubernetes turns that into a
            # CrashLoopBackOff under the default `restartPolicy: Always`,
            # which is the AS-IMPL-004 acceptance-criterion equivalent
            # of "service refuses to start". Recovery: operator runs
            # `custos migrate up` against the configured DSNs and the
            # pod restart picks up the new ledger state.
            app.state.schema_gate_error = exc
            logger.error("%s", schema_gate_explainer(exc))
            raise
        # Phase D (AS-IMPL-008 / AS-IMPL-009): load + validate the
        # permission registry, then seed the built-in role table.
        # Both calls are idempotent across restarts and re-raise so a
        # misconfigured registry crash-loops the pod with an
        # actionable diagnostic.
        builtin_roles_spl = [role.to_spl() for role in BUILTIN_ROLES]
        declared = await seed_permissions_and_validate_roles(
            local_providers.auth_store,
            paths=effective_settings.permissions_paths,
            roles=builtin_roles_spl,
        )
        app.state.declared_permissions = declared
        await seed_builtin_roles(local_providers.auth_store)
        # Phase E (AS-IMPL-012): wire the authz decision cache to the
        # binding-changed bus on both sides.
        #
        # * The in-process publisher (``LocalBindingChangedBus``)
        #   delivers events on the same replica that performed the
        #   binding mutation. Subscribing the cache satisfies the
        #   single-replica "revoke-then-recheck within one round
        #   trip" acceptance criterion without standing up a real
        #   transport.
        # * The cross-replica subscriber (defaults to no-op) is
        #   started here so production deployments that swap in a
        #   Redis pub/sub or SPL-outbox-backed transport deliver
        #   every event to the local cache. ``stop()`` runs on
        #   shutdown so background tasks do not leak.
        #
        # Both paths invoke
        # :meth:`AuthzDecisionCache.on_binding_changed`, which is
        # idempotent — double-delivery from publisher and subscriber
        # against the same replica is harmless (the second call is a
        # no-op against an already-empty bucket).
        if isinstance(local_providers.binding_changed_publisher, LocalBindingChangedBus):
            local_providers.binding_changed_publisher.subscribe(
                local_providers.authz_cache.on_binding_changed,
            )
        await local_providers.binding_changed_subscriber.start(
            local_providers.authz_cache.on_binding_changed,
        )
        # Phase F (AS-IMPL-014): wire the authn cache to the
        # token-revoked bus on both sides. Same pattern as the
        # binding-changed bus above — the in-process publisher
        # delivers locally, the (defaults-to-no-op) subscriber
        # picks up cross-replica events when a real transport is
        # plugged in.
        if isinstance(local_providers.token_revoked_publisher, LocalTokenRevokedBus):

            async def _on_token_revoked_local(event: object) -> None:
                # Type-narrow at call site so the dataclass attrs are
                # visible without importing the event class into the
                # lifespan signature.
                from custos_auth.token_revoked_events import TokenRevokedEvent

                assert isinstance(event, TokenRevokedEvent)
                local_providers.authn_cache.invalidate_by_token_id(event.token_id)

            local_providers.token_revoked_publisher.subscribe(_on_token_revoked_local)

        async def _on_token_revoked_remote(event: object) -> None:
            from custos_auth.token_revoked_events import TokenRevokedEvent

            assert isinstance(event, TokenRevokedEvent)
            local_providers.authn_cache.invalidate_by_token_id(event.token_id)

        await local_providers.token_revoked_subscriber.start(_on_token_revoked_remote)
        # Phase G (AS-IMPL-017 / AS-IMPL-018): build the call-context
        # signing-key ring, signer, and rotation loop. The lifespan
        # owns the long-lived KeyRing + signer so the JWKS route and
        # any future internal mint paths share a single source of
        # truth. The resolver is a :class:`StaticSigningKeyResolver`
        # initialised either from a Dapr Secrets reference (production
        # / non-development) or from a freshly generated keypair
        # (development convenience — operators get a working signer
        # without a Dapr sidecar).
        initial_signing_key = await _load_initial_signing_key(effective_settings)
        signing_key_resolver = StaticSigningKeyResolver(key=initial_signing_key)
        # When rotation is disabled (operator manages rotation
        # externally via ``CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION=0``)
        # the ring still needs a positive rotation_period_seconds so
        # the JWKS ``Cache-Control: max-age`` makes sense. Fall back
        # to the design default (7 days) — that is the longest
        # plausible interval and matches the JWKS overlap window the
        # operator should target.
        from custos_auth.callctx_keyring import DEFAULT_ROTATION_PERIOD_SECONDS

        rotation_for_ring = (
            effective_settings.call_context_key_rotation_seconds
            if effective_settings.call_context_key_rotation_seconds > 0
            else DEFAULT_ROTATION_PERIOD_SECONDS
        )
        key_ring = KeyRing(
            initial_signing_key,
            rotation_period_seconds=rotation_for_ring,
        )
        call_context_signer = CallContextSigner(
            signing_key_resolver,
            audience=effective_settings.call_context_audience,
            default_ttl_seconds=effective_settings.call_context_ttl_seconds,
        )
        app.state.call_context_key_ring = key_ring
        app.state.call_context_signing_key_resolver = signing_key_resolver
        app.state.call_context_signer = call_context_signer
        install_key_age_metric(key_ring)
        rotation_task: asyncio.Task[None] | None = None
        if effective_settings.call_context_key_rotation_seconds > 0:
            rotation_task = asyncio.create_task(
                run_rotation_loop(
                    key_ring=key_ring,
                    resolver=signing_key_resolver,
                    rotation_period_seconds=(effective_settings.call_context_key_rotation_seconds),
                ),
                name="custos-auth.callctx-rotation",
            )
        app.state.call_context_rotation_task = rotation_task
        # Phase F (AS-IMPL-016): launch the token-expiry sweeper.
        # The loop coroutine handles the ``interval=0`` (disabled)
        # case itself by returning immediately — we still spawn the
        # task so the lifespan teardown has a uniform cancel/await
        # shape regardless of configuration.
        sweeper_task = asyncio.create_task(
            run_sweeper_loop(
                auth_store=local_providers.auth_store,
                metadata_store=local_providers.metadata_store,
                publisher=local_providers.token_revoked_publisher,
                interval_seconds=effective_settings.token_sweeper_interval_seconds,
            ),
            name="custos-auth.token-sweeper",
        )
        app.state.token_sweeper_task = sweeper_task
        app.state.ready = True
        logger.info("schema-revision gate passed; auth-service is ready")
        try:
            yield
        finally:
            # Best-effort subscriber shutdown. A failure here is
            # logged but does not propagate; the pod is going away
            # anyway and we do not want a noisy shutdown to mask the
            # real exit reason.
            sweeper_task.cancel()
            try:
                await sweeper_task
            except asyncio.CancelledError:
                pass
            except Exception:  # guard lifespan shutdown
                logger.warning("token sweeper failed to stop cleanly", exc_info=True)
            if rotation_task is not None:
                rotation_task.cancel()
                try:
                    await rotation_task
                except asyncio.CancelledError:
                    pass
                except Exception:  # guard lifespan shutdown
                    logger.warning(
                        "call-context rotation loop failed to stop cleanly",
                        exc_info=True,
                    )
            try:
                await local_providers.binding_changed_subscriber.stop()
            except Exception:  # guard lifespan shutdown
                logger.warning(
                    "binding-changed subscriber failed to stop cleanly",
                    exc_info=True,
                )
            try:
                await local_providers.token_revoked_subscriber.stop()
            except Exception:  # guard lifespan shutdown
                logger.warning(
                    "token-revoked subscriber failed to stop cleanly",
                    exc_info=True,
                )

    app = FastAPI(
        title="Custos Auth Service",
        version=__version__,
        description=(
            "Identity issuance, identity verification, authorization "
            "decisions, and the internal signed call-context contract."
        ),
        lifespan=lifespan,
    )
    # Health probes are mounted before the call-context middleware so
    # liveness/readiness checks never carry a call-context header.
    app.include_router(health_router)

    app.add_middleware(
        CallContextMiddleware,
        verifier_url=effective_settings.callctx_verifier_url,
        environment=effective_settings.environment,
    )
    register_exception_handlers(app)
    for router in all_routers:
        app.include_router(router)
    return app
