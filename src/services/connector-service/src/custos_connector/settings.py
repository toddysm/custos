"""Runtime configuration parsed from environment variables (CONN-IMPL-003).

Connector Service is configured through the ``CONN_*`` env vars documented
in ``design/components/connector-service/design.md`` and projected by the
Helm subchart at ``deploy/helm/charts/connector-service/templates/``. This
module also reads ``ENVIRONMENT`` to enforce the production dev-shim guard.

This module is deliberately stdlib-only so it can be imported by both the
ASGI app factory and lightweight test fixtures without dragging in FastAPI
or asyncpg.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

#: Required. DSN that resolves the ``CatalogStoreProvider`` adapter.
#: Connector Service writes connector-type versions and reads them back via
#: this provider (the same Postgres database catalog-service reads from).
ENV_CATALOG_STORE: Final[str] = "CONN_CATALOG_STORE"

#: Required. DSN that resolves the ``MetadataStoreProvider`` adapter.
#: Backs ``ConnectorInstance`` rows, ``ConnectorCursor`` rows, the audit
#: outbox, and the lease primitive for the pull-loop.
ENV_METADATA_STORE: Final[str] = "CONN_METADATA_STORE"

#: Required. URL of the in-cluster Catalog Service (used by Phase J routes
#: that resolve activity-type references during bind).
ENV_CATALOG_ENDPOINT: Final[str] = "CONN_CATALOG_ENDPOINT"

#: Required in production. Empty switches the service to the dev-shim
#: call-context middleware (CONN-IMPL-004), which refuses to start when
#: ``ENVIRONMENT=production``.
ENV_AUTHZ_ENDPOINT: Final[str] = "CONN_AUTHZ_ENDPOINT"

#: Optional. OCI referrers discovery timeout in milliseconds (reached by
#: the OCI sample plugin in CONN-IMPL-032).
ENV_OCI_REFERRERS_TIMEOUT_MS: Final[str] = "CONN_OCI_REFERRERS_TIMEOUT_MS"

#: Optional. Maximum publish body size in MiB (mirrors catalog-service for
#: parity; reached by Phase J publish-style routes).
ENV_PUBLISH_MAX_BODY_MB: Final[str] = "CONN_PUBLISH_MAX_BODY_MB"

#: Optional. Default lease TTL in seconds requested from the Lease Manager
#: when a plugin does not specify a ``leaseHint`` (CONN-IMPL-017).
ENV_SIDECAR_DEFAULT_TTL: Final[str] = "CONN_SIDECAR_DEFAULT_TTL"

#: Optional. Per-instance concurrent-lease cap (CONN-IMPL-017 default = 16).
ENV_LEASE_MAX_CONCURRENT: Final[str] = "CONN_LEASE_MAX_CONCURRENT"

#: Optional. Minimum pull-loop tick interval in seconds (CONN-IMPL-023; the
#: design pins ``>= 10`` so we refuse to honour smaller values).
ENV_PULL_LOOP_MIN_INTERVAL_SEC: Final[str] = "CONN_PULL_LOOP_MIN_INTERVAL_SEC"

#: Optional. Connector-instance health snapshot cache TTL in seconds
#: (CONN-IMPL-013 default = 60).
ENV_HEALTH_CACHE_TTL_S: Final[str] = "CONN_HEALTH_CACHE_TTL_S"

#: Optional. PKI issuer for the secret-bridge sidecar mTLS material
#: (CONN-IMPL-020).
ENV_SIDECAR_MTLS_ISSUER: Final[str] = "CONN_SIDECAR_MTLS_ISSUER"

#: Optional. Dapr sidecar HTTP base URL. When set, Connector Service
#: publishes normalized connector events through Dapr Pub/Sub instead
#: of the dev-mode no-op publisher (CONN-IMPL-027). Empty disables Dapr
#: publishing and keeps the in-process :class:`LocalEventBus` /
#: :class:`NoOpEventPublisher` wiring for local dev + tests.
ENV_DAPR_HTTP_ENDPOINT: Final[str] = "CONN_DAPR_HTTP_ENDPOINT"

#: Optional. Name of the Dapr Pub/Sub component the Connector Service
#: publishes events through. Returned to Trigger Service in the
#: ``SubscribeEvents`` RPC response so the subscriber wires its Dapr
#: subscription against the same component (CONN-IMPL-027). Defaults to
#: ``custos-pubsub`` to match the Helm subchart default.
ENV_DAPR_PUBSUB_NAME: Final[str] = "CONN_DAPR_PUBSUB_NAME"

#: Optional. Name of the Dapr Pub/Sub topic the Connector Service
#: publishes connector events on. Defaults to ``custos.connector.events``
#: per design § Public Interface → Internal RPCs (CONN-IMPL-027).
ENV_DAPR_EVENT_TOPIC: Final[str] = "CONN_DAPR_EVENT_TOPIC"

#: Optional. Name of the Dapr secret-store Component the Connector
#: Service reads ``x-dapr-secret`` credentials from at bind time.
#: Defaults to ``custos-secretstore`` to match the Helm chart's
#: secret-store Component name (NOT the ``secretstores.kubernetes``
#: component *type*).
ENV_DAPR_SECRET_STORE: Final[str] = "CONN_DAPR_SECRET_STORE"

#: Optional. Base URL of the OCI registry that hosts connector *plugin*
#: images. When set, the connector-type registration surface
#: (``POST /internal/v1/connectors:register``, CONN-REG) is wired with an
#: ``httpx.AsyncClient`` bound to this base URL; the registration request
#: carries only a host-relative ``<repository>@sha256:<digest>``. Empty
#: disables the registration surface (the Loader is not constructed).
ENV_CONNECTOR_REGISTRY_URL: Final[str] = "CONN_CONNECTOR_REGISTRY_URL"

#: Optional. Static bearer token presented as ``Authorization: Bearer``
#: on connector-image pulls for private registries. Empty means anonymous
#: pulls (the default for public registries). Per-registry credential
#: resolution is a deliberate follow-up.
ENV_CONNECTOR_REGISTRY_TOKEN: Final[str] = "CONN_CONNECTOR_REGISTRY_TOKEN"

#: Operational env tag. The call-context dev shim refuses to run when this
#: is ``production`` (case-insensitive).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

DEFAULT_OCI_REFERRERS_TIMEOUT_MS: Final[int] = 5000
DEFAULT_PUBLISH_MAX_BODY_MB: Final[int] = 4
DEFAULT_SIDECAR_DEFAULT_TTL: Final[int] = 600
DEFAULT_LEASE_MAX_CONCURRENT: Final[int] = 16
DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC: Final[int] = 10
DEFAULT_HEALTH_CACHE_TTL_S: Final[int] = 60
DEFAULT_DAPR_PUBSUB_NAME: Final[str] = "custos-pubsub"
DEFAULT_DAPR_EVENT_TOPIC: Final[str] = "custos.connector.events"
DEFAULT_DAPR_SECRET_STORE: Final[str] = "custos-secretstore"

#: Minimum pull-loop interval the design allows (CONN-IMPL-023). Set
#: ``CONN_PULL_LOOP_MIN_INTERVAL_SEC`` above this; smaller values are rejected.
PULL_LOOP_HARD_FLOOR_SEC: Final[int] = 10


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated connector-service configuration."""

    catalog_store_dsn: str
    metadata_store_dsn: str
    catalog_endpoint: str
    authz_endpoint: str  # empty string means "dev shim active"
    oci_referrers_timeout_ms: int
    publish_max_body_mb: int
    sidecar_default_ttl_sec: int
    lease_max_concurrent: int
    pull_loop_min_interval_sec: int
    health_cache_ttl_s: int
    sidecar_mtls_issuer: str | None
    environment: str
    # CONN-IMPL-027 (Phase J) — Dapr Pub/Sub wiring. Defaults keep
    # local-dev + the existing test fixtures (which construct
    # ``Settings`` positionally-by-name) working without per-call updates:
    # empty endpoint = dev-mode :class:`NoOpEventPublisher`.
    dapr_http_endpoint: str = ""  # empty string means "Dapr publishing disabled"
    dapr_pubsub_name: str = DEFAULT_DAPR_PUBSUB_NAME
    dapr_event_topic: str = DEFAULT_DAPR_EVENT_TOPIC
    #: Dapr secret-store Component name used by the ``x-dapr-secret``
    #: identity resolver (CONN-DAPRSEC-01). Default keeps local-dev +
    #: existing test constructions working.
    dapr_secret_store: str = DEFAULT_DAPR_SECRET_STORE
    #: Base URL of the OCI registry hosting connector plugin images
    #: (CONN-REG). Empty disables the connector-type registration
    #: surface. Default keeps local-dev + existing test constructions
    #: working.
    connector_registry_url: str = ""
    #: Optional static bearer for private connector-image pulls
    #: (CONN-REG). ``None`` means anonymous pulls.
    connector_registry_token: str | None = None

    @property
    def use_callctx_dev_shim(self) -> bool:
        """True when the dev-shim call-context middleware should be wired."""
        return self.authz_endpoint == ""

    @property
    def dapr_pubsub_enabled(self) -> bool:
        """True when Connector Service should publish events through Dapr.

        Toggled by ``CONN_DAPR_HTTP_ENDPOINT``: when set, the
        :class:`custos_connector.listen.publisher.DaprPubSubEventPublisher`
        is wired as the production event publisher (CONN-IMPL-027). Empty
        keeps the dev-mode :class:`NoOpEventPublisher` so single-node
        development continues to work without standing up Dapr.
        """
        return self.dapr_http_endpoint != ""

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "")
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/connector-service/design.md § Configuration)"
        )
    return value


def _opt_int(name: str, env: dict[str, str], default: int, *, minimum: int | None = None) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise SettingsError(
            f"{name} must be >= {minimum} (got {value}); the design pins this floor"
        )
    return value


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``).

    ``CONN_AUTHZ_ENDPOINT`` is required in production but accepted as empty
    here so local development and tests can opt into the dev-shim
    call-context middleware. The shim itself refuses to start when
    :meth:`Settings.is_production` is true; see CONN-IMPL-004.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)
    mtls_issuer = src.get(ENV_SIDECAR_MTLS_ISSUER, "").strip() or None
    return Settings(
        catalog_store_dsn=_require(ENV_CATALOG_STORE, src),
        metadata_store_dsn=_require(ENV_METADATA_STORE, src),
        catalog_endpoint=_require(ENV_CATALOG_ENDPOINT, src),
        authz_endpoint=src.get(ENV_AUTHZ_ENDPOINT, "").strip(),
        oci_referrers_timeout_ms=_opt_int(
            ENV_OCI_REFERRERS_TIMEOUT_MS, src, DEFAULT_OCI_REFERRERS_TIMEOUT_MS
        ),
        publish_max_body_mb=_opt_int(ENV_PUBLISH_MAX_BODY_MB, src, DEFAULT_PUBLISH_MAX_BODY_MB),
        sidecar_default_ttl_sec=_opt_int(ENV_SIDECAR_DEFAULT_TTL, src, DEFAULT_SIDECAR_DEFAULT_TTL),
        lease_max_concurrent=_opt_int(ENV_LEASE_MAX_CONCURRENT, src, DEFAULT_LEASE_MAX_CONCURRENT),
        pull_loop_min_interval_sec=_opt_int(
            ENV_PULL_LOOP_MIN_INTERVAL_SEC,
            src,
            DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC,
            minimum=PULL_LOOP_HARD_FLOOR_SEC,
        ),
        health_cache_ttl_s=_opt_int(
            ENV_HEALTH_CACHE_TTL_S, src, DEFAULT_HEALTH_CACHE_TTL_S, minimum=0
        ),
        sidecar_mtls_issuer=mtls_issuer,
        dapr_http_endpoint=src.get(ENV_DAPR_HTTP_ENDPOINT, "").strip(),
        dapr_pubsub_name=src.get(ENV_DAPR_PUBSUB_NAME, "").strip() or DEFAULT_DAPR_PUBSUB_NAME,
        dapr_event_topic=src.get(ENV_DAPR_EVENT_TOPIC, "").strip() or DEFAULT_DAPR_EVENT_TOPIC,
        dapr_secret_store=src.get(ENV_DAPR_SECRET_STORE, "").strip() or DEFAULT_DAPR_SECRET_STORE,
        connector_registry_url=src.get(ENV_CONNECTOR_REGISTRY_URL, "").strip(),
        connector_registry_token=src.get(ENV_CONNECTOR_REGISTRY_TOKEN, "").strip() or None,
        environment=src.get(ENV_ENVIRONMENT, "development").strip() or "development",
    )


__all__ = [
    "DEFAULT_DAPR_EVENT_TOPIC",
    "DEFAULT_DAPR_PUBSUB_NAME",
    "DEFAULT_DAPR_SECRET_STORE",
    "DEFAULT_HEALTH_CACHE_TTL_S",
    "DEFAULT_LEASE_MAX_CONCURRENT",
    "DEFAULT_OCI_REFERRERS_TIMEOUT_MS",
    "DEFAULT_PUBLISH_MAX_BODY_MB",
    "DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC",
    "DEFAULT_SIDECAR_DEFAULT_TTL",
    "ENV_AUTHZ_ENDPOINT",
    "ENV_CATALOG_ENDPOINT",
    "ENV_CATALOG_STORE",
    "ENV_CONNECTOR_REGISTRY_TOKEN",
    "ENV_CONNECTOR_REGISTRY_URL",
    "ENV_DAPR_EVENT_TOPIC",
    "ENV_DAPR_HTTP_ENDPOINT",
    "ENV_DAPR_PUBSUB_NAME",
    "ENV_DAPR_SECRET_STORE",
    "ENV_ENVIRONMENT",
    "ENV_HEALTH_CACHE_TTL_S",
    "ENV_LEASE_MAX_CONCURRENT",
    "ENV_METADATA_STORE",
    "ENV_OCI_REFERRERS_TIMEOUT_MS",
    "ENV_PUBLISH_MAX_BODY_MB",
    "ENV_PULL_LOOP_MIN_INTERVAL_SEC",
    "ENV_SIDECAR_DEFAULT_TTL",
    "ENV_SIDECAR_MTLS_ISSUER",
    "PULL_LOOP_HARD_FLOOR_SEC",
    "Settings",
    "SettingsError",
    "load_settings",
]
