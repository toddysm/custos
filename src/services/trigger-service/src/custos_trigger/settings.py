"""Runtime configuration parsed from environment variables.

Trigger Service is configured exclusively through the ``TRIGGER_*`` env vars
documented in ``design/components/trigger-service/design.md`` § Configuration
and projected by the Helm subchart at
``deploy/helm/charts/trigger-service/templates/`` (TS-IMPL-002).

This module is deliberately stdlib-only so it can be imported by both the
ASGI app factory and lightweight test fixtures without dragging in FastAPI,
asyncpg, or the Dapr SDK.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

# --- § Configuration knobs (design.md § Configuration) -----------------------

#: Required. External base URL for the webhook receiver. The design marks this
#: Required with no in-cluster default; the Helm subchart ships a placeholder
#: until the M2 Generic Webhook Receiver lands (TS-IMPL-002).
ENV_WEBHOOK_BASE_URL: Final[str] = "TRIGGER_WEBHOOK_BASE_URL"

#: Optional. Dedup window in seconds. Default ``86400`` (24 h).
ENV_DEDUP_TTL_SECONDS: Final[str] = "TRIGGER_DEDUP_TTL_SECONDS"

#: Optional. Default pull interval (seconds) when a subscription does not
#: specify its own. Default ``60``.
ENV_POLLER_DEFAULT_INTERVAL_SECONDS: Final[str] = "TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS"

#: Optional. Default expiry (seconds) for resume subscriptions. Default
#: ``604800`` (7 days).
ENV_RESUME_DEFAULT_TTL_SECONDS: Final[str] = "TRIGGER_RESUME_DEFAULT_TTL_SECONDS"

#: Optional. Max retries dispatching to the Workflow Service. Default ``5``.
ENV_DISPATCH_MAX_RETRIES: Final[str] = "TRIGGER_DISPATCH_MAX_RETRIES"

#: Optional. Scheduler leader lock TTL (seconds) — the single-fire guarantee
#: across replicas. Default ``30``.
ENV_SCHEDULER_LEADER_LEASE_SECONDS: Final[str] = "TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS"

# --- Dapr Pub/Sub transport (chart § Dapr Pub/Sub subscriptions) -------------

#: Optional. Dapr pub/sub component name. Default ``custos-pubsub``.
ENV_PUBSUB_COMPONENT: Final[str] = "TRIGGER_PUBSUB_COMPONENT"

#: Optional. Topic carrying normalized trigger events. Default
#: ``custos.triggers.normalized``.
ENV_NORMALIZED_TOPIC: Final[str] = "TRIGGER_NORMALIZED_TOPIC"

#: Optional. Topic carrying internal workflow lifecycle events. Default
#: ``custos.workflow.events``.
ENV_WORKFLOW_EVENTS_TOPIC: Final[str] = "TRIGGER_WORKFLOW_EVENTS_TOPIC"

# --- Dependencies (design.md § Dependencies) ---------------------------------

#: Required. In-cluster endpoint of the Workflow Service.
ENV_WORKFLOW_ENDPOINT: Final[str] = "TRIGGER_WF_ENDPOINT"

#: Required. In-cluster endpoint of the Connector Service.
ENV_CONNECTOR_ENDPOINT: Final[str] = "TRIGGER_CONNECTOR_ENDPOINT"

#: Required. DSN that resolves the ``MetadataStoreProvider`` adapter
#: (Subscription / Schedule / DedupKey / ResumeSubscription persistence).
ENV_METADATA_STORE: Final[str] = "TRIGGER_METADATA_STORE"

# --- Call-context / operational ----------------------------------------------

#: Required in production. Empty switches the service to the dev-shim
#: call-context middleware (TS-IMPL-003), which refuses to start when
#: ``ENVIRONMENT=production``.
ENV_AUTHZ_ENDPOINT: Final[str] = "TRIGGER_AUTHZ_ENDPOINT"

#: Operational env tag. The call-context dev shim refuses to run when this is
#: ``production`` (case-insensitive).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

# --- Defaults (design.md § Configuration) ------------------------------------

DEFAULT_DEDUP_TTL_SECONDS: Final[int] = 86_400
DEFAULT_POLLER_DEFAULT_INTERVAL_SECONDS: Final[int] = 60
DEFAULT_RESUME_DEFAULT_TTL_SECONDS: Final[int] = 604_800
DEFAULT_DISPATCH_MAX_RETRIES: Final[int] = 5
DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS: Final[int] = 30
DEFAULT_PUBSUB_COMPONENT: Final[str] = "custos-pubsub"
DEFAULT_NORMALIZED_TOPIC: Final[str] = "custos.triggers.normalized"
DEFAULT_WORKFLOW_EVENTS_TOPIC: Final[str] = "custos.workflow.events"
DEFAULT_ENVIRONMENT: Final[str] = "development"


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated trigger-service configuration."""

    webhook_base_url: str
    dedup_ttl_seconds: int
    poller_default_interval_seconds: int
    resume_default_ttl_seconds: int
    dispatch_max_retries: int
    scheduler_leader_lease_seconds: int
    pubsub_component: str
    normalized_topic: str
    workflow_events_topic: str
    workflow_endpoint: str
    connector_endpoint: str
    metadata_store_dsn: str
    authz_endpoint: str  # empty string means "dev shim active"
    environment: str

    @property
    def use_callctx_dev_shim(self) -> bool:
        """True when the dev-shim call-context middleware should be wired."""
        return self.authz_endpoint == ""

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/trigger-service/design.md § Configuration)"
        )
    return value


def _opt_positive_int(name: str, env: dict[str, str], default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a non-negative integer (got {raw!r})") from exc
    if value < 0:
        raise SettingsError(f"{name} must be a non-negative integer (got {raw!r})")
    return value


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``).

    ``TRIGGER_AUTHZ_ENDPOINT`` is required in production but accepted as empty
    here so local development and tests can opt into the dev-shim call-context
    middleware. The shim itself refuses to start when
    :meth:`Settings.is_production` is true; see TS-IMPL-003.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)
    return Settings(
        webhook_base_url=_require(ENV_WEBHOOK_BASE_URL, src),
        dedup_ttl_seconds=_opt_positive_int(ENV_DEDUP_TTL_SECONDS, src, DEFAULT_DEDUP_TTL_SECONDS),
        poller_default_interval_seconds=_opt_positive_int(
            ENV_POLLER_DEFAULT_INTERVAL_SECONDS, src, DEFAULT_POLLER_DEFAULT_INTERVAL_SECONDS
        ),
        resume_default_ttl_seconds=_opt_positive_int(
            ENV_RESUME_DEFAULT_TTL_SECONDS, src, DEFAULT_RESUME_DEFAULT_TTL_SECONDS
        ),
        dispatch_max_retries=_opt_positive_int(
            ENV_DISPATCH_MAX_RETRIES, src, DEFAULT_DISPATCH_MAX_RETRIES
        ),
        scheduler_leader_lease_seconds=_opt_positive_int(
            ENV_SCHEDULER_LEADER_LEASE_SECONDS, src, DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS
        ),
        pubsub_component=src.get(ENV_PUBSUB_COMPONENT, "").strip() or DEFAULT_PUBSUB_COMPONENT,
        normalized_topic=src.get(ENV_NORMALIZED_TOPIC, "").strip() or DEFAULT_NORMALIZED_TOPIC,
        workflow_events_topic=(
            src.get(ENV_WORKFLOW_EVENTS_TOPIC, "").strip() or DEFAULT_WORKFLOW_EVENTS_TOPIC
        ),
        workflow_endpoint=_require(ENV_WORKFLOW_ENDPOINT, src),
        connector_endpoint=_require(ENV_CONNECTOR_ENDPOINT, src),
        metadata_store_dsn=_require(ENV_METADATA_STORE, src),
        authz_endpoint=src.get(ENV_AUTHZ_ENDPOINT, "").strip(),
        environment=src.get(ENV_ENVIRONMENT, "").strip() or DEFAULT_ENVIRONMENT,
    )


__all__ = [
    "DEFAULT_DEDUP_TTL_SECONDS",
    "DEFAULT_DISPATCH_MAX_RETRIES",
    "DEFAULT_ENVIRONMENT",
    "DEFAULT_NORMALIZED_TOPIC",
    "DEFAULT_POLLER_DEFAULT_INTERVAL_SECONDS",
    "DEFAULT_PUBSUB_COMPONENT",
    "DEFAULT_RESUME_DEFAULT_TTL_SECONDS",
    "DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS",
    "DEFAULT_WORKFLOW_EVENTS_TOPIC",
    "ENV_AUTHZ_ENDPOINT",
    "ENV_CONNECTOR_ENDPOINT",
    "ENV_DEDUP_TTL_SECONDS",
    "ENV_DISPATCH_MAX_RETRIES",
    "ENV_ENVIRONMENT",
    "ENV_METADATA_STORE",
    "ENV_NORMALIZED_TOPIC",
    "ENV_POLLER_DEFAULT_INTERVAL_SECONDS",
    "ENV_PUBSUB_COMPONENT",
    "ENV_RESUME_DEFAULT_TTL_SECONDS",
    "ENV_SCHEDULER_LEADER_LEASE_SECONDS",
    "ENV_WEBHOOK_BASE_URL",
    "ENV_WORKFLOW_ENDPOINT",
    "ENV_WORKFLOW_EVENTS_TOPIC",
    "Settings",
    "SettingsError",
    "load_settings",
]
