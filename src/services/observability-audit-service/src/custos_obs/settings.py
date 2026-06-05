"""Runtime configuration parsed from environment variables (OBS-IMPL-002).

The Observability and Audit Service is configured exclusively through the
``CUSTOS_*`` env vars documented in
``design/components/observability-audit-service/design.md`` § Configuration and
projected by the Helm subchart at
``deploy/helm/charts/observability-audit-service/``.

This module is deliberately stdlib-only so it can be imported by both the ASGI
app factory and lightweight test fixtures without dragging in FastAPI, asyncpg,
or the SPL adapters.

Scope note (M1): the ``opensearch`` ``LogQueryProvider`` is out of scope for
this milestone (see the implementation plan's *Out of scope* section), so
``CUSTOS_LOG_QUERY_PROVIDER`` accepts only ``loki`` / ``noop`` here and
``CUSTOS_OPENSEARCH_URL`` is intentionally not parsed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final

# --- § Configuration knobs (design.md § Configuration) -----------------------

#: Required. Adapter identifier for ``LogQueryProvider``. ``loki`` (default) or
#: ``noop``. ``opensearch`` is deferred to M2.
ENV_LOG_QUERY_PROVIDER: Final[str] = "CUSTOS_LOG_QUERY_PROVIDER"

#: Required. Adapter identifier for ``MetricsQueryProvider``. ``prometheus``
#: (default) or ``noop``.
ENV_METRICS_QUERY_PROVIDER: Final[str] = "CUSTOS_METRICS_QUERY_PROVIDER"

#: Required. libpq DSN resolving the ``MetadataStoreProvider`` adapter
#: (``custos-postgres``) that owns the audit-event writer + outbox drain. The
#: service cannot drain the audit outbox without it, so it is required at
#: startup (mirrors auth-/catalog-service's ``*_METADATA_STORE_DSN``).
ENV_METADATA_STORE_DSN: Final[str] = "CUSTOS_OBS_METADATA_STORE_DSN"

#: Conditional. Required when ``LogQueryProvider=loki``.
ENV_LOKI_URL: Final[str] = "CUSTOS_LOKI_URL"

#: Conditional. Required when ``MetricsQueryProvider=prometheus``.
ENV_PROMETHEUS_URL: Final[str] = "CUSTOS_PROMETHEUS_URL"

#: Conditional. Required when ``LogQueryProvider=noop`` — surfaced to the UI as
#: the "view in external system" pointer.
ENV_LOGS_EXTERNAL_URL: Final[str] = "CUSTOS_LOGS_EXTERNAL_URL"

#: Conditional. Required when ``MetricsQueryProvider=noop``.
ENV_METRICS_EXTERNAL_URL: Final[str] = "CUSTOS_METRICS_EXTERNAL_URL"

#: Optional. ConfigMap holding the Collector's effective config.
ENV_OTEL_COLLECTOR_CONFIGMAP: Final[str] = "CUSTOS_OTEL_COLLECTOR_CONFIGMAP"

#: Optional. ConfigMap watched by the External Exporter Loader for customer
#: exporter blocks.
ENV_OTEL_EXPORTERS_CONFIGMAP: Final[str] = "CUSTOS_OTEL_EXPORTERS_CONFIGMAP"

#: Optional. Audit retention window in days. Configurable upward without bound.
ENV_AUDIT_RETENTION_DAYS: Final[str] = "CUSTOS_AUDIT_RETENTION_DAYS"

#: Optional. ``listen`` (LISTEN/NOTIFY) or ``poll`` (interval). Adapters
#: without LISTEN support fall back to ``poll`` automatically (OBS-IMPL-005).
ENV_AUDIT_OUTBOX_DRAIN_MODE: Final[str] = "CUSTOS_AUDIT_OUTBOX_DRAIN_MODE"

#: Optional. Polling interval (seconds) when in ``poll`` mode.
ENV_AUDIT_OUTBOX_POLL_INTERVAL_S: Final[str] = "CUSTOS_AUDIT_OUTBOX_POLL_INTERVAL_S"

#: Optional. Minimum age (seconds) before a fully-drained outbox row is
#: eligible for garbage collection (OBS-IMPL-007).
ENV_AUDIT_OUTBOX_RETENTION_MARGIN: Final[str] = "CUSTOS_AUDIT_OUTBOX_RETENTION_MARGIN"

#: Optional. Drain lag (rows behind the outbox head) at which a pipeline emits
#: ``obs.outbox.lagging`` (OBS-IMPL-006).
ENV_AUDIT_OUTBOX_LAG_THRESHOLD: Final[str] = "CUSTOS_AUDIT_OUTBOX_LAG_THRESHOLD"

#: Optional. ConfigMap holding the alert-rule DSL (OBS-IMPL-008).
ENV_ALERT_RULES_CONFIGMAP: Final[str] = "CUSTOS_ALERT_RULES_CONFIGMAP"

#: Optional. Comma-separated default webhook destinations (overridable
#: per-rule). Empty means no default webhook sink.
ENV_ALERT_WEBHOOK_URLS: Final[str] = "CUSTOS_ALERT_WEBHOOK_URLS"

# --- SMTP relay (conditional; consumed by the alerting dispatcher) -----------

#: Conditional. SMTP relay host. Required (together with the other SMTP vars)
#: only when an SMTP sink is configured in the alert rules (OBS-IMPL-009);
#: validation of that coupling lives with the dispatcher, so the settings
#: loader treats every SMTP var as optional and merely surfaces them.
ENV_SMTP_HOST: Final[str] = "CUSTOS_SMTP_HOST"
ENV_SMTP_PORT: Final[str] = "CUSTOS_SMTP_PORT"
ENV_SMTP_USERNAME: Final[str] = "CUSTOS_SMTP_USERNAME"
ENV_SMTP_PASSWORD: Final[str] = "CUSTOS_SMTP_PASSWORD"
ENV_SMTP_FROM: Final[str] = "CUSTOS_SMTP_FROM"

#: Operational env tag (shared across services).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

# --- Defaults (design.md § Configuration) ------------------------------------

DEFAULT_LOG_QUERY_PROVIDER: Final[str] = "loki"
DEFAULT_METRICS_QUERY_PROVIDER: Final[str] = "prometheus"
DEFAULT_OTEL_COLLECTOR_CONFIGMAP: Final[str] = "custos-otel-collector-config"
DEFAULT_OTEL_EXPORTERS_CONFIGMAP: Final[str] = "custos-otel-exporters"
DEFAULT_AUDIT_RETENTION_DAYS: Final[int] = 90
DEFAULT_AUDIT_OUTBOX_DRAIN_MODE: Final[str] = "listen"
DEFAULT_AUDIT_OUTBOX_POLL_INTERVAL_S: Final[int] = 5
DEFAULT_AUDIT_OUTBOX_RETENTION_MARGIN_S: Final[int] = 86_400  # 24h
DEFAULT_AUDIT_OUTBOX_LAG_THRESHOLD: Final[int] = 1_000
DEFAULT_ALERT_RULES_CONFIGMAP: Final[str] = "custos-alert-rules"
DEFAULT_SMTP_PORT: Final[int] = 587
DEFAULT_ENVIRONMENT: Final[str] = "development"

#: Accepted adapter identifiers per provider. ``opensearch`` is intentionally
#: absent from the log set for M1.
LOG_QUERY_PROVIDERS: Final[frozenset[str]] = frozenset({"loki", "noop"})
METRICS_QUERY_PROVIDERS: Final[frozenset[str]] = frozenset({"prometheus", "noop"})
AUDIT_OUTBOX_DRAIN_MODES: Final[frozenset[str]] = frozenset({"listen", "poll"})


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated observability-audit-service configuration."""

    log_query_provider: str
    metrics_query_provider: str
    metadata_store_dsn: str
    loki_url: str | None
    prometheus_url: str | None
    logs_external_url: str | None
    metrics_external_url: str | None
    otel_collector_configmap: str
    otel_exporters_configmap: str
    audit_retention_days: int
    audit_outbox_drain_mode: str
    audit_outbox_poll_interval_s: int
    audit_outbox_retention_margin_s: int
    audit_outbox_lag_threshold: int
    alert_rules_configmap: str
    alert_webhook_urls: tuple[str, ...] = field(default=())
    smtp_host: str | None = None
    smtp_port: int = DEFAULT_SMTP_PORT
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from: str | None = None
    environment: str = DEFAULT_ENVIRONMENT

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    @property
    def smtp_configured(self) -> bool:
        """True when an SMTP relay host is set (the dispatcher may use SMTP)."""
        return self.smtp_host is not None


def _opt(name: str, env: dict[str, str]) -> str | None:
    """Return a stripped optional value, or ``None`` when unset/empty."""
    value = env.get(name, "").strip()
    return value or None


def _enum(name: str, env: dict[str, str], default: str, allowed: frozenset[str]) -> str:
    value = env.get(name, "").strip() or default
    if value not in allowed:
        choices = ", ".join(sorted(allowed))
        raise SettingsError(
            f"{name} must be one of {{{choices}}} (got {value!r}); "
            f"see design/components/observability-audit-service/design.md § Configuration"
        )
    return value


def _opt_positive_int(name: str, env: dict[str, str], default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a positive integer (got {raw!r})") from exc
    if value <= 0:
        raise SettingsError(f"{name} must be a positive integer (got {raw!r})")
    return value


def _require_for(
    name: str,
    env: dict[str, str],
    *,
    because: str,
) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise SettingsError(
            f"{name} is required {because} "
            f"(see design/components/observability-audit-service/design.md § Configuration)"
        )
    return value


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse and validate a :class:`Settings` from the env mapping.

    Conditional validation fails fast at startup, naming the offending variable:

    - ``CUSTOS_LOKI_URL`` is required iff ``CUSTOS_LOG_QUERY_PROVIDER=loki``.
    - ``CUSTOS_PROMETHEUS_URL`` is required iff
      ``CUSTOS_METRICS_QUERY_PROVIDER=prometheus``.
    - ``CUSTOS_LOGS_EXTERNAL_URL`` is required iff the log provider is ``noop``.
    - ``CUSTOS_METRICS_EXTERNAL_URL`` is required iff the metrics provider is
      ``noop``.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)

    log_provider = _enum(
        ENV_LOG_QUERY_PROVIDER, src, DEFAULT_LOG_QUERY_PROVIDER, LOG_QUERY_PROVIDERS
    )
    metrics_provider = _enum(
        ENV_METRICS_QUERY_PROVIDER, src, DEFAULT_METRICS_QUERY_PROVIDER, METRICS_QUERY_PROVIDERS
    )
    drain_mode = _enum(
        ENV_AUDIT_OUTBOX_DRAIN_MODE, src, DEFAULT_AUDIT_OUTBOX_DRAIN_MODE, AUDIT_OUTBOX_DRAIN_MODES
    )

    loki_url: str | None = None
    logs_external_url: str | None = None
    if log_provider == "loki":
        loki_url = _require_for(ENV_LOKI_URL, src, because="when CUSTOS_LOG_QUERY_PROVIDER=loki")
    else:  # noop
        logs_external_url = _require_for(
            ENV_LOGS_EXTERNAL_URL, src, because="when CUSTOS_LOG_QUERY_PROVIDER=noop"
        )

    prometheus_url: str | None = None
    metrics_external_url: str | None = None
    if metrics_provider == "prometheus":
        prometheus_url = _require_for(
            ENV_PROMETHEUS_URL, src, because="when CUSTOS_METRICS_QUERY_PROVIDER=prometheus"
        )
    else:  # noop
        metrics_external_url = _require_for(
            ENV_METRICS_EXTERNAL_URL, src, because="when CUSTOS_METRICS_QUERY_PROVIDER=noop"
        )

    raw_webhooks = src.get(ENV_ALERT_WEBHOOK_URLS, "").strip()
    webhook_urls: tuple[str, ...] = (
        tuple(u.strip() for u in raw_webhooks.split(",") if u.strip()) if raw_webhooks else ()
    )

    metadata_store_dsn = _require_for(
        ENV_METADATA_STORE_DSN,
        src,
        because="to wire the audit MetadataStoreProvider (custos-postgres)",
    )

    return Settings(
        log_query_provider=log_provider,
        metrics_query_provider=metrics_provider,
        metadata_store_dsn=metadata_store_dsn,
        loki_url=loki_url,
        prometheus_url=prometheus_url,
        logs_external_url=logs_external_url,
        metrics_external_url=metrics_external_url,
        otel_collector_configmap=(
            src.get(ENV_OTEL_COLLECTOR_CONFIGMAP, "").strip() or DEFAULT_OTEL_COLLECTOR_CONFIGMAP
        ),
        otel_exporters_configmap=(
            src.get(ENV_OTEL_EXPORTERS_CONFIGMAP, "").strip() or DEFAULT_OTEL_EXPORTERS_CONFIGMAP
        ),
        audit_retention_days=_opt_positive_int(
            ENV_AUDIT_RETENTION_DAYS, src, DEFAULT_AUDIT_RETENTION_DAYS
        ),
        audit_outbox_drain_mode=drain_mode,
        audit_outbox_poll_interval_s=_opt_positive_int(
            ENV_AUDIT_OUTBOX_POLL_INTERVAL_S, src, DEFAULT_AUDIT_OUTBOX_POLL_INTERVAL_S
        ),
        audit_outbox_retention_margin_s=_opt_positive_int(
            ENV_AUDIT_OUTBOX_RETENTION_MARGIN, src, DEFAULT_AUDIT_OUTBOX_RETENTION_MARGIN_S
        ),
        audit_outbox_lag_threshold=_opt_positive_int(
            ENV_AUDIT_OUTBOX_LAG_THRESHOLD, src, DEFAULT_AUDIT_OUTBOX_LAG_THRESHOLD
        ),
        alert_rules_configmap=(
            src.get(ENV_ALERT_RULES_CONFIGMAP, "").strip() or DEFAULT_ALERT_RULES_CONFIGMAP
        ),
        alert_webhook_urls=webhook_urls,
        smtp_host=_opt(ENV_SMTP_HOST, src),
        smtp_port=_opt_positive_int(ENV_SMTP_PORT, src, DEFAULT_SMTP_PORT),
        smtp_username=_opt(ENV_SMTP_USERNAME, src),
        smtp_password=_opt(ENV_SMTP_PASSWORD, src),
        smtp_from=_opt(ENV_SMTP_FROM, src),
        environment=src.get(ENV_ENVIRONMENT, "").strip() or DEFAULT_ENVIRONMENT,
    )


__all__ = [
    "AUDIT_OUTBOX_DRAIN_MODES",
    "DEFAULT_ALERT_RULES_CONFIGMAP",
    "DEFAULT_AUDIT_OUTBOX_DRAIN_MODE",
    "DEFAULT_AUDIT_OUTBOX_LAG_THRESHOLD",
    "DEFAULT_AUDIT_OUTBOX_POLL_INTERVAL_S",
    "DEFAULT_AUDIT_OUTBOX_RETENTION_MARGIN_S",
    "DEFAULT_AUDIT_RETENTION_DAYS",
    "DEFAULT_ENVIRONMENT",
    "DEFAULT_LOG_QUERY_PROVIDER",
    "DEFAULT_METRICS_QUERY_PROVIDER",
    "DEFAULT_OTEL_COLLECTOR_CONFIGMAP",
    "DEFAULT_OTEL_EXPORTERS_CONFIGMAP",
    "DEFAULT_SMTP_PORT",
    "ENV_ALERT_RULES_CONFIGMAP",
    "ENV_ALERT_WEBHOOK_URLS",
    "ENV_AUDIT_OUTBOX_DRAIN_MODE",
    "ENV_AUDIT_OUTBOX_LAG_THRESHOLD",
    "ENV_AUDIT_OUTBOX_POLL_INTERVAL_S",
    "ENV_AUDIT_OUTBOX_RETENTION_MARGIN",
    "ENV_AUDIT_RETENTION_DAYS",
    "ENV_ENVIRONMENT",
    "ENV_LOGS_EXTERNAL_URL",
    "ENV_LOG_QUERY_PROVIDER",
    "ENV_LOKI_URL",
    "ENV_METADATA_STORE_DSN",
    "ENV_METRICS_EXTERNAL_URL",
    "ENV_METRICS_QUERY_PROVIDER",
    "ENV_OTEL_COLLECTOR_CONFIGMAP",
    "ENV_OTEL_EXPORTERS_CONFIGMAP",
    "ENV_PROMETHEUS_URL",
    "ENV_SMTP_FROM",
    "ENV_SMTP_HOST",
    "ENV_SMTP_PASSWORD",
    "ENV_SMTP_PORT",
    "ENV_SMTP_USERNAME",
    "LOG_QUERY_PROVIDERS",
    "METRICS_QUERY_PROVIDERS",
    "Settings",
    "SettingsError",
    "load_settings",
]
