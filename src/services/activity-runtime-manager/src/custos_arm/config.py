"""Runtime configuration parsed from environment variables (ARM-IMPL-002).

The Activity Runtime Manager is configured through the ``ARM_*`` env vars
documented in ``design/components/activity-runtime-manager/design.md``
§ Configuration. This module also reads ``ENVIRONMENT`` to enforce the
call-context dev-shim production guard, and ``HOST`` / ``PORT`` for the
ASGI server binding.

The module is deliberately stdlib-only so it can be imported by both the
ASGI app factory and lightweight test fixtures without dragging in FastAPI
or the Storage Provider Layer.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Final

# ---------------------------------------------------------------------------
# Environment variable names
# ---------------------------------------------------------------------------

#: Required. ``ArtifactStoreProvider`` binding for activity artifact I/O.
ENV_ARTIFACT_STORE: Final[str] = "ARM_ARTIFACT_STORE"

#: Required. ``MetadataStoreProvider`` binding for execution + artifact records.
ENV_METADATA_STORE: Final[str] = "ARM_METADATA_STORE"

#: Required. Catalog Service endpoint for ``activityRef`` resolution.
ENV_CATALOG_ENDPOINT: Final[str] = "ARM_CATALOG_ENDPOINT"

#: Required. Connector Service endpoint for ``RefreshLease`` on long steps.
ENV_CONNECTOR_ENDPOINT: Final[str] = "ARM_CONNECTOR_ENDPOINT"

#: Required in production. Empty switches the call-context middleware to a
#: dev-shim that trusts ``x-custos-callctx``, logs a WARNING per request,
#: and refuses to start when ``ENVIRONMENT=production``.
ENV_AUTHZ_ENDPOINT: Final[str] = "ARM_AUTHZ_ENDPOINT"

#: Required. Kubernetes namespace in which activity ``Job``s are created.
ENV_SANDBOX_NAMESPACE: Final[str] = "ARM_SANDBOX_NAMESPACE"

#: Required. Connector sidecar image injected into every activity Pod.
ENV_SIDECAR_IMAGE: Final[str] = "ARM_SIDECAR_IMAGE"

#: Optional. Helper image for the io-bridge init container (input injector) and
#: native sidecar (output collector). Must ship ``sh`` + ``tar``; pin by digest
#: in production. Defaults to :data:`DEFAULT_IO_BRIDGE_IMAGE`.
ENV_IO_BRIDGE_IMAGE: Final[str] = "ARM_IO_BRIDGE_IMAGE"

#: Optional. Test/dev escape hatch (default ``false``) relaxing strict digest
#: pinning so a locally ``kind load``ed, registry-less image (which has no
#: manifest digest) can run. Production stays strictly digest-pinned.
ENV_ALLOW_UNPINNED_IMAGES: Final[str] = "ARM_ALLOW_UNPINNED_IMAGES"

#: Optional. Cluster-default isolation tier when a manifest omits
#: ``isolation.minTier``.
ENV_DEFAULT_TIER: Final[str] = "ARM_DEFAULT_TIER"

#: Optional. ``RuntimeClass`` for the ``process`` tier (empty = runc).
ENV_RUNTIME_CLASS_PROCESS: Final[str] = "ARM_RUNTIME_CLASS_PROCESS"

#: Optional. ``RuntimeClass`` for the ``vm`` tier (empty = unavailable).
ENV_RUNTIME_CLASS_VM: Final[str] = "ARM_RUNTIME_CLASS_VM"

#: Optional. ``RuntimeClass`` for the ``microvm`` tier (empty = unavailable).
ENV_RUNTIME_CLASS_MICROVM: Final[str] = "ARM_RUNTIME_CLASS_MICROVM"

#: Optional. Platform-default CPU request / limit applied when the manifest
#: is silent.
ENV_DEFAULT_CPU_REQUEST: Final[str] = "ARM_DEFAULT_CPU_REQUEST"
ENV_DEFAULT_CPU_LIMIT: Final[str] = "ARM_DEFAULT_CPU_LIMIT"

#: Optional. Platform-default memory request / limit applied when the
#: manifest is silent.
ENV_DEFAULT_MEMORY_REQUEST: Final[str] = "ARM_DEFAULT_MEMORY_REQUEST"
ENV_DEFAULT_MEMORY_LIMIT: Final[str] = "ARM_DEFAULT_MEMORY_LIMIT"

#: Optional. Platform-default ephemeral-storage limit.
ENV_DEFAULT_EPHEMERAL_STORAGE_LIMIT: Final[str] = "ARM_DEFAULT_EPHEMERAL_STORAGE_LIMIT"

#: Optional. Absolute ISO-8601 ceiling clamping the manifest timeout and the
#: step deadline.
ENV_MAX_TIMEOUT: Final[str] = "ARM_MAX_TIMEOUT"

#: Optional. Maximum ``outputs.json`` size in bytes.
ENV_OUTPUT_MAX_BYTES: Final[str] = "ARM_OUTPUT_MAX_BYTES"

#: Optional. Per-artifact upload ceiling in bytes.
ENV_ARTIFACT_MAX_BYTES: Final[str] = "ARM_ARTIFACT_MAX_BYTES"

#: Optional. ISO-8601 retention of terminal execution records for replay dedup.
ENV_IDEMPOTENCY_TTL: Final[str] = "ARM_IDEMPOTENCY_TTL"

#: Operational env tag. The dev-shim refuses to run when this is
#: ``production`` (case-insensitive).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

#: ASGI server binding.
ENV_HOST: Final[str] = "HOST"
ENV_PORT: Final[str] = "PORT"

# ---------------------------------------------------------------------------
# Defaults (design § Configuration)
# ---------------------------------------------------------------------------
DEFAULT_TIER: Final[str] = "process"
DEFAULT_CPU_REQUEST: Final[str] = "250m"
DEFAULT_CPU_LIMIT: Final[str] = "1"
DEFAULT_MEMORY_REQUEST: Final[str] = "256Mi"
DEFAULT_MEMORY_LIMIT: Final[str] = "1Gi"
DEFAULT_EPHEMERAL_STORAGE_LIMIT: Final[str] = "2Gi"
DEFAULT_MAX_TIMEOUT: Final[str] = "PT1H"
DEFAULT_OUTPUT_MAX_BYTES: Final[int] = 1_048_576
DEFAULT_ARTIFACT_MAX_BYTES: Final[int] = 5_368_709_120
DEFAULT_IDEMPOTENCY_TTL: Final[str] = "PT24H"
DEFAULT_HOST: Final[str] = "0.0.0.0"
DEFAULT_PORT: Final[int] = 8080

#: Default for :data:`ENV_ALLOW_UNPINNED_IMAGES`: strict digest pinning on.
DEFAULT_ALLOW_UNPINNED_IMAGES: Final[bool] = False

#: Default io-bridge helper image: upstream BusyBox (ships ``sh`` + ``tar``),
#: pinned by its multi-arch index digest so the default is reproducible.
#: Operators override ``ARM_IO_BRIDGE_IMAGE`` to point at an internal mirror.
DEFAULT_IO_BRIDGE_IMAGE: Final[str] = (
    "busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"
)

#: Recognised isolation tiers (design § Sandbox and Isolation Model). The
#: tier→``RuntimeClass`` mapping is exposed as
#: :attr:`Settings.runtime_class_for_tier`.
VALID_TIERS: Final[frozenset[str]] = frozenset({"process", "vm", "microvm"})

#: ISO-8601 duration grammar — the same ``P[nD]T[nH][nM][nS]`` / ``PnW``
#: subset the Workflow Service accepts (months / years are calendar
#: dependent and rejected). Mirrored locally so this module stays
#: stdlib-only.
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


def parse_iso8601_duration(name: str, value: str) -> timedelta:
    """Parse an ISO-8601 duration into a strictly-positive :class:`timedelta`.

    Accepts the ``PnW`` weeks form OR ``P[nD][T[nH][nM][nS]]`` with at
    least one non-zero component. Months / years are rejected because
    they are calendar-dependent.

    Args:
        name: The originating env-var name, surfaced in the error message.
        value: The raw ISO-8601 duration string.

    Returns:
        The parsed positive :class:`~datetime.timedelta`.

    Raises:
        SettingsError: ``value`` does not match the grammar or parses to a
            non-positive duration.
    """
    match = _ISO8601_DURATION_PATTERN.match(value)
    if match is None:
        raise SettingsError(
            f"{name} must be a recognised ISO-8601 duration "
            f"(P[nD]T[nH][nM][nS] or PnW subset); got {value!r}"
        )
    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)
    delta = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
    if delta <= timedelta(0):
        raise SettingsError(f"{name} must be a positive ISO-8601 duration; got {value!r}")
    return delta


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated activity-runtime-manager configuration."""

    artifact_store: str
    metadata_store: str
    catalog_endpoint: str
    connector_endpoint: str
    authz_endpoint: str  # empty string means "dev shim active"
    sandbox_namespace: str
    sidecar_image: str
    io_bridge_image: str
    default_tier: str
    runtime_class_process: str
    runtime_class_vm: str
    runtime_class_microvm: str
    default_cpu_request: str
    default_cpu_limit: str
    default_memory_request: str
    default_memory_limit: str
    default_ephemeral_storage_limit: str
    max_timeout: timedelta
    output_max_bytes: int
    artifact_max_bytes: int
    idempotency_ttl: timedelta
    environment: str
    host: str
    port: int
    #: Test/dev escape hatch relaxing strict image-digest pinning.
    allow_unpinned_images: bool = field(default=DEFAULT_ALLOW_UNPINNED_IMAGES)
    #: Raw ISO-8601 sources retained for diagnostics / round-tripping.
    max_timeout_raw: str = field(default=DEFAULT_MAX_TIMEOUT)
    idempotency_ttl_raw: str = field(default=DEFAULT_IDEMPOTENCY_TTL)

    @property
    def use_callctx_dev_shim(self) -> bool:
        """True when the dev-shim call-context middleware should be wired."""
        return self.authz_endpoint == ""

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    def runtime_class_for_tier(self, tier: str) -> str:
        """Return the configured ``RuntimeClass`` for ``tier`` (empty if unset).

        Args:
            tier: One of :data:`VALID_TIERS`.

        Returns:
            The mapped ``RuntimeClass`` name, or the empty string when the
            tier maps to the cluster-default runtime (``process``) or is
            unavailable (``vm`` / ``microvm`` with no class configured).

        Raises:
            ValueError: ``tier`` is not a recognised tier.
        """
        if tier == "process":
            return self.runtime_class_process
        if tier == "vm":
            return self.runtime_class_vm
        if tier == "microvm":
            return self.runtime_class_microvm
        raise ValueError(f"unknown isolation tier {tier!r}; expected one of {sorted(VALID_TIERS)}")


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/activity-runtime-manager/design.md § Configuration)"
        )
    return value


def _opt_str(name: str, env: dict[str, str], default: str) -> str:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def _opt_int(name: str, env: dict[str, str], default: int, *, minimum: int | None = None) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise SettingsError(f"{name} must be >= {minimum} (got {value})")
    return value


def _opt_bool(name: str, env: dict[str, str], default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise SettingsError(f"{name} must be a boolean (got {raw!r})")


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``).

    ``ARM_AUTHZ_ENDPOINT`` is required in production but accepted as empty
    here so local development and tests can opt into the dev-shim
    call-context middleware; the shim itself refuses to start when
    :meth:`Settings.is_production` is true (enforced in the middleware
    constructor).

    Raises:
        SettingsError: A required variable is missing, a numeric value is
            malformed, ``ARM_DEFAULT_TIER`` is not a recognised tier, or an
            ISO-8601 duration does not parse to a positive value.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)

    default_tier = _opt_str(ENV_DEFAULT_TIER, src, DEFAULT_TIER)
    if default_tier not in VALID_TIERS:
        raise SettingsError(
            f"{ENV_DEFAULT_TIER} must be one of {sorted(VALID_TIERS)} (got {default_tier!r})"
        )

    max_timeout_raw = _opt_str(ENV_MAX_TIMEOUT, src, DEFAULT_MAX_TIMEOUT)
    idempotency_ttl_raw = _opt_str(ENV_IDEMPOTENCY_TTL, src, DEFAULT_IDEMPOTENCY_TTL)

    return Settings(
        artifact_store=_require(ENV_ARTIFACT_STORE, src),
        metadata_store=_require(ENV_METADATA_STORE, src),
        catalog_endpoint=_require(ENV_CATALOG_ENDPOINT, src),
        connector_endpoint=_require(ENV_CONNECTOR_ENDPOINT, src),
        authz_endpoint=src.get(ENV_AUTHZ_ENDPOINT, "").strip(),
        sandbox_namespace=_require(ENV_SANDBOX_NAMESPACE, src),
        sidecar_image=_require(ENV_SIDECAR_IMAGE, src),
        io_bridge_image=_opt_str(ENV_IO_BRIDGE_IMAGE, src, DEFAULT_IO_BRIDGE_IMAGE),
        default_tier=default_tier,
        runtime_class_process=_opt_str(ENV_RUNTIME_CLASS_PROCESS, src, ""),
        runtime_class_vm=_opt_str(ENV_RUNTIME_CLASS_VM, src, ""),
        runtime_class_microvm=_opt_str(ENV_RUNTIME_CLASS_MICROVM, src, ""),
        default_cpu_request=_opt_str(ENV_DEFAULT_CPU_REQUEST, src, DEFAULT_CPU_REQUEST),
        default_cpu_limit=_opt_str(ENV_DEFAULT_CPU_LIMIT, src, DEFAULT_CPU_LIMIT),
        default_memory_request=_opt_str(ENV_DEFAULT_MEMORY_REQUEST, src, DEFAULT_MEMORY_REQUEST),
        default_memory_limit=_opt_str(ENV_DEFAULT_MEMORY_LIMIT, src, DEFAULT_MEMORY_LIMIT),
        default_ephemeral_storage_limit=_opt_str(
            ENV_DEFAULT_EPHEMERAL_STORAGE_LIMIT, src, DEFAULT_EPHEMERAL_STORAGE_LIMIT
        ),
        max_timeout=parse_iso8601_duration(ENV_MAX_TIMEOUT, max_timeout_raw),
        output_max_bytes=_opt_int(ENV_OUTPUT_MAX_BYTES, src, DEFAULT_OUTPUT_MAX_BYTES, minimum=1),
        artifact_max_bytes=_opt_int(
            ENV_ARTIFACT_MAX_BYTES, src, DEFAULT_ARTIFACT_MAX_BYTES, minimum=1
        ),
        idempotency_ttl=parse_iso8601_duration(ENV_IDEMPOTENCY_TTL, idempotency_ttl_raw),
        environment=_opt_str(ENV_ENVIRONMENT, src, "development"),
        host=_opt_str(ENV_HOST, src, DEFAULT_HOST),
        port=_opt_int(ENV_PORT, src, DEFAULT_PORT, minimum=1),
        allow_unpinned_images=_opt_bool(
            ENV_ALLOW_UNPINNED_IMAGES, src, DEFAULT_ALLOW_UNPINNED_IMAGES
        ),
        max_timeout_raw=max_timeout_raw,
        idempotency_ttl_raw=idempotency_ttl_raw,
    )


__all__ = [
    "DEFAULT_ALLOW_UNPINNED_IMAGES",
    "DEFAULT_ARTIFACT_MAX_BYTES",
    "DEFAULT_CPU_LIMIT",
    "DEFAULT_CPU_REQUEST",
    "DEFAULT_EPHEMERAL_STORAGE_LIMIT",
    "DEFAULT_HOST",
    "DEFAULT_IDEMPOTENCY_TTL",
    "DEFAULT_IO_BRIDGE_IMAGE",
    "DEFAULT_MAX_TIMEOUT",
    "DEFAULT_MEMORY_LIMIT",
    "DEFAULT_MEMORY_REQUEST",
    "DEFAULT_OUTPUT_MAX_BYTES",
    "DEFAULT_PORT",
    "DEFAULT_TIER",
    "ENV_ALLOW_UNPINNED_IMAGES",
    "ENV_ARTIFACT_STORE",
    "ENV_AUTHZ_ENDPOINT",
    "ENV_CATALOG_ENDPOINT",
    "ENV_CONNECTOR_ENDPOINT",
    "ENV_ENVIRONMENT",
    "ENV_IDEMPOTENCY_TTL",
    "ENV_IO_BRIDGE_IMAGE",
    "ENV_MAX_TIMEOUT",
    "ENV_METADATA_STORE",
    "ENV_SANDBOX_NAMESPACE",
    "ENV_SIDECAR_IMAGE",
    "VALID_TIERS",
    "Settings",
    "SettingsError",
    "load_settings",
    "parse_iso8601_duration",
]
