"""Collector-config merge + validation for the External Exporter Loader.

Concern A (outbound telemetry export) ships customer telemetry to customer-chosen
backends through an OTel Collector. Customers declare extra exporters in a
Kubernetes ConfigMap (``custos-otel-exporters``); this module merges those
customer *exporter blocks* into the Custos base Collector config
(``custos-otel-collector-config``) and validates the result before it is ever
handed to the Collector.

The merge is **deterministic** (same inputs always render the same YAML — keys
sorted, pipeline exporter lists sorted + de-duplicated) and **idempotent**
(merging the same customer blocks twice yields the same effective config). When a
customer block is invalid the merge is rejected with a captured reason and the
*last-good* effective config is retained, so a bad ConfigMap edit can never break
a running Collector (design TODO-002).

This module is pure config algebra: it parses/validates/renders YAML and holds
the last-good state. The ConfigMap watch, Collector reload signalling, and the
``obs.exporter.config.*`` audit events live in the loader (OBS-IMPL-011).

### Customer ConfigMap shape

```yaml
exporters:
  loki/customer:        # OTel exporter name: ``type`` or ``type/name``
    endpoint: https://loki.example/loki/api/v1/push
  datadog:
    api:
      key: ${env:DD_API_KEY}
pipelines:              # optional: attach exporters to base pipelines
  logs: [loki/customer]
  traces: [datadog]
```

Only the ``exporters`` and ``pipelines`` keys are accepted. Customer exporter
names may not collide with exporters already defined in the base config, and a
``pipelines`` attachment may only reference pipelines that exist in the base
config and exporters that are defined (base or customer).
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import yaml

#: The telemetry signal pipelines a customer block may attach exporters to.
ALLOWED_PIPELINES: frozenset[str] = frozenset({"logs", "metrics", "traces"})

#: The only top-level keys accepted in a customer exporter block.
_ALLOWED_CUSTOMER_KEYS: frozenset[str] = frozenset({"exporters", "pipelines"})


class CollectorConfigError(ValueError):
    """A base or customer Collector config was malformed or invalid."""


def _parse_yaml_mapping(raw: str | Mapping[str, Any] | None, *, where: str) -> dict[str, Any]:
    """Parse ``raw`` (YAML text or a mapping) into a plain ``dict``."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            loaded = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise CollectorConfigError(f"{where} is not valid YAML: {exc}") from exc
    else:
        loaded = raw
    if loaded is None:
        return {}
    if not isinstance(loaded, Mapping):
        raise CollectorConfigError(f"{where} must be a mapping, got {type(loaded).__name__}")
    return dict(loaded)


def _require_exporter_mapping(value: Any, *, where: str) -> dict[str, Any]:
    """Validate and return an ``exporters`` mapping (name -> config)."""
    if not isinstance(value, Mapping):
        raise CollectorConfigError(f"{where} must be a mapping of exporter name to config")
    exporters: dict[str, Any] = {}
    for name, config in value.items():
        if not isinstance(name, str) or not name.strip():
            raise CollectorConfigError(f"{where} has a non-string exporter name: {name!r}")
        _validate_exporter_name(name, where=where)
        exporters[name] = config
    return exporters


def _validate_exporter_name(name: str, *, where: str) -> None:
    """Enforce the OTel ``type`` or ``type/name`` exporter-id convention."""
    parts = name.split("/")
    if len(parts) > 2 or any(not part.strip() for part in parts):
        raise CollectorConfigError(f"{where} exporter name {name!r} must be 'type' or 'type/name'")


def _base_pipelines(base: Mapping[str, Any]) -> dict[str, Any]:
    """Return the ``service.pipelines`` mapping from the base config."""
    service = base.get("service", {})
    if not isinstance(service, Mapping):
        raise CollectorConfigError("base config 'service' must be a mapping")
    pipelines = service.get("pipelines", {})
    if not isinstance(pipelines, Mapping):
        raise CollectorConfigError("base config 'service.pipelines' must be a mapping")
    return dict(pipelines)


def _merge_pipeline_attachments(
    merged: dict[str, Any],
    attachments: Mapping[str, Any],
    *,
    defined_exporters: set[str],
) -> None:
    """Attach customer exporters to the named base pipelines (in place)."""
    service = merged.setdefault("service", {})
    pipelines = service.setdefault("pipelines", {})
    for pipeline_name, exporter_list in attachments.items():
        if pipeline_name not in ALLOWED_PIPELINES:
            raise CollectorConfigError(
                f"customer pipelines.{pipeline_name} is not a valid signal "
                f"(allowed: {sorted(ALLOWED_PIPELINES)})"
            )
        if pipeline_name not in pipelines:
            raise CollectorConfigError(
                f"customer pipelines.{pipeline_name} references a pipeline "
                "not present in the base Collector config"
            )
        if not isinstance(exporter_list, list):
            raise CollectorConfigError(
                f"customer pipelines.{pipeline_name} must be a list of exporter names"
            )
        pipeline = pipelines[pipeline_name]
        if not isinstance(pipeline, dict):
            raise CollectorConfigError(
                f"base config 'service.pipelines.{pipeline_name}' must be a mapping"
            )
        existing = pipeline.get("exporters", [])
        if not isinstance(existing, list):
            raise CollectorConfigError(
                f"base config 'service.pipelines.{pipeline_name}.exporters' must be a list"
            )
        names = set(existing)
        for exporter_name in exporter_list:
            if exporter_name not in defined_exporters:
                raise CollectorConfigError(
                    f"customer pipelines.{pipeline_name} references undefined "
                    f"exporter {exporter_name!r}"
                )
            names.add(exporter_name)
        # Sort for deterministic, idempotent output.
        pipeline["exporters"] = sorted(names)


def _validate_merged(merged: Mapping[str, Any]) -> None:
    """Validate that every pipeline exporter reference resolves to a definition."""
    exporters = merged.get("exporters", {})
    defined = set(exporters) if isinstance(exporters, Mapping) else set()
    pipelines = _base_pipelines(merged)
    for pipeline_name, pipeline in pipelines.items():
        if not isinstance(pipeline, Mapping):
            continue
        referenced = pipeline.get("exporters", [])
        if not isinstance(referenced, list):
            raise CollectorConfigError(
                f"merged 'service.pipelines.{pipeline_name}.exporters' must be a list"
            )
        missing = [name for name in referenced if name not in defined]
        if missing:
            raise CollectorConfigError(
                f"merged pipeline {pipeline_name!r} references undefined exporters: "
                f"{sorted(missing)}"
            )


def _render(config: Mapping[str, Any]) -> str:
    """Render a config to deterministic YAML (sorted keys)."""
    return yaml.safe_dump(dict(config), sort_keys=True, default_flow_style=False)


@dataclass(frozen=True, slots=True)
class MergeResult:
    """The outcome of merging customer exporter blocks into the base config."""

    effective_config: str
    exporter_names: tuple[str, ...]


def merge_collector_config(
    base: str | Mapping[str, Any],
    customer: str | Mapping[str, Any] | None,
) -> MergeResult:
    """Merge customer exporter blocks into the base Collector config.

    Returns the deterministic, validated effective config plus the sorted tuple
    of customer-supplied exporter names. Raises :class:`CollectorConfigError`
    when the base or customer config is malformed or the merged result would be
    invalid; in that case the caller keeps the last-good config.
    """
    base_map = _parse_yaml_mapping(base, where="base config")
    _base_pipelines(base_map)  # validate base shape up front
    base_exporters = base_map.get("exporters", {})
    if not isinstance(base_exporters, Mapping):
        raise CollectorConfigError("base config 'exporters' must be a mapping")

    customer_map = _parse_yaml_mapping(customer, where="customer exporter block")
    unknown = set(customer_map) - _ALLOWED_CUSTOMER_KEYS
    if unknown:
        raise CollectorConfigError(
            f"customer exporter block has unsupported keys: {sorted(unknown)} "
            f"(allowed: {sorted(_ALLOWED_CUSTOMER_KEYS)})"
        )

    customer_exporters = _require_exporter_mapping(
        customer_map.get("exporters", {}), where="customer exporters"
    )
    collisions = set(customer_exporters) & set(base_exporters)
    if collisions:
        raise CollectorConfigError(
            f"customer exporters may not redefine base exporters: {sorted(collisions)}"
        )

    merged = deepcopy(base_map)
    merged_exporters = dict(base_exporters)
    merged_exporters.update(customer_exporters)
    merged["exporters"] = merged_exporters

    attachments = customer_map.get("pipelines", {})
    if not isinstance(attachments, Mapping):
        raise CollectorConfigError("customer 'pipelines' must be a mapping")
    _merge_pipeline_attachments(merged, attachments, defined_exporters=set(merged_exporters))

    _validate_merged(merged)
    return MergeResult(
        effective_config=_render(merged),
        exporter_names=tuple(sorted(customer_exporters)),
    )


@dataclass(frozen=True, slots=True)
class MergeOutcome:
    """The result of applying a customer block, with last-good fallback."""

    effective_config: str
    exporter_names: tuple[str, ...]
    applied: bool
    rejection_reason: str | None

    @property
    def rejected(self) -> bool:
        """Whether the customer block was rejected and last-good was retained."""
        return not self.applied


class CollectorConfigMerger:
    """Stateful merger that retains the last-good effective Collector config.

    Constructed from the validated base config; :meth:`apply` merges a customer
    block, returning a :class:`MergeOutcome`. Customer blocks are always merged
    against the pristine base (so :meth:`apply` is idempotent — re-applying the
    same block yields the same effective config). On success the effective config
    becomes the new last-good; on failure the previous last-good is retained and
    the rejection reason is captured, so a bad ConfigMap edit never disturbs the
    running Collector.
    """

    def __init__(self, *, base_config: str | Mapping[str, Any]) -> None:
        # Establish the initial last-good from the base config (no customer
        # blocks). This also validates the base config eagerly.
        result = merge_collector_config(base_config, None)
        self._base = result.effective_config
        self._last_good = result.effective_config

    @property
    def last_good(self) -> str:
        """The most recent effective config that passed validation."""
        return self._last_good

    def apply(self, customer: str | Mapping[str, Any] | None) -> MergeOutcome:
        """Merge a customer block, falling back to last-good on rejection."""
        try:
            result = merge_collector_config(self._base, customer)
        except CollectorConfigError as exc:
            return MergeOutcome(
                effective_config=self._last_good,
                exporter_names=(),
                applied=False,
                rejection_reason=str(exc),
            )
        self._last_good = result.effective_config
        return MergeOutcome(
            effective_config=result.effective_config,
            exporter_names=result.exporter_names,
            applied=True,
            rejection_reason=None,
        )


__all__ = [
    "ALLOWED_PIPELINES",
    "CollectorConfigError",
    "CollectorConfigMerger",
    "MergeOutcome",
    "MergeResult",
    "merge_collector_config",
]
