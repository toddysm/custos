"""External Exporter Loader (Concern A) for the Observability and Audit Service.

Merges customer-supplied OTel exporter blocks (from the ``custos-otel-exporters``
ConfigMap) into the Custos base Collector config, validating the result and
retaining the last-good config on failure. :mod:`merge` holds the pure merge +
validation algorithm; the ConfigMap watcher and Collector reload live in
:mod:`loader` (OBS-IMPL-011).
"""

from __future__ import annotations

from custos_obs.exporters.loader import (
    CollectorConfigWriter,
    CollectorReloadSignaller,
    ExporterConfigSource,
    ExporterLoader,
    ReconcileOutcome,
)
from custos_obs.exporters.merge import (
    ALLOWED_PIPELINES,
    CollectorConfigError,
    CollectorConfigMerger,
    MergeOutcome,
    MergeResult,
    merge_collector_config,
)

__all__ = [
    "ALLOWED_PIPELINES",
    "CollectorConfigError",
    "CollectorConfigMerger",
    "CollectorConfigWriter",
    "CollectorReloadSignaller",
    "ExporterConfigSource",
    "ExporterLoader",
    "MergeOutcome",
    "MergeResult",
    "ReconcileOutcome",
    "merge_collector_config",
]
