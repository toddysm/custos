"""MetricsQueryProvider — inbound read-back facade over a customer metrics backend.

Same shape, same rationale, same outbound/inbound split as
`LogQueryProvider`. Holds no persistent state, owns no schema, and runs
no migration.

**Concern separation**: outbound metric scraping (customer's Prometheus
pulls component `/metrics`) is handled by Prometheus directly. This
interface is the inbound read-back path used by the run-scoped metrics
view in the Custos UI.

See `design/components/storage-provider-layer/design.md` § MetricsQueryProvider.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar, Protocol, runtime_checkable

from custos_spl.ids import RunId, WorkspaceId


@dataclass(frozen=True, slots=True)
class MetricSelector:
    """Selects one metric by name plus optional label matchers.

    The adapter is responsible for adding the workspace (and, where
    applicable, run) filter on top of `label_matchers` — callers MUST
    NOT encode workspace/run into the selector themselves.
    """

    name: str
    label_matchers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MetricRange:
    """Time window plus bucket size for a range query.

    `start` is inclusive, `end` is exclusive (Prom-style). `step_seconds`
    is the bucket width — every sample in the returned series is
    aligned to a multiple of this from `start`.
    """

    start: datetime
    end: datetime
    step_seconds: int


@dataclass(frozen=True, slots=True)
class MetricSample:
    """A single time-bucketed value with its labels.

    Labels are returned as-resolved by the backend (after applying the
    selector's matchers); adapters MUST NOT strip the workspace label
    if the backend exposes it — callers may rely on the round-trip.
    """

    timestamp: datetime
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MetricSeries:
    """A series of samples plus the resolved label set common to all of them.

    `samples` is in ascending timestamp order. `labels` carries the
    series-level labels that apply to every sample; per-sample label
    differences (if any — most range queries return one series at a
    time) live on the samples themselves.
    """

    name: str
    labels: Mapping[str, str] = field(default_factory=dict)
    samples: tuple[MetricSample, ...] = ()


@runtime_checkable
class MetricsQueryProvider(Protocol):
    """Inbound read-back over the customer's metrics backend.

    Workspace-scoped: `workspace_id` is the first arg on every method.
    Adapters MUST add the workspace filter to every backend query.

    Failure surface:
      - `BackendUnavailable` — transient backend failure; retry with backoff.
      - `QueryUnsupported` — returned by the `noop` adapter; the UI
        falls back to the `CUSTOS_METRICS_EXTERNAL_URL` pointer.
      - `WorkspaceMismatch` — run exists in a different workspace;
        caller maps to HTTP 404.

    No `SCHEMA_REVISION` is declared by query facades.
    """

    SCHEMA_REVISION: ClassVar[int] = 0

    async def query_run_metrics(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        """Time-bucketed range query scoped to one run.

        The adapter adds the workspace + run filters on top of
        `selector.label_matchers`. `range.step_seconds` is the bucket
        width; samples in the returned series are aligned to multiples
        of it from `range.start`.
        """
        ...

    async def query_workspace_metrics(
        self,
        workspace_id: WorkspaceId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        """Workspace-scoped range query (no run filter).

        Used by workspace-wide dashboards that aggregate across runs.
        """
        ...

    async def query_instant_metric(
        self,
        workspace_id: WorkspaceId,
        selector: MetricSelector,
        at: datetime,
    ) -> MetricSample:
        """Single-point lookup at `at`.

        Used by dashboards that show a "right-now" value. The adapter
        evaluates the selector at the supplied instant and returns
        exactly one sample. If the backend has no value at `at`,
        adapters MAY return the most recent value within a backend-
        specific staleness window; otherwise they MUST raise
        `BackendUnavailable`.
        """
        ...


__all__ = [
    "MetricRange",
    "MetricSample",
    "MetricSelector",
    "MetricSeries",
    "MetricsQueryProvider",
]
