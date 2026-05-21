# custos-prometheus

Custos MetricsQueryProvider adapters for Prometheus and PromQL-speaking backends.

## Overview

`custos-prometheus` provides two adapters:

1. **Prometheus adapter** — queries historical and instant metrics from Prometheus, Mimir, Thanos, Cortex, VictoriaMetrics, Grafana Cloud, or any PromQL-speaking backend.
2. **Noop adapter** — always returns `QueryUnsupported`; UI falls back to external metrics URL (e.g., Grafana dashboard link).

## Features

- **Range queries** — time-bucketed time-series data with configurable bucket width
- **Instant queries** — single-point metric lookups at a specific instant
- **Workspace isolation** — all queries scoped to workspace; cross-workspace access blocked
- **Label matchers** — flexible metric selection with PromQL label matchers
- **Instant query execution** — performs a single instant query at the requested timestamp

## Installation

```bash
pip install custos-prometheus
```

## Configuration

### Environment Variables

| Adapter | Variable | Required | Default | Description |
|---|---|---|---|---|
| prometheus | `CUSTOS_PROMETHEUS_URL` | Yes | — | Base URL of Prometheus instance (e.g., `http://prometheus:9090`) |
| prometheus | `CUSTOS_PROMETHEUS_TENANT_ID` | No | — | Tenant ID for multi-tenant Prometheus (Cortex/Mimir) |
| prometheus | `CUSTOS_PROMETHEUS_TLS_VERIFY` | No | `true` | Verify TLS certificates (`true`, `false`, or path to CA bundle) |
| noop | `CUSTOS_METRICS_EXTERNAL_URL` | No | — | URL to external metrics system (e.g., Grafana dashboard) for UI fallback |

### Example

```bash
export CUSTOS_PROMETHEUS_URL=http://prometheus:9090
# For multi-tenant Prometheus (Cortex/Mimir):
export CUSTOS_PROMETHEUS_TENANT_ID=my-tenant
```

## Usage

### Query Historical Metrics

```python
import asyncio
from custos_prometheus.adapters import make_prometheus_adapter
from custos_spl.ids import WorkspaceId, RunId
from custos_spl.interfaces.metrics_query import MetricSelector, MetricRange
from datetime import datetime, timedelta

async def main():
    adapter = make_prometheus_adapter()
    workspace_id = WorkspaceId("my-workspace")
    run_id = RunId("run-123")
    
    # Query CPU usage over last hour with 1m buckets
    now = datetime.utcnow()
    range = MetricRange(
        start=now - timedelta(hours=1),
        end=now,
        step_seconds=60
    )
    
    selector = MetricSelector(
        name="container_cpu_usage_seconds_total",
        label_matchers={"pod": "my-pod"}
    )
    
    series = await adapter.query_run_metrics(
        workspace_id, run_id, selector, range
    )
    for sample in series.samples:
        print(f"{sample.timestamp}: {sample.value}")

asyncio.run(main())
```

### Instant Query

```python
async def get_current_value():
    adapter = make_prometheus_adapter()
    workspace_id = WorkspaceId("my-workspace")
    
    selector = MetricSelector(name="up")
    
    sample = await adapter.query_instant_metric(
        workspace_id, selector, datetime.utcnow()
    )
    print(f"Current value: {sample.value}")

asyncio.run(get_current_value())
```

### Noop Adapter (Fallback)

```python
from custos_prometheus.adapters import make_noop_adapter
from custos_spl.errors import QueryUnsupported

async def main():
    adapter = make_noop_adapter()
    
    try:
        result = await adapter.query_run_metrics(...)
    except QueryUnsupported:
        print("Metrics disabled — check CUSTOS_METRICS_EXTERNAL_URL env var")

asyncio.run(main())
```

## Prometheus Setup

### Docker Compose (Local Development)

```yaml
services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'

volumes:
  prometheus-data:
```

### Helm (Kubernetes)

See [Prometheus Helm Chart](https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus) documentation.

## Development

### Install dev dependencies

```bash
pip install -e ".[dev]"
```

### Run tests

```bash
pytest tests/
```

### Type checking

```bash
mypy src/custos_prometheus/ --strict
```

## License

Apache 2.0 — see LICENSE in the repository root.
