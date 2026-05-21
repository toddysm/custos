# custos-loki

Custos LogQueryProvider adapter for Loki log storage backend.

## Overview

`custos-loki` provides query access to logs stored in Loki, enabling the Custos UI/API to retrieve historical logs and live tails without coupling to Loki's native API.

## Features

- **Historical log queries** — paginated access to run/step logs with filtering
- **Live log tails** — streaming access to logs as they're written
- **Workspace isolation** — all queries scoped to workspace; cross-workspace access blocked
- **Filter support** — by severity level, time range, step, message content
- **Cursor-based pagination** — stateless opaque cursors for result navigation

## Installation

```bash
pip install custos-loki
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CUSTOS_LOKI_URL` | Yes | — | Base URL of Loki instance (e.g., `http://loki:3100`) |
| `CUSTOS_LOKI_TENANT_ID` | No | — | Loki tenant ID (for multi-tenant deployments) |
| `CUSTOS_LOKI_TLS_VERIFY` | No | `true` | Verify TLS certificates (`true`, `false`, or path to CA bundle) |

### Example

```bash
export CUSTOS_LOKI_URL=http://loki:3100
# For multi-tenant Loki:
export CUSTOS_LOKI_TENANT_ID=my-tenant
```

## Usage

### Query Historical Logs

```python
import asyncio
from custos_loki.adapters import make_adapter
from custos_spl.ids import WorkspaceId, RunId
from custos_spl.interfaces.log_query import LogFilter
from datetime import datetime, timedelta

async def main():
    adapter = make_adapter()
    workspace_id = WorkspaceId("my-workspace")
    run_id = RunId("run-123")
    
    # Query logs from last hour
    now = datetime.utcnow()
    filter = LogFilter(
        start=now - timedelta(hours=1),
        end=now,
        severity_at_least="warn"
    )
    
    page = await adapter.query_run_logs(workspace_id, run_id, filter)
    for record in page.items:
        print(f"[{record.severity}] {record.timestamp}: {record.message}")
    
    # Fetch next page if available
    if page.next_cursor:
        next_page = await adapter.query_run_logs(workspace_id, run_id, filter, page.next_cursor)

asyncio.run(main())
```

### Live Tail

```python
async def tail_logs():
    adapter = make_adapter()
    workspace_id = WorkspaceId("my-workspace")
    run_id = RunId("run-123")
    
    # Stream logs as they arrive
    async for record in adapter.tail_run_logs(workspace_id, run_id):
        print(f"[{record.severity}] {record.message}")

asyncio.run(tail_logs())
```

## Severity Mapping

Loki adapter normalizes log levels to OTel standard (6 values):

| OTel Level | Typical Sources |
|---|---|
| `trace` | TRACE, VERBOSE |
| `debug` | DEBUG |
| `info` | INFO |
| `warn` | WARN, WARNING |
| `error` | ERROR, ERR |
| `fatal` | FATAL, CRITICAL, PANIC |

## Loki Setup

### Docker Compose (Local Development)

```yaml
services:
  loki:
    image: grafana/loki:latest
    ports:
      - "3100:3100"
    volumes:
      - loki-data:/loki
    command: -config.file=/etc/loki/local-config.yaml

  promtail:
    image: grafana/promtail:latest
    volumes:
      - /var/log:/var/log
    command: -config.file=/etc/promtail/config.yml
```

### Helm (Kubernetes)

See [Loki Helm Chart](https://grafana.com/docs/loki/latest/installation/helm/) documentation.

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
mypy src/custos_loki/ --strict
```

## License

Apache 2.0 — see LICENSE in the repository root.
