# Observability & Audit API

Last Updated: 2026-06-05

The Observability & Audit Service (COMP-009) is the platform's single seam for
**telemetry** and the **audit trail**. It is deliberately split into two
independent concerns that share nothing but a process:

- **Concern A — Outbound telemetry export.** Logs, traces, and metrics flow
  *out* of the platform to customer-chosen backends (Loki, Datadog, Splunk,
  CloudWatch, …) through an OTel Collector. Custos ships a base Collector config
  and an **External Exporter Loader** that merges customer-supplied exporter
  blocks into the Collector ConfigMap. This is pure OTel configuration; Custos
  provides the scaffold and validates the merge, customers define the endpoints.
- **Concern B — Inbound query (read-back).** The Custos UI/API reads *from* the
  backends to show per-run logs and metrics and to search the audit trail. This
  is a set of thin, stateless read facades — the `LogQueryProvider` and
  `MetricsQueryProvider` SPL interfaces for logs/metrics, and the SPL
  `MetadataStoreProvider.query_audit` for audit — surfaced over a tenant-scoped
  REST API.

The service additionally runs the **audit-outbox drainer** (fanning the
platform-wide audit stream into the durable store), the **retention worker**,
and the **alert engine + dispatcher**, and exposes its own operational metrics
on a Prometheus `/metrics` endpoint.

> **Doc-as-contract.** Every fenced `json` and `yaml` block in this guide is
> parsed and validated against the real wire models, the alert-rule loader, and
> the exporter merge by
> [`tests/test_docs_examples.py`](../../src/services/observability-audit-service/tests/test_docs_examples.py),
> and the error/event/config tables are checked for completeness against the
> locked taxonomies and the settings module — so the examples cannot drift from
> the code.

> **Milestone note.** M1 ships the read-back Query API, the audit pipeline +
> retention + alerting, the exporter loader, and self-telemetry. The
> `opensearch` log adapter and the vendor-push deep-search integrations are
> [deferred to M2](#deferred-to-m2). Where this document describes a deferred
> surface it says so explicitly.

---

## Contents

- [Concern A vs Concern B](#concern-a-vs-concern-b)
- [Authentication and call context](#authentication-and-call-context)
- [Query API (Concern B)](#query-api-concern-b)
  - [Run logs](#run-logs)
  - [Tail run logs (SSE)](#tail-run-logs-sse)
  - [Run metrics](#run-metrics)
  - [Audit search](#audit-search)
  - [Single audit event](#single-audit-event)
  - [Wire models](#wire-models)
  - [Error taxonomy (RFC 7807)](#error-taxonomy-rfc-7807)
- [Operational metrics (`/metrics`)](#operational-metrics-metrics)
- [`obs.*` event taxonomy](#obs-event-taxonomy)
- [Alert-rule DSL](#alert-rule-dsl)
- [Exporter loader (Concern A)](#exporter-loader-concern-a)
- [Configuration reference](#configuration-reference)
- [Deferred to M2](#deferred-to-m2)

---

## Concern A vs Concern B

| | Concern A — Export | Concern B — Read-back |
| --- | --- | --- |
| **Direction** | Telemetry flows *out* to customer backends | Custos UI reads *in* from backends |
| **Mechanism** | OTel Collector pipelines + exporters | `LogQueryProvider` / `MetricsQueryProvider` / audit query |
| **State** | None (config algebra over ConfigMaps) | None (pure read facades, no schema) |
| **Custos artifact** | Base Collector config + Exporter Loader | The REST Query API in this document |
| **Where** | `exporters/merge.py`, `exporters/loader.py` | `api/routes/{logs,metrics,audit}.py` |

The two concerns never touch the same data. Audit is the one piece of durable
state the service owns, and even that flows through the SPL
`MetadataStoreProvider` (Postgres) rather than any backend either concern talks
to.

---

## Authentication and call context

Every Query API route is tenant-scoped and runs behind the call-context
middleware. The workspace in the path **must** match the workspace in the call
context; a mismatch is rejected before any backend query runs. Each route also
requires a specific permission:

| Route | Permission |
| --- | --- |
| `GET …/runs/{runId}/logs` | `logs:read` |
| `GET …/runs/{runId}/logs/tail` | `logs:read` |
| `GET …/runs/{runId}/metrics` | `metrics:read` |
| `GET …/audit` | `audit:read` |
| `GET …/audit/{eventId}` | `audit:read` |

Call-context failures are returned as a compact envelope (not RFC 7807 — that
form is reserved for backend-availability failures):

```json
{
  "error": {
    "code": "permission_denied",
    "detail": "missing required permission: audit:read"
  }
}
```

The `code` is one of `callctx_missing` (401), `callctx_malformed` (400),
`callctx_invalid` (401), `permission_denied` (403), or `workspace_mismatch`
(403). In production a signed JWT verified against the Auth Service JWKS is
mandatory; the unsigned dev-shim header is refused at startup when
`ENVIRONMENT=production`.

The health and operational endpoints — `/healthz`, `/readyz`, and `/metrics` —
bypass the middleware and are unauthenticated.

---

## Query API (Concern B)

All read-back routes share the run-scoped prefix
`/v1/workspaces/{workspaceId}/runs/{runId}` (audit is workspace-scoped at
`/v1/workspaces/{workspaceId}`). Timestamps in query parameters are ISO-8601 and
**must** be timezone-aware (a trailing `Z` is accepted); a naive datetime is a
`400`.

### Run logs

`GET /v1/workspaces/{workspaceId}/runs/{runId}/logs`

| Query param | Type | Default | Notes |
| --- | --- | --- | --- |
| `stepId` | string | — | Restrict to a single step (routes to step-log query) |
| `from` | ISO-8601 | — | Window start (inclusive), timezone-aware |
| `to` | ISO-8601 | — | Window end (exclusive), timezone-aware |
| `severity` | enum | — | Minimum severity bucket |
| `cursor` | string | — | Opaque continuation cursor |

Returns a `LogPageModel`:

<!-- doctest: LogPageModel -->
```json
{
  "items": [
    {
      "timestamp": "2026-06-05T12:00:01Z",
      "severity": "INFO",
      "message": "step started",
      "runId": "run-42",
      "stepId": "step-3",
      "attributes": { "trace_id": "9f1c2a" }
    }
  ],
  "nextCursor": "eyJvZmZzZXQiOjUwfQ=="
}
```

When the log backend is `noop` or unreachable the route returns a `503`
[`LogQueryUnavailable`](#error-taxonomy-rfc-7807) problem. In `noop` mode the
problem carries an `externalUrl` extension pointing at the configured
`CUSTOS_LOGS_EXTERNAL_URL`; an unreachable Loki backend returns the same `503`
without that pointer.

### Tail run logs (SSE)

`GET /v1/workspaces/{workspaceId}/runs/{runId}/logs/tail`

A `text/event-stream` of log records. Each record is one SSE `data:` frame whose
body is a JSON `LogRecordModel`:

```text
data: {"timestamp":"2026-06-05T12:00:01Z","severity":"INFO","message":"step started","runId":"run-42","stepId":"step-3","attributes":{}}

```

Resume after a disconnect with the standard `Last-Event-ID` header, or the
`?cursor=` query parameter as a fallback. If the backend is unreachable *before*
the stream opens the route returns a `503` problem; if it fails *mid-stream*
(after the `200` is committed) the stream degrades to a terminal error frame:

```text
event: error
data: {"type":"urn:custos:obs:problem:LogQueryUnavailable","title":"Log query backend unavailable","status":503,"detail":"the log query backend is not available; use the external log system"}

```

### Run metrics

`GET /v1/workspaces/{workspaceId}/runs/{runId}/metrics`

| Query param | Type | Default | Notes |
| --- | --- | --- | --- |
| `metric` | string (required, non-empty) | — | Metric name to select |
| `from` | ISO-8601 (required) | — | Window start (inclusive), timezone-aware |
| `to` | ISO-8601 (required) | — | Window end (exclusive), timezone-aware |
| `step` | int (`>= 1`) | `60` | Bucket width in seconds |

Returns a `MetricSeriesModel`:

<!-- doctest: MetricSeriesModel -->
```json
{
  "name": "workflow_step_duration_seconds",
  "labels": { "run_id": "run-42" },
  "samples": [
    {
      "timestamp": "2026-06-05T12:00:00Z",
      "value": 0.42,
      "labels": { "step_id": "step-3" }
    }
  ]
}
```

An unavailable metrics backend yields a `503`
[`MetricsQueryUnavailable`](#error-taxonomy-rfc-7807). As with logs, the
`externalUrl` extension pointing at `CUSTOS_METRICS_EXTERNAL_URL` is attached
only in `noop` mode; an unreachable Prometheus backend returns the same `503`
without it.

### Audit search

`GET /v1/workspaces/{workspaceId}/audit`

| Query param | Type | Default | Notes |
| --- | --- | --- | --- |
| `actor` | string | — | Filter by acting principal (backend predicate) |
| `eventName` | string | — | Filter by `eventType` (backend predicate) |
| `subjectId` | string | — | Post-filter on subject values (applied in-route) |
| `from` | ISO-8601 | — | `occurredAfter`, timezone-aware |
| `to` | ISO-8601 | — | `occurredBefore`, timezone-aware |
| `cursor` | string | — | Opaque continuation cursor |

`actor`, `eventName`, `from`, and `to` map to a backend `AuditFilter`;
`subjectId` is applied in-route against the returned page (the metadata store has
no subject predicate), so the `nextCursor` is preserved for continuation.
Returns an `AuditEventPageModel`:

<!-- doctest: AuditEventPageModel -->
```json
{
  "items": [
    {
      "workspaceId": "ws-7f3a",
      "eventId": "01HQ8M5N3K9V2T4P6R8W0X2Y4Z",
      "eventType": "workflow.run.started",
      "actor": "user:alice@example.com",
      "subject": { "run_id": "run-42" },
      "payload": { "workflow_id": "wf-9" },
      "occurredAt": "2026-06-05T12:00:00Z"
    }
  ],
  "nextCursor": "eyJvZmZzZXQiOjEwMH0="
}
```

### Single audit event

`GET /v1/workspaces/{workspaceId}/audit/{eventId}`

Returns one `AuditEventModel`. The route scans cursor-driven pages until the
event id is found (the metadata store offers no point lookup) and returns `404`
when the id is not present in the workspace:

<!-- doctest: AuditEventModel -->
```json
{
  "workspaceId": "ws-7f3a",
  "eventId": "01HQ8M5N3K9V2T4P6R8W0X2Y4Z",
  "eventType": "workflow.run.failed",
  "actor": "system:workflow-service",
  "subject": { "run_id": "run-42", "severity": "ERROR" },
  "payload": { "reason": "step.timeout" },
  "occurredAt": "2026-06-05T12:05:00Z"
}
```

A metadata store that is unreachable or declines the query yields a `503`
[`AuditQueryUnavailable`](#error-taxonomy-rfc-7807).

### Wire models

All response models are frozen and serialize with camelCase aliases:

- **`LogRecordModel`** — `timestamp`, `severity`, `message`, `runId`, `stepId?`,
  `attributes`.
- **`LogPageModel`** — `items: LogRecordModel[]`, `nextCursor?`.
- **`MetricSampleModel`** — `timestamp`, `value`, `labels`.
- **`MetricSeriesModel`** — `name`, `labels`, `samples: MetricSampleModel[]`.
- **`AuditEventModel`** — `workspaceId`, `eventId`, `eventType`, `actor`,
  `subject`, `payload`, `occurredAt`. `subject` and `payload` are free-form JSON
  objects.
- **`AuditEventPageModel`** — `items: AuditEventModel[]`, `nextCursor?`.

### Error taxonomy (RFC 7807)

Backend-availability and validation failures are returned as
`application/problem+json` with a stable `type` URN of the form
`urn:custos:obs:problem:<Kind>`. The locked Problem Details taxonomy:

| Kind | Status | Title | Raised when |
| --- | --- | --- | --- |
| `LogQueryUnavailable` | 503 | Log query backend unavailable | Log provider is `noop` or the backend is unreachable |
| `MetricsQueryUnavailable` | 503 | Metrics query backend unavailable | Metrics provider is `noop` or the backend is unreachable |
| `AuditQueryUnavailable` | 503 | Audit query backend unavailable | The audit metadata store is unreachable or declines the query |
| `AuditDrainLagging` | 500 | Audit outbox drainer lagging | The outbox drainer fell behind its configured lag threshold |
| `AlertSinkUnavailable` | 502 | Alert sink unavailable | A webhook/SMTP sink was unreachable after retries |
| `ExporterConfigInvalid` | 422 | Exporter configuration invalid | A customer exporter block failed Collector validation |

A problem body for the `noop` log provider, including the `externalUrl`
extension member the UI uses as a deep-link:

```json
{
  "type": "urn:custos:obs:problem:LogQueryUnavailable",
  "title": "Log query backend unavailable",
  "status": 503,
  "detail": "the log query backend is not available; use the external log system",
  "externalUrl": "https://grafana.example/explore"
}
```

---

## Operational metrics (`/metrics`)

The service exposes its own health on an unauthenticated Prometheus endpoint at
`GET /metrics`, served as `text/plain; version=0.0.4; charset=utf-8`. The
registry is the authoritative source and also mirrors to OTel instruments (a
no-op without an SDK MeterProvider). The exposed families:

| Family | Type | Labels |
| --- | --- | --- |
| `custos_obs_audit_outbox_lag_rows` | gauge | `pipeline_id` |
| `custos_obs_audit_retention_last_run_timestamp_seconds` | gauge | — |
| `custos_obs_audit_retention_rows_deleted_total` | counter | `kind` (`audit`/`outbox`) |
| `custos_obs_exporter_config_status` | gauge | `configmap` |
| `custos_obs_exporter_config_changes_total` | counter | `configmap`, `outcome` |
| `custos_obs_alert_dispatch_total` | counter | `outcome`, `rule`, `sink` |

```text
# HELP custos_obs_audit_outbox_lag_rows Audit outbox drain lag in rows.
# TYPE custos_obs_audit_outbox_lag_rows gauge
custos_obs_audit_outbox_lag_rows{pipeline_id="audit-store"} 0
```

---

## `obs.*` event taxonomy

Besides consuming every other component's audit trail, the service emits a
small, stable set of **operational** audit events about itself. Each is written
to the audit store as a standard `AuditEvent` envelope (`workspaceId`,
`eventId`, `eventType`, `actor`, `subject`, `payload`, `occurredAt`) under the
sentinel platform workspace `__platform__` with actor
`system:observability-audit-service`. The locked `obs.*` event taxonomy:

| Event | Emitted by | Key payload fields |
| --- | --- | --- |
| `obs.retention.applied` | Retention worker | `audit_rows_deleted`, `outbox_rows_deleted` |
| `obs.outbox.lagging` | Audit pipeline | `pipeline_id`, `lag_rows`, `threshold_rows` |
| `obs.exporter.config.rejected` | Exporter loader | `configmap`, `reason` |
| `obs.exporter.config.applied` | Exporter loader | `configmap`, `exporter_names` |
| `obs.alert.dispatched` | Alert dispatcher | `rule_name`, `sink`, `audit_event_id` |
| `obs.alert.failed` | Alert dispatcher | `rule_name`, `sink`, `audit_event_id`, `reason` |

---

## Alert-rule DSL

Alert rules live in the `custos-alert-rules` ConfigMap and are evaluated against
every audit event flowing through the pipeline. A rule matches when **all** of
its criteria match (AND-combined); `severity` and `component` are read from the
event `subject`, while `match` fields are resolved from the `payload` first and
then the `subject`. A matched rule fans out to one or more named sinks, subject
to per-rule throttling and de-duplication.

<!-- doctest: alert-rules -->
```yaml
rules:
  - name: enterprise-run-failures
    eventName: workflow.run.failed
    severity: ERROR
    component: workflow-service
    match:
      tenant_tier: enterprise
    throttle: 5m
    dedupKey:
      - run_id
    sinks:
      - webhook
      - smtp
```

Rule fields:

- **`name`** *(required)* — unique rule identifier; also the default dedup
  identity when `dedupKey` is omitted.
- **`sinks`** *(required, non-empty)* — sink names (`webhook`, `smtp`).
- **`eventName`** / **`severity`** / **`component`** *(optional)* — exact-match
  filters.
- **`match`** *(optional)* — a mapping of field name to expected string value.
- **`throttle`** *(optional)* — a duration with unit `s`, `m`, `h`, or `d`
  (e.g. `30s`, `5m`, `2h`, `1d`); suppresses repeat fires within the window.
- **`dedupKey`** *(optional)* — a list of field names whose values, with the rule
  name, form the dedup identity.

Sinks are dispatched with bounded retries and exponential backoff. A sink that
remains unreachable after retries is dead-lettered (best-effort) and emits
`obs.alert.failed`; a successful delivery emits `obs.alert.dispatched`. Webhook
sinks fan out to every configured URL and aggregate failures; SMTP delivery runs
off the event loop. Default webhook destinations come from
`CUSTOS_ALERT_WEBHOOK_URLS` and are overridable per rule.

---

## Exporter loader (Concern A)

The External Exporter Loader watches the `custos-otel-exporters` ConfigMap and
merges each customer **exporter block** into the Custos **base** Collector
config. The merge is deterministic (keys sorted, pipeline exporter lists sorted
and de-duplicated) and idempotent. Only the `exporters` and `pipelines` keys are
accepted in a customer block; customer exporter names may not collide with base
exporters, and a `pipelines` attachment may only reference a pipeline that
exists in the base config and an exporter that is defined (base or customer).

The base Collector config Custos ships (illustrative):

<!-- doctest: exporter-base -->
```yaml
exporters:
  logging: {}
service:
  pipelines:
    logs:
      receivers:
        - otlp
      exporters:
        - logging
    metrics:
      exporters:
        - logging
    traces:
      exporters:
        - logging
```

A customer block that adds a Loki exporter and attaches it to the `logs`
pipeline:

<!-- doctest: exporter-customer -->
```yaml
exporters:
  loki/customer:
    endpoint: https://loki.example/loki/api/v1/push
pipelines:
  logs:
    - loki/customer
```

On a successful, changing merge the loader writes the new effective config,
signals the Collector to reload, and emits `obs.exporter.config.applied`. An
invalid block is **rejected**: the last-good effective config is retained
untouched and `obs.exporter.config.rejected` is emitted with a captured
`reason`, so a bad ConfigMap edit can never break a running Collector. An
unchanged block is a silent no-op.

---

## Configuration reference

The service is configured exclusively through `CUSTOS_*` environment variables
(plus the shared `ENVIRONMENT` tag). Conditional variables fail fast at startup,
naming the offending variable.

| Environment variable | Default | Notes |
| --- | --- | --- |
| `CUSTOS_LOG_QUERY_PROVIDER` | `loki` | `loki` or `noop` |
| `CUSTOS_METRICS_QUERY_PROVIDER` | `prometheus` | `prometheus` or `noop` |
| `CUSTOS_OBS_METADATA_STORE_DSN` | *(required)* | libpq DSN for the audit `MetadataStoreProvider` |
| `CUSTOS_LOKI_URL` | *(conditional)* | Required when the log provider is `loki` |
| `CUSTOS_PROMETHEUS_URL` | *(conditional)* | Required when the metrics provider is `prometheus` |
| `CUSTOS_LOGS_EXTERNAL_URL` | *(conditional)* | Required when the log provider is `noop`; surfaced as the UI deep-link |
| `CUSTOS_METRICS_EXTERNAL_URL` | *(conditional)* | Required when the metrics provider is `noop` |
| `CUSTOS_OTEL_COLLECTOR_CONFIGMAP` | `custos-otel-collector-config` | Effective Collector config ConfigMap |
| `CUSTOS_OTEL_EXPORTERS_CONFIGMAP` | `custos-otel-exporters` | Watched customer exporter ConfigMap |
| `CUSTOS_AUDIT_RETENTION_DAYS` | `90` | Audit retention window (configurable upward) |
| `CUSTOS_AUDIT_RETENTION_SWEEP_INTERVAL_S` | `3600` | Retention worker sweep interval |
| `CUSTOS_AUDIT_OUTBOX_DRAIN_MODE` | `listen` | `listen` or `poll` |
| `CUSTOS_AUDIT_OUTBOX_POLL_INTERVAL_S` | `5` | Poll interval when in `poll` mode |
| `CUSTOS_AUDIT_OUTBOX_RETENTION_MARGIN` | `86400` | Min age before a drained outbox row is GC-eligible |
| `CUSTOS_AUDIT_OUTBOX_LAG_THRESHOLD` | `1000` | Rows behind the outbox head that trigger `obs.outbox.lagging` |
| `CUSTOS_ALERT_RULES_CONFIGMAP` | `custos-alert-rules` | Alert-rule DSL ConfigMap |
| `CUSTOS_ALERT_WEBHOOK_URLS` | *(empty)* | Comma-separated default webhook destinations |
| `CUSTOS_SMTP_HOST` | *(conditional)* | Required when an SMTP sink is configured |
| `CUSTOS_SMTP_PORT` | `587` | SMTP relay port |
| `CUSTOS_SMTP_USERNAME` | *(conditional)* | Required when an SMTP sink is configured |
| `CUSTOS_SMTP_PASSWORD` | *(conditional)* | Required when an SMTP sink is configured |
| `CUSTOS_SMTP_FROM` | *(conditional)* | Required when an SMTP sink is configured |
| `ENVIRONMENT` | `development` | Deployment tag; gates the unsigned dev-shim auth |

---

## Deferred to M2

- The `opensearch` `LogQueryProvider` adapter (and `CUSTOS_OPENSEARCH_URL`).
- Deep-search UI integration for vendor backends (Splunk/Datadog); those
  deployments run the read-back providers as `noop` and link out via the
  configured external URLs.

Where a customer runs an in-cluster Loki + Prometheus stack the read-back Query
API works out of the box; on a vendor stack the export concern (Concern A) still
ships all telemetry, while the read-back concern degrades gracefully to the
external-URL pointers documented above.
