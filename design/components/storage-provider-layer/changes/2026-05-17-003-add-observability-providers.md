# Change: add-observability-providers

Date: 2026-05-17
Type: component-design
Component: storage-provider-layer
Sequence: 003
GitHub Issue: #73
Status: open

## Summary

Adds two new provider interfaces — `LogQueryProvider` and `MetricsQueryProvider` — and the audit outbox drain protocol (`streamAuditOutbox`, `commitAuditOutboxCursor`, `listenAuditOutbox`) on `MetadataStoreProvider`, in support of the Observability and Audit Service (COMP-009). Bumps SPL from five to seven interfaces. Bumps `MetadataStoreProvider` to schema revision 4. Adds two required configuration variables.

## Context

The COMP-009 Observability and Audit Service design session (2026-05-17) locked the decision to split observability into two cleanly separated concerns:

- **Concern A — Outbound telemetry streaming**: logs, traces, and metrics flow out of the platform to whatever backends the customer operates (Loki, Datadog, Splunk, CloudWatch, Honeycomb, ...). This path is handled entirely by the OTel Collector and customer-supplied exporter blocks. **No SPL surface is needed for this** — the Collector is the abstraction layer.
- **Concern B — Inbound query**: the Custos UI/API needs to read logs and metrics back to render per-run views (log tail, metric panels). This is where SPL must provide a stable interface so the platform is not hardcoded to any one backend.

The user explicitly chose to include both `LogQueryProvider` and `MetricsQueryProvider` in M1 (rather than deferring metrics to M2) so the run-scoped UI works against any Prometheus-compatible metrics backend on day one.

Separately, the audit outbox drain protocol was previously only described in prose; it now becomes part of the interface contract so Observability Service can run as an independent consumer with its own per-pipeline cursor.

## Impact

- **SPL contract**: grows from five to seven interfaces. The two new interfaces are stateless query facades — no schema, no migration, only contract revisions. Adapters that implement only the previous five remain valid; the platform refuses to start without an active adapter for each declared provider.
- **MetadataStoreProvider**: gains three drain-side methods and one new entity (`AuditOutboxCursor`). Schema revision bumps to 4.
- **Configuration**: two new required variables — `CUSTOS_LOG_QUERY_PROVIDER` (default `loki`) and `CUSTOS_METRICS_QUERY_PROVIDER` (default `prometheus`). Each carries conditional companions (`CUSTOS_LOKI_URL`, `CUSTOS_OPENSEARCH_URL`, `CUSTOS_PROMETHEUS_URL`, plus `CUSTOS_LOGS_EXTERNAL_URL` / `CUSTOS_METRICS_EXTERNAL_URL` for `noop` adapters).
- **Workspace-scoping middleware**: the two new interfaces take `workspaceId` on every method; the workspace-filter lint rule applies trivially since they hold no state. No middleware exemption.
- **No cross-interface impact**: no change to `DefinitionStoreProvider`, `CatalogStoreProvider`, `ArtifactStoreProvider`, or `AuthStoreProvider`.

## Concern A vs Concern B (rationale)

The split is what makes the architecture sustainable across customer infrastructures:

- A customer running on Datadog: OTel Collector exports logs and metrics to Datadog (Concern A). `LogQueryProvider=noop` and `MetricsQueryProvider=noop`; Custos UI shows pointer links to the Datadog dashboards. No adapter work required.
- A customer running self-hosted: OTel Collector exports to Loki and Prometheus (Concern A). `LogQueryProvider=loki`, `MetricsQueryProvider=prometheus`; Custos UI renders log tails and metric panels natively (Concern B).
- A customer running mixed (Loki for logs, Datadog for metrics): `LogQueryProvider=loki`, `MetricsQueryProvider=noop` with `CUSTOS_METRICS_EXTERNAL_URL` pointing at Datadog. Per-run log tail in Custos UI; metrics in Datadog.

The same physical bytes flow once via Concern A; Concern B is a separate read-path that may or may not be wired up depending on which backend has an SPL adapter.

## Before / After

**Before**: SPL defined five interfaces. Observability had no SPL surface, and the audit outbox drain was described only in design prose with no method contract. Customers were implicitly tied to Loki for the Custos UI log tail and there was no story for metrics read-back at all.

**After**: SPL defines seven interfaces. Observability Service reads through `LogQueryProvider`, `MetricsQueryProvider`, and the new `streamAuditOutbox` family on `MetadataStoreProvider`. M1 ships `loki` / `opensearch` / `noop` log adapters and `prometheus` / `noop` metrics adapters; customers on other backends use `noop` plus the external-URL pointer until an adapter is contributed.

## Files Changed

- `design/components/storage-provider-layer/design.md` — interface count (5 → 7), internal-structure diagram, entity-to-interface map (AuditOutboxCursor added; LogQueryProvider / MetricsQueryProvider rows added), public interface sections for the two new providers, audit drain methods on `MetadataStoreProvider`, migration revision table (MetadataStoreProvider:4 + provider-rev:1 for the two new ones), configuration table, change history.
- `design/components/observability-audit-service/design.md` — added in the parallel Observability design session; references these new interface surfaces and the two-concerns split.

## Open Follow-ups

- Conformance test suite skeleton must cover both new query-facade interfaces and the outbox drain protocol.
- Adapter implementation cost: `loki`, `opensearch`, `prometheus` adapters are M1 deliverables; `noop` is trivial.
- `listenAuditOutbox` must clearly document its `NotSupported` path so adapters without notify primitives can declare it cleanly and the drainer falls back to polling.
