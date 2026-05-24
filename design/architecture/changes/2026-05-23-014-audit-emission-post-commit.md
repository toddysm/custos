# Change: audit-emission-post-commit

Date: 2026-05-23
Type: architecture
Sequence: 014
GitHub Issue: #220
Status: open

## Summary

CS-IMPL-019 (#220) introduces SPL audit-event emission for every catalog
state-changing operation (workflow publish/deprecate, template
materialize/extract, activity register/deprecate, connector
register/deprecate). The SPL `with_transaction` primitive is
intra-provider only — the workflow + template rows live behind the
`DefinitionStoreProvider`, the activity/connector type rows behind the
`CatalogStoreProvider`, and the audit-event outbox behind the
`MetadataStoreProvider`. The catalog manager cannot enrol all three
adapters in a single XA-style transaction, so audit emission cannot be
strictly atomic with the state mutation it describes.

## Decision

Audit emission is **best-effort post-commit**:

1. The manager runs the state mutation against the catalog /
   definition store. The state row is committed before the audit
   helper is invoked.
2. The manager calls the appropriate `audit_*` helper in
   `custos_catalog.audit`. The helper constructs an `AuditEvent` and
   calls `MetadataStoreProvider.append_audit(workspace_id, event)`.
3. If `append_audit` raises, the failure is logged at WARNING and the
   `custos_audit_emit_failures_total{event_type}` counter is bumped.
   The state mutation is **not** rolled back.

The Observability Service detects emission gaps via:

- `custos_audit_emit_failures_total` rate alerts (Phase H/M alert
  rules; the alert rule lands with the Observability Service Phase M
  work).
- The SPL audit outbox drainer lag metric — if catalog emission stays
  healthy but the outbox is not draining the Observability Service
  page-worthy lag alert fires.

## Consequences

- Audit-trail correctness is best-effort: a hard crash between commit
  and emission, or a metadata-store outage, can drop a single audit
  row even though the user-visible state change succeeded.
- Operators should treat `custos_audit_emit_failures_total > 0` as
  page-worthy because every drop is an audit-trail hole.
- The dev-shim `auth.callctx.shim_used` event still emits through the
  local log-only `emit_event` hook because the middleware that fires
  it runs before any FastAPI dependency-injection can reach the
  configured `MetadataStoreProvider`. CS-IMPL-024 will rewire the
  dev-shim middleware to the real auth + audit path.

## Alternatives considered

- **Outbox-in-catalog-store.** Adding a dedicated outbox table to the
  catalog/definition store and replicating it into the metadata store
  was rejected: it would require new schemas in two providers, a new
  drainer, and operational divergence from the rest of the audit
  pipeline. The audit-trail hole exists only on hard crash / metadata
  outage, both of which are already monitored.
- **Two-phase commit.** psycopg3 supports `XA` only against very
  narrow Postgres deployments; relying on it would constrain the
  reference deployment unnecessarily.

## Implementation

The helpers in `src/services/catalog-service/src/custos_catalog/audit.py`
encapsulate the post-commit contract. Every catalog manager method
that mutates state is wrapped in `_telemetry.observe_operation(...)`;
the audit helper is invoked **after** the wrapping context exits so a
mid-pipeline error does not emit a spurious audit row.
