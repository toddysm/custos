# Change: bundle-j-audit-outbox-cursors

Date: 2026-05-18
Type: component-design
Component: storage-provider-layer
Sequence: 004
GitHub Issue: #104
Status: open

## Summary

Made the audit outbox drain state model normatively cursor-only. Per-pipeline `AuditOutboxCursor` rows are the sole representation of drain progress; the outbox row itself carries no `deliveredAt` and no per-row delivery flag. This eliminates a long-standing contradiction in the SPL design between the sequence diagram (which used `WHERE delivered_at IS NULL` and a "mark delivered" step) and the cursor-based interface (`streamAuditOutbox`, `commitAuditOutboxCursor`) that the same design defined.

## Before

- The audit write-path sequence diagram showed the Observability Service running `SELECT * FROM audit_outbox WHERE delivered_at IS NULL` and then issuing a "mark delivered" UPDATE per row after a successful pipeline write.
- This contradicted the cursor-based drain protocol (`streamAuditOutbox(cursor, batchSize)` / `commitAuditOutboxCursor(pipelineId, cursor)`) defined elsewhere in the same design, and made it ambiguous which model was authoritative.
- The per-row `deliveredAt` model cannot represent multiple consumer pipelines (audit-store, audit-alert, ...) independently advancing through the same outbox, which the design implicitly required.

## After

- Audit write-path sequence rewritten to use SPL's cursor protocol exclusively:
  ```
  Obs->>Meta: streamAuditOutbox(cursor=<pipeline-cursor>, batchSize)
  Meta->>Pg: SELECT * FROM audit_outbox WHERE id > cursor ORDER BY id LIMIT batchSize
  Pg-->>Meta: batch
  Meta-->>Obs: batch
  Note over Obs: ship to pipeline (audit-store / audit-alert / ...)
  Obs->>Meta: commitAuditOutboxCursor(pipelineId, cursor=max(id))
  ```
- New normative paragraph: drain state is cursor-only. `AuditOutboxRow` carries no `deliveredAt` and no per-row delivery flag. Each registered drain pipeline maintains its own high-water-mark in `AuditOutboxCursor`.
- GC rule: a retention worker (owned by Observability Service) deletes outbox rows once `id < min(cursor across all registered pipelines)` AND row age > `CUSTOS_AUDIT_OUTBOX_RETENTION_MARGIN` (default 24h). A stuck pipeline keeps outbox rows around indefinitely; operators observe this via `obs.outbox.lagging` and act on the slow pipeline rather than letting the table grow silently.

## Impact

- SPL `AuditOutboxRow` schema: no `deliveredAt` column — confirms what the existing `streamAuditOutbox` / `commitAuditOutboxCursor` interface already implied.
- Observability Service retention worker logic changes from "delete WHERE delivered_at < now() - margin" to "delete WHERE id < min(cursor) AND created_at < now() - margin".
- Stuck-pipeline behavior is now visible by design — `obs.outbox.lagging` metric becomes the canonical signal.
- Adding a new audit consumer pipeline is now a matter of registering a new `AuditOutboxCursor` row; no schema or write-path change required.

## Files changed

- `design/components/storage-provider-layer/design.md` v1 → v2 (Audit Write Path sequence; new "Drain state is cursor-only" paragraph; Change History)

## Related Change Records

- Observability Service: `2026-05-18-001-bundle-j-audit-outbox-cursors.md` (companion — drops `deliveredAt` from the `AuditOutboxRow` ER block and rewrites the GC sentence to match this rule).
