# Change: bundle-j-audit-outbox-cursors

Date: 2026-05-18
Type: component-design
Component: observability-audit-service
Sequence: 001
GitHub Issue: #104
Status: open

## Summary

Aligned the Observability Service audit outbox documentation with the cursor-only drain model now normatively defined in the SPL design. Removed the `deliveredAt` field from the `AuditOutboxRow` ER block and rewrote the outbox garbage-collection rule to reflect that drain progress is per-pipeline, not per-row.

## Before

- `AuditOutboxRow` ER block included a `timestamp deliveredAt` field, implying per-row delivery tracking.
- The GC sentence read: "`AuditOutboxRow` rows are deleted by the drainer after a successful write to `AuditEvent` plus a retention margin (24h)" — which assumed a single drainer/single consumer and could not represent multiple registered pipelines independently advancing.

## After

- `AuditOutboxRow` ER block no longer contains `deliveredAt`.
- New normative paragraph: `AuditOutboxRow` carries no `deliveredAt` and no per-row delivery flag — drain progress is per-pipeline, not per-row. Each registered pipeline (`audit-store`, `audit-alert`, ...) maintains its own high-water-mark in `AuditOutboxCursor`.
- Retention worker rule: outbox rows are deleted once `id < min(cursor across all registered pipelines)` AND row age > `CUSTOS_AUDIT_OUTBOX_RETENTION_MARGIN` (default 24h). A stuck pipeline keeps outbox rows around indefinitely; operators observe this via `obs.outbox.lagging` and remediate the slow pipeline rather than letting the table grow silently.

## Impact

- Retention worker implementation must use `min(cursor)` across all registered `AuditOutboxCursor` rows + a row-age check rather than a per-row `deliveredAt`.
- New `obs.outbox.lagging` metric becomes the canonical stuck-pipeline signal.
- Adding a new audit consumer pipeline is a configuration concern (register a new `AuditOutboxCursor`), not a schema change.

## Files changed

- `design/components/observability-audit-service/design.md` v1 → v2 (`AuditOutboxRow` ER block; outbox GC paragraph; Change History)

## Related Change Records

- Storage Provider Layer: `2026-05-18-004-bundle-j-audit-outbox-cursors.md` (companion — rewrites the audit-write sequence diagram and defines the cursor-only drain model normatively).
