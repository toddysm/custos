# Change: pull-cursor-model

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 008
GitHub Issue: #59
Status: open

## Summary

Locks the normative contract for the pull-mode cursor model — cursor envelope shape, advancement and commit semantics, single-writer safety, crash recovery, encoding migration, admin rewind, and the cursor audit event taxonomy. Closes the corresponding open item in `connector-service/todos.md`. Builds on the ownership/granularity rules already documented for INCON-011 (closed via #36 on 2026-05-17).

## Before

`design.md` § Cursor Ownership defined *who* owned the cursor (Connector Service, per-instance) and how Trigger Service's dedup concerns stayed orthogonal, but did not specify the cursor's data shape, when it advances relative to fan-out, what guarantees the platform makes on crash, how single-writer safety is enforced, how encoding evolution is handled, or what operator-facing controls exist. The `ConnectorCursor` ER entity carried no field detail beyond its name. Manifests had no declared cursor encoding or initial-position behavior. The todo "Define pull cursor model and dedup key strategy for trigger streams" remained open.

This left activity-side authors of pull-mode connectors without a contract for cursor behavior and prevented `MetadataStoreProvider` from finalizing the `ConnectorCursor` table schema.

## After

§ Cursor Ownership rewritten and expanded as § Pull Cursor Model with sub-sections:

- **Ownership and granularity**: preserved from prior text (one cursor per `ConnectorInstance`, one pull loop fans out to N subscriptions, Trigger Service holds no cursor state).
- **Cursor shape**: structured envelope `{ encoding, value, advancedAt }`. `encoding` is plugin-declared via manifest; `value` is opaque to the platform; `advancedAt` is platform-managed for operator visibility.
- **Initial value**: `value: null` sentinel; plugin chooses starting position per manifest-declared `initialCursorBehavior` (`now` | `beginning` | `custom`).
- **Advancement and commit**: 4-step contract — plugin returns batch → publish events to fan-out (Dapr Pub/Sub) → on publish-ack of every event → commit `nextCursor`. **At-least-once** delivery to Trigger Service; Trigger's `DedupKey` absorbs dups. Cursor advance is decoupled from per-subscription ACK.
- **Event emission requirement** (normative): pull-mode events MUST include stable `eventId = sha256(upstreamEventNaturalKey)`. Trigger Service computes `DedupKey = sha256(eventId + subscriptionId)` from it.
- **Single-writer safety**: DB-row-level lease via `leaseHolder` + `leaseExpiresAt` columns on `ConnectorCursor`. 60-second claim window; `SELECT ... FOR UPDATE` or provider CAS. No Raft/etcd dependency. v1 tick frequency ≥10s makes this comfortable.
- **Crash recovery and cursor expiry**: on restart, resume from last committed cursor. No replay-window cap in v1. Upstream-rejected cursors surface as `CursorExpired` from the plugin → `cursor.expired` audit → ticks halt pending operator action.
- **Encoding migration**: bumping `events.pull.cursorEncoding` triggers `CursorEncodingMismatch` from the plugin, instance state set to `cursorMigrationRequired`, `cursor.encoding_mismatch` audit, ticks halted. Operator resolves by admin rewind. No automatic in-place migration in v1.
- **Admin rewind / replay**: `POST /v1/workspaces/{ws}/connectors/{id}/cursor:rewind` with body `{ to: "now" | "beginning" | { encoding, value } }`. Fires `cursor.advanced` with `reason: admin-rewind`. Trigger Service dedup keys are cleared independently if downstream re-firing is desired.
- **Cursor audit events**: three new event kinds — `cursor.advanced`, `cursor.expired`, `cursor.encoding_mismatch`. Audit payloads truncate `value` to a fixed prefix; never carry secret material.

`ConnectorCursor` data-model entry updated with the five field names (`encoding`, `value`, `advancedAt`, `leaseHolder`, `leaseExpiresAt`).

Plugin manifest gains a new optional block `events.pull`:

```json
"events": {
  "delivery": ["pull"],
  "pull": {
    "cursorEncoding": "oci-list-tags-v1",
    "initialCursorBehavior": "now"
  },
  "produced": [ "..." ]
}
```

REST API gains two admin endpoints: `GET .../cursor` (read envelope) and `POST .../cursor:rewind` (operator rewind/replay).

`todos.md`: closes the pull-cursor-model item with a note that the dedup-key half re-points to Trigger Service plus the `eventId` emission rule added here.

## Key Decisions Locked This Session

1. **Structured envelope, opaque value.** `{encoding, value, advancedAt}` — platform never parses `value`. Lets every connector type pick whatever its upstream needs (timestamp, change-token string, opaque blob) without platform-side schema changes.
2. **At-least-once delivery, not effectively-once.** Commit cursor after publish-ack to fan-out channel, *not* after Trigger Service ACK. Trigger's `DedupKey` already absorbs dups; effectively-once would force per-subscription tracking back into Connector Service and break the per-instance ownership model.
3. **Stable `eventId` is a normative emission requirement.** Trigger's dedup can only be as strong as the upstream natural key Connector exposes. Plugins that can't produce one must document their fallback. This is the contract that lets at-least-once + Trigger dedup actually work.
4. **DB-row lease for single-writer, not external leader election.** Postgres `SELECT ... FOR UPDATE` (or CAS) plus 60s lease window. Sufficient for v1's ≥10s tick frequency. Avoids Raft/etcd dependency in M1.
5. **`null` is the sentinel for "uninitialized cursor".** Platform writes the envelope on first tick; plugin sees `value: null` and applies its manifest-declared `initialCursorBehavior`.
6. **`initialCursorBehavior` is plugin-declared, operator-overridable.** Three values — `now`, `beginning`, `custom` — picked by the connector type author at manifest time. Operators override via admin rewind without recompiling the plugin.
7. **No automatic encoding migration.** Bumping `cursorEncoding` halts the instance; operator rewinds explicitly. Keeps blast radius observable and avoids subtle bugs from machine-translated cursors.
8. **No replay-window cap in v1.** If the upstream allows old positions, replay works; if not, `cursor.expired` surfaces it. Adding bounded replay windows would require per-connector-type semantics the platform doesn't have yet.
9. **Admin rewind and Trigger dedup-clear are independent operations.** Cursor rewind alone yields a *quiet* replay (events emitted, dups absorbed). Clearing Trigger dedup keys yields a *re-firing* replay. Keeping them split lets operators pick.

## Impact

- `MetadataStoreProvider` can now finalize the `ConnectorCursor` table schema (5 columns).
- Plugin authors targeting pull mode have a complete contract: cursor shape, when it advances, what they own (`value`) vs what the platform owns (`encoding`/`advancedAt`/lease fields), what errors they return, and what `eventId` they must emit.
- Trigger Service can lock its pull-receiver implementation against the at-least-once + `DedupKey` model without further coordination.
- Operator runbook gains a concrete recipe for "reprocess events from yesterday" via admin rewind + (optional) Trigger dedup-clear.
- Connector Service `todos.md` reduced from 5 open items to 4. Component approaches `Designed` once the remaining 3 design questions (capability namespace governance, fallback tag naming, lease expiry operator UX) land or are explicitly deferred. (The connector test harness item is implementation-track and does not block status flip.)

## Out of Scope (Deferred)

- Bounded replay windows per connector type (e.g. "this connector only supports rewind up to 7 days") — would require per-connector-type replay-budget semantics in the manifest.
- Automatic in-place cursor encoding migration via plugin-provided migrators — current contract halts and requires operator rewind.
- Cursor-level metrics dashboard (lag, staleness, rewind frequency) — Observability work, covered by the Observability component design.
- Multi-region active-active cursor coordination — v1 assumes a single Connector Service deployment per workspace; multi-region is M3+.
- Replay-with-time-window (rewind to a specific upstream timestamp) when the upstream cursor is opaque — operators rewind to `beginning` or a known-good explicit envelope value.

## Related Requirements

- REQ-079 (source-agnostic receivers) — § Ownership and granularity preserves single-loop-per-instance fan-out regardless of upstream type.
- REQ-080 / REQ-081 (internal events + dual-purpose delivery) — unchanged; cursor model only affects upstream-facing pull receivers.
- INCON-011 (cursor ownership) — fully resolved (ownership half was closed via #36 on 2026-05-17; this change closes the shape/semantics half).
- ADR-008 (exit codes / failure classes) — `CursorExpired` and `CursorEncodingMismatch` are plugin-side permanent errors that halt the instance pending operator action.
