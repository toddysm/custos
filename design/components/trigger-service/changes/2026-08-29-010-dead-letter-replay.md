# Change: dead-letter-replay

Date: 2026-08-29
Type: component-design
Component: trigger-service
Sequence: 010
GitHub Issue: #22
Status: closed

## Summary

Resolves TODO-005. Defines the **dead-letter destination, retention, and replay
UX** for dispatches to the Workflow Service that exhaust `TRIGGER_DISPATCH_MAX_RETRIES`.
Retry-exhausted events are persisted as `DeadLetterEntry` rows via
`MetadataStoreProvider` (Postgres), retained on a TTL, and driven through
workspace-scoped `trigger:admin` replay/discard/list routes. Replay reuses the
original `idempotencyKey`, so exactly-once is preserved.

## Before

The Failure Modes table said an exhausted dispatch produced a `trigger.dispatch.failed`
audit event and "dead-letter" — but the **destination**, **retention**, and
**replay UX** were undefined (TODO-005). There was no entity, no operator API, and
no statement of how replay interacts with the dedup store and idempotency.

## After

### Destination

`DeadLetterEntry` rows in `MetadataStoreProvider` (Postgres), workspace-scoped —
the same durable store as `Subscription` / `Schedule` / `DedupKey` /
`ResumeSubscription`. No separate broker DLQ; rows are directly queryable for the
replay UX. Each entry stores the full `NormalizedEvent`, the resolved match
(`subscriptionId`, `matchKind`, target ref), the reused `idempotencyKey`, attempt
count, last error, and status.

### Lifecycle

`pending` → (`replaying`) → `replayed` | `discarded` | `expired`, each with a
`trigger.deadletter.*` audit event (`stored` / `replayed` / `replay_failed` /
`discarded` / `expired`).

### Retention

A sweeper GCs entries older than `TRIGGER_DEADLETTER_RETENTION_SECONDS`
(default `2592000`, 30 days) regardless of status; terminal entries are kept until
TTL for audit.

### Replay UX

Workspace-scoped admin routes gated by `trigger:admin`:

- `GET …/triggers/deadletter` — list/filter, paginated.
- `GET …/triggers/deadletter/{id}` — inspect stored event + attempt/error history.
- `POST …/triggers/deadletter/{id}:replay` — re-inject into the Dispatcher.
- `POST …/triggers/deadletter/{id}:discard` — mark `discarded`.
- `POST …/triggers/deadletter:replay` — bulk replay by selector.

### Replay semantics

Replay reuses the **original `idempotencyKey`** (`StartRun` / `RaiseExternalEvent`
are idempotent on it), so a replay cannot double-start a run. On success →
`replayed` + commit the dedup key (deliberately not committed at failure time); on
failure → increment attempts + `trigger.deadletter.replay_failed`, staying
`pending` until `TRIGGER_DEADLETTER_MAX_REPLAY_ATTEMPTS` (default `3`) pins it to
`discarded`.

## Impact

- Added § Dead-Letter Handling & Replay; the `DeadLetterEntry` ER entity;
  `trigger:admin` deadletter routes; two config vars
  (`TRIGGER_DEADLETTER_RETENTION_SECONDS`, `TRIGGER_DEADLETTER_MAX_REPLAY_ATTEMPTS`);
  `trigger.deadletter.*` audit events; updated the Dispatcher row, the
  MetadataStoreProvider and Observability/Audit dependency rows, and the
  dispatch Failure-Modes row.
- Implementation follow-up: the store, sweeper, and admin routes land with the M2
  dispatcher/receiver work (#988).

## References

- `design/components/trigger-service/design.md` § Dead-Letter Handling & Replay, § Failure Modes
- Follow-up implementation: #988 (M2 receivers / dispatcher)
