# custos-postgres

Postgres adapters for the Custos Storage Provider Layer. v1 implements
`DefinitionStoreProvider` and `CatalogStoreProvider` against Postgres 14+.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `CUSTOS_PG_DSN` | Yes | libpq-style DSN, e.g. `postgresql://user:pw@host:5432/custos` |

Adapter factories read `CUSTOS_PG_DSN` once at startup. The platform
never auto-migrates; run `custos-migrate up` to apply pending revisions.

### Optional durable Connector polling

Metadata revision **10** adds workspace/instance-scoped pull controls without
raising the shared revision-4 minimum. The
[pull-control capability](../storage-provider-layer/src/custos_spl/interfaces/pull_control.py)
atomically couples operator pause with audit/outbox writes, and polling due time
with a fresh cursor ownership token. Commit and failure paths retain shared
interval/backoff reservations. Demand comes from current stored starts/waits and
explicit Connector registrations; reconciliation never clears operator pause.

Revision **11**, required by durable polling, adds the pre-invocation fence and
executor identity. Claims persist it atomically before launching a hook; lease
expiry alone cannot clear it. Confirmed completion or audited, identity-checked
operator recovery clears it. Existing cursor holders, including expired ones,
receive conservative legacy fences with unknown executor/container locators.
Cursor holders lacking controls receive paused, fenced controls even when the
instance store is separate or they were created after revision 10.
Ordinary pause/resume and reconciliation preserve these fences.

Known legacy instances are migrated paused; absent policy also fails closed.
Drain old pollers, apply migrations, explicitly confirm operator intent, then
enable durable polling on all replicas. See the
[Connector operating contract](../../services/connector-service/README.md#durable-pull-controls-and-demand).

### Audited dead-letter lifecycle

Dead-letter lifecycle mutations also append their safe audit event and outbox
record in the same transaction. Capture, replay completion/failure, interrupted
claim recovery, operator discard and expiration cannot commit without their
required audit. Expiration locks the entry, rechecks retention/live ownership,
then audits and removes the payload without deleting downstream receipts.
Metadata revision **12** adds bulk-command selections/progress and per-entry
command association. Replay completion, its audit and command progress commit
together; retries cannot reselect another page. Expired selection payloads are
erased without live related claims, retaining identity tombstones.
Cleanup rechecks live claims in a fresh statement after locking the command;
it cannot rely on its candidate-query snapshot.

Revision **13** is the current dead-letter capability prerequisite. It retains
single-command request/generation hashes independently of entry payload deletion.
It also persists complete source target selections before any dispatch, so
redelivery cannot lose removed waits or select newly registered targets. Source
payloads are erased only after expiry and completion of every selected producer
receipt; unresolved selections remain recovery evidence. These additions leave
the general metadata minimum at 4 and the outbox-consumer prerequisite at 12.
Mode-specific and shared dual-mode resume selections bind atomically within
one event budget. A workspace-scoped keyset query discovers unfinished
selections even before producer receipts exist. Receipt cleanup and source
acceptance share a workspace lock, preserving unfinished accepted requests.
Pending-selection pages fetch only keys and UTF-8 byte lengths first, then load
at most 20 MiB of payloads. Selection count alone is not a memory bound.

Revision **14** adds the optional execution-identity capability. Execution-aware
producers and registrations require `check_execution_identity_ready()` before
acceptance. Retained references contain only workspace, principal and binding
IDs, use delivery protocol 2, and cannot be changed or removed by an old writer,
even after a wait expires. Legacy waits remain legacy; they are not silently
assigned the current account. The shared revision-4 baseline is unchanged.

Gateway idempotency operations classify connection, pool and driver failures as
`BackendUnavailable`; recovery routes return the documented 503 without forwarding.
Reservation and follow-up read share a transaction, preventing expiry cleanup
from removing the binding between those statements.

Revision 12 also makes the outbox's default ID allocator acquire a shared
transaction advisory fence before allocating a sequence value. Readers acquire
the corresponding exclusive fence and use a fresh committed snapshot, waiting
for earlier allocators so commit-order inversions cannot lose events.
The internal sequence remains `CACHE 1`; callers must not allocate explicit
outbox IDs or reconfigure its allocator.
This needs no table mutation privilege for readers. Reader lock acquisition is
bounded to five seconds and fails explicitly; retry without advancing the cursor.
Outbox consumers must preflight `check_audit_outbox_ready()` and require revision
12 independently of any Trigger enablement flag; ordinary metadata consumers
retain the general revision-4 minimum.

## Entry points

### Optional durable receiver scan progress

Metadata revision **16** adds the optional
[receiver-progress capability](../storage-provider-layer/src/custos_spl/interfaces/receiver_progress.py).
The shared metadata minimum remains **4**. Activate consumers only after
`check_receiver_workspaces_ready()` and `check_receiver_scan_progress_ready()`
succeed; the provider never migrates at startup or silently falls back to memory.

Discovery queries only bounded metadata ID projections, including orphaned
recovery rows, not Auth's workspace directory. A limit of 50 returns at most 50
IDs and fetches at most 51 including keyset lookahead. Discovery
uses explicit C-collation workspace indexes added by revision16 on every
projected table, so database-default collation cannot force an incompatible
ordering. Native plan tests exercise the production query over 100,000 retained
subscription rows without disabling the normal planner. Discovery registration
and its checkpoint commit together. Its last-seen updates never change an
operation's progress revision or fair service ticket.

Each `(deployment, workspace, operation)` row stores a version-1 continuation
of at most 64 KiB. Claims use database time, unique incarnation/token and
generation/revision checks. An indexed service ticket moves to the tail at claim,
including on failure or crash. Scheduled claims additionally require matching
live scheduler authority checked transactionally; a standby cannot postpone work.
Progress authority never replaces effect-time scheduler fencing or dispatch
authorization. No transaction spans external execution.

Checkpoint only after a bounded page is acknowledged. On ambiguous commit
response, reacquire/reread instead of replaying a stale write. Failure release
retains the stored continuation with a five-second retry floor. A typed scheduled
snapshot retains its due cutoff/cursor without retaining its scheduler lease.
Known absent source payloads advance to an after-selection position with an
explicit diagnostic; malformed or cross-workspace cursors still fail closed.

Pruning requires a completed later discovery sweep, no live claim, null finished
continuation and at least 30 idle days; unfinished rows are not age-evicted.
Finished rows proven absent by a completed discovery sweep are excluded from
new operation claims, so empty polling cannot keep refreshing their inactivity
clock. Rediscovery makes them eligible again without changing retained progress.
Disable/drain old receiver workers before schema-first rollout. Keep the additive
tables on rollback and keep old background admission disabled; do not erase
progress or accepted effect identities to force a rollout.
This prerequisite supplies storage/collaborators; lifespan activation remains
[#1017](https://github.com/toddysm/custos/issues/1017), deployed certification
[#1018](https://github.com/toddysm/custos/issues/1018), and final operator docs
[#1019](https://github.com/toddysm/custos/issues/1019).

Metadata revision **15** adds optional durable activity-attempt ownership.
It follows merged revision 14's execution-identity guards.
Claims bind workspace, opaque run/step coordinates, attempt, request fingerprint
and owner generation. Pending ownership never expires into permission to rerun;
it also fences later attempts for the same step. Completed snapshots retain the
exact result instead of manufacturing an empty success. Activity snapshots are
bounded to 16 MiB at the adapter boundary. The shared revision-4 baseline remains
unchanged. Runtime startup must explicitly require this capability before use.
Credential authority can be irreversibly revoked without marking an unknown
sandbox effect completed. Snapshot transitions preserve identity/scope and may
only shorten the accepted deadline. Fixed-size step indexes support opaque IDs
without oversized index entries. Terminal results and pending fences are retained
for safe replay/roll-forward; there is no timeout-based ownership takeover.

`PgLeaseAdapter.put_lease_bounded` serializes count-and-insert admission using a
transaction-scoped advisory lock. The production issuer requires this capability;
the legacy count-then-put sequence is not treated as a cross-replica guarantee.
Lease revision **2** replaces the raw-coordinate index with bounded hash keys;
queries retain exact coordinate comparisons. Apply it before enabling issuance.
Scoped refresh shares the insertion lock and rechecks expiry inside that
transaction. An expired lease cannot be revived after a replacement consumes
its former capacity; callers must request a new lease.

This package registers two adapters under the `custos_spl.adapters`
entry-point group:

- `postgres-definition` — `DefinitionStoreProvider`
- `postgres-catalog` — `CatalogStoreProvider`

See `design/components/storage-provider-layer/design.md` for the
contract surface.

## Tests

```bash
pytest                       # unit tests only
pytest -m integration        # requires docker (testcontainers) or CUSTOS_PG_DSN
```
