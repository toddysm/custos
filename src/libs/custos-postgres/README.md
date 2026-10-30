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
