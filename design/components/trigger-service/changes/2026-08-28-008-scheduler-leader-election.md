# Change: scheduler-leader-election

Date: 2026-08-28
Type: component-design
Component: trigger-service
Sequence: 008
GitHub Issue: #20
Status: closed

## Summary

Resolves TODO-003. The Scheduler Receiver must fire each active schedule
**exactly once** across replicas (REQ-005). The mechanism is a **Postgres
leader-lease row** held through `MetadataStoreProvider`, reinforced by a
per-fire idempotency key. Kubernetes `Lease` and Dapr distributed locks were
considered and rejected.

## Before

The design assumed "a leader lease" (a `TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS`
config knob and a Failure-Modes split-brain row) but never picked the mechanism.
TODO-003 left three candidates open — Dapr distributed lock, Postgres advisory
lock, and Kubernetes lease — so the exactly-once guarantee had no concrete,
implementable contract.

## After

### Decision

Elect a single scheduler leader via a **Postgres leader-lease row**.

| Option | Verdict | Reason |
|---|---|---|
| Postgres leader-lease row | **Chosen** | Postgres is already a hard dependency (Schedule Store in `MetadataStoreProvider`); no new infra; explicit tunable TTL matching the existing config; pooler-safe; observable; testable without a cluster; portable across connected / eval / air-gapped profiles. |
| Kubernetes `Lease` (`coordination.k8s.io`) | Rejected | Hard Kubernetes-API dependency + RBAC not otherwise needed; breaks non-cluster testing; couples storage to the control plane. |
| Dapr distributed lock | Rejected | Lock building block is alpha and needs a lock store (typically Redis), which is not a base-profile dependency (Redis is only an M2 option for the rate-limiter). |

### Lease protocol

One `scheduler_leader` row (`holder_id`, `epoch`, `expires_at`) per deployment:

1. **Acquire / renew** — conditional update
   `UPDATE scheduler_leader SET holder_id = :me, epoch = epoch + 1, expires_at = now() + :lease WHERE expires_at < now() OR holder_id = :me`;
   the replica that affects the row is the leader.
2. **Renew cadence** — leader renews every `TRIGGER_SCHEDULER_LEADER_RENEW_SECONDS`
   (default `10`, ≈ lease ÷ 3).
3. **Failover** — on leader death the lease expires; the next replica acquires
   within `TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS` (default `30`); `epoch`
   increments per handover as a **fencing token**.
4. Only the leader evaluates cron and enqueues `cron.tick`; non-leaders idle the
   scheduler loop but serve all other traffic.

### Exactly-once

Leader-lease bounds firing to one replica; the `epoch` fence plus a deterministic
per-fire dedup key `hash(scheduleId, plannedFireAt)` (written before dispatch)
absorb any brief failover overlap — a stale leader's duplicate collides on the key
and is dropped (`trigger.deduped`). No distributed transaction required.

## Impact

- New config: `TRIGGER_SCHEDULER_LEADER_RENEW_SECONDS` (default `10`).
- Failure-Modes "split-brain" row updated to name the mechanism and the dedup fence.
- Implementation follow-up: the M2 Scheduler Receiver (#988) implements the
  `scheduler_leader` table, the renew loop, and the per-fire dedup key.

## References

- `design/components/trigger-service/design.md` § Scheduler Leader Election
- REQ-005 (scheduled triggers)
- Follow-up implementation: #988 (M2 receivers)
