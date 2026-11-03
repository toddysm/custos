# Trigger Service M2 implementation plan

Status: **Runtime prerequisite and issuer extension authorized; implementation in progress.** Revision: 6. Updated: 2026-11-02.

Parent: [#988](https://github.com/toddysm/custos/issues/988).
Design: [#1004](https://github.com/toddysm/custos/issues/1004) and the
[combined requirements/design](../../requirements/trigger-m2-receivers.md).
The [M1 plan](implementation-plan.md) remains the historical record.

## Scope and gates

Deliver the scheduler, generic webhook, vendor-push and pull receiver paths,
plus durable failure capture and operator replay. Include their real provider,
verification, authorization and downstream idempotency prerequisites.
No new graphical interface or duplicate connector polling engine is included.

All issues below are native children of #988. Every implementation issue is
blocked by [#1004](https://github.com/toddysm/custos/issues/1004)'s completed
design and human approval. The owner approved the baseline, subsequent recorded
amendments and continuous implementation; publication alone never grants
authorization. Testing mode is **GitHub-hosted CI**, with Copilot review
on each pull request. One implementation item maps to one reviewed pull request.
Only the designated owner may claim/edit an item's worktree and ledger.

## Work breakdown

Each issue contains checkable acceptance criteria, validation and dependencies.
The dependency column below excludes the common
design gate [#1004](https://github.com/toddysm/custos/issues/1004).

| Task | Issue | Deliverable and exit evidence | Depends on |
|---|---|---|---|
| TS-IMPL-022 | [#1005](https://github.com/toddysm/custos/issues/1005) | Production async subscription/resume/schedule queries, including source-scoped external waits; PostgreSQL/memory parity and tenant isolation | - |
| TS-IMPL-023 | [#1006](https://github.com/toddysm/custos/issues/1006) | Persisted source configuration and Workflow resume association/registration; invalid configuration rejected, legacy bodies preserved | 022 |
| TS-IMPL-024 | [#1007](https://github.com/toddysm/custos/issues/1007) | Atomic scheduler lease and fencing; expired/stale holders cannot advance a fire | - |
| TS-IMPL-025 | [#1008](https://github.com/toddysm/custos/issues/1008) | Cron/interval receiver; deterministic identity, restart/failover and time-transition tests | 022, 023, 024 |
| TS-IMPL-026 | [#1009](https://github.com/toddysm/custos/issues/1009) | Connector-owned verification; raw-byte checks, rotation, isolation and indexed sanitized per-event rejection metadata | - |
| TS-IMPL-027 | [#1011](https://github.com/toddysm/custos/issues/1011) | Gateway-to-Trigger webhook ingestion; scoped routing, credential separation and bounded durable acceptance | 022, 023, 026, 028 |
| TS-IMPL-028 | [#1010](https://github.com/toddysm/custos/issues/1010) | Shared connector-envelope adapter and vendor-push delivery; source-scoped resume correlation without payload run/step IDs | 022, 023, 026 |
| TS-IMPL-029 | [#1012](https://github.com/toddysm/custos/issues/1012) | Existing polling/cursor reuse plus durable operator-pause and shared due-time control; resume-only demand and publish-failure recovery | 023, 028 |
| TS-IMPL-030 | [#1013](https://github.com/toddysm/custos/issues/1013) | Durable dead-letter storage and fenced expiring replay claims; pagination and sanitized persistence | - |
| TS-IMPL-031 | [#1014](https://github.com/toddysm/custos/issues/1014) | Durable failure/replay and downstream idempotency; durable Workflow waits/mirrors and versioned step-scoped consumer protection; no duplicate effect across restart and retention | 022, 030 |
| TS-IMPL-032 | [#1015](https://github.com/toddysm/custos/issues/1015) | Production signed call-context verification and trigger:admin; secure Dapr 1.18.9 baseline, authenticated caller app/namespace, Gateway permission propagation and opaque child-resume transport compatibility | - |
| TS-IMPL-033 | [#1016](https://github.com/toddysm/custos/issues/1016) | Protected recovery endpoints and retention; route-collision/concurrent-replay/expiry tests | 031, 032 |
| TS-IMPL-037 | [#1035](https://github.com/toddysm/custos/issues/1035) | Kubernetes-backed connector hooks, bounded confidential I/O, immutable invocation ownership and verified termination; no privileged Docker backend | 029, 032 |
| TS-IMPL-038 | [#1037](https://github.com/toddysm/custos/issues/1037) | Explicit workspace service-account execution identity, fresh authorized signed contexts and real Workflow/Connector request/response alignment across retries/restarts/child execution | 031, 032, 037 |
| TS-IMPL-039 | [#1038](https://github.com/toddysm/custos/issues/1038) | Production activity startup/readiness and resource ownership; private bootstrap delivery; real Connector-owned credential issuance with durable per-attempt authorization | 032, 038 |
| TS-IMPL-034 | [#1017](https://github.com/toddysm/custos/issues/1017) | Activation/readiness/deployment wiring, durable audit and operational signals; consume durable wait, hook execution, execution identity and activity runtime prerequisites; four-profile rendering and safe rollback | 025, 027, 028, 029, 033, 037, 038, 039 |
| TS-IMPL-035 | [#1018](https://github.com/toddysm/custos/issues/1018) | Deployed receiver-to-Workflow failure/recovery and legacy regression coverage in hosted CI | 034 |
| TS-IMPL-036 | [#1019](https://github.com/toddysm/custos/issues/1019) | Developer/operator setup, recovery and migration documentation; checked links/examples | 035 |

## Execution order and overlap

The owner approved moving durable wait execution and consumer-side replay
protection from 034 into 031 on 2026-10-29. Public request formats remain valid;
the protocol is persisted per run, and unsafe legacy replay fails explicitly.

The owner also approved the runtime-security adjustment on 2026-10-29:
032 raises the Dapr minimum to 1.18.6 (pinning stable 1.18.9), updates all
installation/deployment references, and makes child resume transport compatible
with secure method-path validation. This prerequisite does not remove 034's
activation/control-plane responsibilities or 035's deployed certification gate.

One valid sequential order is:

`022 -> 023 -> 024 -> 030 -> 031 -> 032 -> 025 -> 026 -> 028 -> 027 -> 029 -> 033 -> 037 -> 038 -> 039 -> 034 -> 035 -> 036`.

The owner approved 037 on 2026-10-30 after deployment inspection found that the
Docker-only hook runner had no execution backend in the supplied Kubernetes
profile. That prerequisite merged through
[#1036](https://github.com/toddysm/custos/pull/1036) on 2026-11-02.

The owner approved 038 and its identity model on 2026-11-02 after a real
Workflow/Connector probe exposed missing authorization, the wrong method path,
missing execution coordinates and incompatible slot/response shapes. Use an
explicit workspace-scoped service account and fresh short-lived signed contexts,
not the creator's retained credential or blanket platform-service authority.
The four local 034 commits remain parked intact while 038 is implemented
separately from the latest default branch. The plan now has **18 required
delivery items**, of which **13 are merged**; this does not add another poller
or remove the deployed certification/documentation gates.

The owner authorized creating and starting 039 on 2026-11-02 after actual probes
showed readyz 200 with no scheduler (authorized scheduling 503), plus rejection
of ARM-issued bootstrap tokens by the actual sidecar verifier. Start independent
readiness work from main; then complete production resource assembly and private
bootstrap alignment. 038 and the four local 034 commits stay preserved in their
own worktrees. Both 038 and 039 precede activation; no unmerged dependency stack
is implied. The separate revision-5 amendment pull request stays open for review.

The revision-5 amendment merged through #1039 after owner approval. The owner
subsequently approved extending 039 with real Connector Service-owned credential
issuance, reusing identity resolvers and per-attempt authorization, on
2026-11-02T20:56:34.079-08:00. Final 039 integration now depends on merged 038;
its already-implemented readiness/bootstrap work remains preserved. Synthetic
upstream credentials are not an activation fallback.

This order is not a new dependency chain. For example, lease storage (024),
verification (026), dead-letter storage (030) and authorization (032) depend only
on the design gate; their shared-file edits still require coordination.
Do not stack a dependent implementation on an unmerged predecessor without
separate authorization. Proposed branches follow
`feature/trigger-m2-receivers/<task>-<instance>`; live ownership is in the ledger.

## Approved decision baseline

| Decision | Recommendation | Owner/implementation |
|---|---|---|
| D01 | Workspace-scoped webhook URL; no global instance-ID lookup; legacy unscoped ingress fails closed | [#1004](https://github.com/toddysm/custos/issues/1004) / [#1009](https://github.com/toddysm/custos/issues/1009), [#1011](https://github.com/toddysm/custos/issues/1011) |
| D02 | External bearer travels as private verification data, never platform identity; strip forged context and secret-bearing evidence | [#1004](https://github.com/toddysm/custos/issues/1004) / [#1009](https://github.com/toddysm/custos/issues/1009), [#1011](https://github.com/toddysm/custos/issues/1011) |
| D03 | Connector owns the only poll loop/cursor; publish succeeds before cursor commit; Trigger reconciles subscription intent | [#1004](https://github.com/toddysm/custos/issues/1004) / [#1010](https://github.com/toddysm/custos/issues/1010), [#1012](https://github.com/toddysm/custos/issues/1012) |
| D04 | Explicit cron/timezone/missed-tick policy; stable lease epoch during renewal; fenced durable pending fires | [#1004](https://github.com/toddysm/custos/issues/1004) / [#1007](https://github.com/toddysm/custos/issues/1007), [#1008](https://github.com/toddysm/custos/issues/1008) |
| D05 | Durable fenced replay and downstream dedup over the full recovery horizon; no reliance on a volatile/24-hour key alone | [#1004](https://github.com/toddysm/custos/issues/1004) / [#1013](https://github.com/toddysm/custos/issues/1013), [#1014](https://github.com/toddysm/custos/issues/1014), [#1016](https://github.com/toddysm/custos/issues/1016) |
| D06 | Explicit workspace service-account identity, fresh authorized contexts and the actual Workflow/Connector execution contract; no credential persistence or replay retargeting | Owner approval 2026-11-02 / [#1037](https://github.com/toddysm/custos/issues/1037) |

The design document owns the exact contracts, alternatives and migration impact.
All six decisions were explicitly approved; approval and amendment history
are recorded in #988 rather than inferred from issue creation.

Revision 2 incorporates the initial Codex/Copilot findings: operator pauses need
durable intent, cursor leases alone do not enforce the polling interval across
replicas, external resume waits need source association/correlation and polling
demand, and verification must return sanitized per-event rejection metadata.
These refinements belong to the existing linked items, not untracked extra work.

## Validation and completion

Code items add tests alongside their code. Reuse Ruff, strict Mypy, the existing
Python 3.11/3.12 matrix, real PostgreSQL integration and at least 90% Trigger
coverage. TS-IMPL-035 adds deployed cross-service evidence; it does not postpone
unit testing until the end. Use controlled clocks for lease, timezone, replay
and retention boundaries.

Required cases include both start and resume; duplicate events; two replicas;
lost responses after successful remote calls; replay after 24 hours and a
Workflow restart; wrong workspace; invalid signature; cursor publication
failure; route precedence; pause/deletion; and all existing manual/internal
flows. Do not call a test-only raw SQL adapter production coverage.

Also require pause-before-registration and restart-while-paused tests, staggered
two-Connector-replica interval tests, external resume-only polling without
source-supplied run/step IDs, and exact mixed-batch acceptance/rejection counts.
For 038, require real-client/real-handler binding compatibility, signed
workspace/permission/caller/audience checks, credential expiry and refresh after
restart, child execution coordinates, rotation/revocation bounds and preservation
of accepted execution identity across configuration changes and replay.
For 039, require truthful readiness/admission, startup-failure cleanup and drain
ordering, actual minter/verifier compatibility, private key/token delivery and
scope/expiry/restart regressions. Do not replace these with fake scheduler health
or a token verifier disabled for tests.

The documentation issue uses applicable documentation/link checks, not unit
coverage. The plan-only pull request may not trigger runtime workflows because
of path filters; report the real check state.

Before closing #988, every required child must be complete, documentation
merged, and a separately reviewed close-out must promote the capability
inventory. Never close the parent merely because this plan is merged.
