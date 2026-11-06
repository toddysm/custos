# Trigger Service M2 implementation plan

Status: **Revision-8/D09 proposed for #1048; awaiting design/breakdown approval. Revision-7/D08 remains approved.** Revision: 8 proposed. Updated: 2026-11-05.

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
authorization. Testing mode is **GitHub-hosted continuous integration (CI)**, with Copilot-only review
on each pull request. One implementation item maps to one reviewed pull request.
Only the designated owner may claim/edit an item's worktree and ledger.
On 2026-11-04T16:20:02.742-08:00 the owner approved creating one prerequisite
issue and one design-amendment pull request to address #1044's full-window
starvation. That initial authorization covered planning only. The owner approved
the concrete D08/19-item design on 2026-11-04T16:41:03.590-08:00 and authorized
TS-IMPL-040 Build on 2026-11-04T17:03:00.903-08:00. Design #1046 merged as
`8eb0414`; existing D01-D07 approvals and completed items are not invalidated.

The owner approved adding the configuration-capacity prerequisite
[#1048](https://github.com/toddysm/custos/issues/1048) on
2026-11-05T16:15:23.655-08:00 and opening one scoped design-amendment pull
request on 2026-11-05T17:27:24.414-08:00. That authorizes planning only.
Proposed D09 adds TS-IMPL-041 before deployed certification, making **20 proposed
delivery items, 17 already merged**. The previously approved 19-item/D01-D08
baseline remains approved; D09/the added scope requires renewed approval.
No implementation or merge of this amendment is authorized.

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
| TS-IMPL-040 | [#1045](https://github.com/toddysm/custos/issues/1045) | Approved D08 storage/progress capability and extraction of #1044's bounded workspace discovery, migration, fair fenced claims with scheduled-leader admission, durable continuations and typed scheduled-scan restore; native saturation/restart/isolation evidence | 1046 merged, 022, 024, 031, 033 |
| TS-IMPL-034 | [#1017](https://github.com/toddysm/custos/issues/1017) | Activation/readiness/deployment wiring, durable audit and operational signals; consume all prerequisites, independent bounded discovery and fair progress workers; four-profile rendering and safe rollback | 025, 027, 028, 029, 033, 037, 038, 039, 040 |
| TS-IMPL-041 (proposed) | [#1048](https://github.com/toddysm/custos/issues/1048) | D09 bounded digest-verified file registry; unchanged legacy 64 KiB path; external versioned registry/Dapr/RBAC bundle and compact Helm descriptor; verified retention bridge for first inline-to-file migration; every consumer/profile; actual Helm storage and restart-safe rollback tests | Revision-8/D09 approval/merge, completed 038 and 034 |
| TS-IMPL-035 | [#1018](https://github.com/toddysm/custos/issues/1018) | Deployed receiver-to-Workflow failure/recovery and legacy regression coverage in hosted CI; full-cardinality managed effects after the configuration prerequisite | 034, 041 |
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

`022 -> 023 -> 024 -> 030 -> 031 -> 032 -> 025 -> 026 -> 028 -> 027 -> 029 -> 033 -> 037 -> 038 -> 039 -> [D08 approval] -> 040 -> 034 -> [D09 approval] -> 041 -> 035 -> 036`.

Current scoped checkpoint (2026-11-05): 038/039/040/034 are merged through
[#1040](https://github.com/toddysm/custos/pull/1040),
[#1041](https://github.com/toddysm/custos/pull/1041),
[#1047](https://github.com/toddysm/custos/pull/1047) and
[#1044](https://github.com/toddysm/custos/pull/1044). Seventeen approved delivery
items are complete. #1018 is blocked by native prerequisite #1048; the 1,000-
workspace production-loader/managed-chart preflight exceeds the 64 KiB registry
ceiling. 041 is one proposed implementation item covering the shared library,
API Gateway, Trigger, Workflow and Activity Runtime Manager consumers and
managed chart wiring. The plan PR changes only design/plan/inventory, remains
open for approval and must not close #1048 or start Build.

The receiver-disabled
[hosted baseline](https://github.com/toddysm/custos/actions/runs/42131707104)
failed Custos installation with final activity-runtime readiness reporting an
unconfigured scheduler; disposable kind cleanup succeeded. #1018 must address
that independent deployment configuration finding before certification. D09 is
not represented as its fix.

Historical revision-7 checkpoint (superseded): 038 and 039 merged through
[#1040](https://github.com/toddysm/custos/pull/1040) and
[#1041](https://github.com/toddysm/custos/pull/1041). Fifteen of the original
18 delivery items are merged. The approved D08 amendment adds one prerequisite,
making **19 delivery items, 15 merged**. #1045 is in review through #1047.
#1044 retains the lifecycle implementation at `e34dfa1`; it must not merge
until its saturation finding is fixed and its current-head gates pass.
The historical 13/18 and parked-work descriptions below are earlier checkpoints,
not the current status.

040 is one provider-focused implementation pull request from current main,
not a stack based on unmerged #1044. It implements the progress capability,
schema and serializable scheduled collaborator contract, and extracts #1044's
bounded `ReceiverWorkspaceProvider`/keyset projection and tests into this
prerequisite. Auth's unpaginated workspace directory is not the discovery source.
034 then integrates
that merged prerequisite normally into its existing worktree, preserving its
commits, and replaces the in-memory-only queue. No duplicate provider contract
or unrelated copied lifecycle implementation is permitted. Coordinate overlapping
metadata/scheduled-receiver changes and stop on unresolved conflicts.
035 adds deployed saturation and restart checks; the existing 036 documentation
item expands to progress diagnostics and migration/rollback. There is no second
documentation issue or additional scope beyond this single prerequisite.

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
| D07 | Truthful production activity readiness, private compatible bootstrap, Connector-owned credential issuance | Owner approvals recorded in #988; [#1038](https://github.com/toddysm/custos/issues/1038) merged through #1041 |
| D08 (approved) | Durable per-workspace/operation progress, independent bounded discovery, ticket-ordered fenced claims and safe restart/cleanup | Design approved/merged via #1046; [#1045](https://github.com/toddysm/custos/issues/1045) Build authorized 2026-11-04T17:03:00.903-08:00 |
| D09 (proposed, unapproved) | Bounded digest-verified file and external immutable/version-pinned registry/authorization bundle; compact Helm references; verified legacy retention bridge; 1,000,000-byte file, unchanged 65,536-byte inline and 1,024-entry limits; real Helm storage and restart-safe rollback | Planning authorized 2026-11-05; [#1048](https://github.com/toddysm/custos/issues/1048) before #1018; no Build/merge yet |

The design document owns the exact contracts, alternatives and migration impact.
D01-D08 were explicitly approved. Approval and amendment
history are recorded in #988 rather than inferred from issue creation.
D09 is excluded from that approval. Its exact contract and updated 20-item
breakdown require explicit owner approval of this amendment.

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

For 040/034, require the exact saturated boundary: 51, 200 and 1,000 workspaces
all continuing, with all N eligible rows served in N successful claims after
registration under a controlled provider clock. Verify discovery progresses
while every local operation slot is occupied. Assert 50 returned discovery IDs,
at most 51 IDs fetched including lookahead, and one claimed continuation per
operation in process memory; use actual
producer cursors for deep delivery, due-schedule cutoff and retention progress.
Race two native PostgreSQL connections, lose checkpoint responses, expire and
replace owners, restart every collaborator and attempt wrong-scope/stale writes.
Test inactive-row cleanup and source-retention races without resetting unfinished
work. 034 additionally exercises actual lifespans, old-schema refusal,
off-mode compatibility and shutdown; 035 remains the deployed certification.
Require scheduler leadership before scheduled claims and transactional
verification in the provider; a standby cannot repeatedly postpone work.
Two-replica tests must measure actual scheduled-page progress and loss of
leadership both before claim and before effect, not merely queue claim counts.

For 041, use D09's exact 1,000,000/1,000,001-byte file and
65,536/65,537-byte inline boundaries, 1,024/1,025-entry limits and conservative
complete ConfigMap data budget. Test the production loader with actual files,
all four managed consumers' lifespan failures/successes, and exact externally
provisioned file bytes for every profile at 51/200/1,000 workspaces and 1,024
bindings. Include nonrepetitive 120-character ASCII references, multibyte/escaped
values, duplicate/unknown/cross-scope fields, ambiguous/missing/special/unreadable
sources and missing/malformed/mismatched expected digests.

Provision immutable versioned ConfigMaps and versioned Dapr Configuration/
Secret Role/RoleBinding resources outside Helm using the shared existing policy
rules. Compare effective policies with legacy inline mode; retain default deny
and exact named-Secret `get` permissions, database/broker access and pinned
identities. Test create-or-verify reuse, conflicting resources, closed compact
descriptors and no registry/authorization bulk in Helm values/manifests.
On disposable hosted kind, actually install/upgrade/roll back with Helm 3's
default Secret storage and maximum-length nonrepetitive 1,000/1,024-entry
registries. Measure release Secret data below 1,048,576 bytes; file/render
measurements alone do not satisfy this gate. Restart old Pod containers during
upgrade and roll back using reused and recreated Pods; verify expected
snapshots/defaults, digest refusal and original work/credential identities.
Keep immutable bundles and explicit named-Secret permissions for retained work
and rollback; test verified cleanup only after no retained reference needs them.
For first inline-to-file migration, require a same-policy inline bridge release
with `retainLegacyExecutionResources: true`; verify `helm.sh/resource-policy:
keep` in its **stored Helm manifest** on all 12 legacy Configurations/Roles/
RoleBindings before allowing the file upgrade. Keep those fixed-name resources
and ownership metadata for old sidecar restarts and compatible recovery.
Test direct-migration refusal, actual bridge/file upgrades with restarted old
sidecars, bounded same-identity bridge **recovery upgrade**/re-migration and
verified eventual legacy-resource cleanup. With pinned Helm 3.16.2, native
rollback cannot recover live retained resources absent from the current
manifest: prohibit native file-to-bridge rollback and `--atomic` first
migration. Recover using a new `helm upgrade` revision with the exact bridge
chart/complete values and `--reset-values --wait`; verify matching ownership,
unchanged object identifiers/policy and all consumers. Before recovery Helm
runs, reconcile each live Deployment's managed source settings to the verified
bridge rendering using resource-version-tested targeted patches: restore inline
and Dapr config, remove all file/digest environments and file-only mounts/
volumes/digest annotations, and preserve unrelated fields/resources. Resetting
values alone does not remove live-only fields after a failed upgrade.
Force a first-file rollout to update live Deployments and then time out under
`--wait`; prove reconciliation/recovery yields one valid source and readiness.
Test concurrent/partial-patch refusal, wrong ownership and incompatible/oversized inline
recovery; never force, bypass ownership or delete/recreate. File-to-file native
rollback remains covered. Fresh file
installs and ordinary inline deployments do not need this migration opt-in.
Wire actual cases into existing hosted jobs. This is prerequisite evidence,
not a substitute for 035's deployed receiver-to-Workflow/security/shutdown work.
Direct configuration/source docs are part of 041; final user docs remain 036.

Before closing #988, every required child must be complete, documentation
merged, and a separately reviewed close-out must promote the capability
inventory. Never close the parent merely because this plan is merged.
