# Trigger M2 receivers - product requirements and engineering design

- Status: **Revision-8/D09 proposed for #1048; awaiting concrete design/breakdown approval. Revision-7/D08 remains approved.**
- Planning format: full; revision 8 proposed. D01-D08 approvals are retained; D09, implementation and deployed validation are separate gates.
- Tracking: [#988](https://github.com/toddysm/custos/issues/988).
- Design work item: [#1004](https://github.com/toddysm/custos/issues/1004).
- Updated: 2026-11-05; original baseline commit `e344ab2`.
- Validation: GitHub-hosted continuous integration (CI), plus Copilot-only review.
- Delivery plan: [implementation-plan-m2.md](../components/trigger-service/implementation-plan-m2.md).

## Part I - Product requirements

### 1. Executive overview

Complete the four external/clock-driven receivers in Milestone 2 (M2):
scheduled triggers, generic webhooks, vendor push and polling. Operators should
be able to configure these sources, start or resume workflows, and inspect and
recover failed dispatches without writing source-specific orchestration code.
The existing manual/internal receivers and shared dispatch pipeline remain.
The three original design blockers are closed; the remaining work is production
implementation and reconciliation of the contracts identified below.

### 2. Problem statement and evidence

The evidence is the owner-approved scope of [#988](https://github.com/toddysm/custos/issues/988),
the [requirements](requirements.md) and source inspected at the baseline:

| Evidence | Gap |
|---|---|
| [Receiver directory](../../src/services/trigger-service/src/custos_trigger/receivers) | Only the internal-event receiver is implemented here. |
| [Trigger store contracts](../../src/services/trigger-service/src/custos_trigger/stores/base.py) | Read capabilities used by memory tests are not the PostgreSQL production query surface. |
| [Dispatcher](../../src/services/trigger-service/src/custos_trigger/pipeline/dispatch.py) | A dead-letter outcome does not persist a replayable entry. |
| [Signature verification](../../src/services/connector-service/src/custos_connector/listen/signature.py) | Production per-instance signing-material resolution is still missing. |
| [Trigger authorization](../../src/services/trigger-service/src/custos_trigger/middleware/callctx.py) | The production verification branch raises `NotImplementedError`. |
| [Workflow internal routes](../../src/services/workflow-service/src/custos_workflow/api/routes/rpc.py) | Resume deduplication is in-process; replay safety cannot be inferred merely from supplying an idempotency key. |

This is an existing-platform completion effort, not a market-validation study.
No external customer interviews, three named customer references, adoption
baseline or commercial impact measurements were supplied. Those remain absent;
this document does not invent them or claim general-availability readiness.

Revision-7 evidence: [#1044](https://github.com/toddysm/custos/pull/1044) at
`e34dfa1a40cf07088356d18094ff2d2ddeadbe24` retains at most 50 continuing
workspaces in memory. A 51-workspace reproduction made 1,457 delivery passes
but visited only the first 50. Its
[open review finding](https://github.com/toddysm/custos/pull/1044#discussion_r4474284303)
blocks lifecycle completion despite passing hosted checks. The owner authorized
one prerequisite issue, [#1045](https://github.com/toddysm/custos/issues/1045),
and this design-amendment pull request on 2026-11-04T16:20:02.742-08:00.
That initial authorization was for planning. The owner subsequently approved
the concrete D08 design and 19-item breakdown on 2026-11-04T16:41:03.590-08:00;
[#1046](https://github.com/toddysm/custos/pull/1046) merged as `8eb0414`.
Build for #1045 was authorized on 2026-11-04T17:03:00.903-08:00.

Revision-8 evidence: at merged main
`4d7d067d5e4380c0adb5e242bdf4193528edc5fb`, #1018's production
execution-binding loader and managed Helm rendering accepted 51/200 compact
workspace registries (5,005/19,807 bytes) but rejected 1,000 (99,807 bytes):
both impose a 65,536-byte ceiling. See the
[measured preflight](https://github.com/toddysm/custos/issues/1018#issuecomment-6672885330).
The owner authorized adding [#1048](https://github.com/toddysm/custos/issues/1048)
on 2026-11-05T16:15:23.655-08:00 and opening this scoped design amendment on
2026-11-05T17:27:24.414-08:00. Neither authorization approves D09, Build or merge.
#1045/#1017 are now closed through #1047/#1044; the revision-7 finding above is
historical, not an outstanding approval gate.

### 3. Customer scenarios

**S1 - Scheduled checks.** A security analyst currently fires a workflow manually.
After creating a scheduled subscription, the analyst expects each planned scan
to start once, including when a Trigger replica fails. Pausing the subscription
stops future fires. After an outage, the documented missed-tick policy explains
which occurrence runs rather than silently flooding the cluster.

**S2 - External push.** An integration developer configures a registry connector
and copies its workspace-scoped webhook URL into the source. A valid signed
event starts the matching workflows; the same connector instance identifier in
another workspace cannot redirect that event. A bad signature returns an
actionable unauthorized response without matching subscriptions.

**S3 - Polling fallback.** A platform operator enables pull delivery for a source
without reliable webhooks. Several subscriptions share one connector polling
loop. If publishing a batch fails, the cursor does not advance; replay after
recovery does not duplicate a workflow effect. Operator pause takes precedence
over automatic subscription reconciliation.

**S4 - Recovery.** An on-call operator sees a durable pending dead-letter entry
after Workflow Service is unavailable. After fixing the dependency, an authorized
replay uses the original target and idempotency key. Concurrent replay is bounded
and explicit; a crashed replay worker can be recovered. An unauthorized user
cannot inspect retained payloads or replay another workspace's entry.

**S5 - Managed capacity and safe configuration.** A deployment operator prepares
the approved 1,000-workspace receiver certification workload. Today even compact,
reference-only account mappings fail before deployment, despite the loader's
1,024-entry allowance. With D09, the operator prepares a reference-only registry
and its narrowly scoped authorization resources outside the Helm release, then
passes only a compact version-pinned bundle descriptor to Helm. Every consumer
loads and verifies the same bounded, immutable file snapshot; credentials still
come from the existing secret boundary. A missing mount, digest mismatch,
ambiguous source or malformed/oversized file blocks readiness with a safe reason
instead of selecting another account. The operator creates a corrected bundle
and rolls again; already accepted work keeps its original identity.

### 4. Outcomes, goals and scope

All five deliverables in #988 are must-have: four receiver types and durable
dead-letter/replay operations. Production storage reads, verification, signed
administration and the narrowly coupled Workflow idempotency contract are
prerequisites, not separate unrelated enhancements.

For S5, D09 must admit the 1,000-workspace reference registry through every
managed consumer without weakening account scope or legacy inline limits.
This is a capacity prerequisite to certification, not a claim that the workload
has run successfully.

Non-goals: the web designer ([#994](https://github.com/toddysm/custos/issues/994)),
the general Workflow observability integration ([#989](https://github.com/toddysm/custos/issues/989)),
the general gateway M2 backlog ([#990](https://github.com/toddysm/custos/issues/990)),
a new polling engine, new broker infrastructure, arbitrary vendor plugins,
public dedup-clear operations, and a general multi-revision migration policy
([#993](https://github.com/toddysm/custos/issues/993)). No #988 requirement is
deferred. This design does not close those other issues.

### 5. Existing internal solutions

Reuse the existing normalizer, selector evaluator, start/resume matchers,
dispatcher, connector listener, cursor service, pull scheduler, metadata
provider, shared signed call-context verifier and deployment profiles. Add
production capabilities at their existing boundaries; do not copy SQL into
receiver handlers or make Trigger Service load connector plugins.

### 6. Competitive solutions

No competitive comparison is needed to choose between the existing Custos
components. No claim about competing products is made.

### 7. Desired experience

The application programming interface (API) is the primary surface. Existing
subscription endpoints gain validated source configuration; webhook setup
returns an unambiguous workspace-scoped address. Operator recovery uses protected
dead-letter endpoints. Helm provides configuration and enable/disable control.
The existing command-line tool can continue invoking public APIs; a new command
set is not required. A graphical interface is out of scope.

### 7A. Proposed user-facing output

These are approved contract examples, not proof that every path is deployed.
Successful single replay:

```http
POST /v1/workspaces/team-a/triggers/deadletter/dl-42:replay
HTTP/1.1 200 OK
Content-Type: application/json

{"status":"replayed"}
```

Concurrent replay follows the Trigger Service's existing Problem JSON shape:

```http
HTTP/1.1 409 Conflict
Content-Type: application/problem+json

{"type":"https://errors.custos.dev/trigger/replay_in_progress","title":"Replay already in progress","status":409,"detail":"Another worker owns this replay. Retry after the replay lease expires.","code":"trigger.replay_in_progress"}
```

An authenticated operator without the required permission receives the existing
call-context middleware error shape:

```http
HTTP/1.1 403 Forbidden
Content-Type: application/json

{"error":{"code":"permission_denied","detail":"missing required permission: trigger:admin"}}
```

For proposed D09, the primary runtime surface is startup/readiness configuration
diagnostics, not a new service endpoint. An operator preparation helper produces
the external deployment bundle described in D09. These two samples are
**unapproved proposed output**, not shipped behavior or certification evidence:

```text
Execution bindings: loaded
Source: file
Workspaces: 1000
Bindings: 1000
UTF-8 bytes: 927027
Snapshot: expected SHA-256 verified; fixed until process restart
Credentials: resolved through existing Secret/Auth boundaries
```

```text
Execution bindings: rejected
Source: file
Reason: file exceeds 1000000 bytes
Readiness: not ready; no new work admitted
Fallback: none
```

### 8. Goals and measurements

| Goal | Baseline | Acceptance target | When measured |
|---|---|---|---|
| Receiver coverage | 0/4 M2 receiver paths connected | 4/4 receiver-to-Workflow start/resume scenarios pass | TS-IMPL-035 |
| Durable recovery | No persisted dead-letter entry | Failure survives restart; original-key replay succeeds | TS-IMPL-030/031/035 |
| Duplicate protection | Not established for M2/replay | Zero duplicate effects in the specified concurrency/crash tests | Each path and TS-IMPL-035 |
| Isolation | Missing new ingress/admin paths | All spoofing, wrong-workspace and unauthorized cases rejected | TS-IMPL-026/027/032/035 |
| Regression | Existing suites and coverage gate | Existing behavior preserved; Trigger coverage at least 90% | Every relevant code change |
| Managed binding capacity, S5 | 1,000 compact bindings rejected at 99,807 bytes | Exact provisioned 1,000-workspace files load in all four consumers/profiles; real Helm release storage fits, and source/limit/digest errors remain fail-closed | TS-IMPL-041; deployed effects remain TS-IMPL-035 |

These are bounded acceptance tests, not proof of exactly-once behavior for all
possible outages. Track dispatch latency, queue age, retained bytes and upstream
poll count as guardrails; do not trade durability for a green latency graph.

### 9. Pricing

Not applicable: this change introduces no billing or packaging changes.

### 10. Performance and scale

The inherited platform envelope is 1,000-10,000 workflow runs/day across dozens
of teams (REQ-030). REQ-031's p95 step-transition target is not a new receiver
latency promise. Record receiver ingress-to-dispatch p50/p95/p99, queue age,
CPU/memory and database load under a documented workload in TS-IMPL-035.
No measured M2 latency or availability certification exists yet.

Proposed bounded test workload: two Trigger replicas, 100 simultaneously due
schedules and 10,000 connector events with duplicates. This is a test fixture,
not a new supported-scale claim. Preserve the one-MiB webhook cap; reject larger
bodies with 413. Pull intervals retain the existing minimum of 10 seconds.
Paginated administration defaults to 50 entries, maximum 100; bulk replay
selects at most 100 entries per request. These bounds were approved with the baseline.

### 11. Observability and troubleshooting

Expose scheduler leadership/lag, receive/verify/normalize/dispatch outcomes,
connector cursor lag, pending/replaying dead letters, replay age and retention
cleanup failures. Correlate with workspace, subscription, event and run
identifiers in access-controlled logs/audit, not high-cardinality metric labels.
Operators diagnose signature errors through credential configuration, poll
stalls through cursor/pause state, dispatch failures through the retained error,
and replay contention through its lease. The final docs include these recovery
recipes and the limits of retry versus operator discard.

### 12. Security

External webhooks authenticate using connector-owned signing material or a
verification-only bearer credential. Administrative endpoints require a valid
signed Custos call context and `trigger:admin`. Internal delivery requires the
existing Dapr service identity and network policy; bypassing call-context checks
never means allowing arbitrary public internal requests.

All lookups and durable records are workspace-scoped. Incoming call-context and
private forwarding headers are untrusted and stripped at public ingress. No
signing secret, bearer credential or signature header enters retained event
data, audit payloads or logs. Normalized event payloads may contain customer
data; operators must apply connector redaction policies and existing encrypted
storage/backup policies. Dead-letter retention defaults to 30 days.

### 13. Risks and open decisions

D01-D05 and the recorded amendments are approved. D06's workspace-scoped
execution identity and new contract-alignment prerequisite were explicitly
approved on 2026-11-02. On the same date the owner authorized creating and
starting D07's startup/bootstrap prerequisite. Those prerequisites subsequently
merged through #1040/#1041. D08 subsequently passed its owner-approval gate
and merged through #1046; that is not runtime completion.
The baseline design owner is
[#1004](https://github.com/toddysm/custos/issues/1004); implementation owners
are the linked child issues.
Copilot findings can change a recommendation but cannot approve it on behalf of
the repository owner. A material change to scope or contracts invalidates any
earlier approval and updates both the plan and affected acceptance criteria.

D08 adds durable scan progress and fair admission, rather than discarding
unfinished cursors to free memory. Its schema, ownership, retention and rollout
contract received explicit approval of revision 7 before #1045 implementation.
Those prerequisites have since merged through #1047/#1044. D09 is a new,
unapproved capacity amendment, not a reopening of D01-D08. Its exact source,
byte/count limits and rollout contract below require approval before #1048 Build.

### 14. Rollout and milestones

Planning began 2026-10-27. There is no committed delivery date. Gates are:
(1) reviewed design and explicit approval, (2) additive provider/schema work,
(3) receiver and recovery paths tested while disabled, (4) deployed integration
verification, (5) documentation and controlled enablement, (6) reviewed inventory
close-out before closing #988. Existing production behavior remains the default
until new receiver prerequisites are ready. This is not a public-preview or
general-availability announcement.

## Part II - Engineering design

### A. Abstract

This design completes Custos's clock and external-event ingestion using the
existing Trigger Service pipeline and Connector Service execution boundary.
Schedules use a PostgreSQL-backed fenced leader lease and durable pending
fires; webhook requests are routed to an exact workspace and connector instance
and verified without exposing connector signing material. Push and pull
connector events retain their existing producer envelope and stable identity,
then pass through a shared adapter into Trigger's normalized envelope. The
Connector Service remains the sole upstream polling and cursor owner. Failed
dispatches are durably retained before delivery is acknowledged, and
administrators can replay them using the original idempotency key. Production
provider reads, signed administrative authorization and downstream replay
deduplication are explicit prerequisites. The design resolves the original
five cross-service contract gaps and adds the approved background execution
identity/Workflow-to-Connector alignment prerequisite, plus production activity
startup and compatible private bootstrap delivery.
Implementation proceeds through native dependent work items, with tests on each
change and deployed failure/recovery coverage before enablement. No new broker,
polling engine or graphical interface is introduced.

### B. Prerequisites

The scope is [#988](https://github.com/toddysm/custos/issues/988). The original
decisions [#20](https://github.com/toddysm/custos/issues/20),
[#22](https://github.com/toddysm/custos/issues/22) and
[#23](https://github.com/toddysm/custos/issues/23) remain resolved.
The [M1 implementation plan](../components/trigger-service/implementation-plan.md)
does not cover this work. Baseline approval and subsequent amendments are recorded
in the #988 ledger. The owner approved D06 and its new prerequisite on 2026-11-02;
the same date's explicit start authorization covers D07/#1038. The existing
hosted review, validation and merge gates remain binding.

### C. Introduction

Receiver names, schema rows and in-memory tests are not sufficient evidence of
production behavior. In particular, PostgreSQL read operations, real signature
resolution, durable failure capture and production call-context verification
must work through their deployed providers. This plan includes those narrow
dependencies so #988 cannot be declared complete on mock-only tests.

### D. Design and decision register

#### D01 - Tenant-safe webhook addressing

**Recommendation:** add the canonical public route
`POST /v1/webhooks/workspaces/{workspaceId}/connectors/{connectorInstanceId}/events`.
Use the exact workspace/instance pair for verification and matching. Connector
identifiers are workspace-local, as the [existing connector router](../../src/services/connector-service/src/custos_connector/listen/router.py)
already documents. The old unscoped gateway route must fail closed with
404 `webhook-route-not-found`; never scan workspaces or select the first match.
Document migration to the scoped URL. No globally unique identifier assumption
or new global alias registry is introduced.

Alternative rejected for this proposal: silently treating connector instance
IDs as globally unique. A global opaque ingress-ID registry is viable but adds
another persistent lookup/rotation lifecycle without being required by #988.

Approval/test gate: [#1004](https://github.com/toddysm/custos/issues/1004);
implementation [#1009](https://github.com/toddysm/custos/issues/1009) /
[#1011](https://github.com/toddysm/custos/issues/1011). Test two workspaces with
the same instance ID, spoofed workspace headers and unscoped requests.

#### D02 - Webhook credentials are verification data, not platform identity

**Recommendation:** on the exact new webhook route only, the gateway strips
incoming `x-custos-callctx` and caller-supplied
`x-custos-webhook-authorization`. It copies an external `Authorization` value
into the latter private transport header, removes `Authorization`, and forwards
the raw body unchanged over the service mesh. Trigger passes that value only
as `headers.authorization` in the internal verifier request, then discards it.
Neither Trigger nor Connector may use it for Custos authorization. Signing
secrets remain in Connector Service; the transmitted bearer is the request
credential to be verified, not the stored secret used to validate signatures.

Keep the current stripping policy for every other route, including the legacy
unscoped webhook route. Reject direct public access to the private verifier and
require an allowed gateway/service identity for the private forwarding header.
Strip credentials and signatures before constructing retained event envelopes.

Alternatives: forwarding `Authorization` as ambient service authorization risks
identity confusion; dropping it entirely cannot support bearer-only sources.
This limited transport change needs explicit approval, not a silent gateway fix.
Approval/test gate: [#1004](https://github.com/toddysm/custos/issues/1004);
implementation [#1009](https://github.com/toddysm/custos/issues/1009) /
[#1011](https://github.com/toddysm/custos/issues/1011), including spoofed-header,
rotation, wrong-signature and non-webhook-route regression tests.

**Owner-approved runtime-security amendment (2026-10-29):** production mesh
caller identity requires Dapr 1.18.6 or newer with mutual transport
authentication. Pin the verified stable 1.18.9 patch across prerequisite
installation, hosted deployment tests and offline images. Authenticate the
sidecar-to-app hop before checking both the authenticated caller app ID and
namespace; configure those independently of sibling HTTP endpoint addresses.
Gateway-minted contexts carry only the permission actually authorized for the
route, not caller-supplied grants.

Newer Dapr rejects encoded slash/backslash/dot method names. Preserve opaque
root/child identities in version-2 resume request bodies on a fixed internal
method, retaining existing public and legacy request formats. Move these
runtime/security prerequisites and compatibility regressions into #1015;
#1017 still owns activation, remaining deployment controls and lifecycle
wiring, and #1018 still owns complete deployed recovery certification.

#### D03 - One polling owner, publish before cursor commit

**Recommendation:** Connector Service keeps its existing per-instance
`PullLoopScheduler`, cursor lease and publisher bridge. Trigger owns subscription
intent and reconciles that intent to connector registrations; it does not run
a second upstream timer or store a second cursor. The effective instance interval
is the minimum interval among active pull subscriptions, never below the
connector's configured floor. Subscription intervals describe observation
frequency, not a guarantee of upstream-event timing.

A batch advances its cursor only after every publish succeeds, as the
[connector design](../components/connector-service/design.md#advancement-and-commit-semantics)
and implementation require. Failure leaves the previous cursor intact.
Subscription pause/remove updates registration demand; an explicit connector
operator pause/revoke always wins. Registration reconciliation runs on restart
and on subscription changes.

The existing scheduler's pause flag and next-due time are process-local, so
reusing its loop alone does not provide either restart-safe pause or an
instance-wide interval floor. TS-IMPL-029 adds a workspace/instance-scoped
`ConnectorPullControl` provider record containing `operatorPaused`, `revision`,
`nextEligibleAt`, `updatedAt` and `updatedBy`, with an additive PostgreSQL
migration and in-memory adapter parity. Pause/resume endpoints persist intent
and its audit record before acknowledging or updating the local scheduler,
including when no local registration exists. Reconciliation reads this state;
subscription changes never clear an operator pause. An already-running tick
may complete, but no subsequent tick starts until explicit operator resume.

Before calling an upstream plugin, every replica uses an atomic provider
operation to check the persisted pause state and database-time `nextEligibleAt`,
validate/claim the cursor lease and reserve the next eligible time. Successful
cursor commit may release the cursor lease, but must not clear that interval
reservation. Failure/backoff updates the same shared next-eligible state;
another replica cannot immediately poll after the first releases its lease.
Interval changes apply to future reservations without shortening an existing
reservation.

**Owner-approved recovery amendment (2026-10-30):** the initial atomic claim
also persists an invocation fence before upstream work can begin. Lease expiry
alone never clears that fence. A crashed or unconfirmed invocation leaves the
cursor unchanged and blocks subsequent polls until termination is verified or
an operator explicitly confirms recovery. Ordinary confirmed completion clears
the matching fence without shortening the interval reservation.

Recovery must name the observed invocation and be audited atomically; stale
confirmations cannot clear a newer invocation, and an unexpired owner cannot be
replaced by recovery. Ordinary pause/resume and demand reconciliation do not
clear an invocation fence. Operator confirmation means the previous invocation
and executor cannot resume, not merely that a container was absent momentarily.
This intentionally trades automatic retry after an unclean crash for
fail-closed prevention of overlapping invocations.

**Owner-approved Kubernetes execution prerequisite (2026-10-30):**
[TS-IMPL-037 / #1035](https://github.com/toddysm/custos/issues/1035) precedes
production activation in TS-IMPL-034. The current Docker command-line hook
runner is not a configured execution backend in the supplied Kubernetes
deployment. Do not mount a Docker socket or introduce a privileged daemon to
bridge this gap.

Implement a Kubernetes-backed `HookRunner` using the existing sandbox
isolation/lifecycle patterns. Connector remains the sole polling/cursor owner;
the runner owns only one invocation's execution and cleanup. Resolve immutable
OCI image/launch metadata and preserve the original entrypoint plus hook
arguments and bounded JSON stdin/stdout contract. Existing plugin images must
not acquire a Python or shell requirement.

Use an explicitly owned, non-restarting sandbox with namespace-scoped least
privilege, non-root execution, a read-only root filesystem, dropped capabilities,
runtime-default seccomp, no host namespaces/mounts, no plugin service-account
token and bounded ephemeral memory-backed I/O. A trusted static process bridge
may adapt length-framed control-plane streaming to the plugin's unchanged
stdin/stdout protocol. Request/response bytes and credentials must not appear
in Kubernetes object specifications, environment variables, ConfigMaps, pod
application logs, controller logs or audit payloads.

Creation collisions are not permission to adopt or re-execute an old sandbox.
Bind operations to the observed resource UID. Confirm terminal execution of
the selected sandbox before clearing invocation uncertainty; deletion or a
404 response alone does not establish that an executor on an unreachable node
has stopped. Unknown creation/exec outcomes, cancellation and unavailable
termination evidence retain the existing polling fence and surface
`PluginCleanupFailed` for the established audited recovery path.

Configure the same invoker for bind, health, listen and verification hooks.
Kubernetes production prerequisites fail closed; explicit Docker development
compatibility remains. TS-IMPL-037 supplies executor/configuration/deployment
primitives and real Kubernetes contract tests; TS-IMPL-034 owns final lifecycle
and four-profile activation. TS-IMPL-035 still owns deployed capacity and
resilience certification. Neither the new executor nor its tests certify
production resource limits by themselves.

Legacy instances require an explicit control-state migration during disabled
rollout. If prior operator-pause intent cannot be established, leave polling
blocked pending operator confirmation rather than infer "running" from active
subscriptions. A control-store failure also blocks new ticks. Test pause before
registration, restart while paused, subscription mutation while paused, explicit
resume and staggered ticks on two Connector replicas within one interval.

Rejected: moving or duplicating the working polling/cursor machinery in Trigger.
Approval/test gate: [#1004](https://github.com/toddysm/custos/issues/1004);
implementation [#1010](https://github.com/toddysm/custos/issues/1010) /
[#1012](https://github.com/toddysm/custos/issues/1012). Test shared subscriptions,
restart, lease contention, partial publish and operator pause.

#### Source-scoped correlation for external resume waits

External sources must not supply trusted Workflow run/step identifiers.
The existing direct `runId`/`stepId` extraction remains only for legacy internal
events. Extend `RegisterResumeSubscription` additively with `workspaceId` and
`source: {connectorInstanceId, deliveryModes, pollIntervalSeconds?}`. If `source`
is present, require a trusted workspace and a non-empty event selector; validate
the instance association through the calling Workflow's context. Extend the
Workflow wait configuration, compiler/validator and trigger client to transmit
these fields. Existing internal/manual registrations without `source` retain
their current behavior and are never enumerated as external-source candidates.

Store this source association with the existing run, step, event key, selector
and expiry. Query active resume registrations by workspace and connector
instance/delivery mode before selector evaluation. Evaluate the selector against
the normalized external event, then dispatch each matching registration using
its stored run/step/event key; never trust equivalent fields from the event
payload. External events can therefore match starts, resumes or both even when
the source payload contains no run/step IDs. Use bounded, paginated candidate
queries and the existing per-registration delivery identity.

Polling demand is the union of active pull-start subscriptions and unexpired
source-associated pull-resume registrations. A resume-only wait starts demand
without requiring a dummy start subscription. Cancellation, completion and
expiry remove that demand; they do not erase connector operator-pause intent.
Reconcile after registration changes and on startup. A source selector may
intentionally match multiple waiting runs, but never another workspace.

TS-IMPL-022 owns the query/index support; TS-IMPL-023 owns source-association
models, Workflow registration/client changes and validation; TS-IMPL-028 owns
candidate correlation and delivery; TS-IMPL-029 includes resume-only polling
demand. TS-IMPL-035 tests push and pull resumes without payload run/step IDs,
two workspaces, multiple waits, expiry/cancellation and a resume-only poller.

#### D04 - Scheduling and leader-fencing semantics

**Recommendation:** five-field cron uses an explicit IANA timezone (default
UTC). Invalid expressions/zones fail at configuration time. Nonexistent local
times during a daylight-saving jump are skipped; repeated local times represent
two distinct UTC instants. Interval schedules use positive whole seconds and a
persisted UTC anchor. On recovery, coalesce missed occurrences into the most
recent due occurrence and audit the skipped count; do not silently replay an
unbounded backlog. This missed-tick policy is new and requires approval.

Use database time for the deployment-scoped scheduler lease. Acquisition after
expiry increments the epoch; renewal by the same live holder preserves it.
This clarifies the older example that incremented on every update. Defaults
remain 30 seconds for the lease and 10 seconds for renewal. Failover occurs on
the next successful acquisition scan after expiry, not a promise that network
and database outages finish within 30 seconds.

Create a durable pending-fire record and advance `nextFireAt` in one fenced
provider transaction. Publish the pending event and mark it delivered only
after acknowledgment. A crash can republish the same event, not lose it.
Its identity derives from schedule ID and planned UTC fire instant; preserve the
existing subscription/event dedup key encoding rather than changing old keys.
Approval/test gate: [#1004](https://github.com/toddysm/custos/issues/1004);
implementation [#1007](https://github.com/toddysm/custos/issues/1007) /
[#1008](https://github.com/toddysm/custos/issues/1008).

#### D05 - Durable replay and downstream idempotency

**Recommendation:** persist failed dispatches with their original normalized
event, resolved target/mapping, idempotency key and attempt history. Use expiring
atomic replay claims with fencing; stale workers cannot overwrite a newer
claim. Recovery after a crash reuses the same key. Retention and discard must
not race an active replay. A bounded replay limit defaults to 3 operator
attempts; a transient interrupted attempt still counts and is visible.

The original same-key design is necessary but insufficient: Workflow's
[provider wiring](../../src/services/workflow-service/src/custos_workflow/providers.py)
defaults the start-key lifetime to 24 hours, while the
[resume route](../../src/services/workflow-service/src/custos_workflow/api/routes/rpc.py)
describes in-process deduplication. Thirty-day replay must not rely on either.
TS-IMPL-031 must make receiver-triggered start/resume deduplication durable
across replicas/restarts and align its lifetime to the latest allowed replay
and in-flight completion horizon. Persist target/result identity so an
already-successful call returns or reconciles its original outcome.
If that safety cannot be demonstrated for an entry, replay returns an explicit
409 `trigger.replay_unsafe`; it must never generate a fresh key or blindly start
another run. Safe replay across the approved retention window is an acceptance
criterion, not something satisfied solely by returning this error.

**Approved adjustment, 2026-10-29:** TS-IMPL-031 also owns durable Workflow
wait execution, persisted resume mirrors and consumer-side replay protection.
These are prerequisites for demonstrating safe resume replay, not deferred
TS-IMPL-034 integration. Resume delivery uses a versioned, step-scoped contract:
duplicate delivery for one wait must not satisfy a later wait with the same
user-facing event name. Persist the protocol choice so replay/restart does not
silently reinterpret an existing run. Preserve existing public request formats;
legacy entries without a provably safe handoff fail replay explicitly rather
than being silently migrated. TS-IMPL-034 retains activation, readiness,
deployment, operational signals and rollout/rollback wiring. The remaining
delivery order and D01-D05 requirements are unchanged.

Public administrative dedup clearing remains out of scope. Internal reservation
rollback must be ownership-checked; suppressing a failure until key expiry is
not a successful replay implementation. This narrowly coupled Workflow change
does not include #989's observability work.
Approval/test gate: [#1004](https://github.com/toddysm/custos/issues/1004);
implementation [#1013](https://github.com/toddysm/custos/issues/1013) /
[#1014](https://github.com/toddysm/custos/issues/1014) /
[#1016](https://github.com/toddysm/custos/issues/1016), including a replay
after 24 hours, Workflow restart/replica changes and lost-response success.

#### D06 - Workspace-scoped execution identity and the real binding contract

**Owner-approved amendment, 2026-11-02:**
[TS-IMPL-038 / #1037](https://github.com/toddysm/custos/issues/1037) precedes
production activation in TS-IMPL-034. Autonomous scheduled, webhook/vendor-push
and polling-triggered workflows use an explicitly configured, workspace-scoped
service account with fresh, short-lived signed call contexts. They do not
inherit the subscription creator's expiring credential or gain blanket authority
from a platform caller name.

The concrete integration probe found that the
[Workflow Connector client](../../src/services/workflow-service/src/custos_workflow/clients/connector.py)
sends `BindForStep` without a call context and with only `stepKey`/`slots`.
The [Connector receiver](../../src/services/connector-service/src/custos_connector/binding/router.py)
exposes `/internal/v1/bind-for-step` and requires real `runId`, `stepId` and
`attempt` coordinates, signed workspace/permission context and different slot
and response shapes. The actual client/application relay returns 401 without a
context, then 404 with a synthetic development context; body validation confirms
the missing execution coordinates. A route-only fix or fabricated identifiers
would not establish the contract.

Authorize explicit account selection within its workspace. Persist only stable
identity/credential references with the accepted target and run; retries,
reconciliation, child execution and replay retain the originally selected
identity. Editing a subscription or account configuration cannot retarget
already accepted work or a waiting run. Resolve credential material through the
existing secret boundary; never persist service-account credentials or signed
call-context tokens in event payloads, workflow history, audit records or logs.

Reuse Auth's service-account, authorization, signer and verifier primitives.
Context issuance must authenticate the selected account's credential and
authorize the current operation for the exact workspace and receiving audience.
Derive caller identity from the authenticated boundary, not asserted request
fields. Do not copy caller-supplied principal/permission lists into signed claims.
The current [signing RPC](../../src/services/auth-service/src/custos_auth/api/routes/rpc.py)
retains an M1 trust assumption; it is not an unrestricted minting shortcut for
this execution path. Missing/disabled accounts, expired/revoked credentials,
denied grants and Auth/secret outages fail explicitly. Test existing cache,
invalidation and token-lifetime bounds rather than promising instantaneous
revocation.

Align request and response contracts through the real Workflow runtime,
Connector handler and directly coupled activity consumer. Execution coordinates
come from the owning run/step/attempt, including child runs. Preserve opaque
endpoint/handle metadata; any lease expiry or connector-kind data must come
from actual Connector authority, not placeholder values or lossy string casts.
Preserve signed authorization where required and authenticated app/namespace
checks for private mesh-only routes. Keep public/manual/internal compatibility
behind explicit rollout/version gates. Pending legacy work without a trustworthy
identity requires explicit migration/operator action, not silent reassignment.

Illustrative operator outcomes below describe the target behavior, not an
already-shipped command or a new wire schema:

```text
Execution authorization: allowed
Workspace: ws-team
Service account: receiver-executor
Operation: connector:bind
Run/step/attempt: run-123 / fetch / 1
Context: freshly issued; credential and token redacted
```

```text
Execution authorization: denied
Workspace: ws-team
Service account: receiver-executor
Reason: required permission is not granted
Outcome: no new execution; original durable target/identity retained
```

This is not a platform-wide identity redesign, SPIFFE migration, new privilege
model or second polling owner. TS-IMPL-034 retains lifecycle/profile activation;
TS-IMPL-035 retains deployed resilience/capacity certification; TS-IMPL-036
retains final user documentation. The four unpublished TS-IMPL-034 commits stay
preserved while this prerequisite is implemented from the latest default branch.

#### D07 - Production activity startup and bootstrap compatibility

**Owner-authorized scope and implementation, 2026-11-02:**
[TS-IMPL-039 / #1038](https://github.com/toddysm/custos/issues/1038) addresses
two independently reproduced gaps before production activation:

- The actual Activity Runtime Manager factory advertises `/readyz` 200 while
  leaving its scheduler unset. A valid signed `ScheduleActivity` request
  returns 503. The command-line entry point uses that same factory.
- The actual bootstrap minter emits an opaque random string, while the actual
  sidecar verifier requires a signed versioned payload bound to run, step,
  attempt and expiry. The ARM-issued token fails with 401; a correctly signed
  positive control succeeds.

Start with a fail-closed readiness regression. Liveness remains independent,
but an unwired, unhealthy or draining runtime must not claim it can schedule
activities. Then assemble the existing scheduler collaborators in the production
lifespan: resolver, durable execution repository, artifact I/O, secret injection
and runtime driver. Readiness follows successful initialization and dependency
probes, not merely process startup. Startup failure releases acquired resources;
shutdown closes admission before draining workers and closing their resources.
Do not install in-memory or test collaborators as production fallbacks.

Align bootstrap issuance with the sidecar's existing signed format and exact
attempt/expiry checks. Preserve separation between the bearer token and its
verification key: the activity must not gain the key that lets it manufacture
another attempt's authority. Use the private sandbox delivery boundary, not
ordinary inputs, workflow history, job arguments, plaintext key values in pod
specifications, logs or audit payloads. Missing configuration, malformed/tampered
tokens, expiry and scope mismatch fail explicitly. Do not remove verification
or widen workspace privileges to make the path work.

The change locations are the existing [application factory](../../src/services/activity-runtime-manager/src/custos_arm/app.py),
[health routes](../../src/services/activity-runtime-manager/src/custos_arm/healthz.py),
[scheduler](../../src/services/activity-runtime-manager/src/custos_arm/scheduler),
[runtime drivers](../../src/services/activity-runtime-manager/src/custos_arm/runtime),
[secret injector/issuer](../../src/services/activity-runtime-manager/src/custos_arm/secrets)
and [sidecar verifier](../../src/services/connector-sidecar/src/custos_sidecar/auth.py).
Existing HTTP paths, response envelopes and the sidecar's versioned token payload
remain the contracts; no new public API or cryptographic algorithm is introduced.
The startup ownership path is:

```text
Application lifespan -> owned providers/clients -> probes and schema gates
                     -> ActivityScheduler -> runtime driver -> sandbox + sidecar
                                                private bootstrap -> verifier
Shutdown -> close admission -> drain -> release resources
```

Retry/restart and cleanup must retain durable attempt ownership. A missing pod,
expired lease or accepted delete request alone is not proof that replay cannot
duplicate an effect. The complete prerequisite requires real adapter/runtime
tests and actual issuer-to-verifier compatibility, not mutually compatible mocks.

The intentional first behavior change is observable without activating receivers:

```text
GET /healthz: 200
GET /readyz: 503
Reason: activity scheduler is not configured
ScheduleActivity: 503; no activity admitted
```

The target success state, after the complete production graph is initialized:

```text
GET /readyz: 200
ScheduleActivity: admitted to the configured durable scheduler
Bootstrap verification: accepted for the original run/step/attempt
Credential material: private; not included in diagnostics
```

The first readiness/bootstrap work is independent of #1037 and starts from the
default branch. Keep #1037's identity/binding changes and #1017's lifecycle work
in their existing worktrees; integrate merged companion changes normally, without
copying unpublished code or stacking branches. Both #1037 and #1038 must complete
before #1017 activates the production profile. #1018 remains the deployed
resilience/capacity gate, and #1019 remains final documentation.

Rollout keeps receiver acceptance disabled until the runtime and identity gates
pass. Rollback closes admission and preserves in-flight ownership/evidence before
unwiring dependencies; it must not restore unsigned bootstrap acceptance or
truthy readiness for an unwired scheduler. This prerequisite does not duplicate
[#991](https://github.com/toddysm/custos/issues/991)'s deferred features or
[#763](https://github.com/toddysm/custos/issues/763)'s Kata-specific certification.

#### D08 - Durable scan progress and fair workspace admission (approved)

**Decision and scope.** Replace #1044's in-memory-only continuation queue with
bounded discovery plus a durable service-order queue in the existing PostgreSQL
metadata store. This is a quality prerequisite for S1-S4 and the existing
multi-tenancy requirement, not a new receiver, external queue, public endpoint
or coordinated rate limiter. No payload, authorization identity or dispatch
receipt moves into scan-progress storage. Existing effects remain authoritative.

**Operator scenarios and requirements.**

- **SP1, saturation:** an operator enables receivers for 51 or more workspaces.
  Even when every early workspace keeps returning a continuation, later
  workspaces must be discovered and receive bounded passes. A small active
  window must limit memory, not which workspaces can ever run.
- **SP2, restart:** an on-call operator restarts a replica midway through a
  selection with more than 32 targets or a multi-page scheduled scan. The next
  owner resumes the committed target position/cutoff, rather than restarting
  every busy workspace at page one. Repeating an uncheckpointed page is safe;
  skipping unprocessed work is not.
- **SP3, degraded storage:** when progress cannot be read or checkpointed, the
  process reports unavailable, stops claiming new work and retains the last
  committed state. Restoring the database resumes normal work; there is no
  success-shaped empty scan or automatic in-memory fallback.

The existing scheduled occurrence, downstream idempotency, operator pause and
credential contracts remain unchanged. This amendment does not promise a fixed
wall-clock latency for arbitrarily many tenants or arbitrarily slow upstreams.

**Approved component flow (revision 7; implementation in progress).** Solid arrows denote
provider or collaborator calls, not new network endpoints:

```text
Trigger discovery task -- bounded ID pages --> receiver workspace provider
         |
         +-- atomic register + discovery checkpoint --> metadata scan store
                                                        ^
Trigger operation task -- claim next fair row -----------|
         |                                               |
         +-- restore typed continuation                  |
         +-- one bounded existing operation page         |
         +-- fenced checkpoint + release ----------------+
                    |
        existing schedule / delivery / retention / demand collaborators
                    |
        existing effect fences, receipts and downstream authorization
```

The [storage interfaces](../../src/libs/storage-provider-layer) and
[PostgreSQL adapter](../../src/libs/custos-postgres) gain the optional progress
capability and additive migration in #1045. The
[scheduled receiver](../../src/services/trigger-service/src/custos_trigger/receivers/scheduled.py)
gains a typed continuation snapshot/restore contract there as well.
#1017 owns lifecycle/discovery integration, readiness and four-profile wiring
in its preserved PR; #1018 certifies deployment; #1019 owns final user docs.

**Discovery is not Auth workspace enumeration.** The current candidate
[#1044 interface](https://github.com/toddysm/custos/blob/e34dfa1a40cf07088356d18094ff2d2ddeadbe24/src/libs/storage-provider-layer/src/custos_spl/interfaces/receiver_workspaces.py)
already defines `ReceiverWorkspaceProvider` with
`check_receiver_workspaces_ready()` and
`query_receiver_workspaces(*, cursor: Cursor | None = None,
limit: int | None = None) -> Page[WorkspaceId]`. Its default page is 50,
maximum 200; runtime requests exactly 50. Its
[PostgreSQL implementation](https://github.com/toddysm/custos/blob/e34dfa1a40cf07088356d18094ff2d2ddeadbe24/src/libs/custos-postgres/src/custos_pg/adapters/metadata.py#L737)
projects only distinct workspace IDs from subscription, resume, schedule,
pending-fire, dead-letter, replay-command, source-selection, receipt and
connector-control/cursor tables. It uses C-collation keyset order, bound
`workspace_id > after` and `limit + 1` both per table and on the outer union.
At runtime limit 50, at most 51 IDs cross the database boundary, including the
one-row lookahead; only 50 are returned for registration. No upstream
`AuthStoreProvider.list_workspaces` or `GET /v1/workspaces` call is permitted.
The opaque, versioned query-scoped cursor resumes the keyset query itself,
not slicing a previously materialized full workspace list.

These are reviewed candidate capabilities, **not yet on main**. #1045 must
extract this bounded interface, PostgreSQL projection, memory parity and native
tests from the owned #1044 changes into the standalone prerequisite, alongside
the new progress capability. The memory fixture may own all seeded test data;
the production discovery path must never materialize all workspace IDs.
Reconcile the identical extracted changes when #1017 integrates main; do not
copy its unrelated lifespan/deployment code or pretend the prerequisite already
exists. Continue including orphaned recovery state, not only live subscriptions.

##### Persisted model and lifecycle

Use the next available additive metadata revision (16 at the inspected main
`a4eaa5e`); recheck numbering before implementation. Model version 1 has:

| Record | Key / bounded fields | Purpose |
|---|---|---|
| Discovery | `deployment_id`; opaque workspace cursor; sweep generation; completed sweep generation; holder, generation, claim token, database-time expiry | Resume bounded discovery independently of operation occupancy. |
| Workspace operation | `(deployment_id, workspace_id, operation)`; unique row incarnation; format version; progress revision; continuation; service ticket; `next_eligible_at`; holder/generation/token/expiry; `last_seen_sweep`, `last_seen_at`, `updated_at` | Retain one operation's progress and fair eligibility across replicas/restarts. |

`operation` is exactly `scheduled`, `delivery`, `pull-demand` or `retention`.
Continuation is a closed tagged structure, serialized as at most 64 KiB of
UTF-8 JSON with unknown fields/versions rejected:

| Operation | Version-1 continuation |
|---|---|
| `scheduled` | nullable opaque due cursor plus its UTC due cutoff; either both present or both null. A restored receiver reacquires scheduler leadership; no lease, Python object, callable or pending-fire payload is serialized. |
| `delivery` | nullable opaque source-reconciliation cursor, including the existing within-selection target index unchanged. |
| `retention` | nullable opaque dead-letter sweep cursor. |
| `pull-demand` | no continuation; one bounded reconciliation hint completes the pass. |

Nullable cursor means the next pass starts a new scan, not that retained source
work is acknowledged or can be deleted. Opaque cursor contents are not decoded
by the progress adapter; the owning operation validates their workspace/query
scope before effects. Validate the entire record at the provider boundary and
again when restoring it, including enum, type, timestamp and size constraints.
Malformed/unsupported state raises an explicit provider/validation error;
never silently reset it. Contract tests must prove the cap accepts the largest
legal cursor from each actual producer; do not truncate to make it fit.

Registration creates missing operation rows with null progress and an initial
service ticket. Repeated discovery only refreshes last-seen fields: it cannot
reset progress, ownership, retry eligibility or service order. A completed
operation page persists its exact next continuation and releases the claim;
a finished scan persists the corresponding null continuation. Failure,
cancellation or lost lease preserves the previous committed continuation.
Last-seen refreshes do not increment the progress revision: background discovery
must not invalidate an otherwise current worker's checkpoint. Format version,
progress revision and ownership generation are distinct fields.

Discovery has its own fenced claim, shared per deployment, and reads at most
50 IDs per pass. Register all four operations for that page and advance the
discovery cursor in one transaction. Duplicate attempts are idempotent.
Generation completion is recorded only after the terminal discovery page
commits; a crash cannot turn a partial scan into proof of absence.
No processing-queue length condition may prevent the next discovery page.

##### Internal provider contract, fairness and ownership

Add a runtime-checkable `ReceiverScanProgressProvider` capability rather than
expanding every existing metadata consumer. The Python contract includes:

| Operation | Required inputs / result | Failure or retry behavior |
|---|---|---|
| `check_receiver_scan_progress_ready()` | Required schema/capability probe | `MigrationRequired`/`QueryUnsupported` blocks activated receiver readiness. |
| Claim discovery | deployment, owner, lease duration -> lease plus cursor/sweep, or no available claim | Database time; no claim is a normal competing-owner outcome. |
| Commit discovery page | current discovery claim, at most 50 workspace IDs, next opaque cursor -> committed cursor/sweep | Atomic registration/checkpoint. Wrong or expired ownership raises `LeaseExpired`; retry by rereading, never by assuming a timed-out write failed. |
| Claim operation | deployment, operation, owner, lease duration; live scheduler lease additionally required for `scheduled` -> one typed progress row and claim, or no eligible work | Atomic indexed selection, row lock/`SKIP LOCKED`, then fenced claim update; scheduled claims also verify scheduler authority before any queue mutation. |
| Checkpoint operation | current claim and validated continuation -> committed progress revision | Verify exact scope/incarnation/generation/token, unexpired lease and expected progress revision; atomically update continuation and release. |
| Release failed operation | current claim -> released unchanged continuation, next retry time | Same fencing; never clear another owner's claim. Expired claims recover by takeover, not unfenced release. |
| Prune inactive progress | deployment, completed discovery generation, bounded limit | Delete only eligible finished, unclaimed records proved absent; return bounded count/continuation. |

Names may follow existing provider naming conventions during implementation;
the input/output semantics and error distinctions above are mandatory.
Use existing `BackendUnavailable`, `ImmutableViolation` and `LeaseExpired`
categories where applicable. No new public HTTP error format is introduced.

Within each deployment/operation, assign a strictly increasing database-backed
service ticket at insertion and at every successful claim. Select the eligible,
unleased row with the smallest ticket; move it to the tail **when claimed**,
not only after success. This prevents a failing/crashing workspace from
continually remaining first. Registration cannot move an existing row back to
the head, and new work joins the tail. Persist ticket allocation and claim in
one transaction; an index covers deployment/operation/service order and
eligibility. Gaps in tickets are harmless; wrap/reuse is not permitted.

For a fixed set of N eligible, unleased rows, with one controlled test worker
and advancing retry time, every row must be claimed within N successful claims,
even if every page returns more work. This is a turn-based fairness assertion,
not a wall-clock service-level objective. Multiple replicas must never own the
same unexpired row concurrently. New registrations and completed claims get
later tickets, so they cannot repeatedly jump ahead of older eligible work.
Expired leases and database locks may delay service and must be observable.

The operation lease is 360 seconds for an existing page deadline of at most
300 seconds plus query/checkpoint reserve; its configured maximum is 3,600
seconds. Discovery uses a 30-second lease for a pass bounded to 15 seconds.
The provider validates finite positive durations and caps them; activated
runtime settings reject budgets that do not cover the page and checkpoint
deadlines. Work must stop at its existing deadline and cannot continue effects
on the strength of a progress lease. Successful pages become eligible after
the existing receiver interval (one second by default); failed pages have a
five-second retry floor. Tests use a controlled provider clock, not sleeps.

Progress fencing protects only continuation ownership. Scheduled effects still
require the deployment scheduler lease, delivery still uses immutable snapshots
and downstream deduplication, and pull effects remain Connector-owned.
Before claiming **scheduled** progress, the runtime acquires or force-renews
`SchedulerLeadership`; a standby waits without claiming, moving tickets or
postponing eligibility. The provider also verifies the supplied lease's
deployment, holder, epoch and database-time expiry atomically with the scheduled
progress claim, so expiry between the preflight and transaction cannot let a
standby consume a turn. Lock scheduler authority before progress rows with a
consistent ordering. A missing/stale/expired scheduler lease rejects the claim
without queue mutation. Non-scheduled operations require no scheduler lease.
Retain every existing effect-time scheduler check/heartbeat. If leadership is
lost after claiming, stop the page and fenced-release unchanged progress with
the failure retry floor, or let the progress lease expire if storage is down.
This finite recovery delay must not become a loop of standby postponements.
A lost checkpoint response
requires reading the current row on a later claim; never submit an old position
unconditionally. A crash after an effect but before checkpoint may repeat that
page, using the same existing event/target identity. No exactly-once claim is
made, and progress must never advance before the page's acknowledged outcome.

Each process registers at most 50 discovery IDs per page, with at most one
additional backend lookahead ID (51 distinct IDs materialized by discovery),
plus one claimed continuation per operation, not a map or task per workspace.
The durable bound is at most four
progress rows per discovered workspace per deployment plus one discovery row;
retained history scales with workspace count in PostgreSQL, not process memory.

##### Retention, recovery, security and diagnostics

Retain unfinished continuations without time-based eviction. A bounded cleanup
may remove a row only when its continuation is null, no lease is live, it was
not seen in a fully completed later discovery sweep, and it has been idle for
at least 30 days. Recheck all conditions atomically against concurrent discovery
and claims. Deletion and recreation use a new incarnation so old tokens cannot
write through an ABA race. A workspace absent while still unfinished is retained
and alerted; no automatic deletion invents completion. An operator retires a
deployment only after admission is disabled, tasks drained and its retained
work assessed; do not delete all rows merely because a Pod disappears.

Unsupported/corrupt continuation is a readiness/diagnostic failure with a bounded
reason code, not an empty cursor. A known workspace-scoped delivery cursor whose
referenced payload was legitimately removed by existing retention is different:
the operation must confirm absence through the authoritative store, emit a safe
`retained-source-absent` diagnostic, and checkpoint an after-selection position
using that operation's existing cursor codec. It must not skip a malformed
cursor or recreate an expired payload. Test this exact race natively.

No new service identity, public route, broker, secret, credential exchange or
network port is introduced. Use the existing Trigger-to-PostgreSQL connection,
private network policy and runtime metadata role; migration permission remains
with the migration Job. Every read/write is deployment- and workspace-scoped.
Cursor metadata may contain tenant identifiers, so it remains under the same
encryption/backup/sovereignty policy as metadata and is never exported verbatim.
No workspace, cursor or claim token becomes a metric label.

Keep existing receiver pass counts/durations and add bounded operation/outcome
metrics for discovery, progress checkpoint failure, oldest eligible age and
claim expiry. Existing durable source/dispatch audits remain the longitudinal
per-workspace usage/adoption history; scan checkpoints are current operational
state, not a replacement audit history. Measure additional rows/bytes, indexed
query latency and database writes per pass for the proposed N=51/200/1,000 tests.
There is no new price or committed currency estimate; apply operator database
rates to the measured overhead.

##### Proposed operator output (success and error)

These are illustrative summaries from existing readiness/metric surfaces, not
a proposed new command or public response schema. No live tenant data is shown.

```text
Receiver progress: ready
Discovery: 51 workspaces registered
Delivery fairness probe: 51 of 51 workspaces served in 51 claims
Restart probe: committed target position retained
```

```text
Receiver progress: unavailable
Reason: migration-required
Action: keep receiver admission disabled; apply metadata revision 16;
        deploy compatible providers and recheck readiness.
```

##### Validation, rollout and human gate

Acceptance tests are approved requirements; this design does not itself certify implementation:

1. Register 51, 200 and 1,000 always-continuing workspaces. Discovery must pass
   the initial 50 IDs without waiting for any workspace to finish. After
   registration and with all rows eligible, every operation visits all N within
   N successful claims. Measure the 50-ID returned page/51-ID production fetch
   and four-claim bounds exactly, including the adapter boundary. Persist a
   discovery cursor, recreate the provider, and verify its next SQL keyset
   query resumes after that ID rather than fetching all N and slicing locally.
2. Use real producer cursors: a 1,000-target delivery selection crossing the
   32-target pass budget; more than 50 due schedules retaining the cutoff;
   retention pagination; and finite plus continuously replenished backlogs.
   Repeat across eviction from process memory, new provider instances and two
   PostgreSQL connections. No tail target is perpetually reset to page one.
3. Race claim, checkpoint, failure release, lease expiry and fresh takeover.
   Stale/expired/wrong-scope/incarnation writes fail. Crash before/after effect
   and before/after checkpoint; inject a lost commit response. Verify unchanged
   effect identity, no false acknowledgement and resumed committed progress.
   For scheduled work, run a leader and a competing standby against the actual
   scheduler/progress providers. Repeated standby scans must not acquire or
   postpone rows. Assert actual scheduled pages/occurrences advance for every
   eligible workspace, not just successful claim counts. Expire leadership
   between preflight and claim (no queue mutation), then after claim/before
   effect (existing fencing rejects the effect); verify the new leader resumes
   the unchanged continuation after fenced release or progress-lease expiry.
4. Test restart during discovery, duplicate page commit, deletion/recreation,
   legitimate source retention, invalid/oversized/versioned state and schema 15
   rejection for activated receivers. Off-mode/manual/internal behavior stays
   unchanged. PostgreSQL cases must execute in hosted CI, not be skipped.
5. #1017 tests actual lifespan composition, bounded cancellation/join, defaults,
   activation guards and all four Helm profiles; #1018 remains the deployed
   saturation/restart/partition certification gate. #1019 documents metrics,
   safe maintenance, schema upgrade and rollback; it is not duplicated here.

Apply the additive schema before compatible provider/service binaries. Disable
receiver admission and drain old in-memory workers before enabling the new
progress protocol; old and new receiver workers must not operate together.
Preserve accepted source records and identities throughout. If new code must
roll back, first disable/drain it, keep the additive progress tables, and keep
background admission disabled on old binaries. Do not silently downgrade an
active deployment to the starvation-prone runtime or destructively down-migrate.
A later compatible rollout resumes the retained state.

Risks: added database writes (measure indexed claim/checkpoint cost), stale
owners (fencing and expiry), growing inactive state (bounded conditional cleanup),
corrupt state (fail closed), tenant confusion (scope tests), and restart
duplicates (existing effect identities). These mitigations supplement E-N.
No new commercial/customer-evidence claims or delivery date are introduced.

**Approval gate satisfied:** the owner approved this concrete D08 design and the
revised 19-item breakdown before authorizing #1045 Build. One prerequisite implementation
pull request comes first; after its gated merge, integrate main normally in
#1044's owned checkout and finish #1017. This plan PR does not close #1045,
#1017 or #988. Its approved merge does not waive implementation review gates.

#### D09 - Bounded file-backed execution bindings (proposed)

**Scope and gate.** [TS-IMPL-041 / #1048](https://github.com/toddysm/custos/issues/1048)
removes the measured configuration ceiling before #1018's full-cardinality
managed Workflow-effects workload. This is a quality/capacity amendment to the
current product requirements (S5), not a new identity service. Approval of this
revision and its 20-item breakdown must precede implementation. Keep the parked
#1018 worktree/claim intact; implement 041 separately from current main only
after its own Build authorization. #1019 remains the final documentation item.

**Chosen source contract.** Extend the existing shared
[execution loader](../../src/libs/custos-callctx/src/custos_callctx/execution.py);
reuse its closed `bindings`/`defaults` JSON structure and immutable registry.
Add the operator-only environment key `CUSTOS_EXECUTION_BINDINGS_FILE`, whose
value is an absolute filesystem path, not file contents, and its required
companion `CUSTOS_EXECUTION_BINDINGS_SHA256`, the lowercase 64-hex-character
SHA-256 digest of the exact file bytes. The existing
`CUSTOS_EXECUTION_IDENTITY_ENABLED` gate remains authoritative:

| Enabled gate / configured sources | Result |
|---|---|
| Absent or `false` | Existing off-mode behavior: no registry loaded; unused source settings are ignored. |
| Invalid boolean | Existing explicit configuration error. |
| `true`, inline key only | Existing `CUSTOS_EXECUTION_BINDINGS` parser and 65,536-byte limit. |
| `true`, file key only | Require the companion digest; read, verify and validate one bounded file snapshot. |
| `true`, both source keys present | Explicit ambiguous-source error, even if one value is empty; never choose precedence or fallback. |
| `true`, neither key, or the selected value is empty/blank | Explicit missing/invalid-source error; no default registry. |

When enabled, reject a missing/blank/malformed file digest, a digest mismatch,
or a digest key supplied with the inline source. Digest verification is an
integrity/snapshot check, not a replacement for account authorization.

File input is a regular file, including Kubernetes projected-file symlinks.
Reject directories and special files without waiting on a pipe/device. Open one
descriptor and read at most **1,000,001 bytes**; reject if more than
**1,000,000 bytes** are present. Verify the digest against those same bytes,
then decode strict UTF-8 before JSON validation; do not reopen the path.
Whitespace counts toward the byte limit; it is not trimmed to admit an oversized
source. Reject malformed UTF-8/JSON, duplicate keys, unknown fields, duplicate
workspace/binding pairs, invalid identifiers and defaults outside their binding's
workspace using the same closed validation as inline input. The existing
**1,024-binding** count and **120-character** identifier limits remain.
Non-ASCII identifiers remain supported within the byte limit; 1,024 entries are
a count ceiling, not a promise that every multibyte configuration fits.

The implementation must separate the common semantic parser from source-specific
size checks; it must not globally raise the inline parser's 64 KiB limit as a
shortcut. No REST, remote procedure call, queue envelope, persisted identity,
schema revision or idempotency key changes. The JSON structure is unchanged;
this is an additive deployment-input source, not a new version of durable data.
No new package, secret-store implementation or hot-reload mechanism is needed.

**Managed provisioning and Helm storage.** Helm 3 stores supplied values and
rendered manifests in its release Secret. A ConfigMap/file limit alone cannot
bound that release: the registry must not occur in either location in file mode.
Keep `global.receiverRuntime.executionBindings` and its existing **65,536-byte**
managed inline path unchanged for compatible small deployments. File mode is
explicitly selected with a new `global.receiverRuntime.executionBindingsBundle`
descriptor; reject it together with a nonempty legacy registry. An empty legacy
default object is not a second source. Never automatically fall back between
modes or increase the legacy chart guard.

The descriptor has only `configMapName`, `sha256`, `bundleSha256` and
`daprConfigurations`, the latter mapping exactly the four consumer service names
to versioned Dapr Configuration names. Digests are lowercase SHA-256; object
names obey Kubernetes name limits. Validate its closed shape at render time.
Do not pass registry entries, execution Secret-name lists, generated bulk
manifests or an extra copy under another values key through Helm, including
`--set-file`, hooks or a bundle subchart. Only the compact descriptor belongs in
release values; release manifests contain references, mounts and annotations.

Implement one repository-owned operator preparation helper as part of 041,
reusing the production parser and existing managed authorization rules rather
than hand-maintaining a second account-policy specification. It accepts a
bounded local registry file and the deployment's namespace/release/security
inputs. It validates the exact bytes, computes the registry digest, and emits:

- One namespaced ConfigMap with `immutable: true` and exactly one data key,
  `bindings.json`, preserving the validated file bytes.
- For each of Gateway, Trigger, Workflow and Activity Runtime Manager, a
  versioned Dapr Configuration, Secret Role and RoleBinding: the same
  default-deny caller policies, Secret allow-lists, exact ServiceAccount subjects,
  and `get`-only, named-Secret rules currently generated by
  [receiver resources](../../deploy/helm/custos/templates/receiver-runtime.yaml).
  Preserve database/broker Secret allowances in Dapr and the separate
  [shared component Secret permissions](../../deploy/helm/custos/templates/dapr-secret-rbac.yaml).
  No wildcard, Secret enumeration or cross-namespace permission is added.
- The compact descriptor for the consuming Helm release.

This moves the registry-dependent authorization lists outside Helm too; moving
only the ConfigMap would leave four repeated Secret allow-lists/resource-name
lists in release storage. In file mode the chart must not render duplicate
Configurations/Roles/RoleBindings for these four consumers. Other managed
consumers' authorization resources and all inline-mode resources stay chart
owned and behaviorally unchanged, except for the explicit migration retention
opt-in below. Extract/reuse the existing policy generation so the inline and
bundle modes cannot diverge silently.

Bound input to 1,000,000 bytes and 1,024 entries; independently check complete
ConfigMap key-plus-value data at **1,000,013 bytes** or less, below Kubernetes'
**1,048,576-byte** ConfigMap limit. Validate every generated authorization
resource against Kubernetes/Dapr schemas and actual hosted API admission.
Never truncate, compress, shard or select a workspace subset to fit a limit.

Bundle names include a content-derived version, using existing naming limits.
The full bundle digest covers the exact registry digest and all effective
namespace, release, ServiceAccount and authorization inputs, with deterministic
serialization before deriving resource names. Annotate each object with the
full digest; detect shortened-name collisions rather than overwrite. ConfigMap
immutability is API-enforced; the helper treats all other bundle resources as
create-only: an existing object can be reused only after exact effective-content
and digest verification, never patched to represent a different bundle.

The operator creates the controller namespace and provisions/verifies this
external bundle **before** Helm install/upgrade. Missing resources, conflicting
objects or mismatched descriptor/security inputs abort that preparation.
Verify all bundle objects and their bindings, not only the ConfigMap; do not
rely on a missing Dapr Configuration producing safe default behavior. Bundle
resources have separate operator lifecycle ownership, not Helm owner references
or hooks. Helm install/upgrade/rollback and uninstall must not mutate/delete
them. Document preparation, verification, retained-version inventory and explicit
cleanup in the same 041 delivery; this is not another service or end-user API.

Project the descriptor's immutable ConfigMap read-only at
`/custos/execution/bindings.json`, without `subPath`, into exactly API Gateway,
Trigger Service, Workflow Service and Activity Runtime Manager. Set the file
path and expected digest environments, select each pinned Dapr Configuration,
and annotate each Pod template with both registry and bundle digests. Replace
conflicting managed source/digest entries while preserving unrelated operator
environment entries. All four connected/airgapped and evaluation/high
availability profiles use the same contract. Do not mount the registry into
activity sandboxes, Connector hook pods or non-consuming services. Update the
existing [managed helper](../../deploy/helm/custos/templates/_receiver-runtime.tpl),
receiver resources, four consuming [service charts](../../deploy/helm/charts)
and existing render tests.

The external resources contain references, never credentials or signed contexts.
The file mount adds no application Kubernetes API permission, service identity,
network path, port or egress rule. Existing operator resource permissions govern
bundle creation. Versioned RoleBindings coexist during rollout/rollback, so a
ServiceAccount temporarily has the union of **explicitly named** Secrets in
retained bundles, not arbitrary Secret access. Keep old references available for
pinned work, audit that overlap, and remove obsolete permissions only during
verified bundle cleanup. This lifecycle cost is explicit, not a claim that no
additional resource objects are created.

```text
Operator-owned registry file + existing deployment/security inputs
  -> bounded preparation/verification outside Helm
  -> immutable versioned ConfigMap + versioned Dapr/RBAC resources
  -> compact names/digests descriptor only -> Helm release
     -> pinned read-only file and Dapr configuration in four consumers
        -> digest-verified bounded loader -> immutable startup registry
           -> original identity -> existing Secret/Auth boundaries
Legacy inline Helm values -> unchanged managed 64 KiB path
```

**First inline-to-file migration: staged legacy retention.** Merely omitting
legacy chart-owned resources in the first file-mode release would let Helm
delete fixed-name Dapr Configurations still referenced by old Pods. Retaining
new versioned bundles alone does not protect a restarted old sidecar. Therefore
an existing inline deployment must complete this two-upgrade handoff:

1. Pause admission and drain. Install compatible binaries and a **bridge
   release**, still using the same bounded inline registry, service identities
   and effective authorization policy. A new boolean
   `global.receiverRuntime.retainLegacyExecutionResources` defaults to `false`
   and is valid only in inline mode. When explicitly `true`, it adds
   `helm.sh/resource-policy: keep` to exactly the four consumers' existing
   Configurations, execution Secret Roles and RoleBindings (12 resources).
   Do not rename or change their policy/subjects during this bridge step.
   Normal inline deployments keep their existing lifecycle when the opt-in is
   false.
2. Before producing/applying a first file bundle, the preparation helper must
   inspect the current Helm release and **stored bridge manifest**, not only
   live object annotations. Verify that all 12 legacy resources carry `keep`
   in that stored manifest, remain present with matching effective live
   content, and match the registered service identities. Record their names,
   unique identifiers, content digests and bridge revision in the operator's
   retained-resource inventory. An inline predecessor without this verified
   bridge is an explicit preflight error; never proceed directly to file mode.
3. Provision/verify the new versioned bundle, then upgrade to its compact file
   descriptor with the retention flag false. Helm omits the legacy resources
   but preserves them because the **previous stored release** marked them
   `keep`. Do not delete, rename, repurpose or strip their Helm ownership
   metadata. Old Pods keep their original inline environments and fixed-name
   Dapr policy even if either their application container or sidecar restarts.
   New Pods use the versioned file bundle. Verify every consumer before
   resuming admission.

Preparation/verification is a required automated step before deployment, not
an optional manual annotation recipe. A fresh file-mode installation has no
legacy resources to hand off; subsequent file-to-file upgrades retain the
versioned lifecycle above. Missing/changed legacy resources or an unverified
bridge abort migration instead of assuming sidecar defaults are safe.

After Helm omits the kept objects, the operator owns their retention/cleanup
lifecycle; preserve their release metadata so a compatible bridge rollback can
reuse them. Rollback to that bridge may restore chart management only with its
original policy and the same valid bounded identities. Do not select an
inline-only target for an oversized registry or after accepting work whose
identities the target cannot resolve. Keep the legacy objects while any old
Pod, supported retained rollback revision or retained work needs them, including
across uninstall; remove them only through the same verified cleanup protocol
as versioned bundles. This handoff is an intentional, opt-in deployment-resource
ownership change, not automatic garbage collection.

**Snapshot and authority.** Each process loads once during its existing lifespan
initialization, before advertising readiness or starting receiver admission.
A Pod always references one immutable ConfigMap version and one expected
digest. A restarted container in an old Pod must reload the old bytes even
during an upgrade; a replacement Pod loads only its newly pinned version.
Reject digest mismatch before readiness/admission, including tampered local
file mounts. Updating the compact descriptor rolls every consumer; it does not
mutate a retained bundle. Rollback restores old names/digests, so a reused old
Pod or a newly created rollback Pod loads the same old snapshot. Do not add a
background watcher. A successful parse does not authorize an account: existing
credential lookup, disabled/revoked-account/grant checks, fresh signed contexts and
application/namespace checks remain at their existing boundaries.

Defaults choose identities for new work only. Retained targets, runs, child
execution and resume waits resolve their pinned workspace/principal/binding;
changing a default cannot retarget them. Removing/rebinding an original identity
fails closed rather than adopting the new default. Credential rotation follows
the existing Secret/issuer contract; the reference file is not a credential
cache. No plaintext material, file contents or full registry enters error output,
logs, traces, workflow history or audit. Closed-field checks reject added
credential fields; operators must still keep actual credentials out of
reference-valued fields.

**Error handling and operator output.** File I/O, type, digest, decode, size and
validation failures abort enabled initialization and follow the existing
service startup/readiness error path. Do not catch them and return an empty
registry, fall back to inline, or mark the service ready. Diagnostics may report
the source kind, safe reason code and aggregate byte/binding counts, not content
or per-workspace metric labels. The proposed success/error output is rendered in
Part I.7A. The operator helper reports safe preparation/verification failures;
no new service endpoint or `custosctl` command is introduced.

**Measured budget and success criteria.** A compact synthetic registry with all
five reference fields at the existing maximum of 120 ASCII characters and an
explicit default for every workspace has these measured JSON byte counts:

| Workspaces / bindings | File bytes | Conservative key + value bytes |
|---|---:|---:|
| 51 | 47,304 | 47,317 |
| 200 | 185,427 | 185,440 |
| 1,000 | 927,027 | 927,040 |
| 1,024 | 949,275 | 949,288 |

These are local serialization measurements, not file-loader/deployment results;
the new source does not exist yet. The 1,000,000-byte cap admits those fixtures
with ConfigMap headroom while retaining a bounded allocation and count limit.
Those initial padded fixtures are repetitive and **do not** prove compression
or Helm release safety. A separate deterministic synthetic model, using 1,000
bindings with independently generated 120-character lowercase-alphanumeric
references (seed 1048), reproduces a 927,027-byte registry and **1,561,348 bytes**
for Helm's base64-of-gzip release value when both values and a rendered ConfigMap
contain it. That is already larger than the **1,048,576-byte** Secret data
limit before counting other release resources. This is a serialization model,
not an actual installation result. Externalize the authorization lists as well;
do not infer storage safety from compressible fixtures or file size alone.
Record parse duration and peak memory for the documented fixtures during 041;
no unmeasured latency/service-level guarantee or currency estimate is added.
There is one bounded startup read per consumer, not one read per event. Use
existing startup diagnostics and rollout history for source mode/snapshot
adoption; existing workspace-scoped subscription/dispatch audit records continue
to measure tenant usage, stop events and frequency over time. Do not duplicate
that data or introduce customer identifiers as metric labels.

**Validation and traceability.** S5/#1048 map to the shared file source, managed
projection and startup snapshot; S1-S4 retain D01-D08 authority and wire formats.
The same 041 implementation pull request covers the library, every consuming
service's startup boundary, all four profile projections, directly related
configuration/provisioning docs and these tests:

1. Use real temporary/projected files through the production loader. Test
   1,000,000 bytes accepted and 1,000,001 rejected, with valid JSON padded to
   the boundary; 65,536/65,537 for legacy inline; 1,024/1,025 entries;
   UTF-8 byte rather than character counting; malformed/duplicate/unknown/
   cross-scope data; missing/unreadable/special files; ambiguous/empty sources;
   absent/blank/malformed/mismatched expected digests; valid projected symlinks;
   and unchanged disabled behavior. Preserve source/callable documentation.
2. Render all four managed profiles for 51/200/1,000 distinct workspaces, with
   realistic references and **nonrepetitive** maximum-ASCII fixtures, including
   1,024 bindings. Feed exact externally generated ConfigMap bytes through the
   real loader. Assert conservative data budgets, required mounts/digests/Dapr
   names, no bulk registry/authorization lists in Helm values/manifests, and
   no credential literals. Test exact/one-over helper budgets, multibyte and
   escaped serialization, closed descriptors, legacy inline rendering and
   ambiguous mode rejection. Compare both modes' effective caller/Secret
   policies and verify unrelated Secrets remain inaccessible.
3. Exercise Gateway, Trigger, Workflow and ARM startup with valid and rejected
   files; verify admission/readiness and owned-resource cleanup on failure.
   Re-run pinned-identity/default-change and credential/grant/rotation/restart
   regressions; no consumer may infer a missing workspace identity. Reject
   conflicting pre-existing bundle objects and invalid/missing authorization
   resources during preparation; verify same-bundle idempotent reuse.
4. Wire these cases into existing library/service Python 3.11/3.12 and Helm
   jobs, extending actual selectors if needed rather than claiming deselected
   cases ran. On disposable hosted `kind`, perform **real Helm 3 default-Secret
   storage install, upgrade and rollback** with nonrepetitive maximum-length
   1,000- and 1,024-binding bundles and the existing full managed manifests.
   Measure decoded Secret data bytes (including Helm's inner base64 release
   value) against 1,048,576; a render/model is not installation evidence.
   Confirm mounted bytes and each consumer's actual Secret access, including
   the database/broker paths. Restart an old Pod's application container during
   an upgrade, then roll back with both reused and recreated Pods; assert
   old/new snapshot/default selection and pinned-work identities. Test wrong
   digest, attempted immutable ConfigMap mutation, missing bundles, retained
   authorization and safe obsolete-version cleanup. Do not run local Docker or
   mutate existing clusters. Record exact tested commit, commands, results,
   skips and cleanup. Full deployed receiver-to-Workflow effects and saturation
   remain #1018, not 041.
5. Add a dedicated actual inline-to-file migration scenario: install a legacy
   inline release, prove direct migration fails preflight, upgrade to the
   same-policy retention bridge, inspect its stored Helm manifest, then migrate
   to file mode. Restart an **old sidecar** and application container while
   old/new Pods coexist; prove all fixed-name policies/Roles/RoleBindings remain
   usable, denied Secrets stay denied, and original identities survive.
   Exercise a same-identity small-registry rollback to the retained bridge and
   repeat file migration, plus refusal of an incompatible/oversized rollback.
   Test the retention flag's type/mode guards, missing/changed legacy objects,
   rollback metadata preservation and eventual verified cleanup. A
   file-to-file-only test does not cover this migration.

**Risk, rollout and rollback.** Main risks are oversized input, source ambiguity,
configuration drift, Helm release storage, legacy-resource deletion at migration,
external-resource lifecycle,
mixed binaries/chart capabilities and loss of old identity references.
Bounded reads/counts, closed validation, externalized bulk resources,
the verified retention bridge, digest-verified version-pinned snapshots and
retained identities mitigate them. File
references remain inside the deployment's existing sovereignty boundary; no
new customer-data retention, external service or schema migration is introduced.
Explicit retained-version Secret access and cleanup are described above.

Ship compatible shared-library/service binaries first, with the old inline
chart and existing 64 KiB behavior. Do not enable file mode on binaries that
cannot read it. Pause receiver admission and drain before changing the managed
source mode. Prepare/verify the external bundle, then deploy its compact
descriptor with compatible chart/binaries; verify all four consumers' actual
snapshots, Secret access and readiness before resuming. Keep prior versions
untouched. For an existing inline release, complete and verify the retention
bridge above **before** the file-mode upgrade. Test a descriptor-only update
rolls every consumer without hot reload.

For rollback, pause admission and drain first. Roll back to the last known
compatible chart/binary/configuration set, including its exact external bundle,
and retain its ConfigMap, authorization resources and original credential
bindings. Do not reuse a resource name for different contents. Cleanup is an
explicit operator step only after no live Pod, retained Helm rollback revision
or retained work requires that bundle or its permissions; Helm uninstall alone
is not permission cleanup. A registry
above 64 KiB **cannot** safely roll back to the old inline-only contract: keep
admission paused on compatible file-aware binaries until a validated
same-identity rollback configuration exists. Never drop bindings or reassign
workspaces to fit a ceiling; never weaken readiness or authorization to obtain
a green deployment.

The earlier receiver-disabled
[baseline run](https://github.com/toddysm/custos/actions/runs/42131707104)
failed Helm installation with ARM readiness reporting an unconfigured activity
scheduler; its disposable `kind` cleanup succeeded. That independent deployment
configuration finding remains for #1018. File-capacity work must not claim to
repair it without a separate measured check. No full receiver certification has
occurred. After approved 041 merges, resume 035, then 036 and the reviewed
inventory close-out. D09 does not close #1048/#1018/#988 on design merge.

Alternatives rejected: raising only the environment limit leaves oversized
environment/manifest transport and mismatched chart guards; disabling identity
or sharing an ambient account breaks D06; testing claims without real effects
does not certify #1018; compression/sharding/live reload adds unnecessary new
contracts. D09 changes configuration transport and deployment-resource ownership,
not authority selection or credential issuance.

#### Components and flow

**Owner-approved issuer extension (2026-11-02T20:56:34.079-08:00).** Within
[#1038](https://github.com/toddysm/custos/issues/1038), Connector Service owns
real credential issuance through its existing identity resolvers. The sidecar
must not select `StubCredentialMinter` in production or acquire general provider
privileges. This replaces the earlier sidecar-to-provider ownership assumption.

Authorize every issuance/refresh/release for the original workspace, execution
principal, run, step, attempt, slot and requested capability. Obtain that scope
from durable accepted execution/binding state, not caller-asserted instance IDs
or credential references. Expired, completed, revoked, mismatched and unresolved
attempts fail closed. Changed instance configuration must not retarget accepted
authority. Use fresh credential-backed service contexts from #1037 on the
control-plane hop; sandbox bootstrap proof is limited to its own attempt and
never becomes a workspace-wide service-account credential.

Preserve actual resolver material and provider expiry. A platform lease expiry
is an authorization window, not a claim that a static upstream credential has
expired. Represent unknown/non-expiring provider metadata explicitly; do not
fabricate tokens, leases, credential expiries or stringify opaque credential
objects. Credential material is returned only through the private runtime/
sidecar boundary; durable records and audits retain references and sanitized
metadata, not plaintext credentials.

Complete and merge #1037's scoped execution contract before final integration
of this #1038 extension. Keep its owned worktree intact and do not copy its
unpublished implementation into this branch. Existing hosted review and merge
gates remain binding; the extension stays in the same #1038 work item.

```text
External source -> Gateway scoped webhook -> Trigger -> Connector VerifyWebhook
                                                        (credentials/plugins)
                                     verified events -> shared normalization
Connector push/pull -> existing publish channel -------> shared normalization
Fenced scheduler + pending fires ----------------------> normalized event topic
                                                               |
                                             classify/match/dedup/dispatch
                                                               |
                                                        Workflow Service
                                                               |
                                            durable failure store -> admin replay
```

Touch [Trigger Service](../../src/services/trigger-service),
[Connector Service](../../src/services/connector-service),
[API Gateway](../../src/services/api-gateway),
[storage interfaces](../../src/libs/storage-provider-layer),
[PostgreSQL adapter](../../src/libs/custos-postgres),
[Workflow Service](../../src/services/workflow-service), the
[shared call-context library](../../src/libs/custos-callctx) as a reused dependency,
the Auth Service permission registry, and [deployment](../../deploy/helm).
Provider reads remain asynchronous. The new lease, pending-fire, connector pull-control and dead-letter
entities require additive numbered migrations and explicit provider capability
checks; missing production capabilities fail readiness.

### PM-1. Requirements traceability

| Requirement/scenario | Design | Work items |
|---|---|---|
| REQ-005, S1 | D04, persistent schedules/leases/pending fires | 022-025 |
| REQ-006, S2 | D01/D02, connector verification, webhook ingress | 026-028 |
| REQ-079, S2/S3 | Shared envelope adapter; D03 single polling owner | 023, 028-029 |
| REQ-081, S2/S3/S4 | Shared start/resume matching; D05 downstream durability | 028, 031, 035 |
| #988 dead-letter scope, S4 | D05, durable storage, protected recovery | 030-033 |
| REQ-036/037/038 | Signed contexts, D06 execution identity, no secret persistence, durable audit | 026-027, 032-034, 038 |
| Preserve REQ-004/080 | Existing manual/internal routes and regression gates | 022, 034-035 |
| Operator enablement | Deployment/rollback and final documentation | 034-036 |
| SP1-SP3, bounded multi-tenant recovery | D08 durable progress, independent discovery, fair fenced claims and restart-safe continuation | 040, 034-036 |
| S5 / #1048, managed 1,000-workspace configuration | D09 bounded verified file, external versioned registry/authorization bundle, compact Helm references and restart-safe rollback | 041 before 035; final docs 036 |

### PM-2. Interfaces and versioning

Retain `/v1` public routing, existing camelCase serialization and existing
component-specific error envelopes. Do not introduce a new `api-version`
parameter into a repository that versions these routes by path. Connector and
trigger event envelopes retain `schemaVersion: "1"` with an explicit adapter
from `eventType/payload/source.workspaceId/instanceId/deliveryMode` to
`kind/data/source`. No producer field is silently renamed.

Subscription create/patch add a typed `sourceConfig` object:
`connectorInstanceId` for connector-backed types; exactly one of `cron` or
`intervalSeconds` for scheduled sources; optional `timezone` for cron;
`pollIntervalSeconds` for pull. Required combinations are validated against
`sourceType`; older manual/internal requests remain valid. Return persisted
configuration on read. Reject unsupported fields/combinations with 422.

Internal `POST /VerifyWebhook` is versioned by `schemaVersion: "1"` and carries
`workspaceId`, `connectorInstanceId`, `bodyBase64` and bounded `headers`.
It is callable only through the permitted service identity and is
side-effect-free. A verified, structurally valid batch returns:

```json
{
  "schemaVersion": "1",
  "receivedCount": 2,
  "events": [{"index": 0, "event": {"schemaVersion": "1", "eventId": "evt-1", "eventType": "registry.push", "source": {"workspaceId": "team-a", "instanceId": "registry-a", "deliveryMode": "push", "receivedAt": "2026-10-27T20:00:00Z"}, "payload": {"repository": "example"}}}],
  "rejections": [{"index": 1, "code": "missing-event-id"}]
}
```

The envelope inside each success item remains the existing connector envelope.
Each original batch index appears exactly once in `events` or `rejections`;
`receivedCount == len(events) + len(rejections)`. Rejections contain only a
zero-based index and an allow-listed reason code, not raw payloads, headers,
secret material or arbitrary exception messages. Valid events retain input
order. A well-formed verified batch returns this shape even if all events fail
normalization, allowing Trigger to count and audit each rejection.

Invalid/missing signatures return 401; unknown/inactive pair 404; malformed
top-level verified payload (invalid JSON, invalid batch container or empty
batch) returns 400 without claiming per-event acceptance; unavailable
credential resolver/verifier returns 503.
A one-MiB raw body expands in base64; cap the internal encoded request at two
MiB and headers at 16 KiB, rejecting larger requests before parsing.

Generic webhook success is `200 {"accepted":N,"rejected":M}` after durable
publication acknowledgment, not a promise that workflows have finished.
Trigger combines verifier rejections with any subsequent canonical-adapter
rejections, preserving original indices and sanitized reason codes for audit.
`accepted + rejected` equals `receivedCount` for a completed batch. All-invalid
verified batches return 400 `trigger.webhook.no_valid_events`; mixed batches
publish the valid events and expose accurate rejected counts. Publication
failure returns 503, not partial-success counts, so the sender can retry.
Internal topic consumption acknowledges only after successful dispatch or
durable dead-letter capture; storage failure requests redelivery.

External resume registration uses the existing versioned internal registration
surface with additive `workspaceId: string` and `source` fields described in
Part II.D. `source.deliveryModes` is a non-empty subset of `push|pull`;
`pollIntervalSeconds` is an integer at least the connector floor and is valid
only when pull is requested. Missing workspace/selector, unsupported mode,
invalid interval or cross-workspace association fails registration explicitly.
Source fields are immutable for an existing deterministic registration key;
a divergent re-registration preserves the original and emits the existing
divergence audit rather than silently reattaching a wait to another source.

Dead-letter routes under `/v1/workspaces/{ws}/triggers/deadletter`:

| Operation | Contract |
|---|---|
| GET collection | Optional `subscriptionId`, `status`, `since`, `until`, `cursor`, `limit`; returns `{items, nextCursor}`. |
| GET `/{id}` | Returns workspace-scoped entry: IDs, original normalized event/target/key, history, status, timestamps; no credentials. |
| POST `/{id}:replay` | No body; returns `{status}`. Existing successful replay is idempotent; active claim or unsafe replay returns 409. |
| POST `/{id}:discard` | Optional `{reason}`; 204; already-discarded is idempotent, active replay returns 409. |
| POST `:replay` | Filters plus bounded `limit`; returns `{accepted,rejected}`; per-entry durable history records results. |

All require `trigger:admin`: missing/invalid identity 401, insufficient permission
403, unknown/cross-workspace entry 404, invalid filter 422, provider outage 503.
Mount static dead-letter routes before dynamic subscription-ID routes.
Terminal Workflow responses must be reconciled, not automatically described as
successful resume delivery. Preserve the original target on replay even if the
subscription has since changed; require operator action rather than retargeting.

### PM-3. Telemetry, usage and cost

Use existing telemetry/audit providers. New counters cover receiver outcomes,
signature failures, skipped schedule occurrences, replay results and expired
entries; gauges cover pending-fire age, cursor lag and dead-letter backlog.
Persist subscription state transitions and receive/dispatch/replay audit events
with workspace and source type so operators can measure adoption, active versus
stopped subscriptions and daily dispatch frequency over their audit retention.
Do not expose tenant identifiers as metric labels.

Cost is measured from CPU-seconds, memory high-water mark, PostgreSQL rows/bytes,
broker deliveries and upstream poll calls for the documented workload. Apply the
operator's actual infrastructure rates; no fabricated currency estimate is
included. Reusing one poll loop per instance is a measurable cost guardrail.

### PM-4. Identity and networking

Reuse the shared signed-context verifier and existing key discovery. Register
`trigger:admin` in Auth Service; do not grant it to ordinary workflow runners.
Gateway terminates public TLS; east-west traffic uses Dapr identity, its app-ID
allow-list and existing cluster network controls. Database access stays on the
existing internal provider connection. Only Connector plugins contact upstream
sources and resolve signing material. No new public internal-verifier service
or additional cloud network dependency is introduced.

### PM-5. Error handling and edge cases

Malformed input fails before persistence; invalid signature fails before parsing
vendor content. Backpressure and dependency failure surface explicit retryable
errors. Poison broker messages are audited and rejected, not retried forever.
Transient dispatch failures retain existing bounded exponential retry before
durable capture. Failure to persist that capture is not acknowledged.

Use atomic, ownership-checked state transitions for schedule fencing,
dedup reservations and replay claims. Crash/restart may redeliver the same
identity but must not silently lose accepted events. Dead-letter lifecycle:
`pending -> replaying -> replayed|pending|discarded`; expired claim returns to
recoverable pending; retention emits `expired` then removes payloads. Validate
empty batches, maximum sizes, pagination, duplicate IDs, subscription changes,
expired resume waits and Workflow terminal-state conflicts.

### E. Risk checklist

| Risk | Mitigation |
|---|---|
| Contract compatibility | D01/D02 are explicit approval decisions; retain old route failure semantics and document migration. |
| Security/authentication | Exact bypasses, signed administration, per-instance verification, no ambient external bearer. |
| Privacy | Credential stripping, bounded payloads, workspace isolation and finite retention. |
| Infrastructure/dependencies | Reuse existing Postgres/Dapr; capability checks before enabling receivers. |
| Deployment/migration | Additive schema first, receivers disabled, gradual enablement. |
| Multi-tenancy/noisy neighbor | Bounded batches/concurrency, per-instance polling and scoped queries. |
| Regression | Gates per touched service plus deployed manual/internal/start/resume cases. |
| Replay safety | Durable downstream identity and sufficient retention; never infer safety from a reused string alone. |
| False activity readiness | D07 requires an initialized scheduler and successful dependency probes; liveness does not imply schedulability. |
| Bootstrap mismatch/key exposure | D07 tests actual issuer/verifier compatibility, exact attempt/expiry and private delivery; the activity cannot access the signing/verification key. |
| Configuration capacity/source ambiguity | D09 preserves inline limits, bounds file/ConfigMap data, rejects dual sources, externalizes registry/authorization bulk and tests actual Helm Secret storage for all managed consumers/profiles. |
| Snapshot drift, deleted legacy policies or stale permissions | D09 verifies a stored-manifest retention bridge before first file mode, pins immutable versions/digests, tests old sidecar/application restart and rollback, and requires verified legacy/bundle cleanup. |
| Rollback to an incapable binary/chart | D09 stages file-aware binaries first; pause/drain and retain pinned bindings; oversized registries cannot roll back to inline-only mode. |

### F. Breaking changes and compatibility

The proposed scoped URL is an intentional change from the previously documented
unscoped contract. The old gateway route currently has no completed M2 receiver;
keep its fail-closed behavior and provide explicit setup migration instructions.
D02 changes header handling only for the new route. Existing manual/internal
requests, connector event envelopes and workflow public contracts remain valid.
Provider additions require a coordinated internal library release and adapter
conformance; do not pretend an old adapter supports newly required capabilities.
D07 intentionally changes an unwired activity runtime from readyz 200 to 503,
including development configurations. The endpoint schema and liveness response
remain unchanged. Deployment and operator documentation must explain the blocked
rollout; tests must cover both unwired and initialized lifecycles.

### G. Security risk management

Threat cases: tenant confusion from colliding IDs; forged context/private
headers; signature verification after unsafe parsing; replay amplification;
unbounded payload/batch buffering; stale leader/replay writes; secrets in audit
or error bodies. Enforce the controls in D01/D02/D05 and require negative tests.
No cryptographic algorithm is invented; reuse verifier/plugin implementations
and constant-time comparisons. Rotation is a connector credential operation.

### H. Privacy and data sovereignty

Normalized payloads are potentially confidential. Keep them within the
deployment's existing storage/broker boundary, encrypted according to its
configuration. Do not export them to telemetry or review evidence. Default
dead-letter retention is 30 days across all statuses; backup retention follows
the operator policy and must be documented separately. No cross-region replica
or external analytics service is introduced.

### I. Operational observability

Readiness names unavailable required providers; startup reconciliation and
shutdown are observable. Alert on increasing pending-fire age, expired scheduler
lease without a successor, repeated verification failures, cursor stall,
dead-letter growth, old replay claims and retention failures. Include
correlation IDs and sanitized reason codes; never log raw verifier requests.
See Part I.11 for operator diagnosis and recovery.

### J. Dependencies

Reuse Dapr, PostgreSQL, existing provider interfaces and signed-context tooling.
A maintained cron parser may be added in TS-IMPL-025 only after checking its
license and Python 3.11/3.12 compatibility; pin the actual dependency then.
Do not implement an ad hoc cron parser. Dependency outages must preserve durable
pending work and expose readiness/dispatch errors. The DAG is in the linked plan.

### K. Deployment and buildout

Apply additive metadata migrations before deploying new code. Deploy compatible
provider/Workflow support, then connector verification/listening, then Trigger
and Gateway code with M2 receivers disabled. Reconcile registrations and verify
readiness before controlled enablement. Store proposed flags/settings in the
existing Helm profiles, not manual cluster edits. Existing subscriptions need
no sourceConfig backfill unless they enable an M2 receiver; connector pull-control
state is migrated explicitly as described in D03. Invalid legacy
incomplete M2 configuration remains disabled with an explicit diagnostic.

### L. Multi-tenancy

Workspace is part of every candidate query, key, replay operation and verifier
lookup. Administrative filters never broaden across workspaces. Reuse bounded
connector tick concurrency; do not create an unbounded task per subscription.
Process schedule and replay batches with bounded concurrency so a single
workspace cannot monopolize the event loop. This is not the coordinated
distributed rate-limiter project in #990.

### M. Regression

Storage: migration/conformance and existing entity tests. Trigger: manual,
internal, resume registration, selector and dispatch tests. Connector: listener,
cursor, pause, secret-resolution and publication tests. Gateway: non-webhook
authentication, raw forwarding and response shaping. Auth: permission registry
and signed-context validation. Workflow: start/resume idempotency and terminal
state behavior. Activity Runtime Manager: startup failure, truthful readiness,
admission/drain and ownership cleanup. Connector Sidecar: bootstrap scope,
signature, expiry and key/token isolation. Deployment: four Helm profiles and
a real deployed flow; mixed runtime versions remain disabled until compatible
bootstrap and execution-identity prerequisites are present.

### N. Rollback

Disable new ingress/background receivers before rolling back services; retain
pending fires, dead letters and downstream dedup records for roll-forward.
Do not destructively downgrade new tables or shorten dedup retention while
recoverable work exists. Preserve or pause connector cursors rather than
resetting them. Restoring legacy code does not make it safe to consume new
replay state; readiness/feature gates must prevent that combination.

### PM-6. Estimates and phasing

Preliminary engineer-day ranges, not delivery commitments: storage 4-7,
Trigger 8-12, Connector 3-5, Gateway 1-2, narrowly coupled Workflow durability
2-4, deployment/integration/docs 4-7. These include implementation and tests;
cross-service work overlaps, so do not simply add them as elapsed time.
The long poles are D05 crash-safe replay and production cross-service testing.
Re-estimate after design review; split an oversized task only with an updated
breakdown and renewed scope approval.

D08 planning estimates below are provisional engineer-day ranges, not dates or
owner commitments. They assume the existing PostgreSQL test harness and no new
infrastructure; claim/cleanup concurrency and safe cursor restoration are the
long poles. Re-estimate after the first native race tests. Do not silently split
the single authorized prerequisite into additional issues.

| D08 component / phase | Implement | Test/review | Deploy/rollback preparation | Item |
|---|---|---|---|---|
| Provider contract, memory parity, PostgreSQL schema/claims | 2-4 | 2-3 | 0.5-1 | 040 |
| Typed scheduled scan snapshot/restore | 0.5-1 | 0.5-1 | 0.25-0.5 | 040 |
| Runtime discovery/operation integration and readiness | 1-2 | 1-2 | 0.5-1 | 034, after 040 merges |
| Profile/deployed saturation and failure certification | 0.5-1 | 1-2 | 0.5-1 | 034/035 |
| Operator/developer progress documentation | 0.5-1 | 0.5 | 0.25 | Existing 036 |

D09 adds one existing-architecture prerequisite, not another feature tracker.
Provisional effort for 041 is 3-4 engineer-days for the shared source,
bundle preparation/policy reuse, retention handoff and four-consumer/chart
wiring, 2-3 for boundary/consumer/profile/migration tests and review, and 1-2
for actual hosted Helm storage/restart/rollback validation and directly related
lifecycle docs.
These are planning estimates, not measured durations or delivery commitments;
re-estimate after the first mounted-file checks. Final operator documentation
remains 036. No implementation starts from this design PR.

### PM-7. Test plan and hosted CI

Each child includes unit/contract tests and relevant integration coverage.
Use existing Ruff, format, strict Mypy and pytest jobs with Python 3.11/3.12 and
the 90% Trigger coverage floor. Extend real PostgreSQL fixtures to call actual
provider/read paths. Add deployed receiver-to-Workflow tests, clock-controlled
lease/retention tests, and all D01-D06 failure/compatibility cases.

Review-added cases: durable pause before registration and across restart;
staggered two-Connector-replica ticks within one polling interval; source-scoped
resume without payload run/step IDs and with no start subscription; and
mixed/all-invalid verification batches with exact counts and sanitized reasons.
For D06, exercise the actual client against the actual binding handler, complete
request/response shapes, scoped identity/grants, expiry and refresh across
restart/child execution, rotation/revocation bounds, unchanged accepted identity
after configuration edits, and absence of credentials from durable diagnostics.
For D07, test readiness before initialization, at each startup failure and during
shutdown; resource cleanup and admission ordering; and the actual bootstrap minter
against the actual sidecar verifier, including scope, expiry and private delivery.

D08 uses synthetic scoped IDs and controlled clocks. Wire its provider tests
into the PostgreSQL native job in
[python-libs.yml](../../.github/workflows/python-libs.yml), and its Trigger
continuation/lifespan tests into both unit and PostgreSQL jobs in
[python-services.yml](../../.github/workflows/python-services.yml), on every
matching pull request. Extend
[helm-test.yml](../../.github/workflows/helm-test.yml) for capability/activation
projection. Inspect logs to prove native cases execute; deselection is not a
pass. The owned implementation must pass current-head applicable hosted gates
and settled reviews; no new CI system or scheduled production probe is required.

The plan-only pull request changes documentation; path-filtered service jobs
may not run. Report that as not applicable, not green runtime evidence.
Use normal hosted triggers without skip markers. Request Copilot review.
For this amendment and subsequent authorized work, request **only Copilot**;
do not request Codex or another hosted reviewer. D09's source/byte/count,
four-consumer, exact provisioned-file and actual Helm-storage/restart/rollback
checks above are mandatory for 041.
This three-document plan PR has no source or chart changes and does not
establish that those future tests pass.
Repository rules require resolved review threads and include code-quality and
CodeQL rules; inspect actual applicability/check results before any authorized
merge. Missing or pending required results remain blockers.

### O. Next actions and launch checklist

- Preserve the recorded baseline and amendment approvals in #988.
- Implement the approved #1037 prerequisite before #1017 profile activation.
- Complete #1038's startup/bootstrap prerequisite before #1017 activation.
- The revision-5 amendment and execution/runtime prerequisites merged through
  #1039, #1040 and #1041. Revision 7/D08 merged via #1046, and #1045/#1017
  completed through #1047/#1044. Their approval/evidence remain historical facts.
- Review proposed revision 8/D09 and the 20-item breakdown, including #1048;
  obtain explicit approval before design merge or separate 041 Build. Only
  planning publication is authorized now.
- Implement/validate 041 after its separate Build authorization, then resume
  #1018's deployed certification, including the independent baseline readiness
  configuration finding. Keep #1019 and reviewed inventory close-out required.
- Retain the authorized per-item branches/pull requests, hosted checks, Copilot
  reviews, fix-confirmation replies, gated merges and checkpoint-before-cleanup.
- Obtain renewed approval for any further material scope or identity-policy change.
- Complete every required child, including documentation.
- Merge a reviewed inventory close-out before closing #988.

### P. References

- [Trigger design](../components/trigger-service/design.md)
- [Connector design](../components/connector-service/design.md)
- [Gateway design](../components/api-gateway/design.md)
- [Storage design](../components/storage-provider-layer/design.md)
- [Implementation breakdown](../components/trigger-service/implementation-plan-m2.md)
- [Project inventory](../../docs/project-state.md)
