# Trigger M2 receivers - product requirements and engineering design

- Status: **Proposed; awaiting human approval. No implementation authorized.**
- Planning format: full; revision 2 (review refinements; still unapproved).
- Tracking: [#988](https://github.com/toddysm/custos/issues/988).
- Design work item: [#1004](https://github.com/toddysm/custos/issues/1004).
- Updated: 2026-10-27; baseline commit `e344ab2`.
- Validation: GitHub-hosted continuous integration (CI), plus Copilot review.
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

### 4. Outcomes, goals and scope

All five deliverables in #988 are must-have: four receiver types and durable
dead-letter/replay operations. Production storage reads, verification, signed
administration and the narrowly coupled Workflow idempotency contract are
prerequisites, not separate unrelated enhancements.

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

These are proposed wire contracts for approval, not output of implemented code.
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

### 8. Goals and measurements

| Goal | Baseline | Acceptance target | When measured |
|---|---|---|---|
| Receiver coverage | 0/4 M2 receiver paths connected | 4/4 receiver-to-Workflow start/resume scenarios pass | TS-IMPL-035 |
| Durable recovery | No persisted dead-letter entry | Failure survives restart; original-key replay succeeds | TS-IMPL-030/031/035 |
| Duplicate protection | Not established for M2/replay | Zero duplicate effects in the specified concurrency/crash tests | Each path and TS-IMPL-035 |
| Isolation | Missing new ingress/admin paths | All spoofing, wrong-workspace and unauthorized cases rejected | TS-IMPL-026/027/032/035 |
| Regression | Existing suites and coverage gate | Existing behavior preserved; Trigger coverage at least 90% | Every relevant code change |

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
selects at most 100 entries per request. These new bounds require approval.

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

D01-D05 below are **proposals awaiting human approval**. The design owner is the
planning work item [#1004](https://github.com/toddysm/custos/issues/1004);
implementation owners are the linked child issues.
Copilot findings can change a recommendation but cannot approve it on behalf of
the repository owner. A material change to scope or contracts invalidates any
earlier approval and updates both the plan and affected acceptance criteria.

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

This proposal completes Custos's clock and external-event ingestion using the
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
deduplication are explicit prerequisites. The design proposes resolutions for
five cross-service contract gaps, including the mismatch between 30-day
dead-letter retention and shorter or volatile downstream deduplication.
Implementation proceeds through native dependent work items, with tests on each
change and deployed failure/recovery coverage before enablement. No new broker,
polling engine or graphical interface is introduced.

### B. Prerequisites

The scope is [#988](https://github.com/toddysm/custos/issues/988). The original
decisions [#20](https://github.com/toddysm/custos/issues/20),
[#22](https://github.com/toddysm/custos/issues/22) and
[#23](https://github.com/toddysm/custos/issues/23) remain resolved.
Their implementations are not complete. The [M1 implementation plan](../components/trigger-service/implementation-plan.md)
does not cover this work. Explicit approval of this revision and the breakdown
is still required before its design pull request merges or any build begins.

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
reservation. A crashed tick leaves the cursor unchanged and can retry when
both its lease and shared due-time conditions allow it.

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

#### Components and flow

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
| REQ-036/037/038 | Signed contexts, no secret persistence, durable audit | 026-027, 032-034 |
| Preserve REQ-004/080 | Existing manual/internal routes and regression gates | 022, 034-035 |
| Operator enablement | Deployment/rollback and final documentation | 034-036 |

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

### F. Breaking changes and compatibility

The proposed scoped URL is an intentional change from the previously documented
unscoped contract. The old gateway route currently has no completed M2 receiver;
keep its fail-closed behavior and provide explicit setup migration instructions.
D02 changes header handling only for the new route. Existing manual/internal
requests, connector event envelopes and workflow public contracts remain valid.
Provider additions require a coordinated internal library release and adapter
conformance; do not pretend an old adapter supports newly required capabilities.

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
state behavior. Deployment: four Helm profiles and a real deployed flow.

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

### PM-7. Test plan and hosted CI

Each child includes unit/contract tests and relevant integration coverage.
Use existing Ruff, format, strict Mypy and pytest jobs with Python 3.11/3.12 and
the 90% Trigger coverage floor. Extend real PostgreSQL fixtures to call actual
provider/read paths. Add deployed receiver-to-Workflow tests, clock-controlled
lease/retention tests, and all D01-D05 failure/compatibility cases.

Review-added cases: durable pause before registration and across restart;
staggered two-Connector-replica ticks within one polling interval; source-scoped
resume without payload run/step IDs and with no start subscription; and
mixed/all-invalid verification batches with exact counts and sanitized reasons.

The plan-only pull request changes documentation; path-filtered service jobs
may not run. Report that as not applicable, not green runtime evidence.
Use normal hosted triggers without skip markers. Request Copilot review.
Repository rules require resolved review threads and include code-quality and
CodeQL rules; inspect actual applicability/check results before any authorized
merge. Missing or pending required results remain blockers.

### O. Next actions and launch checklist

- Publish the native child issues and this design pull request.
- Request Copilot review and reconcile findings.
- Obtain explicit approval of D01-D05, the document and the issue acceptance criteria.
- Separately authorize the concrete implementation branches, pull requests,
  review-fix replies, merges and safe cleanup before the build loop.
- Complete every required child, including documentation.
- Merge a reviewed inventory close-out before closing #988.

### P. References

- [Trigger design](../components/trigger-service/design.md)
- [Connector design](../components/connector-service/design.md)
- [Gateway design](../components/api-gateway/design.md)
- [Storage design](../components/storage-provider-layer/design.md)
- [Implementation breakdown](../components/trigger-service/implementation-plan-m2.md)
- [Project inventory](../../docs/project-state.md)
