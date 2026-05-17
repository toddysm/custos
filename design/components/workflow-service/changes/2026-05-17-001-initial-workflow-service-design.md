# Change: initial-workflow-service-design

Date: 2026-05-17
Type: component-design
Component: workflow-service
Sequence: 001
GitHub Issue: #40
Status: open

## Summary

Initial detailed design for COMP-003 Workflow Service, resolving INCON-015. The Workflow Service is the most architecturally central component — it owns the orchestration state machine and is the call target for `StartRun` / `RaiseExternalEvent` / `CancelRun`, the publisher of `custos.workflow.events`, and the caller of `ScheduleActivity` / `CancelActivity` (to ARM), `Resolve` (to Connector Service), and `RegisterResumeSubscription` / `CancelResumeSubscription` (to Trigger Service). Prior to this design, all of those interfaces were referenced in sibling component designs (Trigger Service, ARM) without a home document to specify them.

## Before

`design/architecture/components.md` COMP-003 row status: `Defined`. No `design/components/workflow-service/design.md` file existed. Cross-component dependencies were referenced but unowned:

- Trigger Service design § Internal RPC table listed `StartRun`, `RaiseExternalEvent`, `RegisterResumeSubscription`, `CancelResumeSubscription` with WF as the counterparty, but the request/response schemas, error codes, idempotency behavior, and Dapr Workflow binding were not specified anywhere.
- ARM design § Public Interface marked `(pending)` and listed `ScheduleActivity`, `CancelActivity`, and "Activity completion callback delivery to Workflow Service" without committing to a delivery mechanism.
- Architecture overview § Execution Model showed the WF→Dapr→ARM step sequence diagram but the step coordinator logic, retry decision tree, and idempotency model were not documented.
- `let` primitive (REQ-073) had no design specifying it as an inline evaluator step rather than an ARM call.
- Expression Evaluator (ADR-011) and Sub-Orchestration Manager (ADR-007) had architectural decisions but no implementing-component design.
- Resume Subscription Manager responsibilities were named in the Trigger Service design but not specified in the WF design (because no WF design existed).
- `custos.workflow.events` topic publication was specified on the consumer side (Trigger Service Internal Event Receiver) but not on the producer side (WF).

## After

New file `design/components/workflow-service/design.md` (v1, Status: Draft) covering:

- **Responsibility and Boundaries** — what WF owns vs. what TS, ARM, Connector, Catalog, and Observability own.
- **Internal Structure** — the eleven sub-modules already in `components.md` COMP-003 (API Adapter, Validator, Definition Compiler, Run Controller, Step Coordinator, Expression Evaluator, Sub-Orchestration Manager, Idempotency Tracker, Activity Runtime Client, Connector Client, Observability Client) with explicit responsibility per sub-module. Resume Subscription Manager folded into Step Coordinator; Workflow Event Publisher folded into Observability Client (no change to the components.md graph).
- **Six key operations with sequence diagrams**: Start Run, Step Execution (Activity), Step Resume on External Event (REQ-081), Sub-Orchestration (Dynamic Loop, ADR-007), Cancel Run, Pod Restart / Dapr Replay.
- **Dapr Workflow binding** — explicit table mapping Custos primitives to Dapr primitives, with `instanceId = runId` and child instance ID format `<parentRunId>/<stepId>/<iterationKey>`.
- **Step kinds handled** — table covering activity, conditional, parallel block, loop, approval gate, wait/sleep, external wait, `let`, sub-workflow. `let` contract locked in v1; implementation flagged M2.
- **Expression Evaluator** — pure CEL subset, deterministic bindings list, explicit non-bindings (no secrets, no env, no I/O), failure modes (timeout, type error, replay divergence).
- **Sub-Orchestration Manager** — dynamic loop and approval gate patterns; deterministic child instance IDs; approval signals routed through Trigger Service `RaiseExternalEvent`, not a back-channel.
- **Idempotency model** — two layers: caller `StartRun` idempotency `(workspaceId, idempotencyKey)`, engine-derived step-attempt triple `(runId, stepId, attempt)`.
- **Public Interface** — REST API endpoints (`POST /v1/.../runs`, `GET /v1/.../runs/{runId}`, etc.) and Internal RPC tables (inbound: `StartRun`, `RaiseExternalEvent`, `CancelRun`; outbound: `Resolve`, `ScheduleActivity`, `CancelActivity`, `RegisterResumeSubscription`, `CancelResumeSubscription`).
- **Dapr Pub/Sub publications** — `custos.workflow.events` envelope, at-least-once delivery, producer-side dedup on `(runId, eventKind, occurredAt)` to absorb Dapr replay.
- **Data model** — `Run`, `Step`, `StepAttempt`, `ResumeSubscriptionMirror` (the table that makes WF source of truth for resume subscriptions on replay).
- **Configuration** and **Dependencies** tables.
- **Failure Modes** table covering pod restart, ARM/TS/Connector unavailability, expression timeout/divergence, Dapr Pub/Sub publish failure, MetadataStore unavailable, Dapr unavailable, cancel-race conditions.
- **Seven open TODOs reduced to three** through in-design resolution. Three remaining TODOs:
  - TODO-001 (event taxonomy) — cross-link only, tracked under TS-TODO-001 (#18) and ARM TODO-009.
  - TODO-002 (retry-policy YAML schema) — real deferred design item for REQ-010.
  - TODO-003 (`workflow:` step kind ↔ template instantiation) — blocked on Catalog Service design.

Also: `design/architecture/components.md` COMP-003 status changes `Defined` → `Designed`.

## Key Decisions Locked This Session

1. **Activity completion uses native Dapr activity-task return** (not a `custos.activity.events` topic) in v1. Sufficient because WF is the only consumer of activity completion. A topic-based fan-out would be added only if a second consumer ever appears; until then, native return is the source of truth (not deferred work — a closed decision).
2. **WF is the source of truth for resume subscriptions.** Full replay protocol locked: idempotent on `(runId, stepId, eventKey)`, original-wins on divergent `selector` (with `resume.subscription.divergent` audit event), TS GCs on `expiresAt`, mirror persisted **before** TS call so a crash between mirror-write and TS-call leaves WF aware that registration is pending.
3. **`let` primitive contract locked in v1**, implementation flagged M2. `let` steps are inline expression evaluations — durable output, never call ARM, never require ConnectorContext. **Compilation strategy locked**: parse and type-check at workflow compile time in the Definition Compiler; cached AST on `ExecutionGraph`; parse errors reject at `StartRun`, evaluation errors fail the step at runtime. No lazy / first-execution compilation path.
4. **Resume Subscription Manager lives inside Step Coordinator; Workflow Event Publisher lives inside Observability Client.** No new sub-modules added to the `components.md` COMP-003 graph.
5. **Approval-gate timeout locked**: per-gate, ISO-8601 duration in the `approval:` block, default `PT24H`. Timeout fires durable timer in the child sub-orchestration; gate terminates with status `timed_out` (distinct from `cancelled` and `failed`). Workflow-level retry policy does **not** apply to approval-gate timeouts — a timed-out approval is a business decision, not a transient failure.
6. **`workflow:` step kind (sub-workflow invocation) locked for basics**: invokes a fully-qualified `workflowVersionId` (REQ-025 immutability — no name-only references), runs as Dapr sub-orchestration via Sub-Orchestration Manager, child instance ID `<parentRunId>/<stepId>/workflow`, inputs via `with:`, outputs bind to `steps.<id>.outputs.*`. **Deferred**: relationship to `WorkflowTemplateVersion` invocation with inline placeholder values — blocked on Catalog Service design.

## Impact

- INCON-015 (#40) resolved: the most architecturally central component has a design document.
- TS-TODO-004 (resume-subscription registration as a WF responsibility) is now satisfied — the WF design owns `RegisterResumeSubscription` / `CancelResumeSubscription` lifecycle, idempotent replay re-registration, and TTL semantics. The Trigger Service TODO can be closed once this PR merges.
- ARM's § Public Interface `(pending)` marker is resolvable in a follow-up: this design specifies what WF calls on ARM (`ScheduleActivity` / `CancelActivity` signatures, activity completion via native Dapr activity-task return), so ARM can finalize the matching surface.
- ADR-007 (sub-orchestrations) and ADR-011 (CEL expression evaluator) gain implementing-component design ownership.
- REQ-080 (internal workflow-to-workflow triggering): `custos.workflow.events` publication is now fully owned end-to-end (TS already owns consumer side; WF now owns producer side with documented dedup).
- REQ-081 (dual-purpose event delivery for start and resume): the WF side of the resume subscription lifecycle is now specified.
- Three cross-component TODOs cross-link with TS-TODO-001 (#18) and ARM TODO-009 for the unified event taxonomy (INCON-013 anchor).

## Out of Scope (Deferred to Future Sessions)

- Retry-policy YAML schema details (WF TODO-005).
- Approval-gate timeout default and granularity (WF TODO-006).
- Sub-workflow invocation (`workflow:` step kind) specifics — version pinning, template instantiation relationship (WF TODO-007).
- Implementation-level details of the CEL evaluator (which library, sandbox enforcement mechanism) — deferred to implementation planning.
- Migration story between workflow definition versions for in-flight runs — explicitly out of scope; runs reference a specific `WorkflowVersion` and that mapping is immutable (already locked in REQ-025).

## Related Requirements

- REQ-010 (configurable retry policies per step) — WF design § Idempotency Model + § Failure Modes covers the policy enforcement layer; schema specifics deferred to TODO-005.
- REQ-025 (workflow versioning — runs reference a specific version) — Validator reads `workflowVersionId` and the compiled graph is cached on the Run.
- REQ-027 (cancel a running workflow run) — § Cancel Run operation.
- REQ-073 (workflow primitives: branches, loops, fan-out/fan-in, conditions, retries, step coordination) — § Step Kinds Handled.
- REQ-080 (internal workflow-to-workflow triggering) — § Dapr Pub/Sub Publications `custos.workflow.events`.
- REQ-081 (step resume via external event) — § Step Resume on External Event operation.
- ADR-007 (sub-orchestrations) — § Sub-Orchestration Manager.
- ADR-011 (CEL expression evaluator) — § Expression Evaluator.
- Issues: #40 (this change, INCON-015).
