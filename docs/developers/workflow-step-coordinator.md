# Workflow Step Coordinator

Last Updated: 2026-05-31

## Audience

Workflow Service contributors who own a `StepHandler` implementation,
or who consume the Step Coordinator from inside the Run Controller
orchestrator. Plugin authors targeting the **Activity Runtime
Manager** boundary (the typed `ActivityRuntimeClient` Protocol the
coordinator schedules through) and **Connector Service** boundary
(the typed `ConnectorClient` Protocol the coordinator binds slots
through) should also start here — the wire contracts every
adapter ships against (`ScheduleActivityRequest`,
`ActivityResultEnvelope`, `BindForStepRequest`,
`BindForStepResponse`) are exactly the surface the in-process
Step Coordinator drives. Workflow *authors* should start with the
[CEL Expressions](cel-expressions.md) and
[Workflow Compilation](workflow-compilation.md) docs — this page
assumes familiarity with the compiled `ExecutionGraph` shape they
describe, and with the [Run Controller](workflow-run-controller.md)
lifecycle the coordinator runs inside.

## Cross-references

- Design:
  [`design/components/workflow-service/design.md` § Internal
  Structure](../../design/components/workflow-service/design.md#internal-structure)
  and [§ Operation: Step
  Execution](../../design/components/workflow-service/design.md#operation-step-execution)
  — the canonical, locked contract for the Step Coordinator
  sub-module.
- Implementation plan:
  [`design/components/workflow-service/implementation-plan.md`](../../design/components/workflow-service/implementation-plan.md)
  — the WF-IMPL-047..060 task ledger that shipped this sub-module.
- Companion docs:
  [Workflow Run Controller](workflow-run-controller.md) — the
  lifecycle owner that calls the Step Coordinator;
  [Workflow Compilation](workflow-compilation.md) — the upstream
  pipeline that produces the `ExecutionNode` the dispatcher routes
  on (`primitive_handler` + `step_source` + `resolved_retry_policy`
  + `on_error_routes`);
  [CEL Expressions](cel-expressions.md) — the binding model
  `WithInputResolver` evaluates.
- Public Python surface:
  [`custos_workflow.steps`](../../src/services/workflow-service/src/custos_workflow/steps/__init__.py).

## Overview

The Step Coordinator is the **fourth sub-module** in the workflow
service host. Its job is one sentence: *drive the execution of
exactly one step within a Run, end to end.* That covers
`with:` input evaluation, the per-attempt
`(runId, stepId, attempt)` idempotency triple, fresh connector
bind, activity scheduling against the Activity Runtime Manager,
retry-policy application on retryable envelopes, and the locked
`step.*` lifecycle event taxonomy.

The Run Controller orchestrator (WF-IMPL-035) calls the Step
Coordinator through a single Python Protocol:

```python
from custos_workflow.runs import StepHandler  # the Protocol
from custos_workflow.steps import StepCoordinator  # the concrete impl
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.let_step import LetStepHandler

coordinator: StepHandler = StepCoordinator(
    activity_handler=ActivityStepHandler(
        activity_client=activity_client,
        connector_client=connector_client,
    ),
    let_handler=LetStepHandler(),  # default — omit to use the built-in
)
```

The Run Controller registers the result via
`make_run_orchestrator(step_handler=coordinator)` in the FastAPI
lifespan; the orchestrator dispatches one `execute(...)` call per
step in topological order and the coordinator returns a closed
`StepResult` variant (`StepSucceeded` / `StepFailed` /
`StepSkipped` / `StepWaiting`).

### Boundary with the Run Controller

The Run Controller owns:

- The Run lifecycle (start / cancel / pause / resume / get /
  list) and the `workflow.*` lifecycle events
  ([Run Controller § Lifecycle events](workflow-run-controller.md#lifecycle-events)).
- Run-row persistence (`InProcessRunStore` / metadata store).
- The Dapr Workflow runtime call surface (`schedule_new_workflow`,
  `terminate_workflow`, `pause_workflow`, `resume_workflow`,
  `get_workflow_state`).
- The compiled-graph deserialisation gate
  (`run.state_corrupt` envelope).
- The orchestrator function body itself (topological walk +
  `StepResult` dispatch + `RunOutput` synthesis).

The Step Coordinator owns:

- `with:` input resolution via the WF-IMPL-051 `WithInputResolver`.
- The `(run_id, step_id, attempt)` idempotency triple via the
  WF-IMPL-047 `IdempotencyTriple`.
- Per-attempt connector binding via the WF-IMPL-050
  `ConnectorClient` Protocol.
- Activity scheduling via the WF-IMPL-049 `ActivityRuntimeClient`
  Protocol.
- Retry decisions via the WF-IMPL-053 `retry_driver.decide(...)`.
- The locked `step.*` error taxonomy (WF-IMPL-048
  `LOCKED_STEP_KINDS`).
- The locked `step.*` lifecycle event taxonomy (WF-IMPL-056
  `LOCKED_STEP_EVENT_KINDS`).
- The OTel observability surface — four spans + four instruments
  (WF-IMPL-058).

### Boundary with the deferred sub-modules

The dispatcher recognises four `PrimitiveHandler` tags. Two ship
today (`ACTIVITY_RUNTIME`, `EXPRESSION_INLINE`); two are reserved
for the deferred sub-modules in
[`todos.md`](../../design/components/workflow-service/todos.md#deferred-sub-modules):

- `SUB_ORCHESTRATION` — the Sub-Orchestration Manager will own
  `for:` / `approval:` / `workflow:`.
- `RUN_CONTROLLER_TIMER` — the Run Controller's `WaitStepHandler`
  drives the durable timer for `wait:`; reaching the coordinator
  dispatcher with this primitive is a compile-time bug, and the
  coordinator surfaces it as a raised
  `StepKindNotImplementedError` (bumping
  `custos_workflow_step_errors_total{kind="step.kind_not_implemented"}`)
  rather than swallowing it as a `StepFailed`.

`step.kind_not_implemented` is also the envelope kind a
`SUB_ORCHESTRATION` node produces today (the dispatcher returns
`StepFailed` keyed on it) so partial workflows that exercise the
deferred kinds surface in the audit stream rather than silently
no-op. Once the Sub-Orchestration Manager ships, the
`primitive_handler == SUB_ORCHESTRATION` arm gets a real handler
injected at construction time.

## Dispatch table

The dispatcher in
[`custos_workflow.steps.StepCoordinator.execute`](../../src/services/workflow-service/src/custos_workflow/steps/coordinator.py)
routes strictly off `ExecutionNode.primitive_handler`, which the
Definition Compiler pins at compile time. The mapping is:

| `PrimitiveHandler` | Step kind (YAML) | Handler today | Notes |
|---|---|---|---|
| `EXPRESSION_INLINE` | `let:` | `LetStepHandler` (WF-IMPL-052) | Pure CEL evaluation under the run's binding scope; returns `StepSucceeded(outputs=...)`. No I/O. |
| `ACTIVITY_RUNTIME` | `activity:` | `ActivityStepHandler` (WF-IMPL-054) | Full *resolve → bind → schedule → dispatch → retry* loop; the only handler that talks to the Activity Runtime Manager or Connector Service. |
| `SUB_ORCHESTRATION` | `for:` / `approval:` / `workflow:` | (deferred) | Dispatcher returns `StepFailed(step.kind_not_implemented)` carrying the offending `primitive_handler` tag. |
| `RUN_CONTROLLER_TIMER` | `wait:` | n/a — owned by the Run Controller orchestrator's built-in `WaitStepHandler` | Reaching the dispatcher with this tag is a compile-time bug; coordinator **raises** `StepKindNotImplementedError`. |

The dispatcher is **synchronous** (it runs on the Dapr Workflow
orchestrator's replay-safe thread); any I/O the activity handler
needs runs inside `execute(...)` against the synchronous
`ActivityRuntimeClient` / `ConnectorClient` Protocols and the
orchestrator's `ctx.workflow_context.create_timer(...)` token
for retry waits. The orchestrator never awaits inside `execute`
(see [Run Controller § StepHandler
Protocol](workflow-run-controller.md#stephandler-protocol) for
the contract).

## Activity step lifecycle

Every `activity:` step walks the same eight-stage loop, defined
in `design.md` § *Operation: Step Execution* and implemented by
`ActivityStepHandler.execute`:

```mermaid
sequenceDiagram
    autonumber
    participant O as Run Controller<br/>orchestrator
    participant SC as StepCoordinator
    participant ASH as ActivityStepHandler
    participant WIR as WithInputResolver
    participant CC as ConnectorClient<br/>(slot binding)
    participant ARM as ActivityRuntimeClient
    participant RD as retry_driver.decide

    O->>SC: execute(ctx, graph, step_id)
    SC->>ASH: execute(ctx, graph, step_id)
    ASH->>WIR: resolve(with: inputs, scope)
    Note over ASH,WIR: Resolved ONCE,<br/>outside the retry loop
    loop attempt = 1..max_attempts
        ASH->>ASH: derive_triple(run_id, step_id, attempt)
        ASH->>CC: bind_for_step(triple, slots)
        Note over ASH,CC: Fresh lease per attempt<br/>(design pins bind-per-attempt)
        ASH->>ARM: schedule_activity(triple, inputs, contexts)
        ARM-->>ASH: ActivityResultEnvelope(class=...)
        alt success
            ASH-->>SC: StepSucceeded(outputs)
        else retryable / permanent / cancelled
            ASH->>RD: decide(envelope, node, rng, clock)
            alt RetryNow(delay_seconds, next_attempt)
                ASH->>ASH: ctx.create_timer(delay_seconds)
                Note over ASH: Token discarded under fakes;<br/>real Dapr suspends here
                ASH->>ASH: attempt = next_attempt
            else Skip
                ASH-->>SC: StepSkipped(reason)
            else FailNow(envelope)
                ASH-->>SC: StepFailed(envelope)
            end
        end
    end
    SC-->>O: StepResult variant
```

A few invariants the implementation locks:

- **`with:` resolution runs exactly once.** Retries replay the
  same inputs; only the attempt counter and connector lease
  change per pass.
- **Fresh connector bind on every attempt.** A previously-leaked
  `ConnectorContext` cannot leak into the retry path; the
  per-attempt bind key is the triple's `to_str()` form so the
  Connector Service can dedup re-issued binds during Dapr replay.
- **Failures structurally upstream of the envelope dispatch**
  (`WithInputResolver` raise, `ConnectorClient.bind_for_step`
  exception, `ActivityRuntimeClient.schedule_activity` exception)
  are wrapped in the matching `StepCoordinatorError` subclass
  and surfaced as `StepFailed`. They do **not** consult the
  retry driver (the driver only fires on envelope-class
  failures).
- **`step.kind_not_implemented` is a raise, not a return.** The
  `RUN_CONTROLLER_TIMER` dispatcher arm raises so the OTel error
  counter is bumped and the orchestrator's error-handling path
  fires (this is the only Step Coordinator failure surface that
  raises rather than returning `StepFailed`).

## Retry policy application

`retry_driver.decide(...)` is a **pure function** of the inputs
(modulo the supplied `random.Random` for jitter). The
`ActivityStepHandler` seeds that RNG off
`sha256(f"{run_id}|{step_id}|{attempt}")` so replay produces
byte-identical decisions:

```python
import hashlib, random

digest = hashlib.sha256(f"{run_id}|{step_id}|{attempt}".encode()).digest()
seed = int.from_bytes(digest[:8], "big", signed=False)
rng = random.Random(seed)
```

### Route precedence

The compiler (WF-IMPL-024 `on_error.compile`) **always** prepends
a `cls=cancelled → FAIL` short-circuit route to every step's
`on_error_routes`, so a `do: retry` arm can never resurrect an
operator-initiated cancellation. Beyond that synthetic head, routes
are walked **in declaration order** and the first matching arm
wins. A route matches on exactly one of `code`, `code_prefix`,
or `cls` (the compiler enforces the one-of constraint).

The matching scope is the envelope's mapping body (the
`ActivityResultEnvelope.error` mapping, inflated to a dict with
`class` forced to the envelope-level `class_`); a `do: retry`
arm carrying its own inline `retry:` overrides the prevailing
`ResolvedRetryPolicy`, otherwise the node's
`resolved_retry_policy` applies.

### Effective delay

Given an envelope and the chosen retry policy, the driver
computes the effective delay in four stages:

1. **Base delay** — the prevailing `ResolvedBackoffPolicy`'s
   `BackoffStrategyTag` produces a base in milliseconds:

   | Strategy | Formula (`d_n` = delay for attempt `n`, starting at 1) |
   |---|---|
   | `constant` | `d_n = initial_delay_ms` |
   | `linear` | `d_n = initial_delay_ms * n` |
   | `exponential` | `d_n = initial_delay_ms * multiplier^(n-1)` |

2. **Jitter** — the `JitterStrategyTag` perturbs the base via the
   per-attempt RNG:

   | Jitter | Formula (`b` = base from stage 1, `U` = uniform `[0,1)`) |
   |---|---|
   | `none` | `b` |
   | `full` | `U * b` |
   | `equal` | `b/2 + U * (b/2)` |
   | `decorrelated` | `min(max_delay_ms, U * (3 * b - initial_delay_ms) + initial_delay_ms)` |

3. **Clamp** — the result is clamped against
   `ResolvedBackoffPolicy.max_delay_ms`.

4. **`retryAfter` override** — if the envelope carries a
   `retryAfter` hint (ISO-8601 duration or epoch seconds) **and**
   the prevailing policy's `respect_retry_after` is true, the
   computed delay is replaced by the hint (still clamped against
   `max_delay_ms`).

Worked examples (all delays in milliseconds; `initial=100`,
`max=10000`, `multiplier=2.0`, `attempt=3`):

| Backoff | Jitter | Sample `U` | Result |
|---|---|---:|---:|
| `exponential` | `none` | n/a | `400` (`100 * 2^2`) |
| `exponential` | `full` | `0.25` | `100` (`0.25 * 400`) |
| `exponential` | `equal` | `0.5` | `300` (`200 + 0.5 * 200`) |
| `exponential` | `decorrelated` | `0.5` | `650` (`min(10000, 0.5 * (3*400 - 100) + 100)`) |
| `linear` | `none` | n/a | `300` (`100 * 3`) |
| `linear` | `equal` | `0.0` | `150` (`150 + 0 * 150`) |
| `constant` | `full` | `0.9` | `90` (`0.9 * 100`) |
| `constant` | `none` | n/a (`retryAfter=PT2S`, `respect=true`) | `2000` |

The driver returns `RetryNow(delay_seconds=delay_ms / 1000,
next_attempt=attempt + 1)`; the activity handler then opens a
`ctx.workflow_context.create_timer(timedelta(seconds=delay_seconds))`
and loops with the next attempt.

### Budget exhaustion

When `attempt + 1 > max_attempts` and the matched route is
`do: retry`, the driver returns `FailNow` carrying a
`step.retry_budget_exhausted` envelope built from
`RetryBudgetExhaustedError`. The kind string is locked in
`LOCKED_STEP_KINDS` and the OTel
`custos_workflow_step_errors_total` counter bumps with
`kind="step.retry_budget_exhausted"`.

## Idempotency triple

Locked wire form
([`custos_workflow.steps.idempotency`](../../src/services/workflow-service/src/custos_workflow/steps/idempotency.py)):

```
{run_id}|{step_id}|{attempt}
```

The pipe character is the canonical separator
(`IDEMPOTENCY_TRIPLE_SEPARATOR`); the components are validated
on construction (`run_id` and `step_id` non-empty strings,
`attempt` a positive 1-indexed integer).

Downstream usage:

| Consumer | How it uses the triple |
|---|---|
| Activity Runtime Manager (COMP-006) | `ScheduleActivity` idempotency key — re-issued schedules during Dapr Workflow replay collapse to the same activation. |
| Connector Service (COMP-005) | Per-step lease key when binding `slots[]` for an attempt; replay of a previously-bound triple re-issues the same lease. |
| Observability + Audit Service | Correlates `step.*` lifecycle events back to attempts via this exact key. |

The triple is derived in `ActivityStepHandler.execute` once per
attempt via `derive_triple(run_id_str, step_id, attempt)`; the
`ActivityRuntimeClient` adapter then carries the triple components
on `ScheduleActivityRequest.{run_id, step_id, attempt}` (the
wire form is also reconstructible by joining those three fields,
so consumers that only see the wire request can canonicalise it
without depending on the Python class).

## `step.*` event taxonomy

The Step Coordinator publishes through the WF-IMPL-056
`StepLifecyclePublisher` Protocol, which adapts on top of the
WF-IMPL-041 `LifecycleEventPublisher` (the same publisher the Run
Controller uses for `workflow.*` events — all lifecycle traffic
goes through one HTTP path).

Six locked kinds, exported as
[`LOCKED_STEP_EVENT_KINDS`](../../src/services/workflow-service/src/custos_workflow/steps/events.py):

| Constant | Kind | When emitted |
|---|---|---|
| `LIFECYCLE_KIND_STEP_STARTED` | `step.started` | Dispatcher entry into a node (one per attempt). |
| `LIFECYCLE_KIND_STEP_COMPLETED` | `step.completed` | Handler returned `StepSucceeded`. Envelope carries `outputs`. |
| `LIFECYCLE_KIND_STEP_FAILED` | `step.failed` | Handler returned `StepFailed` after the retry budget tipped, OR a structural raise produced a wrapped envelope. Carries `error`. |
| `LIFECYCLE_KIND_STEP_SKIPPED` | `step.skipped` | Handler returned `StepSkipped` — either an `if:`/`when:`/`unless:` gate or an `on_error` `do: skip` arm. Carries `reason`. |
| `LIFECYCLE_KIND_STEP_WAITING` | `step.waiting` | Handler returned `StepWaiting` (today: only the deferred `waitFor:` sub-module). Carries `reason`. |
| `LIFECYCLE_KIND_STEP_RETRY_SCHEDULED` | `step.retry_scheduled` | Retry driver returned `RetryNow`. Carries `retry: {delay_seconds, next_attempt}`. |

### Envelope shape

Every `step.*` event uses the same outer envelope:

```json
{
  "kind": "step.started",
  "runId": "01HZ...",
  "stepId": "scan",
  "attempt": 1,
  "occurredAt": "2026-05-31T12:00:00Z"
}
```

`step.completed` adds `outputs`, `step.failed` adds `error`,
`step.skipped` / `step.waiting` add `reason`, and
`step.retry_scheduled` adds `retry`. JSON key ordering is
**lexical** (the publisher serialises through `json.dumps(...,
sort_keys=True)`); downstream consumers can rely on the byte
ordering.

### Producer-side dedup

The adapter keys dedup on `(run_id, step_id, attempt, kind)`.
Dapr Workflow replays the orchestrator on every step boundary, so
a `step.started` for `(run, step, attempt=2)` may be derived
multiple times during replay — the adapter absorbs the duplicates
silently and only the first emit reaches the publisher.

### Boundary with `workflow.*` events

The Run Controller emits `workflow.started` / `workflow.cancelled`
/ `workflow.paused` / `workflow.resumed` (see
[Run Controller § Lifecycle events](workflow-run-controller.md#lifecycle-events));
terminal `workflow.succeeded` / `workflow.failed` events are owned
by the **Run Reconciler** (WF-IMPL-042). The Step Coordinator never
emits `workflow.*` events.

## Locked error taxonomy

The five Step Coordinator failure classes are pinned in
[`custos_workflow.steps.LOCKED_STEP_KINDS`](../../src/services/workflow-service/src/custos_workflow/steps/errors.py)
and reflected in
[`custos_workflow._telemetry`](../../src/services/workflow-service/src/custos_workflow/_telemetry.py)
as the locked `kind` label set on
`custos_workflow_step_errors_total`. A build-time assertion
fails CI if a sixth `StepCoordinatorError` subclass slips in
without extending the locked set.

| `kind` | Python class | Trigger | Surfaces as |
|---|---|---|---|
| `step.kind_not_implemented` | `StepKindNotImplementedError` | Dispatcher saw a deferred `primitive_handler` (`SUB_ORCHESTRATION`) **or** the bug sentinel `RUN_CONTROLLER_TIMER`. | `StepFailed` envelope for deferred kinds; **raised** for the bug sentinel. |
| `step.with_input_resolution_error` | `WithInputResolutionError` | `WithInputResolver.resolve(...)` failed (CEL parse / type / evaluation error inside a `with:` slot). | `StepFailed` envelope; retry driver is not consulted (structural failure). |
| `step.connector_bind_error` | `ConnectorBindError` | `ConnectorClient.bind_for_step(...)` raised, or returned a malformed `BindForStepResponse`. | `StepFailed` envelope; retry driver is not consulted. |
| `step.activity_schedule_error` | `ActivityScheduleError` | `ActivityRuntimeClient.schedule_activity(...)` raised before producing an envelope. | `StepFailed` envelope; retry driver is not consulted. |
| `step.retry_budget_exhausted` | `RetryBudgetExhaustedError` | Retry driver matched a `do: retry` arm with `attempt + 1 > max_attempts`. | `StepFailed` envelope built by the driver; the carried `cause` is the last envelope-class error. |

Every entry inflates a stable `to_dict()` envelope keyed on
`{kind, message, run_id?, step_id?, attempt?, activity_ref?, cause?}`
— the API surface for the API Adapter + Observability/Audit
client when it consumes the `StepFailed` envelope downstream.

## Observability surface

The WF-IMPL-058 patch added four OTel spans and four OTel
instruments to the Step Coordinator hot path. All four spans
carry a `step_kind` attribute (the value of `StepKind.value` on
the dispatched node):

| Span | Wraps | Attributes |
|---|---|---|
| `custos_workflow.step.execute` | `StepCoordinator.execute` dispatch arm | `step_kind` |
| `custos_workflow.step.bind_connectors` | One per-attempt `ConnectorClient.bind_for_step` call | `step_kind` |
| `custos_workflow.step.schedule_activity` | One per-attempt `ActivityRuntimeClient.schedule_activity` call | `step_kind` |
| `custos_workflow.step.retry_decision` | One `retry_driver.decide(...)` consultation | `step_kind` |

| Instrument | Type | Labels | Bumped by |
|---|---|---|---|
| `custos_workflow_step_execute_duration_ms` | histogram | `step_kind`, `outcome` | Every `StepCoordinator.execute` dispatch; `outcome` is the `StepResult` variant tag or the locked `kind` short-suffix on raise. |
| `custos_workflow_activity_schedule_duration_ms` | histogram | `step_kind`, `class` | Every `ActivityRuntimeClient.schedule_activity` call; `class` is the envelope class or `internal_error` on raise. |
| `custos_workflow_step_attempts_total` | counter | `step_kind`, `final_class` | Once per attempt inside `ActivityStepHandler`; `final_class` is the envelope class or `internal_error`. |
| `custos_workflow_step_errors_total` | counter | `kind` | Once per Step Coordinator failure surface; `kind` is pinned by a build-time assertion to be exactly `LOCKED_STEP_KINDS`. |

The instrumentation remains **no-op when no OTel SDK is
installed** because only `opentelemetry-api` is a runtime
dependency — the SDK is dev-only and only the test harness
wires SDK-backed instruments via an in-memory exporter.

## Configuration knobs

The Step Coordinator is configuration-light by design — most of
the configurable surface lives on the document (retry policy,
backoff, jitter, `respect_retry_after`) and is locked into the
`ResolvedRetryPolicy` at compile time. The runtime knobs are:

| Knob | Type | Default | Where set |
|---|---|---|---|
| `default_activity_deadline` | `timedelta` | 24 hours (`DEFAULT_ACTIVITY_DEADLINE`) | `ActivityStepHandler(default_activity_deadline=...)` constructor argument; carried on `ScheduleActivityRequest.deadline` as `ctx.workflow_context.current_utc_datetime + default_activity_deadline`. |
| `activity_client` | `ActivityRuntimeClient` | — (required) | `ActivityStepHandler` constructor; production wiring binds the real Dapr-backed adapter, tests bind `FakeActivityRuntimeClient`. |
| `connector_client` | `ConnectorClient` | — (required) | `ActivityStepHandler` constructor; production wiring binds the real Dapr-backed adapter, tests bind `FakeConnectorClient`. |
| `with_resolver` | `WithInputResolver` | A fresh `WithInputResolver()` | `ActivityStepHandler` constructor; tests override to inject a recording resolver. |
| `activity_handler` | `StepHandler` | — (required) | `StepCoordinator` constructor; conventionally an `ActivityStepHandler`. |
| `let_handler` | `StepHandler` | A fresh `LetStepHandler()` | `StepCoordinator` constructor; the default is correct for production callers. |

No environment variables drive the Step Coordinator directly; the
service host's [Configuration table](../../src/services/workflow-service/README.md#configuration)
covers the variables the production wiring binds (`WF_ARM_ENDPOINT`,
`WF_CONNECTOR_ENDPOINT`) into the real adapters that satisfy the
`ActivityRuntimeClient` / `ConnectorClient` Protocols.

## Extension points

The Step Coordinator was built around two `Protocol` boundaries
and a single dispatcher tag set, so the deferred sub-modules
slot in without modifying the shipped code:

- **Real Activity Runtime Manager adapter** —
  [`ActivityRuntimeClient`](../../src/services/workflow-service/src/custos_workflow/clients/activity_runtime.py)
  is a Protocol with a single sync method
  `schedule_activity(ScheduleActivityRequest) -> ActivityResultEnvelope`.
  The deferred *Real ARM Client* sub-module ships a Dapr Service
  Invocation adapter that satisfies the Protocol; no change to
  `ActivityStepHandler` is needed.
- **Real Connector Service adapter** —
  [`ConnectorClient`](../../src/services/workflow-service/src/custos_workflow/clients/connector.py)
  is a Protocol with a single sync method
  `bind_for_step(BindForStepRequest) -> BindForStepResponse`.
  Same shape as above: the deferred sub-module ships a Dapr
  Service Invocation adapter, the existing
  `ActivityStepHandler` consumes it untouched.
- **Sub-Orchestration Manager** — the dispatcher's
  `SUB_ORCHESTRATION` arm currently returns
  `StepFailed(step.kind_not_implemented)`. The Sub-Orchestration
  Manager will provide a third handler argument to
  `StepCoordinator.__init__(...)` (paralleling `activity_handler`
  and `let_handler`) and the dispatcher arm flips to delegate to
  it.
- **Resume Subscription Manager** — `waitFor:` is a new step
  kind that the Definition Compiler will surface as a fifth
  `PrimitiveHandler` tag; the dispatcher gains a fourth handler
  argument and a fourth arm. The `step.waiting` lifecycle event
  slot already lands today (it is part of
  `LOCKED_STEP_EVENT_KINDS`) so the sub-module just emits into
  it.

The five-element handler set the dispatcher accepts as a
constructor argument is the long-term shape; today only
`activity_handler` is required and `let_handler` defaults — the
two remaining slots become required as the sub-modules ship.

## Worked examples

The three examples below are exercised end-to-end by
[`tests/test_docs_examples_step_coordinator.py`](../../src/services/workflow-service/tests/test_docs_examples_step_coordinator.py),
which parses each fenced ` ```yaml ` block in this file, runs it
through `custos_workflow.compiler.compile` against a populated
`InMemoryActivityTypeRegistry`, and drives the resulting graph
through the
[`tests/integration/_harness.py`](../../src/services/workflow-service/tests/integration/_harness.py)
in-memory harness wired with a real `StepCoordinator` over the
fake activity / connector clients. The asserted terminal status
for each example is in the prose under the snippet.

### Example 1 — single activity step, success on attempt 1

A single `activity:` step that succeeds on the first attempt.
The fake activity client returns
`ActivityResultEnvelope(class_="success", outputs={"critical": 0,
"findings": []}, attempt=1)`, and the fake connector client binds
one `default` slot. The orchestrator carries the envelope
outputs verbatim on `RunOutput.outputs["scan"]`.

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata: {name: doc-example-activity-success, workspace: ws}
spec:
  inputs:
    image: {type: string, default: 'alpine:3.19'}
  steps:
    - id: scan
      activity: security/scan@1
      connector: primary
      with:
        image: ${{ inputs.image }}
```

Expected terminal status: `succeeded`. `RunOutput.outputs["scan"]`
== `{"critical": 0, "findings": []}`. Exactly one
`bind_for_step` call (`step_key` ends in `|scan|1`) and one
`schedule_activity` call (`activity_ref == "security/scan@1"`,
`attempt == 1`). Lifecycle events on the Run Controller
publisher: `[workflow.started]`.

### Example 2 — multi-step `let → activity → let` with cross-step refs

A linear pipeline where the second `let:` consumes
`${{ steps.scan.outputs.* }}`. The Definition Compiler types
`steps.scan.outputs.critical` as `integer` against the activity
type's output schema (registered in the
`InMemoryActivityTypeRegistry`); the `verdict` step's CEL
expression evaluates the boolean comparison at orchestrator time.

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata: {name: doc-example-cross-step, workspace: ws}
spec:
  inputs:
    image: {type: string, default: 'alpine:3.19'}
    threshold: {type: integer, default: 10}
  steps:
    - id: derive
      let:
        target: ${{ inputs.image }}
    - id: scan
      needs: [derive]
      activity: security/scan@1
      connector: primary
      with:
        image: ${{ steps.derive.outputs.target }}
    - id: verdict
      needs: [scan]
      let:
        critical: ${{ steps.scan.outputs.critical }}
        ok: ${{ steps.scan.outputs.critical <= inputs.threshold }}
```

Expected terminal status: `succeeded`. `RunOutput.outputs`
contains all three step ids; `verdict.ok` is `true` when the
fake activity returns `{"critical": 0, "findings": []}` against
the default `threshold == 10`. The activity's `with.image`
resolves through the first `let:` step's outputs so the recorded
`ScheduleActivityRequest.inputs` is `{"image": "alpine:3.19"}`.

### Example 3 — retry budget exhaustion

Three `retryable` envelopes against `maxAttempts: 3` exhaust the
retry budget. The retry driver matches the implicit
`do: retry` arm twice and the implicit `do: fail` arm on the
third attempt; `FailNow` carries a `step.retry_budget_exhausted`
envelope and the orchestrator surfaces it on `RunOutput`.

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata: {name: doc-example-retry-exhausted, workspace: ws}
spec:
  inputs: {}
  steps:
    - id: scan
      activity: security/scan@1
      connector: primary
      retry:
        maxAttempts: 3
        backoff:
          strategy: exponential
          initialDelay: PT1S
          maxDelay: PT30S
          multiplier: 2.0
```

Expected terminal status: `failed`. `RunOutput.failed_step ==
"scan"`; `RunOutput.failure_envelope["kind"] ==
"step.retry_budget_exhausted"`. Three `schedule_activity` calls
with attempts `[1, 2, 3]` and three fresh `bind_for_step` calls
(one per attempt). On the OTel surface,
`custos_workflow_step_errors_total{kind="step.retry_budget_exhausted"}`
bumps by 1.

## See also

- [`design/components/workflow-service/design.md`](../../design/components/workflow-service/design.md)
  — full design document, including the canonical retry-policy and
  backoff-formula tables this page links to.
- [`design/components/workflow-service/implementation-plan.md`](../../design/components/workflow-service/implementation-plan.md)
  — WF-IMPL-047..060 task ledger that shipped the Step Coordinator.
- [`src/services/workflow-service/README.md`](../../src/services/workflow-service/README.md)
  — service-level overview, including the Configuration table the
  production wiring binds.
- [`tests/integration/_harness.py`](../../src/services/workflow-service/tests/integration/_harness.py)
  — the in-memory harness backing every worked example above.
- [Workflow Run Controller](workflow-run-controller.md) — the
  lifecycle owner that calls into the Step Coordinator.
