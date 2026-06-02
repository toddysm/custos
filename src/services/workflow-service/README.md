# workflow-service

Custos Workflow Service (COMP-003). Owns the orchestration state machine:
Run / Step / StepAttempt lifecycle over Dapr Workflow, the Definition
Compiler that turns a published `WorkflowVersion` into a runtime-ready
`ExecutionGraph`, the Expression Evaluator integration (`custos-cel`),
sub-orchestration management for dynamic loops and approval gates, resume
subscription lifecycle against the Trigger Service, and publication of
workflow lifecycle events to `custos.workflow.events`.

Design: [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md).

## Status

**Phase A scaffold + FastAPI surface + WorkflowDocument models + SchemaBindings derivation + ExecutionGraph data model + call-site collector + Definition Compiler driver + effective retry-policy resolver + on-error route compiler + locked compile-time error taxonomy + exhaustive kind-grid test suite + Hypothesis-driven determinism property tests + OpenTelemetry observability hooks + developer-facing documentation (≥90 % coverage gate)** —
WF-IMPL-013 ([#347](https://github.com/toddysm/custos/issues/347)),
WF-IMPL-014 ([#348](https://github.com/toddysm/custos/issues/348)),
WF-IMPL-015 ([#349](https://github.com/toddysm/custos/issues/349)),
WF-IMPL-016 ([#350](https://github.com/toddysm/custos/issues/350)),
WF-IMPL-017 ([#351](https://github.com/toddysm/custos/issues/351)),
WF-IMPL-018 ([#352](https://github.com/toddysm/custos/issues/352)),
WF-IMPL-019 ([#353](https://github.com/toddysm/custos/issues/353)),
WF-IMPL-020 ([#354](https://github.com/toddysm/custos/issues/354)),
WF-IMPL-021 ([#355](https://github.com/toddysm/custos/issues/355)),
WF-IMPL-022 ([#356](https://github.com/toddysm/custos/issues/356)),
WF-IMPL-023 ([#357](https://github.com/toddysm/custos/issues/357)),
WF-IMPL-024 ([#358](https://github.com/toddysm/custos/issues/358)),
WF-IMPL-025 ([#359](https://github.com/toddysm/custos/issues/359)),
WF-IMPL-026 ([#360](https://github.com/toddysm/custos/issues/360)),
WF-IMPL-027 ([#361](https://github.com/toddysm/custos/issues/361)), and
WF-IMPL-028 ([#362](https://github.com/toddysm/custos/issues/362)). The
package skeleton, the runnable `create_app()` factory, the
`python -m custos_workflow` entry point, the `/healthz` and `/readyz`
probes, the call-context middleware shim, the Helm subchart, the
CI gate (`.github/workflows/python-services.yml`), the typed
`WorkflowDocument` model + YAML loader, the per-step
`SchemaBindings` derivation (with an `ActivityTypeRegistry`
Protocol), the frozen `ExecutionGraph` dataclasses with a
byte-stable JSON serializer, the topology layer (explicit edges,
data-dependency edges, cycle detection, stable topological sort),
the CEL call-site collector, the Definition Compiler driver
(`compile(document, run_meta, registry) -> ExecutionGraph` —
wiring stages 1–6 of the pipeline), the effective retry-policy
resolver (`resolve_step_retry` / `resolve_arm_retry` — per-match →
step → `spec.defaults` → platform overlay, field-by-field), and
the on-error route compiler (`compile_on_error` — implicit-policy
synthesis, prepended `cancelled` short-circuit, disallowed-kind
rejection), and the locked public compiler error taxonomy
(`custos_workflow.errors` — `CompileError` base + four canonical
subclasses `CompileParseError` / `CompileTypeError` /
`CompileTopologyError` / `CompileRetryPolicyError`, each pinning
a stable `compile.*` `kind` string with JSON-safe `to_dict()`),
and the exhaustive kind-grid test suite
(`tests/test_kind_grid.py` — parametrized matrices that enumerate
every documented `StepKind` / `CallSiteKind` / `EdgeKind` /
`PrimitiveHandler` / `BackoffStrategy` / `JitterStrategy` /
`OnErrorAction` member and every locked `compile.*` `kind`,
guarded by `set(observed) == set(EnumClass)` so adding an enum
member without adding a grid row fails the build), and the
Hypothesis-driven determinism property tests
(`tests/test_determinism_property.py` — four properties locking
the replay-safety contract: (1) byte-equal `to_json(compile())`
across 100 repeats, (2) topological-order stability under
`spec.steps` shuffle, (3) `from_json(to_json(g)) == g` JSON
round-trip, (4) per-step `ResolvedRetryPolicy` stability; PR/push
CI runs with `--hypothesis-seed=0` for reproducibility while the
broader random-seed exploration is run nightly by
`.github/workflows/workflow-service-nightly.yml`), and the
OpenTelemetry observability hooks
(`custos_workflow._telemetry` — single `custos_workflow` tracer +
meter, one duration histogram per pipeline stage
(`custos_workflow_compile_{parse,topology,type_check,retry_policy,total}_duration_ms`,
all labelled by `outcome`), and a per-`kind` error counter
(`custos_workflow_compile_errors_total`) keyed to the WF-IMPL-024
taxonomy plus `compile.bindings_error`; spans
`custos_workflow.compile` (outer) and
`custos_workflow.compile.{parse,topology,type_check,retry_policy}`
carry `step_count` / `edge_count` / `call_site_count`
attributes; instrumentation is no-op when no OTel SDK is
installed because only `opentelemetry-api` is a runtime
dependency — the SDK is dev-only and only the test harness wires
in-memory exporters), and the developer-facing documentation
([`docs/developers/workflow-compilation.md`](../../../docs/developers/workflow-compilation.md)
— pipeline overview with Mermaid sequence, `WorkflowDocument`
input contract, `ExecutionGraph` output contract, full error
taxonomy table (every `compile.*` `kind`), retry-policy resolution
worked example, and three end-to-end YAML examples; backed by
`tests/test_docs_examples.py` which parses every fenced ```yaml```
block in the doc, runs it through `compile()`, and asserts the
resulting graph shape so the doc cannot drift away from the code)
backed by the
`--cov-fail-under=90` floor wired into the package's pytest
defaults (current coverage ≈ 99 %, `_telemetry.py`, `errors.py`,
and `graph/serialize.py` at 100 %)
are real. The WF-IMPL-000-COMPILER milestone is complete; tracker
[#363](https://github.com/toddysm/custos/issues/363) closes with
this task.

**WF-IMPL-000-RUN-CONTROLLER** is now in progress. WF-IMPL-029
([#381](https://github.com/toddysm/custos/issues/381)) lands the
thin `WorkflowRuntime` + `WorkflowClient` async adapters around
`dapr-ext-workflow>=1.17,<2` plus the in-memory
`FakeWorkflowRuntime` + `FakeWorkflowClient` test substitute.
Every subsequent Run Controller task (WF-IMPL-030 +) consumes
only these adapters; no other module imports
`dapr.ext.workflow`. WF-IMPL-046
([#398](https://github.com/toddysm/custos/issues/398)) ships the
Run Controller developer documentation at
[`docs/developers/workflow-run-controller.md`](../../../docs/developers/workflow-run-controller.md)
(lifecycle state machine, `RunController` public API, `StepHandler`
Protocol, Dapr Workflow primitive mapping, replay determinism
contract, `run.*` error taxonomy, worked examples — pinned to the
running code by `tests/test_docs_examples_run_controller.py`).
Tracker: [#399](https://github.com/toddysm/custos/issues/399).

**WF-IMPL-000-STEP-COORDINATOR** is now in progress. WF-IMPL-047
([#418](https://github.com/toddysm/custos/issues/418)) lands the
foundation for the fourth workflow-service sub-module: the
deterministic `(runId, stepId, attempt)` idempotency triple at
`custos_workflow.steps.IdempotencyTriple`, with canonical wire
form `f"{run_id}|{step_id}|{attempt}"` and round-trip
`IdempotencyTriple.from_str()`. The triple becomes the shared
scheduling key for the Activity Runtime Manager
(`ScheduleActivity`), the Connector Service lease key, and the
audit-event correlation key across replays. WF-IMPL-048
([#419](https://github.com/toddysm/custos/issues/419)) lands the
public Step Coordinator error taxonomy at
`custos_workflow.steps`: a frozen `StepCoordinatorError` hierarchy
with the five locked `step.*` `kind` strings pinned on
`LOCKED_STEP_KINDS` — `step.kind_not_implemented`,
`step.with_input_resolution_error`, `step.connector_bind_error`,
`step.activity_schedule_error`, and `step.retry_budget_exhausted` —
mirroring the Run Controller pattern from WF-IMPL-031 and
becoming the closed label set the WF-IMPL-058 OTel counter and
downstream audit consumers will rely on. WF-IMPL-049
([#420](https://github.com/toddysm/custos/issues/420)) lands the
first outbound client boundary at `custos_workflow.clients`: the
runtime-checkable `ActivityRuntimeClient` Protocol with frozen
`ScheduleActivityRequest` / `ActivityResultEnvelope` envelopes
(four-value `ActivityResultClass` Literal pinned on
`ACTIVITY_RESULT_CLASSES`) plus `NoopActivityRuntimeClient`
(refuses every call) and `FakeActivityRuntimeClient` (returns
canned envelopes, records calls + cancellations) test doubles —
the Step Coordinator's `ActivityStepHandler` (WF-IMPL-054) and
retry decision driver (WF-IMPL-053) consume this surface, and
the production Dapr-Workflow adapter plugs in behind the same
Protocol via the deferred *Real ARM Client* sub-module. WF-IMPL-050
([#421](https://github.com/toddysm/custos/issues/421)) extends
`custos_workflow.clients` with the matching outbound boundary to
Connector Service: the runtime-checkable `ConnectorClient`
Protocol with frozen `BindForStepRequest` (carrying a tuple of
`SlotSpec(name, connector_ref, capabilities)`) and
`BindForStepResponse` (whose `contexts` is always a
`MappingProxyType` snapshot of `slot_name → ConnectorContext`)
envelopes, the hashable `ConnectorContext` slot-handle dataclass,
plus `NoopConnectorClient` and `FakeConnectorClient` test
doubles — the Step Coordinator's `ActivityStepHandler`
(WF-IMPL-054) binds every slot through this surface before
calling `ScheduleActivity`, and the production Dapr Service
Invocation adapter plugs in behind the same Protocol via the
deferred *Real Connector Client* sub-module. WF-IMPL-051
([#422](https://github.com/toddysm/custos/issues/422)) lands the
pure `WithInputResolver` at `custos_workflow.steps.with_inputs`:
walks an `ExecutionNode`'s `with:` block, evaluates each
pre-typed `${{ ... }}` placeholder against the per-run
`BindingScope` via `custos_cel.evaluate`, and assembles the
resulting input mapping as a `MappingProxyType`. Single-placeholder
values preserve their raw CEL type (an `int` lands as an `int`,
not a stringified `"42"`); mixed strings are interpolated via
`str()` of each segment. Any `CelError` (parse / type /
unbound-name / timeout / evaluation) round-trips into
`WithInputResolutionError` with the underlying `kind` preserved
on `cause_kind` so audit consumers can still dispatch on the
root cause — meeting the five-locked-`kind` acceptance criterion
from #422. The resolver is pure (no I/O, fully replay-safe) and
feeds `ActivityStepHandler` (WF-IMPL-054). WF-IMPL-052
([#423](https://github.com/toddysm/custos/issues/423)) adds the
`LetStepHandler` at `custos_workflow.steps.let_step`: implements
the shared `StepHandler` Protocol for `StepKind.LET` by walking
the step's `let:` block in declaration order, evaluating each
single-`${{ ... }}`-placeholder string against a per-binding
`BindingScope` derived from `ctx.outputs` plus the
already-resolved overlay (so later bindings observe earlier ones
as `let.<name>`), and returning the resolved bag as a
`MappingProxyType` `StepSucceeded.outputs`. Non-string values and
literal strings pass through unchanged. Any `CelError` short-circuits
the remaining bindings and surfaces as a
`step.with_input_resolution_error` envelope on `StepFailed` —
sharing the WF-IMPL-051 taxonomy because the failure mode is
identical. `NoopStepHandler` now delegates `StepKind.LET` to
this dedicated handler via a module-local import, replacing the
former empty-outputs placeholder behaviour. Tracker:
[#432](https://github.com/toddysm/custos/issues/432).

WF-IMPL-053
([#424](https://github.com/toddysm/custos/issues/424)) lands the
**retry decision driver** at `custos_workflow.steps.retry_driver`.
The pure `decide(node, envelope, attempt, prev_delay_seconds, rng)`
function walks the compiled `OnErrorRoute`-s in declaration order
(first match wins; the compiler always prepends the
`cls=cancelled → FAIL` short-circuit so operator-initiated
cancellation can never be converted into a retry loop), then for a
matched `do: retry` arm enforces `attempt + 1 <= max_attempts` —
exhaustion emits a `step.retry_budget_exhausted` envelope carrying
the last underlying `code` / `codePrefix` / `class` for audit
correlation. The effective delay pipeline mirrors `design.md`
§ *Backoff formulas* + § *Jitter strategies* byte-for-byte:
constant / linear / exponential pre-jitter base, clamped to
`max_delay`, then jittered per `none` / `full` / `equal` /
`decorrelated`, and finally `max(jittered, min(retryAfter,
max_delay))` when the prevailing policy's `respect_retry_after`
is true and the envelope carries a parseable ISO-8601 hint.
The three-arm `RetryDecision` union (`RetryNow` / `Skip` /
`FailNow`) is dispatched on by `ActivityStepHandler`
(WF-IMPL-054). The companion `emit_retry_scheduled` /
`build_retry_scheduled_event` helpers publish the
`step.retry_scheduled` lifecycle event through the
`LifecycleEventPublisher` Protocol — its kind constant
(`LIFECYCLE_KIND_STEP_RETRY_SCHEDULED`) is owned here today and
will be folded into the full `step.*` taxonomy in WF-IMPL-056.
Tracker:
[#432](https://github.com/toddysm/custos/issues/432).

`ActivityStepHandler` (WF-IMPL-054, `src/custos_workflow/steps/activity_step.py`)
is the Step Coordinator handler for `StepKind.ACTIVITY` nodes. It
implements the synchronous `StepHandler` Protocol so it runs
inside the Dapr Workflow orchestrator generator. Per attempt, it
resolves the node, builds a `BindingScope` (mirroring
`LetStepHandler`), resolves the step's `with:` block exactly
once before the retry loop, then for each attempt: derives the
`IdempotencyTriple` (`run_id|step_id|attempt`), calls
`ConnectorClient.bind_for_step` for a *fresh per-attempt lease*
(slot specs derived from singular `connector:` → `default` slot
or map → one per alias), and dispatches
`ActivityRuntimeClient.schedule_activity` with the bind contexts
and a 24-hour default per-attempt deadline. On success it returns
`StepSucceeded(outputs)` with a `MappingProxyType` snapshot. On
typed `ActivityResultEnvelope` errors it delegates to the retry
driver, dispatching `RetryNow` (sleeps via
`ctx.workflow_context.create_timer(fire_at)`), `Skip`
(`StepSkipped` with synthesized reason), or `FailNow`
(`StepFailed`). Bind / schedule infrastructure exceptions are
wrapped into `ConnectorBindError` / `ActivityScheduleError`
envelopes (typed instances pass through verbatim; untyped are
wrapped with `cause=repr(exc)`). Replay determinism: a per-attempt
RNG seeded by `sha256("{run_id}|{step_id}|{attempt}")` feeds the
retry driver so jitter is reproducible across replays. **Caveats
for follow-ups**: the constructor does *not* take a lifecycle
publisher — `step.*` event emission is owned by WF-IMPL-056,
which will wrap the dispatcher. Durable timer suspension is not
yet wired: `create_timer` is called for backoff but its task
token is currently discarded, with full durable-task plumbing
deferred to WF-IMPL-055 / WF-IMPL-057. Not yet exported from
`custos_workflow.steps` — import directly from
`custos_workflow.steps.activity_step`. Tracker:
[#432](https://github.com/toddysm/custos/issues/432).


`StepCoordinator` (WF-IMPL-055, `src/custos_workflow/steps/coordinator.py`)
is the concrete `StepHandler` the Run Controller orchestrator
(WF-IMPL-035) drives every step through. It is a pure dispatcher
that routes execution strictly by the compile-time
`PrimitiveHandler` tag on each `ExecutionNode`: `EXPRESSION_INLINE`
(`let:`) flows to `LetStepHandler` (WF-IMPL-049), `ACTIVITY_RUNTIME`
(`activity:`) flows to `ActivityStepHandler` (WF-IMPL-054),
`SUB_ORCHESTRATION` (`for:` / `approval:` / `workflow:`) returns a
`StepFailed` carrying the canonical `step.kind_not_implemented`
envelope (the Sub-Orchestration Manager sub-module owns the real
implementation), and `RUN_CONTROLLER_TIMER` (`wait:`) raises
`StepKindNotImplementedError` — that kind is dispatched inline by
the Run Controller orchestrator via `ctx.create_timer`, so reaching
the dispatcher with one is a compile-time bug we surface loudly. A
module-level `assert` over `set(PrimitiveHandler)` guarantees that
adding a new tag without extending the dispatch table fails at
import time (mirroring WF-IMPL-035's `_STEP_RESULT_VARIANTS`
pattern), and a companion unit test re-derives the same set so the
guard is exercised on every run. The constructor takes the wired
`ActivityStepHandler` (which the dispatcher does not know how to
build — connector / activity-runtime clients are application
concerns) and an optional `LetStepHandler` that defaults to a fresh
instance because `LetStepHandler` is stateless. **No `step.*`
lifecycle events are emitted from this module**; event emission
(`step.started` / `step.completed` / etc.) is owned by WF-IMPL-056,
which wraps this dispatcher with a publisher. Keeping the surfaces
separate lets WF-IMPL-056 land without re-opening the dispatch
table. Re-exported from `custos_workflow.steps` as
`StepCoordinator`. Tracker:
[#432](https://github.com/toddysm/custos/issues/432).


`StepLifecyclePublisher` / `LifecycleEventPublisherAdapter`
(WF-IMPL-056, `src/custos_workflow/steps/events.py`) is the
*publishing* surface that the dispatcher will be wired through in
WF-IMPL-057. The `StepLifecyclePublisher` Protocol exposes one
typed `emit_step_*` method per locked `step.*` kind
(`step.started`, `step.completed`, `step.failed`, `step.skipped`,
`step.waiting`, `step.retry_scheduled` — the full set is pinned
by the `LOCKED_STEP_EVENT_KINDS` frozenset, with a module-level
`assert` keeping the Protocol surface and the locked set in
lockstep). The `LifecycleEventPublisherAdapter` *adapts* — it
does not implement — the wire transport: its inner
`LifecycleEventPublisher` is the same surface the Run Controller
already drives for `workflow.*` events, so every
`custos.workflow.events` publication funnels through one HTTP
client and one Dapr Pub/Sub endpoint. Each emit method builds a
`LifecycleEvent` whose `extra` carries `step_id` + `attempt` +
the kind-specific payload (`outputs` / `error` / `reason` /
`wait_token`), and `LifecycleEvent.to_wire` (extended by this
task) surfaces those fields as first-class wire keys (`stepId` /
`attempt` / `error` / `reason` / `waitToken`) so subscribers see
one envelope shape regardless of producer. `step.retry_scheduled`
delegates envelope construction to WF-IMPL-053's
`build_retry_scheduled_event` and is special-cased in `to_wire`
to re-pack the flat `previous_attempt` / `next_attempt` /
`effective_delay_seconds` / `action` / `previous_*` keys into a
nested `retry` wire block. Dapr Workflow's at-least-once
activity semantics replay each step boundary, so the adapter
maintains an in-memory LRU dedup keyed on
`(run_id, step_id, attempt, kind)` (the existing
`DedupingLifecyclePublisher`'s `(run_id, kind, occurred_at)` key
is too coarse — the same step `kind` legitimately fires for
every step in a graph). The key reservation happens *before* the
awaited inner publish so two concurrent emits for the same key
cannot both forward; if the inner publish raises, the
reservation is dropped so a retry still forwards the event.
**Emission is not yet wired into the dispatcher** — that wiring
lands in WF-IMPL-057 (FastAPI lifespan worker registration).
Re-exported from `custos_workflow.steps` as
`StepLifecyclePublisher`, `LifecycleEventPublisherAdapter`,
`LOCKED_STEP_EVENT_KINDS`, and the six `LIFECYCLE_KIND_STEP_*`
constants. Tracker:
[#432](https://github.com/toddysm/custos/issues/432).


WF-IMPL-057 ([#428](https://github.com/toddysm/custos/issues/428))
**wires the Step Coordinator into the FastAPI lifespan worker**.
The `create_app()` lifespan now builds an `ActivityStepHandler`
(WF-IMPL-054) bound to `RunComponents.activity_client` /
`RunComponents.connector_client`, wraps it in a `StepCoordinator`
(WF-IMPL-055), and registers `make_run_orchestrator(step_handler=…)`
against the `WorkflowRuntime` under the canonical workflow name —
replacing the WF-IMPL-043 `NoopStepHandler` placeholder.
`RunComponents` (the `load_run_components()` bundle) grows two new
fields, `activity_client: ActivityRuntimeClient` and
`connector_client: ConnectorClient`, defaulting to the
WF-IMPL-049 / WF-IMPL-050 `NoopActivityRuntimeClient` /
`NoopConnectorClient` stubs so production startup does not crash
before the real Dapr-Workflow-backed adapters land in the
deferred *Real ARM Client* / *Real Connector Client* sub-modules
(both Noop variants raise `NotImplementedError` on every call, so
any code path that reaches them surfaces loudly in tests).
`make_run_orchestrator` exposes the bound handler via a
`step_handler` attribute so the lifespan-wiring test
(`tests/test_app.py::test_lifespan_binds_step_coordinator_not_noop_handler`)
can assert the registered orchestrator is a `StepCoordinator`,
not the Noop default. Tracker:
[#432](https://github.com/toddysm/custos/issues/432).


WF-IMPL-058 ([#429](https://github.com/toddysm/custos/issues/429))
**adds OpenTelemetry observability hooks for the Step
Coordinator**, completing the visibility surface promised by the
WF-IMPL-048 error taxonomy. Four new instruments land on
`custos_workflow._telemetry`: the histogram
`custos_workflow_step_execute_duration_ms` (labelled `step_kind`,
`outcome`) records every `StepCoordinator.execute` dispatch; the
histogram `custos_workflow_activity_schedule_duration_ms`
(labelled `step_kind`, `class`) records every
`ActivityRuntimeClient.schedule_activity` call with the envelope
class from the response (or `internal_error` when the call
raises); the counter `custos_workflow_step_attempts_total`
(labelled `step_kind`, `final_class`) bumps once per attempt
inside `ActivityStepHandler` with the envelope's class; and the
counter `custos_workflow_step_errors_total` (labelled `kind`)
bumps once per Step Coordinator failure — the `kind` label is
pinned by a build-time assertion to be exactly the
`LOCKED_STEP_KINDS` frozenset, so adding a
`StepCoordinatorError` subclass without updating the locked set
fails the import. Four spans accompany them:
`custos_workflow.step.execute` wraps the dispatcher arm,
`custos_workflow.step.bind_connectors` wraps each per-attempt
connector lease, `custos_workflow.step.schedule_activity` wraps
each schedule call, and `custos_workflow.step.retry_decision`
wraps each `retry_driver.decide()` consultation. Every span
carries the `step_kind` attribute. Instrumentation remains no-op
when no OTel SDK is installed because only `opentelemetry-api`
is a runtime dependency — the SDK is dev-only and only the test
harness (`tests/test_observability_steps.py`, save/restore
fixture, in-memory exporter) wires SDK-backed instruments.
Tracker: [#432](https://github.com/toddysm/custos/issues/432).


WF-IMPL-059 ([#430](https://github.com/toddysm/custos/issues/430))
**ships the Step Coordinator end-to-end integration suite**,
locking the visible contract of the `RunController` →
`make_run_orchestrator` → `StepCoordinator` →
`ActivityStepHandler` → `FakeActivityRuntimeClient` /
`FakeConnectorClient` pipeline against six scenarios:
(1) single-activity success — the orchestrator carries the
envelope outputs onto `RunOutput.outputs[step_id]`, exactly one
bind and one schedule are observed, the lifecycle tape carries
only `workflow.started`; (2) `let → activity → let` with
cross-step references — the second `let:` consumes
`${{ steps.scan.outputs.* }}` and observes the envelope outputs
verbatim; (3) retry loop succeeds on attempt 3 — two
`retryable` envelopes followed by a `success` yield three
schedule calls with attempts `[1, 2, 3]` and three fresh
connector binds (bind-per-attempt is design-locked); (4) retry
budget exhaustion — three `retryable` envelopes with
`maxAttempts: 3` produce a `failed` `RunOutput` carrying the
locked `step.retry_budget_exhausted` envelope kind; (5) cancel
mid-flight — `cancel_run` drives the store row through
`cancelling → cancelled` and emits `workflow.cancelled` on the
publisher tape (the Run Controller cancel surface, preserved
verbatim by the WF-IMPL-055 wiring); (6) replay determinism —
two independent runs with identical inputs and identical canned
fake responses produce byte-equal `RunOutput.to_dict()` payloads
and byte-equal lifecycle event-kind tapes, evidencing that the
StepCoordinator dispatch + retry decision + envelope inflation
paths are pure functions of their inputs. The suite extends
`tests/integration/_harness.py` with an `activity_registry`
parameter so scenarios that drive real `activity:` steps can
seed the compile-time output-schema registry the
`${{ steps.X.outputs.* }}` references type-check against.
Coverage gate remains ≥ 90 % service-wide (currently 98.82 %
across the 117 source files). The Run Controller's
`workflow.completed` event and the orchestrator-side `step.*`
event-emitter wiring (which the WF-IMPL-056 adapter publishes
once the orchestrator threads it through) remain on the deferred
sub-module list and are intentionally **not** asserted by the
suite so it locks only the currently-shipped observable surface.
Tracker: [#432](https://github.com/toddysm/custos/issues/432).


WF-IMPL-060 ([#431](https://github.com/toddysm/custos/issues/431))
**ships the Step Coordinator developer documentation** at
[`docs/developers/workflow-step-coordinator.md`](../../../docs/developers/workflow-step-coordinator.md).
The doc is pinned to the running code by
`tests/test_docs_examples_step_coordinator.py`, which (a) parses
every fenced ```yaml``` block in the doc, compiles it through
`custos_workflow.compiler.compile` against the populated
`InMemoryActivityTypeRegistry`, and drives it through the
in-memory integration harness wired with a real `StepCoordinator`
over fake activity / connector clients — every documented
example reaches the prose-asserted terminal `RunOutput.status`
in CI; (b) reflects the `PrimitiveHandler` enum, the
`LOCKED_STEP_KINDS` frozenset, and the `LOCKED_STEP_EVENT_KINDS`
frozenset against the doc's dispatch table, error taxonomy, and
event-taxonomy tables respectively, with exhaustiveness guards
that fail the build on any drift. The doc itself covers the
sub-module boundary with the Run Controller and the deferred
Sub-Orchestration / Resume Subscription managers, the dispatch
table mapping `PrimitiveHandler` → handler, the eight-stage
activity step lifecycle (Mermaid sequence diagram), retry-policy
application (backoff / jitter formulas with worked examples and
`retryAfter` interaction), the locked `run_id|step_id|attempt`
idempotency triple with its downstream consumers (Activity
Runtime Manager, Connector Service, Observability + Audit
Service), the six `step.*` lifecycle event kinds with envelope
schemas and producer-side dedup contract, the five `step.*`
error taxonomy entries with their Python classes and recovery
semantics, the four OTel spans + four OTel instruments shipped
by WF-IMPL-058, the configuration knobs (`DEFAULT_ACTIVITY_DEADLINE
= 24h` and the constructor-injected protocol clients), and the
extension points the deferred sub-modules slot into. Quality
gate remains ≥ 90 % service-wide (currently 98.82 % across the
118 source files).
Tracker: [#432](https://github.com/toddysm/custos/issues/432).


**WF-IMPL-000-API-ADAPTER** is now in progress. WF-IMPL-061
([#447](https://github.com/toddysm/custos/issues/447)) through
WF-IMPL-072 ([#458](https://github.com/toddysm/custos/issues/458))
build the fifth sub-module — the **API Adapter + Validator** —
mounted as the FastAPI app produced by
`custos_workflow.create_app`. The sub-module owns the inbound
REST surface (`POST /v1/workspaces/{ws}/runs` and four
companion routes), the Internal RPC surface (`POST
/internal/runs:start` and two companion routes), the
pre-execution validator that gates every `StartRun`
(workspace authorization → Catalog lookup → inputs JSON-Schema
match → `(workspaceId, idempotencyKey)` ledger), and the RFC
7807 `application/problem+json` envelope every error travels
in. The 10-entry locked error taxonomy
(`custos_workflow.api.errors.LOCKED_API_KINDS`) covers every
`RunControllerError` / `ValidatorError` subclass plus three
route-local kinds (`workflow.step_not_found`,
`workflow.api.not_implemented`,
`workflow.api.bad_request`); the table is the single source of
truth the wire envelope, the exception-handler chain, the
observability counter (`custos_workflow_api_errors_total`), and
the developer documentation
([`docs/developers/workflow-api.md`](../../../docs/developers/workflow-api.md))
all read from. The `OTelHttpServerMiddleware` (WF-IMPL-070)
adds one `custos_workflow.http.request` span per request +
three workflow-service-prefixed instruments
(`custos_workflow_http_server_duration_ms`,
`custos_workflow_api_errors_total`,
`custos_workflow_idempotency_outcomes_total`); span attributes
include `wf.workspace.id`, `wf.run.id`,
`wf.workflow_version.id`, `wf.idempotency.outcome`, and
`wf.error.kind`. The WF-IMPL-071 end-to-end integration suite
(`tests/integration/test_api_end_to_end.py`) drives the
assembled app via `httpx.AsyncClient` over `httpx.ASGITransport`
through the full FastAPI lifespan (every handler, middleware,
and dependency factory), pinning the visible contract for the
happy paths, idempotency precedence, validator rejection
envelopes, and run-controller failure envelopes. The WF-IMPL-072
developer documentation
([`docs/developers/workflow-api.md`](../../../docs/developers/workflow-api.md))
is pinned to the running code by
`tests/test_docs_examples_api.py`, which (a) parses every
fenced ```json``` block and validates it against the matching
live Pydantic model (Problem+JSON blocks validate against
`ProblemDetail` *and* assert `code ∈ LOCKED_API_KINDS`); (b)
reflects the doc's REST + Internal RPC tables against the
assembled FastAPI app's live route-set with an exhaustiveness
guard that fails on any drift; (c) reflects the doc's error
taxonomy table against `LOCKED_API_KINDS` with the same
exhaustiveness guard. The deferred *Durable `IdempotencyLedger`*
follow-up (filed as a separate post-tracker issue) ships the
`MetadataStoreProvider`-backed adapter that the in-memory
`InMemoryIdempotencyLedger` is the test-and-dev stand-in for.
Tracker: [#459](https://github.com/toddysm/custos/issues/459).


**WF-IMPL-000-ARM-CONNECTOR-ADAPTERS** ships the production
outbound-RPC boundary: the real `DaprActivityRuntimeClient`
(`ScheduleActivity` / `CancelActivity`) and `DaprConnectorClient`
(`BindForStep`) adapters over Dapr Service Invocation, their
`providers.py` lifespan wiring (one shared `httpx.AsyncClient`,
`WF_OUTBOUND_RPC_TIMEOUT_MS` knob), the OTel observability surface
(`custos_workflow.outbound_rpc.call` span + three
`custos_workflow_outbound_rpc_*` instruments), an end-to-end suite
(`tests/integration/test_real_clients_end_to_end.py`) that drives
`create_app()` through the real adapters over `httpx.MockTransport`
without Docker / Dapr, and the developer documentation
([`docs/developers/workflow-outbound-rpc.md`](../../../docs/developers/workflow-outbound-rpc.md))
pinned to the running code by
`tests/test_docs_examples_outbound_rpc.py` (fenced ```json``` blocks
validated against the in-code envelope dataclasses; error-taxonomy,
outcome, span-attribute, and endpoint-path tables reflected against
their locked source-of-truth sets with exhaustiveness guards).
Tracker: [#495](https://github.com/toddysm/custos/issues/495).


**WF-IMPL-000-RESUME-SUBSCRIPTION** is now in progress: the
Resume Subscription Manager sub-module (REQ-081) adds the
`waitFor:` step kind, which parks a run on an external Trigger
Service event until a match arrives or the TTL expires. WF-IMPL-099
([#538](https://github.com/toddysm/custos/issues/538)) lands the
foundation — the `waitFor:` document model (`WaitForStep` /
`WaitForSpec` with CEL-token `eventKey` + optional `selector` and
ISO-8601 `ttl`), the new `StepKind.WAIT_FOR` /
`PrimitiveHandler.RESUME_SUBSCRIPTION` enum members and
`CallSiteKind.WAIT_FOR_EVENT_KEY` / `WAIT_FOR_SELECTOR` call-site
tags, the Definition Compiler dispatch entry that pins
`waitFor:` nodes to `RESUME_SUBSCRIPTION`, call-site collection
for the `eventKey` / `selector` expressions, a permissive output
binding schema (the event payload contract is not yet locked),
and a defensive coordinator dispatch arm that **raises**
`StepKindNotImplementedError` because the Run Controller
orchestrator owns the inline `wait_for_external_event` park.
Tracker: [#552](https://github.com/toddysm/custos/issues/552).

WF-IMPL-100 ([#539](https://github.com/toddysm/custos/issues/539))
extends the locked Step Coordinator error taxonomy
(`custos_workflow.steps.errors`) with three Resume Subscription
Manager `kind` strings — `step.resume_registration_failed`
(`RegisterResumeSubscription` stayed unreachable after
`WF_REGISTER_SUB_MAX_RETRIES`, mapped to `class: retryable`),
`step.resume_subscription_divergent` (a `waitFor.selector`
diverged on Dapr replay; the original wins), and
`step.resume_mirror_persist_error` (the `ResumeSubscriptionMirror`
write could not be issued). All three subclass
`StepCoordinatorError`, carry JSON-safe `to_dict()` payloads, and
join `LOCKED_STEP_KINDS` so the WF-IMPL-058 OTel counter and the
`_telemetry` outcome map stay exhaustive.
Tracker: [#552](https://github.com/toddysm/custos/issues/552).

WF-IMPL-101 ([#540](https://github.com/toddysm/custos/issues/540))
adds the outbound `TriggerServiceClient` contract surface
(`custos_workflow.clients.trigger`) — a `runtime_checkable`
Protocol with `register_resume_subscription(...)` /
`cancel_resume_subscription(...)`, the frozen
`RegisterResumeSubscriptionRequest` /
`RegisterResumeSubscriptionResponse` (carrying `tsSubscriptionId`)
/ `CancelResumeSubscriptionRequest` envelopes, pinned Dapr
method-name constants, and the `NoopTriggerServiceClient` /
`FakeTriggerServiceClient` test doubles. The fake mirrors the
Trigger Service replay contract — it returns the **same**
`tsSubscriptionId` for a repeated `(runId, stepId, eventKey)`, and
cancellation removes a known key (so a later re-registration mints
a fresh id) while being a no-op for an unknown or already-cancelled
key — so the rest of the sub-module can be tested before the
Trigger Service (COMP-004) or its production Dapr adapter
(WF-IMPL-103) exist.

WF-IMPL-102 ([#541](https://github.com/toddysm/custos/issues/541))
lands the persistence foundation that makes the Workflow Service
the **source of truth** for resume subscriptions
(`custos_workflow.steps.resume.mirror`) — the frozen
`ResumeSubscriptionMirror` value object (`mirrorId` / `runId` /
`stepId` / `eventKey` / `selector` / `tsSubscriptionId` /
`registeredAt` / `expiresAt`) with byte-stable `to_dict` /
`to_json` serialization and exact `from_dict` / `from_json`
round-trip, the `runtime_checkable`
`ResumeSubscriptionMirrorRepository` Protocol (`put` /
`list_open` / `list_open_for_step` / `delete` / `list_expired`),
and the in-memory `InMemoryResumeSubscriptionMirrorRepository`
adapter. The coordinator persists a mirror **before** it calls the
Trigger Service so a crash between the mirror write and the TS call
leaves an open mirror to replay; `put` upserts on `mirrorId` so a
replay re-registration can update the stored `tsSubscriptionId`;
and `list_expired` drives the periodic TTL garbage-collection
sweep. The `MetadataStoreProvider`-backed adapter (REQ-048) is a
separate follow-up — same staging as the idempotency ledger.
Tracker: [#552](https://github.com/toddysm/custos/issues/552).


WF-IMPL-103 ([#542](https://github.com/toddysm/custos/issues/542))
adds the production `DaprTriggerServiceClient`
(`custos_workflow.clients.trigger`) — the `TriggerServiceClient`
adapter that talks to the Trigger Service over Dapr
Service-Invocation. It posts `RegisterResumeSubscription`
(`{runId, stepId, eventKey, selector, ttl}` → parses
`subscriptionId` into `tsSubscriptionId`) and
`CancelResumeSubscription` (`{runId, stepId, eventKey}`, no body)
to `…/v1.0/invoke/<trigger-app-id>/method/<Method>`, normalising
every failure through the WF-IMPL-075 `OutboundRpcError` taxonomy
(transport → `OutboundRpcTransportError`, HTTP 499 →
`OutboundRpcCancelledError`, other non-2xx →
`OutboundRpcStatusError` carrying the status code, malformed
register body → `OutboundRpcDecodeError`). Cancellation is
idempotent over the wire — HTTP 404 / 409 for an already-gone key
are treated as a clean no-op. Outbound RPCs are observed through
`observe_outbound_rpc(client="trigger", …)`. The adapter ships
behind the Protocol; true cross-service resume is exercisable only
once the Trigger Service (COMP-004) exists.
Tracker: [#552](https://github.com/toddysm/custos/issues/552).


The Expression Evaluator (the first sub-module) is already in
[`src/libs/custos-cel/`](../../libs/custos-cel) and shipped via
WF-IMPL-001 through WF-IMPL-012 (#176–#187).

## Configuration

Per [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md) § Configuration:

| Variable | Required | Default | Description |
|---|---|---|---|
| `WF_DAPR_WORKFLOW_COMPONENT` | Yes | — | Name of the Dapr Workflow component to bind. |
| `WF_PUBLISH_TOPIC` | No | `custos.workflow.events` | Dapr Pub/Sub topic for lifecycle event publication. |
| `WF_ARM_ENDPOINT` | Yes | — | Activity Runtime Manager service endpoint. |
| `WF_TS_ENDPOINT` | Yes | — | Trigger Service service endpoint. |
| `WF_CONNECTOR_ENDPOINT` | Yes | — | Connector Service endpoint. |
| `WF_CATALOG_ENDPOINT` | Yes | — | Catalog Service endpoint (read-only `WorkflowVersion`). |
| `WF_RUN_HISTORY_RETENTION` | No | `90d` | How long to keep terminal-run metadata before archival. |
| `WF_RESUME_SUB_DEFAULT_TTL` | No | `PT24H` | Default TTL for `RegisterResumeSubscription` when caller does not specify. |
| `WF_REGISTER_SUB_MAX_RETRIES` | No | `5` | Max retries when registering a resume subscription with TS before failing the wait step. |
| `WF_EXPR_TIMEOUT_MS` | No | `100` | Per-expression evaluation timeout. |
| `WF_IDEMPOTENCY_KEY_TTL` | No | `PT24H` | Window for `(workspaceId, StartRun idempotencyKey)` dedup. |

Process bind:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Address the uvicorn process binds to. |
| `PORT` | `8080` | Port the uvicorn process listens on. |
| `WF_REQUIRE_CALL_CONTEXT` | `""` (dev shim) | Set to the exact literal `"1"` to enforce that every non-probe request carries `X-Custos-Workspace` and `X-Custos-Principal` headers (returns `401` `callctx_missing` otherwise). Any other value — including `"true"`, `"yes"`, `"TRUE"` — leaves the dev shim active. |

## Local development

```bash
cd src/services/workflow-service
pip install -e "../../libs/custos-cel[dev]"
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_workflow` starts uvicorn against
`custos_workflow:create_app` (`factory=True`), honouring `HOST` /
`PORT`. The lifespan flips `app.state.ready` immediately because Phase A
has no startup dependencies; WF-IMPL-016+ gates the readiness flip on
the Definition Compiler bootstrap and the Catalog client warm-up.

Probe quick-check:

```bash
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz
```

## Layout

```
src/services/workflow-service/
├── pyproject.toml
├── README.md
├── src/
│   └── custos_workflow/
│       ├── __init__.py        # re-exports __version__ + create_app
│       ├── __main__.py        # python -m custos_workflow entry point
│       ├── _version.py        # package version
│       ├── app.py             # FastAPI factory + lifespan
│       ├── call_context.py    # CallContext + middleware shim
│       ├── healthz.py         # /healthz + /readyz routes
│       ├── document/          # WorkflowDocument Pydantic models + YAML loader
│       │   ├── __init__.py    # public re-exports
│       │   ├── models.py      # WorkflowDocument + Step union + RetryPolicy …
│       │   └── loader.py      # parse_document() + DocumentParseError
│       ├── bindings/          # per-step SchemaBindings + ActivityTypeRegistry
│       │   ├── __init__.py    # public re-exports
│       │   ├── registry.py    # ActivityTypeRegistry Protocol + in-memory impl
│       │   └── derive.py      # derive_bindings(doc, registry) → {step_id: bindings}
│       ├── graph/             # ExecutionGraph dataclasses + byte-stable JSON + topology
│       │   ├── __init__.py    # public re-exports
│       │   ├── model.py       # frozen dataclasses + enum tags
│       │   ├── serialize.py   # to_json / from_json + GraphSerializationError
│       │   └── topology.py    # explicit/implicit edges + cycle detection + stable sort
│       ├── callsites/         # CEL call-site collector (WF-IMPL-020)
│       │   ├── __init__.py    # public re-exports
│       │   ├── model.py       # CallSite + SourcePosition dataclasses
│       │   ├── placeholders.py # ${{ ... }} segment extractor
│       │   └── collect.py     # collect_call_sites(doc) -> {step_id: [CallSite]}
│       ├── compiler.py        # Definition Compiler driver (WF-IMPL-021)
│       ├── retry/             # Effective retry-policy resolver (WF-IMPL-022)
│       │   ├── __init__.py    # public re-exports
│       │   ├── defaults.py    # PLATFORM_RETRY_DEFAULTS (layer 4)
│       │   └── resolve.py     # resolve_step_retry + resolve_arm_retry overlays
│       ├── on_error/          # On-error route compiler (WF-IMPL-023)
│       │   ├── __init__.py    # public re-exports
│       │   └── compile.py     # compile_on_error — implicit policy + cancelled short-circuit
│       ├── errors.py          # Locked compile-time error taxonomy (WF-IMPL-024)
│       ├── _telemetry.py      # OpenTelemetry instrumentation (WF-IMPL-027)
│       ├── runtime/           # Dapr Workflow runtime + client adapters (WF-IMPL-029)
│       │   ├── __init__.py    # public re-exports
│       │   ├── _common.py     # RunStatus, RunState, request dataclasses
│       │   ├── dapr.py        # WorkflowRuntime + WorkflowClient (real adapter)
│       │   └── fake.py        # FakeWorkflowRuntime + FakeWorkflowClient test substitute
│       └── py.typed
└── tests/
    ├── test_smoke.py             # package import + factory smoke
    ├── test_app.py               # factory shape + lifespan + env flag
    ├── test_call_context.py      # header presence/absence + dev/prod mode
    ├── test_healthz.py           # liveness/readiness status codes
    ├── test_document_models.py   # WorkflowDocument + Step union + retry
    ├── test_document_loader.py   # YAML → WorkflowDocument + error wrapping
    ├── test_bindings_registry.py # InMemoryActivityTypeRegistry contract
    ├── test_bindings_derive.py   # per-step ordering + activity / let / sub-wf
    ├── test_graph_model.py       # frozen invariants + __post_init__ checks
    ├── test_graph_serialize.py   # round-trip + byte-stability + schema guards
    ├── test_graph_topology.py    # explicit/implicit edges + cycles + stable sort
    ├── test_callsites.py         # placeholder scanner + call-site collector
    ├── test_compiler.py          # compile() driver: happy path, errors, stubs
    ├── test_retry_resolver.py    # resolve_step_retry + resolve_arm_retry overlays
    ├── test_on_error_compile.py  # compile_on_error: implicit + rejections
    ├── test_errors.py            # CompileError taxonomy: kinds, to_dict, hash, repr
    ├── test_kind_grid.py         # Parametrized kind grids + exhaustiveness guards (WF-IMPL-025)
    ├── test_determinism_property.py  # Hypothesis property-based determinism tests (WF-IMPL-026)
    ├── test_observability.py     # OTel span / metric instrumentation tests (WF-IMPL-027)
    ├── test_docs_examples.py     # Doc-block round-trip smoke test (WF-IMPL-028)
    └── runtime/                  # Dapr runtime + client adapter tests (WF-IMPL-029)
        ├── test_fake.py          # FakeWorkflowRuntime + FakeWorkflowClient behaviour
        └── test_dapr_adapter_shape.py # Real adapter import-safety + delegation shape
```

The WF-IMPL-000-COMPILER milestone is complete.
