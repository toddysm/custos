# Workflow Sub-Orchestration Manager

Last Updated: 2026-06-04

The **Sub-Orchestration Manager** (ADR-007) is the Workflow Service component that spawns
and awaits **child Dapr Workflow instances** on behalf of a parent run. It backs three
workflow primitives:

| Primitive | Surface | Step kind | Primitive handler |
|---|---|---|---|
| Dynamic loop | `forEach:` modifier on a step | `ACTIVITY` (inner body) | `SUB_ORCHESTRATION` |
| Approval gate | `approval:` block | `APPROVAL` | `SUB_ORCHESTRATION` |
| Sub-workflow invocation | `workflow:` reference | `WORKFLOW` | `SUB_ORCHESTRATION` |

The compiler tags every one of these steps with the `SUB_ORCHESTRATION` primitive handler so
the Run Controller dispatches them to the manager rather than to the Step Coordinator. A plain
`activity:` step with no `forEach:` keeps the `ACTIVITY_RUNTIME` handler and never reaches the
manager.

All three primitives produce **deterministic child instance ids** so the child set replays
byte-for-byte under Dapr, and child outputs are addressable from the parent's expression scope
under `steps.<stepId>.outputs.*`.

## Child instance ID format

Every child instance id follows the canonical form `<parentRunId>/<stepId>/<iterationKey>`:

| Step kind | Child instance ID | Iteration key |
|---|---|---|
| Loop (`forEach:`) | `<parentRunId>/<stepId>/<iterationKey>` | derived per item (see below) |
| Approval gate (`approval:`) | `<parentRunId>/<stepId>/approval` | reserved key `approval` |
| Sub-workflow (`workflow:`) | `<parentRunId>/<stepId>/workflow` | reserved key `workflow` |

The component separator is `/`. It is rejected in `stepId` and in derived iteration keys (the
loop layer percent-escapes the reserved character) so the id is unambiguously parseable.

### Loop iteration-key derivation

For a `forEach:` loop the iteration key is derived from each item, in priority order:

1. **Scalar item** (string / number / bool) → the item rendered as a string.
2. **Mapping item** → the first present stable identity field — `id`, then `key` — whose value
   is a scalar.
3. **Otherwise** → the item's 0-based positional `index` in the expanded iterable.

An empty derived identity (`""` or `{"id": ""}`) falls back to the positional index, so the key
is always non-empty. Two distinct items that derive the **same** key collide into one child
instance id; the loop-expansion layer detects duplicate keys across the iteration set and fails
with `step.loop_expansion_error` rather than silently de-duplicating.

## Dynamic loops (`forEach:`)

A `forEach:` modifier on a step fans the step out into **one child workflow instance per
iterable element**. The iterable is a CEL expression resolved against the parent scope:

```text
forEach: ${{ inputs.targets }}
```

The parent waits on `when_all` over the child set, so the loop completes only when every child
has settled. Each child runs the step body (the inner `activity:` / `let:`) against an
item-scoped context where the current element is bound to the loop variable `item`.

The merged loop result binds under a `results` key in the parent's expression scope —
`steps.<stepId>.outputs.results` is the ordered list of per-child outputs.

### `where:` pre-filter

A `forEach:` step may carry an optional `where:` predicate that pre-filters the iterable before
fan-out: only elements for which the predicate evaluates truthy spawn a child. The predicate is
evaluated per item with the element bound to the loop variable `item`. Filtering happens
**before** any child is spawned, so a filtered-out item contributes no child instance id and no
entry to `results`. The surviving set is re-derived deterministically across replay.

### Fan-out cap

The number of children a single loop may spawn is bounded by `WF_MAX_FANOUT_WIDTH`
(default **1000**). A loop whose expanded-and-filtered item count exceeds the cap is rejected
with `step.sub_orchestration_spawn_error` **before any child is spawned** — the cap is a guard
against runaway fan-out, not a runtime limit applied mid-flight.

## Approval gates (`approval:`)

An `approval:` block spawns a single child sub-orchestration that blocks on an external approval
signal. The gate races the approval event against a durable timeout timer:

```text
winner = when_any([approvalEvent, timeoutTimer])
```

The approval signal flows through the **Trigger Service** via `RaiseExternalEvent` — not via a
back-channel API — so approval signals are subject to the same dedup, audit, and idempotency
machinery as every other external event. When the event wins, its payload (the decision) binds
to the gate step's outputs.

### Timeout

The `approval:` block carries an optional `timeout` field (ISO-8601 duration). When omitted the
node carries the model default `PT24H`, and the manager substitutes the platform-configured
default `WF_APPROVAL_DEFAULT_TIMEOUT` (also **PT24H**). Any explicit per-document `timeout`
always wins. Timeouts are per-gate: a workflow with multiple approval gates configures each
independently.

On timeout the gate terminates with `step.approval_timeout`. Workflow-level **retry policy does
not apply** to approval-gate timeouts: a timed-out approval is a business decision, not a
transient failure, so re-running the gate without operator intent is wrong. To re-open a
timed-out gate the run must be explicitly restarted (or re-run with modified inputs).

## Sub-workflow invocation (`workflow:`)

A `workflow:` step invokes another workflow as a single child instance. The reference must be a
fully-qualified `workflowVersionId` UUID or a `<workspace>/<name>@<version>` triple — name-only
references are rejected (REQ-025 immutability):

```text
workflow: security/promote@1
```

Inputs are supplied through the step's `with:` block, which is evaluated against the parent
scope and flows into the child run's `inputs.*` namespace. The parent waits on the single child
instance; on success the child's outputs merge into `steps.<stepId>.outputs.*` in the parent
scope. If the child run fails, the failure propagates to the parent step as
`step.sub_workflow_failed`, carrying the child's failure `kind` and the child instance id.

## Configuration

| Knob | Default | Effect |
|---|---|---|
| `WF_MAX_FANOUT_WIDTH` | `1000` | Upper bound on loop fan-out width. Over-cap loops fail with `step.sub_orchestration_spawn_error` before spawning. |
| `WF_APPROVAL_DEFAULT_TIMEOUT` | `PT24H` | Approval-gate timeout applied when a gate leaves `approval.timeout` at the model default. An explicit per-document timeout always wins. |

## Locked error taxonomy

The manager surfaces the following `step.*` failure kinds. Each is a member of the locked
`StepCoordinatorError` taxonomy and never changes once published:

| Kind | Raised when |
|---|---|
| `step.loop_expansion_error` | A `forEach:` iterable resolves to a non-iterable, or two items derive the same iteration key. |
| `step.sub_orchestration_spawn_error` | A loop exceeds `WF_MAX_FANOUT_WIDTH`, or a `workflow:` step cannot be resolved to a child graph, or a child instance cannot be spawned. |
| `step.sub_workflow_failed` | A `workflow:` child run terminates in a failed state; carries the child's failure `kind` and child instance id. |
| `step.approval_timeout` | An `approval:` gate's durable timer fires before an approval signal arrives. |

## Observability

The manager wraps each dispatched primitive in a span and emits counters
(`custos_workflow.sub_orchestration.{primitive}` where `primitive` is `loop`, `approval`, or
`sub_workflow`):

| Instrument | Type | Labels | Meaning |
|---|---|---|---|
| `custos_workflow.sub_orchestration.{primitive}` | span | `primitive`, `outcome` | One span per dispatched primitive; `outcome` records `ok` or the locked failure kind. |
| `custos_workflow_sub_orchestration_children_spawned_total` | counter | `primitive` (`loop` / `sub_workflow`), `outcome` | One per expanded loop item; one per sub-workflow. A primitive that fails before spawning emits a 0-valued sample under its failure outcome. |
| `custos_workflow_sub_orchestration_approvals_timed_out_total` | counter | `outcome` | Incremented when an approval gate resolves by timing out; non-timeout outcomes emit a 0-valued sample. |

## Worked examples

The following examples are pinned to the running manager by
`tests/test_docs_examples_sub_orchestration.py`: every fenced `yaml` block is compiled and
driven through `SubOrchestrationManager`, so the docs cannot drift from the code.

### Example 1 — dynamic loop

A `forEach:` step fans `security/scan@1` out over the `targets` input, one child per element:

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata:
  name: scan-fanout
  workspace: security
spec:
  inputs:
    targets:
      type: array
  steps:
    - id: scan-all
      activity: security/scan@1
      connector: primary
      forEach: ${{ inputs.targets }}
```

Driving the manager with `targets = [{"id": "alpha"}, {"id": "beta"}]` spawns two children with
instance ids `…/scan-all/alpha` and `…/scan-all/beta`, and the merged per-child outputs bind
under `steps.scan-all.outputs.results`.

### Example 2 — approval gate

An `approval:` gate blocks on an external decision with a 4-hour timeout:

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata:
  name: release-gate
  workspace: security
spec:
  steps:
    - id: gate
      approval:
        approvers: [alice]
        timeout: PT4H
```

When the approval event wins the `when_any` race, its payload (e.g.
`{"by": "alice", "decision": "approved"}`) binds to the gate's outputs. If the timer wins first,
the gate fails with `step.approval_timeout`.

### Example 3 — sub-workflow invocation

A `workflow:` step invokes `security/promote@1`, passing `inputs.who` through the `with:` block:

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata:
  name: promote-pipeline
  workspace: security
spec:
  inputs:
    who:
      type: string
  steps:
    - id: promote
      workflow: security/promote@1
      with:
        name: ${{ inputs.who }}
```

The single child instance is spawned with id `…/promote/workflow`; on success its outputs merge
into `steps.promote.outputs.*`. A child failure propagates as `step.sub_workflow_failed`.
