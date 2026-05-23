# CEL Expressions in Custos Workflows

Last Updated: 2026-05-22

## Audience

Workflow authors who write `if` / `when` / `unless` guards, `with:` input
maps, `for:` loop iterables, `let:` inline bindings, or template-placeholder
expressions in workflow YAML — and downstream service implementers who
consume expression evaluation through the `custos_cel` Python library
([`src/libs/custos-cel`](../../src/libs/custos-cel/README.md)).

## Cross-references

- Design: [`design/components/workflow-service/design.md` § Expression
  Evaluator (ADR-011)](../../design/components/workflow-service/design.md#expression-evaluator-adr-011)
  — the canonical, locked contract for the evaluator.
- ADR-011 entry in the architecture-overview ADR table:
  [`design/architecture/overview.md`](../../design/architecture/overview.md#architecture-decisions).
- Parser-choice change record:
  [`design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md`](../../design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md).
- Library API surface and authoring notes:
  [`src/libs/custos-cel/README.md`](../../src/libs/custos-cel/README.md).

## Quick start

CEL expressions appear inside `${{ ... }}` template placeholders in
workflow YAML. The body of the placeholder is parsed by `custos_cel.parse()`
at workflow **publish time** (by the Catalog Service) and again
type-checked + evaluated by the Workflow Service at **run time**.

```yaml
- id: summarize
  let:
    totalCritical: ${{ steps.scan.outputs.critical + steps["scan-alt"].outputs.critical }}
    label: ${{ let.totalCritical > 0 ? "block" : "allow" }}
```

The expressions inside the two `${{ ... }}` placeholders are pure CEL.
Everything outside the placeholder braces is workflow YAML, not CEL.

## Bindings

Expressions can only read from the six binding roots below. Anything
else — secrets, connector contexts, environment variables, host modules,
arbitrary functions — is structurally unreachable and resolves to an
`expression.unbound_name` error. The table mirrors
[design.md § Expression Evaluator](../../design/components/workflow-service/design.md#expression-evaluator-adr-011);
see [README § Binding scope](../../src/libs/custos-cel/README.md#binding-scope)
for the Python API behind the same model. Some rows below show members of
the `run` and `workflow` roots rather than additional top-level roots.

| Binding | Source | Mutability | Example |
|---|---|---|---|
| `inputs.*` | Run inputs at start | Immutable | `inputs.image` |
| `steps.<id>.outputs.*` | Completed step outputs | Immutable once the step completes | `steps.scan.outputs.critical` |
| `run.id` | Current `runId` | Immutable | `run.id` |
| `run.workspace` | Workspace ID | Immutable | `run.workspace` |
| `workflow.name` | From `WorkflowVersion` metadata | Immutable | `workflow.name` |
| `workflow.version` | From `WorkflowVersion` metadata | Immutable | `workflow.version` |
| `now()` | Dapr Workflow `current_utc_datetime` (replay-deterministic) | Replay-safe wall-clock instant | `now()` |
| `let.<name>` | Inline `let` bindings within the same step | Immutable within the block | `let.totalCritical` |

**Step ids that are not valid CEL identifiers** (e.g. anything containing
`-`) must use the bracket form: `steps["scan-alt"].outputs.critical`, not
`steps.scan-alt.outputs.critical`. The dot form silently mis-parses as
subtraction.

## Sandbox guarantees

The evaluator is **pure**, **deterministic**, and **replay-safe**. The
acceptance criteria live in
[design.md § Expression Evaluator](../../design/components/workflow-service/design.md#expression-evaluator-adr-011);
the runtime enforcement lives in
[`src/custos_cel/eval.py`](../../src/libs/custos-cel/src/custos_cel/eval.py)
and is summarized in
[README § Sandbox guarantees](../../src/libs/custos-cel/README.md#sandbox-guarantees).

| Guarantee | What it means | How it is enforced |
|---|---|---|
| No I/O | Expressions cannot read or write files, network sockets, registries, or any external system. | Static audit (`tests/test_eval.py::test_eval_module_does_not_import_dangerous_stdlib`) asserts the evaluator imports zero `os` / `sys` / `subprocess` / `socket` / `importlib` / `open` / `__import__` / `eval` / `exec` / `compile`. |
| No host namespace | `os`, `sys`, `open`, `__import__`, `eval`, `exec`, `subprocess`, etc. all raise `expression.unbound_name` *before* any attribute or item access happens. | `BindingScope.resolve()` checks an allow-list keyed on the seven roots above. |
| Function allow-list | The only callable names are `now`, `size`, `has`, and `type`. Every other call site (including method-call syntax like `"foo".length()`) is structurally rejected. | The dispatcher's allow-list lives next to the evaluator; `parse()` rejects method-call syntax outright via the converter. |
| Strict typing | No implicit `int↔double`, `bool↔int`, or `str↔bytes` coercion. Numeric ops require matching operand types; comparison requires comparable operands; ternary branches must unify. | StartRun-time `type_check()` rejects mismatches with `expression.type_error`. |
| Bounded execution | A per-evaluation wall-clock budget (default 100 ms, override via the `WF_EXPR_TIMEOUT_MS` env var) caps how long any single expression may run. | The walker increments a per-evaluation node counter and consults `time.monotonic()` every 32 nodes against a precomputed deadline; overrun raises `expression.timeout`. |
| Replay determinism | The same `(expression, bindings, clock)` always produces the same result. `now()` returns the Dapr-Workflow replay-stable instant. | `DaprWorkflowClock` reads `current_utc_datetime` on every call; no other clock source is reachable from inside an expression. Property-based tests in `tests/test_determinism_property.py` cover round-trip and replay invariance. |

## Supported operators and functions

The full operator set below comes from
[`custos_cel.ast.BinaryOp` / `UnaryOp`](../../src/libs/custos-cel/src/custos_cel/ast.py)
and the type-checker rules in
[`custos_cel.types`](../../src/libs/custos-cel/src/custos_cel/types.py).

### Arithmetic

| Operator | Allowed types | Notes |
|---|---|---|
| `+` | `int + int`, `uint + uint`, `double + double`, `string + string`, `bytes + bytes`, `list + list` | Strings, bytes, and lists concatenate. No mixed-numeric promotion: `1 + 1.0` is a type error. |
| `-` | `int - int`, `uint - uint`, `double - double` | Unary negation also supported on numeric operands. |
| `*` | `int * int`, `uint * uint`, `double * double` | |
| `/` | `int / int`, `uint / uint`, `double / double` | Integer division **truncates toward zero** (C semantics, not Python floor). `7 / -2 == -3`. |
| `%` | `int % int`, `uint % uint` | Sign follows the truncated quotient. |

### Comparison

| Operator | Allowed types |
|---|---|
| `==`, `!=` | Any matching pair, including `null` on either side. |
| `<`, `<=`, `>`, `>=` | Matching pair of `int`, `uint`, `double`, `string`, `bytes`, or `timestamp`. |

### Logical and membership

| Operator | Allowed types | Notes |
|---|---|---|
| `&&`, `\|\|` | `bool && bool`, `bool \|\| bool` | Short-circuiting. |
| `!` | `!bool` | |
| `in` | `<T> in list<T>`, `<K> in map<K, V>` | Element type must match the iterable's element / key type. |
| `c ? a : b` | `bool ? <T> : <T>` | Both branches must unify; `null` promotes to the non-null sibling type. |

### Indexing and member access

| Form | Meaning |
|---|---|
| `a.b` | Static member access. Resolved at type-check time against the binding root's JSON Schema or the typed scope. |
| `a["b"]` | Bracket form. Required for step ids and map keys that are not valid CEL identifiers (`steps["scan-alt"]`). |
| `a[i]` | Dynamic indexing — `list[int]` returns the element, `map[K]` returns the value. Out-of-range / missing-key raises `expression.evaluation_error`. |

### Built-in functions

| Function | Signature | Behavior |
|---|---|---|
| `now()` | `() -> timestamp` | Returns the Dapr Workflow replay-stable current UTC datetime. The only way to read wall-clock time from inside an expression. |
| `size(x)` | `(string \| bytes \| list \| map) -> int` | Length in code points, bytes, elements, or entries — depending on operand. |
| `has(x.y)` | macro on `member-access` chain | Returns `true` when the chain ends in a real mapping key, `false` otherwise. Propagates `expression.unbound_name` if the *target* is unbound, so typos still surface. |
| `type(x)` | `(any) -> celtype` | Returns the static CEL type when available, otherwise the runtime type. |

No other functions are callable. `print()`, `open()`, `__import__()`,
`eval()`, `exec()`, `len()`, `range()`, `time.time()`, etc. all resolve
to `expression.unbound_name`.

## Where expressions are used

The Step Coordinator evaluates expressions at the following call sites,
all enumerated in
[design.md § Expression Evaluator](../../design/components/workflow-service/design.md#expression-evaluator-adr-011)
and the
[step-kinds table](../../design/components/workflow-service/design.md#workflow-schema-step-kinds-handled).

| Call site | YAML surface | Expected return type | Effect |
|---|---|---|---|
| Guards | `if:`, `when:`, `unless:` | `bool` | Skips the step when the result is `false` (for `unless:` when the result is `true`). |
| Input mapping | `with:` | Per-key types from the activity / sub-workflow input schema | Materializes the activity's input record before dispatch. |
| Loop iterable | `for: in:` | `list<T>` | One child sub-orchestration per element; the iterable's stable identity (index or item-key field) drives the deterministic child instance id. |
| Inline binding | `let:` | Any | Computes `let.<name>` for use within the same step. Inline-only, durable on `Step.outputs`, no ARM / connector dispatch. |
| Template placeholders | `${{ expr }}` anywhere in step YAML | Per-field type | Resolved at run start by the Definition Compiler; the result is substituted into the step's compiled execution graph. |

## Failure modes

Every error class lives in
[`custos_cel.errors`](../../src/libs/custos-cel/src/custos_cel/errors.py)
(the WF-IMPL-008 locked taxonomy). Each row below shows the canonical
`kind` string, an example expression that triggers it, and the resulting
step status under the Workflow Service Step Coordinator's failure-mode
matrix in
[design.md § Expression Evaluator](../../design/components/workflow-service/design.md#expression-evaluator-adr-011).

| `kind` | When | Example trigger | Caught at | Resulting step status |
|---|---|---|---|---|
| `expression.parse_error` | Source is not syntactically valid CEL. | `1 + ` (truncated) | Catalog publish gate (workflow rejected). | Publish refused; no step status. |
| `expression.type_error` | The AST is well-formed but operand or argument types do not match. | `1 + "two"` | Workflow Service Definition Compiler at `StartRun`. | `StartRun` rejected before a `runId` is issued. |
| `expression.unbound_name` | The name chain is not in the binding allow-list, the step id is unknown, the schema field is missing, or the function is not allow-listed. | `os.environ.HOME` | Type-check (preferred) or evaluation (fallback). | Step fails `permanent`. |
| `expression.timeout` | A single evaluation exceeds the per-evaluation wall-clock budget (default 100 ms). | A deeply-nested expression that exhausts the budget. | Evaluation. | Step fails `permanent`. |
| `expression.evaluation_error` | Well-typed AST hit a runtime failure: integer division by zero, list index out of range, missing map key on a typed value, etc. | `inputs.count / 0` | Evaluation. | Step fails `permanent`. |
| `expression.divergence` | A re-execution under Dapr replay produced a different value from the original execution (the Workflow Service Step Coordinator emits this `kind`; `custos_cel` itself never raises it). | A non-deterministic activity that mutates a `steps.<id>.outputs` value between replays. | Dapr Workflow replay. | Run fails. |

The Python class behind each `kind` is documented in
[README § Public surface](../../src/libs/custos-cel/README.md#public-surface).
Every class subclasses `Exception` (never `BaseException`) so process-
control unwinds — `KeyboardInterrupt`, `SystemExit`, `GeneratorExit` —
propagate untouched and never appear in the evaluator's metrics or
spans.

## Determinism contract

The evaluator is a **pure function** of `(typed_ast, binding_scope, clock)`.
That contract carries three operational guarantees:

1. **`now()` is replay-stable.** `DaprWorkflowClock` reads
   `current_utc_datetime` on every call; under Dapr Workflow replay the
   same logical instant is observed on every re-execution of the same
   instance. No other clock source is reachable from inside an
   expression — `time.time()`, `datetime.now()`, `time.monotonic()` all
   resolve to `expression.unbound_name`.
2. **No non-deterministic functions.** The only allow-listed functions
   are `now`, `size`, `has`, and `type`; none of them read process,
   filesystem, or network state.
3. **Inputs are frozen.** `inputs`, `run`, `workflow`, and completed
   `steps.<id>.outputs.*` are immutable from the evaluator's point of
   view; mutating any of them across replays is what the Step
   Coordinator surfaces as `expression.divergence`.

This is what allows the evaluator to be invoked many times across the
lifetime of a single Run — including under Dapr Workflow replay — and
always return the same value for the same input triple.

## Examples

The five expressions below are exactly the ones exercised by
[`src/libs/custos-cel/tests/test_docs_examples.py`](../../src/libs/custos-cel/tests/test_docs_examples.py),
which parses, type-checks, and evaluates each one against representative
bindings. Copy-paste them into a workflow YAML and they will compile.

### Example 1 — `let` (from design.md § `let` Primitive)

```cel
steps.scan.outputs.critical + steps["scan-alt"].outputs.critical
```

YAML context:

```yaml
- id: summarize
  let:
    totalCritical: ${{ steps.scan.outputs.critical + steps["scan-alt"].outputs.critical }}
```

### Example 2 — `let` ternary label (from design.md § `let` Primitive)

```cel
let.totalCritical > 0 ? "block" : "allow"
```

YAML context:

```yaml
- id: summarize
  let:
    totalCritical: ${{ steps.scan.outputs.critical + steps["scan-alt"].outputs.critical }}
    label: ${{ let.totalCritical > 0 ? "block" : "allow" }}
```

The expression assumes `totalCritical` was produced by the previous
`let:` line. Within a `let:` block, prior `let.<name>` bindings are in
scope under the `let.` root for later expressions.

### Example 3 — `if` guard

```cel
inputs.enabled && size(inputs.tags) > 0
```

YAML context:

```yaml
- id: scan
  activity: custos.builtin/vuln-scan@2
  if: ${{ inputs.enabled && size(inputs.tags) > 0 }}
  with:
    image: ${{ inputs.image }}
```

Skip the step entirely when the run was not enabled, or when no tags were
supplied. `size()` works on lists; the result is an `int` comparable to
the integer literal `0`.

### Example 4 — `with:` input mapping

```cel
inputs.image + ":" + inputs.tags[0]
```

YAML context:

```yaml
- id: scan
  activity: custos.builtin/vuln-scan@2
  with:
    target: ${{ inputs.image + ":" + inputs.tags[0] }}
```

String concatenation with `+`; dynamic list indexing with `[0]`. A
runtime out-of-range index raises `expression.evaluation_error`, which
fails the step `permanent` — so type-aware authoring should pair this
with an `if:` guard on `size(inputs.tags) > 0`.

### Example 5 — `for:` loop iterable

```cel
inputs.targets
```

YAML context:

```yaml
- id: scan-all
  for:
    in: ${{ inputs.targets }}
    as: item
  activity: custos.builtin/vuln-scan@2
  with:
    image: ${{ item.image }}
    tag: ${{ item.tag }}
```

The iterable is a `list<map<string, string>>`. The sub-orchestration
manager fans out one child workflow per element; the item-key field
(`item.image` here) drives the deterministic child instance id under
replay.

## See also

- Library README: [`src/libs/custos-cel/README.md`](../../src/libs/custos-cel/README.md)
- Design: [`design/components/workflow-service/design.md`](../../design/components/workflow-service/design.md)
- Architecture ADRs: [`design/architecture/overview.md`](../../design/architecture/overview.md#architecture-decisions)
