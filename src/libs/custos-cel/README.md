# custos-cel

Sandboxed CEL-like expression evaluator for the Custos Workflow Service.

This shared Python library hosts the parser, type checker, and
replay-deterministic runtime for workflow expressions. It is consumed by:

- The **Workflow Service** Step Coordinator at run time (parse → type-check →
  evaluate, inside the sandbox).
- The **Catalog Service** at publish time (parser half only) for syntactic
  validation of workflow and activity manifests.

## Status

**Scaffold + parser dependency + AST data model + binding scope + type checker + sandboxed evaluator + per-evaluation timeout + locked error taxonomy** — WF-IMPL-001 ([#176](https://github.com/toddysm/custos/issues/176)), WF-IMPL-002 ([#177](https://github.com/toddysm/custos/issues/177)), WF-IMPL-003 ([#178](https://github.com/toddysm/custos/issues/178)), WF-IMPL-004 ([#179](https://github.com/toddysm/custos/issues/179)), WF-IMPL-005 ([#180](https://github.com/toddysm/custos/issues/180)), WF-IMPL-006 ([#181](https://github.com/toddysm/custos/issues/181)), WF-IMPL-007 ([#182](https://github.com/toddysm/custos/issues/182)), WF-IMPL-008 ([#183](https://github.com/toddysm/custos/issues/183)). `custos_cel.parse()` is real; `custos_cel.BindingScope` is real (immutable scope exposing only the design's seven bindings); `custos_cel.type_check()` is real (JSON-Schema-backed StartRun-time gate); `custos_cel.evaluate()` is real (sandboxed walk over a TypedAST against a `BindingScope` and a `Clock`, gated by a per-evaluation wall-clock budget configurable via `WF_EXPR_TIMEOUT_MS`); `custos_cel.errors` is the locked structured-error taxonomy (`CelError` base + `ParseError` / `TypeError` / `UnboundNameError` / `TimeoutError` / `EvaluationError` / `DivergenceError`, each with a stable `kind` string and a JSON-safe `to_dict()`). Sandbox hardening, observability, and developer-docs work continues in WF-IMPL-009 through WF-IMPL-012.

## Parser / runtime

[`cel-python`](https://github.com/cloud-custodian/cel-python) (PyPI distribution `cel-python`, import name `celpy`) — pinned `cel-python>=0.5.0,<0.6`. Apache-2.0, Python-based with a Lark-generated CEL grammar, stewarded by the Cloud Custodian project. Full scorecard and rationale in change record [`2026-05-21-005-cel-parser-choice.md`](../../../design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md).

`celpy`'s `Environment.compile(source)` is the parser surface; `Environment.program(ast, functions)` + `program.evaluate(activation)` are the runtime surface. Custos consumes only `compile()` for Catalog publish-time validation, and `program()` / `evaluate()` for the sandbox at run time (the latter two are implemented in WF-IMPL-005 / WF-IMPL-006).

**Authoring note for workflow definitions**: a step id that is not a valid CEL identifier (anything containing `-` or other non-`[A-Za-z_][A-Za-z0-9_]*` characters) **must** be referenced via the bracket form in expressions — e.g. `steps["scan-alt"].outputs.critical`, not `steps.scan-alt.outputs.critical`. The dot form silently mis-parses as subtraction under any CEL implementation. See the change record § “Parser behavior worth documenting”.

## Public surface

Two distinct AST shapes are part of the contract:

- **`AST`** — the untyped, purely structural tree returned by `parse()`. Carries source positions; no resolved types, no binding information.
- **`TypedAST`** — the AST annotated with resolved types after `type_check()` resolves every identifier against a binding scope and its JSON Schemas. Required input to `evaluate()`.

Both names resolve to `custos_cel.ast.Node` today. The same Python class represents both stages; the distinction is carried per-node in `Node.cel_type` (`None` after `parse()`, populated everywhere after `type_check()`).

| Symbol | Lands in | Purpose |
|---|---|---|
| `custos_cel.AST` | WF-IMPL-003 | Untyped parse-tree node type alias (= `Node`). |
| `custos_cel.TypedAST` | WF-IMPL-005 | Type-annotated parse-tree node type alias (= `Node`). |
| `custos_cel.parse(source) -> Node` | WF-IMPL-002, WF-IMPL-003 | Parse expression source into an **untyped** AST via `celpy`. |
| `custos_cel.type_check(ast, bindings) -> Node` | WF-IMPL-005 | Resolve identifiers against bindings + JSON Schemas; return a TypedAST. |
| `custos_cel.evaluate(ast, scope, clock, *, timeout_ms=None) -> Any` | WF-IMPL-006, WF-IMPL-007 | Evaluate a TypedAST inside the sandbox under a wall-clock budget. Rejects an untyped AST. `timeout_ms=None` falls back to `WF_EXPR_TIMEOUT_MS`; `timeout_ms=0` disables the gate. |
| `custos_cel.Clock` | WF-IMPL-006 | Runtime protocol for replay-deterministic ``now()``. |
| `custos_cel.DaprWorkflowClock` | WF-IMPL-006 | Adapter wrapping a Dapr Workflow context's `current_utc_datetime`. |
| `custos_cel.FixedClock` | WF-IMPL-006 | Deterministic test clock returning a single timezone-aware datetime. |
| `custos_cel.EvalError` | WF-IMPL-006, WF-IMPL-008 | Backwards-compat alias for `custos_cel.errors.EvaluationError` (`kind="expression.evaluation_error"`). |
| `custos_cel.EvalTimeoutError` | WF-IMPL-007, WF-IMPL-008 | Backwards-compat alias for `custos_cel.errors.TimeoutError` (`kind="expression.timeout"`); subclass of built-in `TimeoutError`, carries `elapsed_ms` / `timeout_ms`. |
| `custos_cel.DEFAULT_TIMEOUT_MS` | WF-IMPL-007 | Default per-evaluation budget (`100`), used when neither `timeout_ms` nor `WF_EXPR_TIMEOUT_MS` is set. |
| `custos_cel.TIMEOUT_ENV_VAR` | WF-IMPL-007 | Name of the env var (`"WF_EXPR_TIMEOUT_MS"`) the wrapper consults. |
| `custos_cel.to_json(node) -> str` / `from_json(text) -> Node` | WF-IMPL-003 | Byte-stable JSON serialization for `Run.compiledGraph` caching. |
| `custos_cel.BindingScope` | WF-IMPL-004 | Immutable binding scope for the evaluator (see below). |
| `custos_cel.StepBinding` | WF-IMPL-004 | Per-step output container; sealable. |
| `custos_cel.RunInfo` / `WorkflowInfo` | WF-IMPL-004 | Frozen run / workflow metadata. |
| `custos_cel.UnboundNameError` | WF-IMPL-004, WF-IMPL-008 | Raised by `BindingScope.resolve()` on any unknown name (`kind="expression.unbound_name"`); carries `name_chain` and `reason`. |
| `custos_cel.SchemaBindings` | WF-IMPL-005 | JSON-Schema-backed binding declarations for the type checker. |
| `custos_cel.TypeCheckError` | WF-IMPL-005, WF-IMPL-008 | Backwards-compat alias for `custos_cel.errors.TypeError` (`kind="expression.type_error"`); carries `expected_type` / `actual_type`. |
| `custos_cel.TimestampType` | WF-IMPL-005 | New `CelType` for `google.protobuf.Timestamp` (used as the static return type of `now()`). |
| `custos_cel.errors` submodule | WF-IMPL-008 | Locked structured-error taxonomy: `CelError` (base), `ParseError`, `TypeError`, `UnboundNameError`, `TimeoutError`, `EvaluationError`, `DivergenceError`. Each class carries `kind` / `message` / `source_position` and a JSON-safe `to_dict()`. |
| `custos_cel.CelError` | WF-IMPL-008 | Re-export of `custos_cel.errors.CelError` for convenience. |
| `custos_cel.ParseError` | WF-IMPL-008 | Re-export of `custos_cel.errors.ParseError` (`kind="expression.parse_error"`). Raised by `parse()` for both `celpy` lexer/parser failures and converter-level rejections (e.g. method-call syntax). |
| `custos_cel.EvaluationError` | WF-IMPL-008 | Re-export of `custos_cel.errors.EvaluationError`. Canonical name for the WF-IMPL-006 runtime catch-all (e.g. division by zero). |
| `custos_cel.DivergenceError` | WF-IMPL-008 | Re-export of `custos_cel.errors.DivergenceError` (`kind="expression.divergence"`); raised by the Workflow Service Step Coordinator on replay non-determinism. |

The locked structured error taxonomy is part of the WF-IMPL-008 surface and
is described in detail below.

## AST data model

Defined in [`custos_cel.ast`](src/custos_cel/ast.py). Every node is a frozen `dataclass(kw_only=True)` with:

- `pos: SourcePosition` (1-indexed `line` / `column` / `offset`, any of which may be `None` when the parser does not emit them).
- `cel_type: CelType | None` (set by the type checker; `None` after `parse()`).

Node types: `Literal`, `Ident`, `Member` (`a.b`), `Index` (`a[b]`), `Call` (`f(args)`), `Conditional` (`c ? a : b`), `Binary`, `Unary`, `ListLit`, `MapLit`. Literal value type is discriminated by `LiteralKind` (`int`, `uint`, `double`, `bool`, `string`, `bytes`, `null`). Binary/unary operators are discriminated by `BinaryOp` / `UnaryOp` enums.

`CelType` hierarchy: scalar singletons (`IntType`, `UintType`, `DoubleType`, `BoolType`, `StringType`, `BytesType`, `NullType`) plus parameterized `ListType(element)` and `MapType(key, value)`.

**Serialization**: `Node.to_dict()` returns a JSON-safe dict tagged with `node`, `pos`, and (when typed) `cel_type`. `node_from_dict()` is the inverse. The versioned envelope helpers `to_dict_envelope(root)` / `from_dict(envelope)` add a `schema_version` (currently `AST_SCHEMA_VERSION = 1`); the convenience wrappers `to_json(node)` / `from_json(text)` produce byte-stable canonical JSON (sorted keys, minimized separators) suitable for `Run.compiledGraph` cache keys per the [bundle-h change record](../../../design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md).

Bytes literals serialize as hex strings inside the JSON form (so the envelope is plain JSON, no base64 dependency). Map entries are serialized as ordered `[key, value]` pairs to preserve source order.

## Binding scope

Defined in [`custos_cel.scope`](src/custos_cel/scope.py). The Step Coordinator constructs one `BindingScope` per evaluation; it exposes only the seven roots from [design.md § Expression Evaluator](../../../design/components/workflow-service/design.md):

| Root | Source | Mutability |
|---|---|---|
| `inputs.*` | Run inputs at start | Immutable (wrapped in `MappingProxyType`) |
| `steps.<id>.outputs.*` | Completed step outputs | Immutable once `StepBinding.seal()` is called |
| `run.id` / `run.workspace` | Run identity | Frozen at construction |
| `workflow.name` / `workflow.version` | Workflow metadata | Frozen at construction |
| `now()` | Replay-deterministic clock callable | Injected by the Step Coordinator (typically Dapr Workflow's `current_utc_datetime`) |
| `let.<name>` | Inline `let` bindings | Per-evaluation overlay; immutable within one block |

Nothing else is resolvable. The host Python namespace is structurally unreachable: names like `os`, `sys`, `open`, `__import__`, `eval`, `exec`, `subprocess` all raise `UnboundNameError` from `BindingScope.resolve()` *before* any attribute or item access happens — the allow-list is keyed on the allowed root identifiers.

`BindingScope.resolve(chain, *, pos=None)` takes a flattened dotted-name chain (e.g. `["steps", "scan", "outputs", "critical"]`) and returns the resolved value. `UnboundNameError` carries the original `chain`, the optional `SourcePosition`, and a short machine-readable `reason` (`unknown root`, `no such step`, `is not a mapping`, etc.).

`BindingScope.with_let(**overlay)` returns a new scope with additional `let` bindings overlaid — the original scope is unchanged, which is what lets `let:` blocks expand into fresh child scopes without invalidating the parent.

## Type checker

Defined in [`custos_cel.types`](src/custos_cel/types.py). `custos_cel.type_check(ast, bindings)` walks an untyped AST and returns a `TypedAST` with `cel_type` populated on every node, or raises a structured `TypeCheckError` (subclass of Python's `TypeError`) with the source position of the offending node. Unknown identifiers, step ids, and schema fields raise the existing `UnboundNameError`.

`SchemaBindings` carries the JSON-Schema declarations the checker resolves against:

| Field | Shape | Meaning |
|---|---|---|
| `inputs` | JSON Schema (object) | The run's inputs schema. Object `properties` drill into per-key types; `additionalProperties` model homogeneous maps. |
| `prior_steps` | Ordered `[(step_id, outputs_schema), ...]` | Each completed step's outputs JSON Schema. Lookup is by id; order is preserved for error messages. |
| `let` | `name -> CelType` | Declared `let.<name>` types. The Catalog Service publish gate validates these structurally; the type checker trusts them at StartRun. |
| `run` | `name -> CelType` (default: `{id: string, workspace: string}`) | Static types of `run.*` members. |
| `workflow` | `name -> CelType` (default: `{name: string, version: string}`) | Static types of `workflow.*` members. |
| `now` | `CelType` (default: `TimestampType`) | Static return type of `now()`. |

JSON Schema → `CelType` translation: `integer→int`, `string→string`, `boolean→bool`, `number→double`, `array→list<T>` (requires an `items` sub-schema), `object→map<string, T>` (homogeneous via `additionalProperties`) or placeholder `map<string, null>` for heterogeneous records (member access drills into `properties` directly). Nullable scalars (`"type": ["string", "null"]`) are accepted and modeled as the non-null type.

Operator typing matches CEL standard rules: arithmetic requires matching numeric operands (no implicit int↔double promotion), comparison requires matching comparable operands, equality allows null on either side, `in` checks element/key types against the right-hand `list`/`map`, ternary branches must unify (with null promotion). Only `now()` is whitelisted as a function for this phase; any other call site raises `TypeCheckError("unknown function")` until further stdlib functions land alongside the evaluator.

Where it runs: the Workflow Service Definition Compiler at `StartRun` (per the bundle-h change record [`2026-05-18-003-bundle-h-cel-parse-surface.md`](../../../design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md)). Catalog Service has already gated syntax at publish time; this is the only place a type-error path runs, and failure is permanent — the Validator rejects the `StartRun` request before a `runId` is issued.

## Sandbox guarantees

The runtime guarantees, as of WF-IMPL-008:

- **No side effects** — expressions cannot perform I/O, mutate bindings, or
  observe wall-clock time except through the injected `Clock`. A static
  audit (`tests/test_eval.py::test_eval_module_does_not_import_dangerous_stdlib`)
  asserts `custos_cel/eval.py` contains zero `os`/`sys`/`subprocess`/
  `socket`/`importlib`/`open`/`__import__`/`eval`/`exec`/`compile` imports
  or calls.
- **Replay determinism** — the same `(ast, scope, clock)` always produces
  the same result. `DaprWorkflowClock` reads `current_utc_datetime` on
  every call so Dapr Workflow replays observe identical timestamps;
  `FixedClock` returns a single byte-identical instant for tests.
  Property-based coverage continues in WF-IMPL-010.
- **Function allow-list** — the evaluator dispatches `now`, `size`, `has`,
  and `type`. Every other `Call.function` raises `UnboundNameError(reason="function ... is not in the evaluator allow-list")`,
  making `open()`, `__import__()`, `eval()`, `exec()` structurally
  unreachable from inside an expression.
- **Strict typing** — the evaluator does not implicitly coerce `int↔double`,
  `bool↔int`, or `str↔bytes`. Each binary operator's defensive branch
  surfaces a structured `EvalError` if the type checker ever admitted an
  ill-typed tree.
- **Bounded execution** — per-evaluation timeout (default 100ms,
  configurable via `WF_EXPR_TIMEOUT_MS`) enforced by
  `evaluate(..., timeout_ms=...)`. The walker increments a per-evaluation
  counter on each node entry and consults `time.monotonic()` every 32
  nodes against a precomputed deadline; on overrun an
  `EvalTimeoutError` (subclass of `TimeoutError`) carries
  `kind="expression.timeout"`, `elapsed_ms`, and `timeout_ms`. See the
  Timeout section below for details.

## Evaluator

Defined in [`custos_cel.eval`](src/custos_cel/eval.py). `custos_cel.evaluate(ast, scope, clock)` walks the type-checked tree:

- Member / Index chains are *chain-collapsed*: a contiguous prefix of
  compile-time-known accessors (a `Member.name` or a string-literal
  `Index.index`) hands a single dotted chain to
  `BindingScope.resolve()`. Any trailing dynamic accessors (runtime ints
  for list indexing, runtime strings for map lookup) apply against the
  resolved value via `_runtime_access`. This keeps the scope's strict
  root allow-list in front of every host access while still supporting
  the design's full expression shapes (`steps.scan.outputs.critical +
  steps["scan-alt"].outputs.critical`).
- Operator dispatch enforces CEL strict typing. Integer `/` and `%`
  truncate toward zero (C semantics) rather than Python's floor;
  `_trunc_div` makes the divergence explicit.
- Builtins follow CEL macro semantics: `has(x.y)` is true when `x` is a
  reachable mapping and `"y"` is a key in it, false otherwise (lists,
  scalars, datetimes, and `RunInfo` / `WorkflowInfo` all report False),
  but propagates `UnboundNameError` when the *target* is itself unbound
  so typos surface loudly. `type(x)` prefers the static
  `Node.cel_type` (CEL declared types) and falls back to a runtime
  isinstance probe only when called against an untyped node.

## Clock

Defined in [`custos_cel.clock`](src/custos_cel/clock.py). The `Clock`
protocol is a single-method, `@runtime_checkable` `Protocol` (`now() ->
datetime`). Two adapters ship with the library:

- **`DaprWorkflowClock(ctx)`** — wraps any object exposing
  `current_utc_datetime`. Reads the attribute on every call so replays
  observe Dapr's replay-stable timestamps. Naive datetimes are
  defensively retagged `UTC` so downstream comparisons stay tz-aware.
  Imports zero Dapr packages — production wiring injects the workflow
  context duck-typed at call sites.
- **`FixedClock(fixed)`** — a frozen dataclass returning a single
  timezone-aware datetime byte-identically on every call. Rejects naive
  datetimes at construction. Equal `FixedClock` instances compare equal,
  which makes byte-determinism tests trivial.

## Timeout

Defined in [`custos_cel.eval`](src/custos_cel/eval.py). Every evaluation
runs under a wall-clock budget enforced cooperatively by the walker.

**Resolution order** (the public `custos_cel.evaluate(...)` wrapper):

1. The explicit `timeout_ms=` keyword argument, if the caller passed one.
2. The `WF_EXPR_TIMEOUT_MS` environment variable, parsed as an int. An
   invalid value (non-integer or empty string) raises `ValueError`
   referencing `TIMEOUT_ENV_VAR` — fail-loud on misconfiguration rather
   than silently falling back to the default.
3. `DEFAULT_TIMEOUT_MS = 100` if both are absent.

**Special values**:

- `timeout_ms=0` disables the gate. No ContextVar deadline is armed,
  no deadline-sampling counter is active, and `time.monotonic()` is
  not consulted; evaluation proceeds without deadline enforcement.
  Intended for tests and for callers that wrap the evaluator in their
  own deadline machinery.
- `timeout_ms < 0` raises `ValueError`. `bool` is rejected (it
  subclasses `int` in Python but is a programming bug as a budget).

**Mechanics**:

- The deadline source is `time.monotonic()`, independent of the
  user-visible `now()` clock. A wall-clock jump cannot expand or
  shrink the budget.
- The walker keeps a per-evaluation counter on a `_Deadline` state
  object propagated via `ContextVar`. Every recursive `_eval()` entry
  increments the counter; every 32 nodes (`counter & 31 == 0`) it
  reads `time.monotonic()` and compares against the precomputed
  deadline. The amortized syscall keeps fast-path overhead within the
  20% ceiling the acceptance criterion imposes vs. the bare
  WF-IMPL-006 evaluator.
- Detection latency is bounded by the sample interval times the
  per-node cost. For a 10ms budget with ~3µs/node, detection lands
  within ~100µs of the deadline — well inside the
  `timeout_ms + 50ms` slack the acceptance criterion permits.
- On overrun the walker raises `EvalTimeoutError`, a subclass of
  built-in `TimeoutError`, carrying:
  - `kind = "expression.timeout"`
  - `message` (e.g. `"expression evaluation exceeded 100ms budget (123ms elapsed)"`)
  - `elapsed_ms` (integer milliseconds since deadline arming)
  - `timeout_ms` (the budget that was exceeded)

**Nested evaluations**: the `_Deadline` ContextVar is set/reset around
each `evaluate()` call, so a nested `evaluate()` (e.g. a future
activity-level wrapper that re-enters the evaluator) does not
contaminate the outer call's budget. After every `evaluate()` returns
or unwinds, the ContextVar is restored to its prior value.

## Error taxonomy

Defined in [`custos_cel.errors`](src/custos_cel/errors.py). Every
public entry point raises exactly one of these classes. The taxonomy
is locked: the `kind` strings are part of the audit-event contract
with the Observability Service and the Step Coordinator's emission
path, so changing one is a downstream contract break.

| Class | Kind | Python parent (besides `CelError`) | Extra fields | Raised by |
|---|---|---|---|---|
| `CelError` | (abstract) | `Exception` | — | (base; never raised directly) |
| `ParseError` | `expression.parse_error` | `ValueError` | — | `parse()` (wraps `celpy.celparser.CELParseError` and the internal `CelConvertError`) |
| `TypeError` | `expression.type_error` | builtin `TypeError` | `expected_type`, `actual_type` | `type_check()` |
| `UnboundNameError` | `expression.unbound_name` | `LookupError` | `name_chain`, `reason` | `BindingScope.resolve()`; surfaces from `type_check()` and `evaluate()` for unknown roots / step ids / schema fields / non-allow-listed functions |
| `TimeoutError` | `expression.timeout` | builtin `TimeoutError` | `elapsed_ms`, `timeout_ms` | `evaluate()` when the wall-clock budget is exceeded |
| `EvaluationError` | `expression.evaluation_error` | `RuntimeError` | — | `evaluate()` for value-level runtime failures (division by zero, out-of-range index, runtime type-shape mismatch that escaped the type checker) |
| `DivergenceError` | `expression.divergence` | `RuntimeError` | — | Not raised by `custos_cel` itself; constructed and emitted by the Workflow Service Step Coordinator on Dapr Workflow replay non-determinism. Lives in this taxonomy so downstream audit consumers key off a single `kind` regardless of which component fired |

**Shared shape**. Every class — including `CelError` — exposes:

- `kind: str` — the locked string above (also available as `cls.KIND`).
- `message: str` — human-readable summary; `str(err)` returns the same.
- `source_position: SourcePosition | None` — 1-indexed `line` / `column` / 0-indexed `offset` of the offending node when available.
- `to_dict() -> dict[str, Any]` — JSON-safe dict for audit emission. Key order is stable: `kind`, `message`, `source_position` first, then any subclass extras in declaration order. The subclass extras hook is `_extra_fields()`; downstream code never needs to introspect class structure.
- Structured `__repr__` echoing the same fields.

**Backwards-compatible names** (preserved for existing call sites and
the `custos_cel` public re-export):

- `custos_cel.EvalError` → `custos_cel.errors.EvaluationError` (same class object).
- `custos_cel.EvalTimeoutError` → `custos_cel.errors.TimeoutError`.
- `custos_cel.TypeCheckError` → `custos_cel.errors.TypeError`.
- `UnboundNameError.chain` and `.pos` are kept alongside the canonical `.name_chain` and `.source_position`.

The `TypeError` and `TimeoutError` names live on `custos_cel.errors`
only (and not on the top-level `custos_cel` package) so
`from custos_cel import *` cannot accidentally shadow the Python
builtins of the same name. Import them explicitly when needed:

```python
from custos_cel.errors import TypeError as CelTypeError, TimeoutError as CelTimeoutError
```

**Catching contracts**. Every taxonomy class is hashable (via
`Exception`'s identity hash) and `json.dumps(err.to_dict())` round-trips
without a custom encoder. Generic-builtin catches still work:

```python
try:
    typed = custos_cel.type_check(custos_cel.parse(src), bindings)
    result = custos_cel.evaluate(typed, scope, clock)
except custos_cel.errors.CelError as err:
    audit.emit(err.to_dict())  # locked kind + structured fields
    raise
except ValueError:
    # Still fires for ParseError, since ParseError(CelError, ValueError).
    ...
```

## Design references

- [Workflow Service design § Expression Evaluator](../../../design/components/workflow-service/design.md)
  (ADR-011 — sandboxed CEL with capability-restricted bindings).
- [bundle-h change — parser surface for Catalog publish-time validation](../../../design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md).
- [change 005 — CEL parser dependency choice](../../../design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md).

## Development

```bash
cd src/libs/custos-cel
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

CI for this library lives in
[`.github/workflows/python-libs.yml`](../../../.github/workflows/python-libs.yml)
(job: `custos-cel`).

## License

Apache-2.0. See [LICENSE](../../../LICENSE).
