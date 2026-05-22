# custos-cel

Sandboxed CEL-like expression evaluator for the Custos Workflow Service.

This shared Python library hosts the parser, type checker, and
replay-deterministic runtime for workflow expressions. It is consumed by:

- The **Workflow Service** Step Coordinator at run time (parse → type-check →
  evaluate, inside the sandbox).
- The **Catalog Service** at publish time (parser half only) for syntactic
  validation of workflow and activity manifests.

## Status

**Scaffold + parser dependency** — WF-IMPL-001 ([#176](https://github.com/toddysm/custos/issues/176)) and WF-IMPL-002 ([#177](https://github.com/toddysm/custos/issues/177)). The public surface
is in place but every entry point still raises `NotImplementedError`. The
underlying CEL parser/runtime is chosen and pinned (see below). Concrete
AST wrapping, type checker, evaluator, sandbox, observability, and
developer-docs work is split across WF-IMPL-003 through WF-IMPL-012.

## Parser / runtime

[`cel-python`](https://github.com/cloud-custodian/cel-python) (PyPI distribution `cel-python`, import name `celpy`) — pinned `cel-python>=0.5.0,<0.6`. Apache-2.0, pure Python with a Lark-generated CEL grammar, stewarded by the Cloud Custodian project. Full scorecard and rationale in change record [`2026-05-21-005-cel-parser-choice.md`](../../../design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md).

`celpy`'s `Environment.compile(source)` is the parser surface; `Environment.program(ast, functions)` + `program.evaluate(activation)` are the runtime surface. Custos consumes only `compile()` for Catalog publish-time validation, and `program()` / `evaluate()` for the sandbox at run time (the latter two are implemented in WF-IMPL-005 / WF-IMPL-006).

**Authoring note for workflow definitions**: a step id that is not a valid CEL identifier (anything containing `-` or other non-`[A-Za-z_][A-Za-z0-9_]*` characters) **must** be referenced via the bracket form in expressions — e.g. `steps["scan-alt"].outputs.critical`, not `steps.scan-alt.outputs.critical`. The dot form silently mis-parses as subtraction under any CEL implementation. See the change record § “Parser behavior worth documenting”.

## Public surface

Two distinct AST shapes are part of the contract:

- **`AST`** — the untyped, purely structural tree returned by `parse()`. Carries source positions; no resolved types, no binding information.
- **`TypedAST`** — the AST annotated with resolved types after `type_check()` resolves every identifier against a binding scope and its JSON Schemas. Required input to `evaluate()`.

Both names are exported from `custos_cel` today (aliased to `Any` in the scaffold, re-pointed at concrete classes in their respective issues), so downstream consumers — Workflow Service Step Coordinator and Catalog Service publish-time validator — can write signatures against stable names now.

| Symbol | Lands in | Purpose |
|---|---|---|
| `custos_cel.AST` | WF-IMPL-003 | Untyped parse-tree node type alias. |
| `custos_cel.TypedAST` | WF-IMPL-005 | Type-annotated parse-tree node type alias. |
| `custos_cel.parse(source) -> AST` | WF-IMPL-002, WF-IMPL-003 | Parse expression source into an **untyped** `AST`. |
| `custos_cel.type_check(ast: AST, bindings) -> TypedAST` | WF-IMPL-005 | Resolve identifiers against bindings + JSON Schemas; return a `TypedAST`. |
| `custos_cel.evaluate(ast: TypedAST, bindings) -> Any` | WF-IMPL-006 | Evaluate a `TypedAST` inside the sandbox. Rejects an untyped `AST`. |

The locked structured error taxonomy and the rest of the public API surface
land in WF-IMPL-008.

## Sandbox guarantees (target)

The runtime that lands in later issues will guarantee:

- **No side effects** — expressions cannot perform I/O, mutate bindings, or
  observe wall-clock time except through the injected `Clock` interface.
- **Replay determinism** — the same `(ast, bindings)` always produces the
  same result on Dapr Workflow replay (property-based coverage in
  WF-IMPL-010).
- **Bounded execution** — per-evaluation timeout via `WF_EXPR_TIMEOUT_MS`
  (enforced in WF-IMPL-007).

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
