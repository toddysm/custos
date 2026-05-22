# custos-cel

Sandboxed CEL-like expression evaluator for the Custos Workflow Service.

This shared Python library hosts the parser, type checker, and
replay-deterministic runtime for workflow expressions. It is consumed by:

- The **Workflow Service** Step Coordinator at run time (parse → type-check →
  evaluate, inside the sandbox).
- The **Catalog Service** at publish time (parser half only) for syntactic
  validation of workflow and activity manifests.

## Status

**Scaffold + parser dependency + AST data model** — WF-IMPL-001 ([#176](https://github.com/toddysm/custos/issues/176)), WF-IMPL-002 ([#177](https://github.com/toddysm/custos/issues/177)), WF-IMPL-003 ([#178](https://github.com/toddysm/custos/issues/178)). `custos_cel.parse()` is real: it delegates to `celpy` and converts the parse tree into the internal `custos_cel.ast.Node` shape, which is the same Python class used for both the untyped AST and the TypedAST. `custos_cel.type_check()` and `custos_cel.evaluate()` remain `NotImplementedError` stubs until WF-IMPL-005 / WF-IMPL-006. Sandbox, observability, and developer-docs work is split across WF-IMPL-007 through WF-IMPL-012.

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
| `custos_cel.evaluate(ast, bindings) -> Any` | WF-IMPL-006 | Evaluate a TypedAST inside the sandbox. Rejects an untyped AST. |
| `custos_cel.to_json(node) -> str` / `from_json(text) -> Node` | WF-IMPL-003 | Byte-stable JSON serialization for `Run.compiledGraph` caching. |

The locked structured error taxonomy and the rest of the public API surface
land in WF-IMPL-008.

## AST data model

Defined in [`custos_cel.ast`](src/custos_cel/ast.py). Every node is a frozen `dataclass(kw_only=True)` with:

- `pos: SourcePosition` (1-indexed `line` / `column` / `offset`, any of which may be `None` when the parser does not emit them).
- `cel_type: CelType | None` (set by the type checker; `None` after `parse()`).

Node types: `Literal`, `Ident`, `Member` (`a.b`), `Index` (`a[b]`), `Call` (`f(args)`), `Conditional` (`c ? a : b`), `Binary`, `Unary`, `ListLit`, `MapLit`. Literal value type is discriminated by `LiteralKind` (`int`, `uint`, `double`, `bool`, `string`, `bytes`, `null`). Binary/unary operators are discriminated by `BinaryOp` / `UnaryOp` enums.

`CelType` hierarchy: scalar singletons (`IntType`, `UintType`, `DoubleType`, `BoolType`, `StringType`, `BytesType`, `NullType`) plus parameterized `ListType(element)` and `MapType(key, value)`.

**Serialization**: `Node.to_dict()` returns a JSON-safe dict tagged with `node`, `pos`, and (when typed) `cel_type`. `node_from_dict()` is the inverse. The versioned envelope helpers `to_dict_envelope(root)` / `from_dict(envelope)` add a `schema_version` (currently `AST_SCHEMA_VERSION = 1`); the convenience wrappers `to_json(node)` / `from_json(text)` produce byte-stable canonical JSON (sorted keys, minimized separators) suitable for `Run.compiledGraph` cache keys per the [bundle-h change record](../../../design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md).

Bytes literals serialize as hex strings inside the JSON form (so the envelope is plain JSON, no base64 dependency). Map entries are serialized as ordered `[key, value]` pairs to preserve source order.

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
