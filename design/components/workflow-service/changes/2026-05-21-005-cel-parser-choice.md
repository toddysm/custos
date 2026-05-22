# Change: cel-parser-choice

Date: 2026-05-21
Type: component-design
Component: workflow-service
Sequence: 005
GitHub Issue: #177
Status: closed

## Summary

Locks the CEL parser/runtime dependency for the Custos Workflow Service Expression Evaluator (`src/libs/custos-cel`) at **[`cel-python`](https://github.com/cloud-custodian/cel-python)** (PyPI distribution `cel-python`, import name `celpy`). Pinned `cel-python>=0.5.0,<0.6` in [`src/libs/custos-cel/pyproject.toml`](../../../../src/libs/custos-cel/pyproject.toml). This resolves WF-IMPL-002 and unblocks WF-IMPL-003 (AST wrapping) and WF-IMPL-005/006 (type checker + sandboxed evaluator).

The parser surface is also the only one Catalog Service needs at workflow-publish time (per change record [`bundle-h-cel-parse-surface`](2026-05-18-003-bundle-h-cel-parse-surface.md)), so the same dependency satisfies both consumers without dragging the runtime into Catalog's process.

## Before

[`src/libs/custos-cel/pyproject.toml`](../../../../src/libs/custos-cel/pyproject.toml) shipped with `dependencies = []`. The Expression Evaluator design in [`design/components/workflow-service/design.md`](../design.md) § Expression Evaluator names "CEL subset (ADR-011)" as the language but does not pin an implementation. No candidate survey existed.

## After

`custos-cel` declares a single runtime dependency:

```toml
# src/libs/custos-cel/pyproject.toml
dependencies = [
    "cel-python>=0.5.0,<0.6",
]
```

A scaffold-level smoke test ([`tests/test_parser_smoke.py`](../../../../src/libs/custos-cel/tests/test_parser_smoke.py)) exercises `celpy.Environment().compile(source)` against the canonical issue example and against an obviously malformed expression. It does **not** call `program()` or `evaluate()` — those are implemented in WF-IMPL-005 / WF-IMPL-006.

[`src/libs/custos-cel/README.md`](../../../../src/libs/custos-cel/README.md) is updated to name `celpy` as the parser/runtime, with a pointer back to this record.

## Candidate survey

Two real PyPI projects ship under names matching `cel*` plus the always-available option of a hand-rolled subset:

| Option | Verdict | Why |
|---|---|---|
| **`cel-python`** (cloud-custodian/cel-python, import `celpy`) | ✅ **chosen** | See full analysis below. |
| `CelPy` (PyPI distribution name `CelPy`, version 0.0.1, released 2022-03-18) | ❌ rejected on first inspection | Name-squat / unrelated joke package ("changes some method calls so that they have a different name … because of their OCD"). Not a CEL implementation. |
| Hand-rolled CEL subset on `lark` or `pyparsing` | ❌ rejected | Significant effort to author a CEL-spec-compliant grammar, bug-compatibility with cel-spec is hard to maintain, and we would re-implement work that `cel-python` already does well. Would only be justified if `cel-python` were unmaintained or its license were incompatible — neither is the case. |

### `cel-python` scorecard

| Criterion | Finding |
|---|---|
| **License** | Apache-2.0. Compatible with Custos (Apache-2.0). |
| **Maintenance signal** | Active. Latest PyPI release **0.5.0 on 2026-01-31** (≈3.5 months ago); `0.5.1` already cut on `main` (not yet published). 4 releases total. Most-recent commits on `main` from "yesterday" and "2 days ago" at the time of this writing. 13 contributors, 163 stars, 35 forks. |
| **Provenance** | Stewarded by the Cloud Custodian project (Capital One Services originally). Embedded inside C7N as the security-policy filter — meaning it is exercised in production at non-trivial scale. |
| **Supply-chain footprint** | 5 runtime deps: `google-re2` (binary wheel), `jmespath`, `lark`, `pendulum`, `pyyaml`. Larger than ideal but each is mature and well-known. We accept this cost in exchange for not authoring our own CEL parser. The `google-re2` wheel resolution can fail on exotic platforms, so this remains a packaging risk to validate during platform bring-up. |
| **ADR-011 subset conformance** | The library implements full CEL including macros (`has`, `all`, `exists`, `exists_one`, `filter`, `map`) and `dyn`. Subsetting happens **outside** the parser: we walk the parse tree in WF-IMPL-003/005 and reject any node that uses a disallowed macro or `dyn`. Extension functions are out-of-band — the `functions` map passed to `Environment.program(ast, functions)` is the **only** injection point, and we will pass an empty/whitelisted map in WF-IMPL-006. The parser stage does not reach the function map at all, so the parser surface is naturally pure. |
| **API ergonomics** | Three-stage `Environment.compile(source) → ast`, `env.program(ast, functions) → prgm`, `prgm.evaluate(activation) → value` matches our needs perfectly. We use only `compile()` in the parser-only path that Catalog needs, and we add `program()`/`evaluate()` on top in WF-IMPL-005/006. AST is a `lark.Tree` we can walk, copy, and serialize. |
| **Python version support** | `requires-python = ">=3.10"`. tox env list covers `py310, py311, py312, py313, py314`. Our `>=3.11` requirement is well inside the supported range. |
| **Type stubs** | No PEP 561 marker as of 0.5.0. Mitigated by a `[[tool.mypy.overrides]]` block in `pyproject.toml` declaring `celpy` and `celpy.*` as `ignore_missing_imports = true`. All `custos_cel` public surface stays `mypy --strict`-clean. |
| **Pre-1.0 ABI risk** | 0.x series — minor versions may break API. Mitigated by the tight `>=0.5.0,<0.6` pin. When 1.0 lands we revisit, and an internal wrapper around `Environment` (introduced in WF-IMPL-003) will absorb most breakage. |
| **License of bundled `google-re2`** | BSD-3-Clause (per the upstream re2 project). Compatible with Apache-2.0. |

### Parser behavior worth documenting

The WF-IMPL-002 issue cites the example expression `steps.scan.outputs.critical + steps.scan-alt.outputs.critical`. **This literal form silently mis-parses** under any CEL implementation (including `celpy`), because `-` is the subtraction operator and CEL identifiers are `[A-Za-z_][A-Za-z0-9_]*`. The verified parse on celpy 0.5.0 is the expected `steps.scan.outputs.critical + (steps.scan) - (alt.outputs.critical)`, not the intended addition.

Consequence: any workflow step `id` that contains a hyphen (or any other non-identifier character) **must** be referenced via the bracket form `steps["scan-alt"].outputs.critical` in CEL expressions. The bracket form parses correctly under `celpy` and is already exercised in [`test_parser_smoke.py`](../../../../src/libs/custos-cel/tests/test_parser_smoke.py).

Two places will enforce this downstream (out of scope for this change):

- **Catalog Service** at publish time, using the same `celpy` parser, can reject any CEL expression where a dot-form member access references an identifier whose source name in the binding scope is not a valid CEL identifier. This will be added as a publish-validation rule in a later catalog change.
- **`custos_cel` AST walker** (WF-IMPL-003) will normalize and surface a structured error if a dot-form access encounters a name that is not in the typed binding scope, with a hint suggesting the bracket form.

This finding is captured here (not as a code change to celpy) because it is a property of the CEL grammar, not of the implementation.

## Impact

- **`src/libs/custos-cel`** gains one runtime dependency (`cel-python`) and a parser-only smoke-test file. No public-API surface changes — the `parse() / type_check() / evaluate()` stubs still raise `NotImplementedError`; wrapping the celpy AST happens in WF-IMPL-003.
- **`mypy --strict`** continues to pass: celpy is declared untyped at the package boundary via `[[tool.mypy.overrides]]`.
- **CI** (`.github/workflows/python-libs.yml` job `custos-cel`) now resolves and installs `cel-python` on Python 3.11 × 3.12.
- **WF-IMPL-003** (AST wrapping) and **WF-IMPL-005/006** (type checker + sandboxed evaluator) are unblocked.
- **Catalog Service** publish-time validation work can begin pulling `custos-cel` as a build dep with the parser surface guaranteed stable (no `program()` or `evaluate()` ever needed on the Catalog side).
- **No design-document changes**. The Expression Evaluator design (`design.md` § Expression Evaluator) remains implementation-agnostic at the design-doc level; the dependency choice is an implementation concern recorded here.

## Files changed

- [`src/libs/custos-cel/pyproject.toml`](../../../../src/libs/custos-cel/pyproject.toml) — added `cel-python>=0.5.0,<0.6` runtime dep + mypy override for `celpy.*`.
- [`src/libs/custos-cel/tests/test_parser_smoke.py`](../../../../src/libs/custos-cel/tests/test_parser_smoke.py) — new file; 4 parser-only proof-of-life assertions.
- [`src/libs/custos-cel/README.md`](../../../../src/libs/custos-cel/README.md) — names `celpy` as the parser/runtime; cross-references this record and the hyphen-in-step-id finding.
- [`design/components/workflow-service/todos.md`](../todos.md) — WF-IMPL-002 moved to Closed.
- [`design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md`](2026-05-21-005-cel-parser-choice.md) — this file.

## Related Requirements

- REQ-010 (retry policy expressions) — uses the evaluator.
- REQ-051 / REQ-081 (workflow execution semantics) — depends on a deterministic sandboxed evaluator.

## Related Issues

- Closes #177 (WF-IMPL-002).
- Unblocks #178 (WF-IMPL-003: AST + serializable typed-AST data model).
- Cross-references #100 / change record [`bundle-h-cel-parse-surface`](2026-05-18-003-bundle-h-cel-parse-surface.md) — the parser-only contract that Catalog Service depends on.
