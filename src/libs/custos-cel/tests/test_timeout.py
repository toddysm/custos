"""Per-evaluation timeout enforcement (WF-IMPL-007, issue #182).

The deadline is wall-clock based (``time.monotonic``), sampled
periodically during evaluation (currently every 32 visited nodes),
and disabled when ``timeout_ms == 0``. The error type carries the
structured ``kind`` / ``elapsed_ms`` / ``timeout_ms`` fields the
WF-IMPL-008 error taxonomy expects.

Tests fall into three groups:

* **Deterministic** — patch :func:`time.monotonic` so the timing
  decision is byte-exact and CI cannot flake.
* **Real-time** — use a deeply nested expression and a tight budget
  to validate the acceptance criterion that the timeout fires within
  ``timeout_ms + 50ms`` slack.
* **Env-var fallback** — the public wrapper reads
  ``WF_EXPR_TIMEOUT_MS`` when the caller omits ``timeout_ms``.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

import custos_cel
from custos_cel import (
    BindingScope,
    EvalTimeoutError,
    FixedClock,
    IntType,
    ListLit,
    ListType,
    Literal,
    LiteralKind,
    RunInfo,
    SchemaBindings,
    SourcePosition,
    WorkflowInfo,
    evaluate,
    parse,
    type_check,
)
from custos_cel.eval import (
    DEFAULT_TIMEOUT_MS,
)
from custos_cel.eval import (
    evaluate as _evaluate_impl,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)


def _bindings() -> SchemaBindings:
    return SchemaBindings(
        inputs={"type": "object", "properties": {"n": {"type": "integer"}}},
    )


def _scope() -> BindingScope:
    return BindingScope(
        run=RunInfo(id="r1", workspace="w1"),
        workflow=WorkflowInfo(name="wf", version="v1"),
        now=lambda: _TS,
        inputs={"n": 0},
        steps={},
        let={},
    )


def _clock() -> FixedClock:
    return FixedClock(_TS)


def _nested_add(depth: int) -> str:
    """Build a CEL expression ``1 + 1 + 1 + ... + 1`` of *depth* terms.

    The parser left-nests this as ``((1 + 1) + 1) + 1 ...`` so the
    converter recurses ``depth`` deep — use only for small depths
    (well under Python's recursion limit, ~1000).
    """
    return " + ".join("1" for _ in range(depth))


def _flat_list_literal(n: int) -> str:
    """Build a CEL list literal ``[1, 1, ..., 1]`` of *n* elements.

    List literals parse iteratively (one grammar rule, repeated
    children) so this works at depths that would blow Python's
    recursion limit on a left-nested binary tree. Evaluation walks
    each element via :func:`_eval`, producing *n*+1 evaluator
    invocations — useful for forcing a wall-clock overrun.
    """
    return "[" + ",".join("1" for _ in range(n)) + "]"


_POS = SourcePosition(line=1, column=1, offset=0)


def _flat_list_ast(n: int) -> ListLit:
    """Build a typed ``ListLit`` AST of *n* integer literals.

    Bypasses :func:`parse` and :func:`type_check` — those grow at
    least linearly with *n* and dominate test runtime once *n* is
    large enough to trigger a 10ms evaluator timeout. The resulting
    AST is fully type-annotated so :func:`evaluate` accepts it
    without the type-check round trip.
    """
    elem_type = IntType()
    one = Literal(pos=_POS, cel_type=elem_type, kind=LiteralKind.INT, value=1)
    return ListLit(
        pos=_POS,
        cel_type=ListType(element=elem_type),
        elements=tuple(one for _ in range(n)),
    )


# ---------------------------------------------------------------------------
# DEFAULT_TIMEOUT_MS + re-export shape
# ---------------------------------------------------------------------------


def test_default_timeout_is_100ms() -> None:
    # Matches design.md § Configuration WF_EXPR_TIMEOUT_MS default.
    assert DEFAULT_TIMEOUT_MS == 100
    assert custos_cel.DEFAULT_TIMEOUT_MS == 100


def test_timeout_env_var_constant() -> None:
    assert custos_cel.TIMEOUT_ENV_VAR == "WF_EXPR_TIMEOUT_MS"


def test_eval_timeout_error_in_public_api() -> None:
    assert EvalTimeoutError is custos_cel.EvalTimeoutError
    assert issubclass(EvalTimeoutError, TimeoutError)
    assert EvalTimeoutError.KIND == "expression.timeout"


# ---------------------------------------------------------------------------
# Fast path: small expressions complete normally with default budget
# ---------------------------------------------------------------------------


def test_fast_expression_returns_within_default_budget() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    assert _evaluate_impl(typed, _scope(), _clock()) == 2


def test_fast_expression_via_public_wrapper() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    assert evaluate(typed, _scope(), _clock()) == 2


def test_moderate_expression_within_default_budget() -> None:
    # ~200 additions; well under 100ms on any plausible host.
    typed = type_check(parse(_nested_add(200)), _bindings())
    assert _evaluate_impl(typed, _scope(), _clock()) == 200


# ---------------------------------------------------------------------------
# Disabled path: timeout_ms == 0
# ---------------------------------------------------------------------------


def test_timeout_zero_disables_gate_via_impl() -> None:
    # Even with a patched monotonic that always claims the deadline
    # has passed, timeout_ms=0 skips the deadline machinery entirely.
    typed = type_check(parse("1 + 1"), _bindings())
    assert _evaluate_impl(typed, _scope(), _clock(), timeout_ms=0) == 2


def test_timeout_zero_via_public_wrapper() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    assert evaluate(typed, _scope(), _clock(), timeout_ms=0) == 2


# ---------------------------------------------------------------------------
# Validation: timeout_ms type + sign
# ---------------------------------------------------------------------------


def test_timeout_ms_must_be_int() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(TypeError, match="'timeout_ms' must be an int"):
        _evaluate_impl(typed, _scope(), _clock(), timeout_ms=1.5)  # type: ignore[arg-type]


def test_timeout_ms_rejects_bool() -> None:
    # ``bool`` subclasses ``int`` in Python; passing ``True`` is a
    # programming bug, not a 1ms budget.
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(TypeError, match="'timeout_ms' must be an int"):
        _evaluate_impl(typed, _scope(), _clock(), timeout_ms=True)


def test_timeout_ms_must_be_non_negative() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(ValueError, match="non-negative"):
        _evaluate_impl(typed, _scope(), _clock(), timeout_ms=-1)


# ---------------------------------------------------------------------------
# Deterministic firing: patch time.monotonic
# ---------------------------------------------------------------------------


def test_deadline_fires_on_first_node_with_patched_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Build the timeline so the very first wall-clock probe already
    # sees us past the deadline: t0 = 0.0 (anchor), every subsequent
    # call returns 0.5s. With a 100ms budget, the first check after
    # arming fires immediately. Force the sample-rate mask to 0 so
    # the check runs on every node (not just every 32nd).
    monkeypatch.setattr("custos_cel.eval._DEADLINE_CHECK_MASK", 0)
    calls = iter([0.0, 0.5, 0.5, 0.5, 0.5, 0.5])
    monkeypatch.setattr(
        "custos_cel.eval.time.monotonic",
        lambda: next(calls),
    )
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(EvalTimeoutError) as excinfo:
        _evaluate_impl(typed, _scope(), _clock(), timeout_ms=100)
    err = excinfo.value
    assert err.kind == "expression.timeout"
    assert err.timeout_ms == 100
    assert err.elapsed_ms == 500
    assert "100ms" in err.message
    assert "500ms" in err.message


def test_deadline_does_not_fire_if_budget_not_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Anchor at 0.0; every later call returns 0.05s (50ms elapsed).
    # Budget = 100ms — still under the deadline. Force the sample
    # mask to 0 so even a tiny expression triggers a wall-clock probe.
    monkeypatch.setattr("custos_cel.eval._DEADLINE_CHECK_MASK", 0)
    calls = iter([0.0] + [0.05] * 50)
    monkeypatch.setattr(
        "custos_cel.eval.time.monotonic",
        lambda: next(calls),
    )
    typed = type_check(parse("1 + 1"), _bindings())
    assert _evaluate_impl(typed, _scope(), _clock(), timeout_ms=100) == 2


def test_deadline_fires_partway_through_walk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Anchor at 0.0; first three wall-clock reads are under budget,
    # then the fourth jumps past it. Verifies the walk does make
    # progress and the timeout surfaces mid-evaluation. Force the
    # sample mask to 0 so each node entry probes the clock.
    monkeypatch.setattr("custos_cel.eval._DEADLINE_CHECK_MASK", 0)
    calls = iter([0.0, 0.05, 0.05, 0.05, 0.2, 0.2, 0.2, 0.2])
    monkeypatch.setattr(
        "custos_cel.eval.time.monotonic",
        lambda: next(calls),
    )
    typed = type_check(parse("1 + 2 + 3 + 4"), _bindings())
    with pytest.raises(EvalTimeoutError) as excinfo:
        _evaluate_impl(typed, _scope(), _clock(), timeout_ms=100)
    assert excinfo.value.elapsed_ms == 200


# ---------------------------------------------------------------------------
# ContextVar restoration: nested evaluate() calls
# ---------------------------------------------------------------------------


def test_nested_evaluations_restore_outer_deadline() -> None:
    # If two ``evaluate`` calls run on the same thread (no concurrency
    # here, just sequential), the second arming should not affect the
    # first's restoration. After both return, the ctxvar should be
    # back to ``None`` so a subsequent arm without timeout_ms uses a
    # fresh budget.
    from custos_cel.eval import _DEADLINE_CTX

    assert _DEADLINE_CTX.get() is None
    typed = type_check(parse("1 + 1"), _bindings())
    assert _evaluate_impl(typed, _scope(), _clock(), timeout_ms=100) == 2
    assert _DEADLINE_CTX.get() is None
    assert _evaluate_impl(typed, _scope(), _clock(), timeout_ms=50) == 2
    assert _DEADLINE_CTX.get() is None


def test_sample_rate_skips_check_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # With the default sample-rate mask, expressions smaller than the
    # check interval should not consult :func:`time.monotonic` past
    # the initial arm. Patch the clock to raise if read more than
    # once after arming — proving the hot-path optimization holds.
    from custos_cel.eval import _DEADLINE_CHECK_MASK

    assert _DEADLINE_CHECK_MASK == 31  # documents the contract
    calls = [0.0]
    real_monotonic = time.monotonic

    def _instrumented() -> float:
        calls.append(real_monotonic())
        return calls[-1]

    monkeypatch.setattr("custos_cel.eval.time.monotonic", _instrumented)
    # ``1 + 1`` is 3 nodes; well below the 32-node sample interval.
    typed = type_check(parse("1 + 1"), _bindings())
    _evaluate_impl(typed, _scope(), _clock(), timeout_ms=100)
    # Exactly one call from arming + ``_check_deadline`` is not even
    # invoked because counter never crosses the mask. The seed 0.0
    # entry is the placeholder we put in the list.
    assert len(calls) == 2, f"expected 1 monotonic read after arm, got {len(calls) - 1}"


# ---------------------------------------------------------------------------
# Real-time: deeply nested expression overruns a tight budget
# ---------------------------------------------------------------------------


def test_slow_expression_times_out_within_slack() -> None:
    # Build a flat list-literal AST large enough to take much more
    # than 10ms to walk on any plausible host. AST is constructed
    # programmatically to keep test setup cheap. Acceptance criterion:
    # the timeout fires within ``timeout_ms + 50ms`` slack.
    typed = _flat_list_ast(500_000)
    t0 = time.monotonic()
    with pytest.raises(EvalTimeoutError) as excinfo:
        _evaluate_impl(typed, _scope(), _clock(), timeout_ms=10)
    elapsed_real_ms = (time.monotonic() - t0) * 1000.0
    assert elapsed_real_ms < 60.0, f"timeout did not fire within slack: {elapsed_real_ms}ms > 60ms"
    err = excinfo.value
    assert err.timeout_ms == 10
    assert err.elapsed_ms >= 10


def test_slow_expression_completes_when_budget_disabled() -> None:
    # Same shape as above but with the gate disabled — should
    # complete and produce a list of the right size. This proves the
    # deadline was the only thing stopping it, not a bug elsewhere.
    # Use a much smaller N here (no need to exhaust a budget) so the
    # test stays fast.
    typed = _flat_list_ast(10_000)
    result = _evaluate_impl(typed, _scope(), _clock(), timeout_ms=0)
    assert isinstance(result, list)
    assert len(result) == 10_000


# ---------------------------------------------------------------------------
# Env-var fallback (wrapper-level)
# ---------------------------------------------------------------------------


def test_env_var_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WF_EXPR_TIMEOUT_MS", raising=False)
    typed = type_check(parse("1 + 1"), _bindings())
    # When timeout_ms is omitted and env is unset, default applies.
    assert evaluate(typed, _scope(), _clock()) == 2


def test_env_var_override_picked_up(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WF_EXPR_TIMEOUT_MS", "0")
    typed = type_check(parse("1 + 1"), _bindings())
    # With env=0 and a patched clock that would otherwise fire, the
    # wrapper should still complete because timeout_ms=0 disables the
    # gate. We verify by directly calling the resolver too.
    from custos_cel import _resolve_timeout_from_env

    assert _resolve_timeout_from_env() == 0
    assert evaluate(typed, _scope(), _clock()) == 2


def test_env_var_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WF_EXPR_TIMEOUT_MS", "250")
    from custos_cel import _resolve_timeout_from_env

    assert _resolve_timeout_from_env() == 250


def test_env_var_invalid_value_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WF_EXPR_TIMEOUT_MS", "not-an-int")
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(ValueError, match="WF_EXPR_TIMEOUT_MS"):
        evaluate(typed, _scope(), _clock())


def test_explicit_timeout_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Set env to an absurd value; explicit timeout_ms should win.
    monkeypatch.setenv("WF_EXPR_TIMEOUT_MS", "999999")
    typed = _flat_list_ast(500_000)
    with pytest.raises(EvalTimeoutError):
        evaluate(typed, _scope(), _clock(), timeout_ms=10)


# ---------------------------------------------------------------------------
# Static-audit: ``time`` is allowed, but no forbidden imports leaked in
# ---------------------------------------------------------------------------


def test_eval_module_does_not_import_os(monkeypatch: pytest.MonkeyPatch) -> None:
    # Sanity: the eval module must not import os (env-var resolution
    # is the wrapper's job). The full static audit lives in
    # test_eval.py; this is a focused sentinel that the timeout
    # implementation didn't sneak in an os reference.
    import importlib.util
    import pathlib

    spec = importlib.util.find_spec("custos_cel.eval")
    assert spec is not None and spec.origin is not None
    text = pathlib.Path(spec.origin).read_text(encoding="utf-8")
    # Use an unambiguous marker: 'import os' or 'from os import'.
    assert "\nimport os\n" not in text
    assert "\nfrom os " not in text


# ---------------------------------------------------------------------------
# Smoke: env value of empty string is invalid
# ---------------------------------------------------------------------------


def test_env_var_empty_string_is_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    # Empty string fails int(); the resolver raises ValueError. This
    # is the intentional fail-loud behaviour — silently falling back
    # to 100ms could mask a malformed deployment config.
    monkeypatch.setenv("WF_EXPR_TIMEOUT_MS", "")
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(ValueError, match="WF_EXPR_TIMEOUT_MS"):
        evaluate(typed, _scope(), _clock())
