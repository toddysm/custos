"""Tests for :mod:`custos_cel.scope`.

Acceptance criteria from issue #179 (WF-IMPL-004):

* Every binding listed in design.md § Expression Evaluator resolves.
* Nothing else resolves — in particular the host Python namespace is not
  exposed.
* Sealed step outputs are observably immutable; attempted mutation
  raises.
* ``mypy --strict`` clean (verified out-of-band by the gate).
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import pytest

from custos_cel import (
    BindingScope,
    RunInfo,
    SourcePosition,
    StepBinding,
    UnboundNameError,
    WorkflowInfo,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_FIXED_NOW = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)


def _clock() -> datetime:
    return _FIXED_NOW


def _scope(
    *,
    inputs: dict[str, Any] | None = None,
    steps: dict[str, StepBinding] | None = None,
    let: dict[str, Any] | None = None,
) -> BindingScope:
    return BindingScope(
        run=RunInfo(id="run-123", workspace="ws-abc"),
        workflow=WorkflowInfo(name="scan-pipeline", version="1.4.0"),
        now=_clock,
        inputs=inputs or {},
        steps=steps or {},
        let=let or {},
    )


# ---------------------------------------------------------------------------
# Every binding resolves (positive path)
# ---------------------------------------------------------------------------


def test_resolve_inputs_top_level() -> None:
    s = _scope(inputs={"image": "alpine:3.19"})
    assert s.resolve(["inputs", "image"]) == "alpine:3.19"


def test_resolve_inputs_nested_mapping() -> None:
    s = _scope(inputs={"config": {"timeout": 30, "tags": ["a", "b"]}})
    assert s.resolve(["inputs", "config", "timeout"]) == 30
    assert s.resolve(["inputs", "config", "tags"]) == ["a", "b"]


def test_resolve_run_id_and_workspace() -> None:
    s = _scope()
    assert s.resolve(["run", "id"]) == "run-123"
    assert s.resolve(["run", "workspace"]) == "ws-abc"


def test_resolve_workflow_name_and_version() -> None:
    s = _scope()
    assert s.resolve(["workflow", "name"]) == "scan-pipeline"
    assert s.resolve(["workflow", "version"]) == "1.4.0"


def test_resolve_now_returns_callable_that_returns_fixed_clock() -> None:
    s = _scope()
    now = s.resolve(["now"])
    assert callable(now)
    assert now() == _FIXED_NOW


def test_resolve_let_overlay() -> None:
    base = _scope()
    child = base.with_let(threshold=5, label="prod")
    assert child.resolve(["let", "threshold"]) == 5
    assert child.resolve(["let", "label"]) == "prod"
    # Parent scope is unchanged.
    with pytest.raises(UnboundNameError):
        base.resolve(["let", "threshold"])


def test_resolve_steps_outputs() -> None:
    scan = StepBinding({"critical": 2, "report": {"sha": "abc"}}, sealed=True)
    s = _scope(steps={"scan": scan})
    assert s.resolve(["steps", "scan", "outputs", "critical"]) == 2
    assert s.resolve(["steps", "scan", "outputs", "report", "sha"]) == "abc"


def test_resolve_steps_with_hyphenated_id() -> None:
    # Hyphenated ids reach the resolver via the bracket form
    # (steps["scan-alt"]) — see change record 005. From the scope's
    # point of view this is just a chain element.
    scan_alt = StepBinding({"ok": True}, sealed=True)
    s = _scope(steps={"scan-alt": scan_alt})
    assert s.resolve(["steps", "scan-alt", "outputs", "ok"]) is True


# ---------------------------------------------------------------------------
# Nothing else resolves (negative path)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host_name",
    [
        "os",
        "sys",
        "open",
        "__import__",
        "eval",
        "exec",
        "globals",
        "locals",
        "input",
        "__builtins__",
        "subprocess",
        "compile",
    ],
)
def test_resolve_rejects_host_python_names(host_name: str) -> None:
    s = _scope()
    with pytest.raises(UnboundNameError) as ei:
        s.resolve([host_name])
    assert ei.value.chain == (host_name,)
    assert "unknown root" in str(ei.value)


def test_resolve_rejects_empty_chain() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="empty name chain"):
        s.resolve([])


def test_resolve_rejects_str_chain() -> None:
    # ``str`` is itself a ``Sequence[str]``; without an explicit guard,
    # a stray dotted string would be silently split into characters and
    # produce a confusing UnboundNameError. Catch the type error early.
    s = _scope(inputs={"image": "alpine"})
    with pytest.raises(TypeError, match="not a single string"):
        s.resolve("inputs.image")
    with pytest.raises(TypeError, match="not a single string"):
        s.resolve("inputs")


def test_resolve_rejects_non_str_chain_element() -> None:
    s = _scope()
    with pytest.raises(TypeError, match=r"chain\[1\] must be a str"):
        s.resolve(["inputs", 0])  # type: ignore[list-item]
    with pytest.raises(TypeError, match=r"chain\[0\] must be a str"):
        s.resolve([None])  # type: ignore[list-item]


def test_resolve_rejects_unknown_step_id() -> None:
    s = _scope(steps={"scan": StepBinding({"ok": True}, sealed=True)})
    with pytest.raises(UnboundNameError) as ei:
        s.resolve(["steps", "ghost", "outputs", "ok"])
    assert ei.value.chain == ("steps", "ghost", "outputs", "ok")
    assert "no such step" in str(ei.value)


def test_resolve_rejects_steps_root_alone() -> None:
    s = _scope(steps={"scan": StepBinding(sealed=True)})
    with pytest.raises(UnboundNameError, match="pick a step id"):
        s.resolve(["steps"])


def test_resolve_rejects_step_without_outputs_segment() -> None:
    s = _scope(steps={"scan": StepBinding({"x": 1}, sealed=True)})
    with pytest.raises(UnboundNameError, match=r"pick '\.outputs\.<name>'"):
        s.resolve(["steps", "scan"])


def test_resolve_rejects_step_field_other_than_outputs() -> None:
    # design.md exposes only `.outputs` on a step. Anything else
    # (e.g. internal status, attempt count) is not part of the binding
    # surface.
    s = _scope(steps={"scan": StepBinding({"x": 1}, sealed=True)})
    with pytest.raises(UnboundNameError, match="only 'outputs' is"):
        s.resolve(["steps", "scan", "status"])


def test_resolve_rejects_step_outputs_root_alone() -> None:
    s = _scope(steps={"scan": StepBinding({"x": 1}, sealed=True)})
    with pytest.raises(UnboundNameError, match="pick a member"):
        s.resolve(["steps", "scan", "outputs"])


def test_resolve_rejects_run_bare() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="pick a member"):
        s.resolve(["run"])


def test_resolve_rejects_unknown_run_field() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="no such run field"):
        s.resolve(["run", "secrets"])


def test_resolve_rejects_workflow_bare() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="pick a member"):
        s.resolve(["workflow"])


def test_resolve_rejects_unknown_workflow_field() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="no such workflow field"):
        s.resolve(["workflow", "checksum"])


def test_resolve_rejects_sub_member_of_scalar() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="no sub-members"):
        s.resolve(["run", "id", "length"])


def test_resolve_rejects_now_with_members() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError, match="'now' has no members"):
        s.resolve(["now", "year"])


def test_resolve_rejects_inputs_root_alone() -> None:
    s = _scope(inputs={"image": "alpine"})
    with pytest.raises(UnboundNameError, match="pick a member"):
        s.resolve(["inputs"])


def test_resolve_rejects_missing_inputs_key() -> None:
    s = _scope(inputs={"image": "alpine"})
    with pytest.raises(UnboundNameError):
        s.resolve(["inputs", "nonexistent"])


def test_resolve_rejects_descent_into_scalar_input() -> None:
    s = _scope(inputs={"image": "alpine"})
    with pytest.raises(UnboundNameError, match="is not a mapping"):
        s.resolve(["inputs", "image", "tag"])


def test_resolve_rejects_let_root_alone() -> None:
    s = _scope().with_let(x=1)
    with pytest.raises(UnboundNameError, match="pick a member"):
        s.resolve(["let"])


def test_resolve_rejects_missing_let_name() -> None:
    s = _scope().with_let(x=1)
    with pytest.raises(UnboundNameError):
        s.resolve(["let", "y"])


# ---------------------------------------------------------------------------
# Error metadata
# ---------------------------------------------------------------------------


def test_unbound_name_error_carries_chain_and_position() -> None:
    s = _scope()
    pos = SourcePosition(line=4, column=12, offset=42)
    with pytest.raises(UnboundNameError) as ei:
        s.resolve(["mystery", "x"], pos=pos)
    err = ei.value
    assert err.chain == ("mystery", "x")
    assert err.pos == pos
    assert err.reason is not None
    assert "mystery" in str(err)


def test_unbound_name_error_without_position() -> None:
    s = _scope()
    with pytest.raises(UnboundNameError) as ei:
        s.resolve(["mystery"])
    assert ei.value.pos is None


# ---------------------------------------------------------------------------
# StepBinding sealing semantics
# ---------------------------------------------------------------------------


def test_step_binding_starts_unsealed_and_accepts_outputs() -> None:
    step = StepBinding()
    assert step.sealed is False
    step.set_output("x", 1)
    assert step.outputs["x"] == 1


def test_step_binding_seal_makes_outputs_immutable() -> None:
    step = StepBinding({"x": 1})
    step.seal()
    assert step.sealed is True
    with pytest.raises(ValueError, match="sealed"):
        step.set_output("y", 2)


def test_step_binding_seal_is_idempotent() -> None:
    step = StepBinding({"x": 1}).seal()
    step.seal()  # second call must not raise
    assert step.sealed is True


def test_step_binding_outputs_view_is_immutable() -> None:
    step = StepBinding({"x": 1}, sealed=True)
    view = step.outputs
    assert isinstance(view, MappingProxyType)
    with pytest.raises(TypeError):
        view["x"] = 2  # type: ignore[index]
    with pytest.raises(TypeError):
        del view["x"]  # type: ignore[attr-defined]


def test_step_binding_outputs_view_is_immutable_even_before_seal() -> None:
    # The public ``outputs`` accessor never returns a mutable view, even
    # while the step is still unsealed. Callers that need to record
    # outputs must go through ``set_output``.
    step = StepBinding({"x": 1})
    view = step.outputs
    assert isinstance(view, MappingProxyType)
    with pytest.raises(TypeError):
        view["x"] = 2  # type: ignore[index]


def test_step_binding_equality() -> None:
    a = StepBinding({"x": 1}, sealed=True)
    b = StepBinding({"x": 1}, sealed=True)
    c = StepBinding({"x": 1}, sealed=False)
    d = StepBinding({"x": 2}, sealed=True)
    assert a == b
    assert a != c
    assert a != d


def test_step_binding_is_not_hashable() -> None:
    step = StepBinding({"x": 1}, sealed=True)
    with pytest.raises(TypeError):
        hash(step)


# ---------------------------------------------------------------------------
# Scope immutability invariants
# ---------------------------------------------------------------------------


def test_scope_inputs_view_is_immutable() -> None:
    s = _scope(inputs={"image": "alpine"})
    assert isinstance(s.inputs, MappingProxyType)
    with pytest.raises(TypeError):
        s.inputs["image"] = "ubuntu"  # type: ignore[index]


def test_scope_steps_view_is_immutable() -> None:
    s = _scope(steps={"scan": StepBinding(sealed=True)})
    assert isinstance(s.steps, MappingProxyType)
    with pytest.raises(TypeError):
        s.steps["other"] = StepBinding(sealed=True)  # type: ignore[index]


def test_scope_let_view_is_immutable() -> None:
    s = _scope(let={"x": 1})
    assert isinstance(s.let, MappingProxyType)
    with pytest.raises(TypeError):
        s.let["y"] = 2  # type: ignore[index]


def test_scope_external_mutation_of_source_dict_does_not_leak() -> None:
    source: dict[str, Any] = {"image": "alpine"}
    s = _scope(inputs=source)
    source["image"] = "ubuntu"
    source["new"] = "leaked"
    # The scope owns its own copy.
    assert s.resolve(["inputs", "image"]) == "alpine"
    with pytest.raises(UnboundNameError):
        s.resolve(["inputs", "new"])


def test_scope_run_and_workflow_are_frozen() -> None:
    s = _scope()
    # ``frozen=True`` dataclasses raise ``FrozenInstanceError`` (subclass
    # of ``AttributeError``) on attribute assignment.
    with pytest.raises(AttributeError):
        s.run.id = "other"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        s.workflow.version = "9.9.9"  # type: ignore[misc]


def test_scope_itself_is_frozen() -> None:
    s = _scope()
    with pytest.raises(AttributeError):
        s.run = RunInfo(id="x", workspace="y")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# let overlay semantics
# ---------------------------------------------------------------------------


def test_with_let_does_not_mutate_parent() -> None:
    parent = _scope(let={"a": 1})
    child = parent.with_let(b=2)
    assert child.resolve(["let", "a"]) == 1
    assert child.resolve(["let", "b"]) == 2
    with pytest.raises(UnboundNameError):
        parent.resolve(["let", "b"])


def test_with_let_overlays_replace_existing() -> None:
    parent = _scope(let={"k": "old"})
    child = parent.with_let(k="new")
    assert child.resolve(["let", "k"]) == "new"
    assert parent.resolve(["let", "k"]) == "old"


def test_with_let_preserves_other_bindings() -> None:
    s = _scope(
        inputs={"image": "alpine"},
        steps={"scan": StepBinding({"ok": True}, sealed=True)},
    ).with_let(x=1)
    assert s.resolve(["inputs", "image"]) == "alpine"
    assert s.resolve(["steps", "scan", "outputs", "ok"]) is True
    assert s.resolve(["run", "id"]) == "run-123"
