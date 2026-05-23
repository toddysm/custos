"""Immutable binding scope for the Custos CEL evaluator.

Implements the binding model from
``design/components/workflow-service/design.md`` § Expression Evaluator.
The Step Coordinator (WF-IMPL-006) constructs a :class:`BindingScope` once
per evaluation and hands it to :func:`custos_cel.evaluate`. The scope
exposes only the names listed in the design's bindings table:

* ``inputs.*`` — run inputs at start (immutable).
* ``steps.<id>.outputs.*`` — completed step outputs (immutable once sealed).
* ``run.id`` / ``run.workspace`` — run-scoped identity (frozen at construction).
* ``workflow.name`` / ``workflow.version`` — workflow metadata
  (frozen at construction).
* ``now()`` — replay-deterministic clock callable injected by the Step
  Coordinator (typically Dapr Workflow's ``current_utc_datetime``).
* ``let.<name>`` — per-evaluation overlay; immutable within a single
  ``let`` block expansion.

Nothing else is resolvable. In particular, the host Python namespace is
**not** exposed: names like ``os``, ``sys``, ``open``, ``__import__``,
``eval``, ``exec`` all raise :class:`UnboundNameError`. Resolution is a
strict allow-list keyed on the allowed root identifiers above; unknown
roots are rejected before any attribute or item access happens.

See the issue: https://github.com/toddysm/custos/issues/179
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Final

from custos_cel.ast import SourcePosition
from custos_cel.errors import UnboundNameError

# A CEL-evaluable value. Concrete shape is constrained by the type checker
# (WF-IMPL-005); the scope itself is value-shape-agnostic, so this is a
# documented alias for ``Any`` rather than a structural type.
BindingValue = Any


# :class:`UnboundNameError` is re-exported from :mod:`custos_cel.errors`
# (WF-IMPL-008) so the locked taxonomy has a single home. The constructor
# is back-compat with WF-IMPL-004 callers that pass the chain
# positionally as ``chain=`` (the new field is ``name_chain``).


# ---------------------------------------------------------------------------
# Frozen value containers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True, slots=True)
class RunInfo:
    """Run-scoped bindings. Maps to ``run.id`` and ``run.workspace``.

    Frozen at scope construction; both fields are required.
    """

    id: str
    workspace: str


@dataclass(frozen=True, kw_only=True, slots=True)
class WorkflowInfo:
    """Workflow-scoped bindings. Maps to ``workflow.name`` and
    ``workflow.version``.

    Frozen at scope construction; both fields are required.
    """

    name: str
    version: str


# ---------------------------------------------------------------------------
# Step outputs with sealing
# ---------------------------------------------------------------------------


class StepBinding:
    """Outputs of a single step.

    A step starts unsealed. The Step Coordinator may call
    :meth:`set_output` zero or more times as the step's activity reports
    results, then calls :meth:`seal` once the step finishes. After
    sealing, any attempt to add, replace, or delete an output raises
    :class:`ValueError`. Reads always go through an immutable
    :class:`types.MappingProxyType` view, so even pre-seal a caller
    cannot mutate the map via the public surface.

    Instances are not frozen dataclasses because the unsealed → sealed
    transition is a deliberate one-way state change. Once sealed, the
    instance is observably immutable.
    """

    __slots__ = ("_outputs", "_sealed")

    def __init__(
        self,
        outputs: Mapping[str, Any] | None = None,
        *,
        sealed: bool = False,
    ) -> None:
        self._outputs: dict[str, Any] = dict(outputs) if outputs is not None else {}
        self._sealed: bool = sealed

    @property
    def outputs(self) -> Mapping[str, Any]:
        """Immutable view of the step's outputs."""
        return MappingProxyType(self._outputs)

    @property
    def sealed(self) -> bool:
        """``True`` once :meth:`seal` has been called."""
        return self._sealed

    def set_output(self, name: str, value: Any) -> None:
        """Record an output. Rejected after the step is sealed."""
        if self._sealed:
            raise ValueError(
                f"step outputs are sealed; cannot set {name!r}",
            )
        self._outputs[name] = value

    def seal(self) -> StepBinding:
        """Make this step's outputs immutable. Idempotent."""
        self._sealed = True
        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StepBinding):
            return NotImplemented
        return self._outputs == other._outputs and self._sealed == other._sealed

    def __hash__(self) -> int:  # pragma: no cover - StepBinding is not hashable
        # Outputs are mutable until sealed; equality + identity-based
        # hashing would mislead. Forbid hashing entirely.
        raise TypeError("StepBinding is not hashable")

    def __repr__(self) -> str:
        return f"StepBinding(outputs={dict(self._outputs)!r}, sealed={self._sealed})"


# ---------------------------------------------------------------------------
# Binding scope
# ---------------------------------------------------------------------------


# The complete set of root identifiers a scope will resolve. Anything else
# — every host Python name, every CEL keyword, every typo — is rejected.
_ALLOWED_ROOTS: Final[frozenset[str]] = frozenset(
    {"inputs", "steps", "run", "workflow", "let", "now"}
)

# Allowed leaves under ``run`` and ``workflow``. Frozen here (rather than
# derived from ``dataclasses.fields``) so the resolver's allow-list is
# explicit and trivially auditable.
_RUN_ATTRS: Final[frozenset[str]] = frozenset({"id", "workspace"})
_WORKFLOW_ATTRS: Final[frozenset[str]] = frozenset({"name", "version"})


@dataclass(frozen=True, kw_only=True, slots=True)
class BindingScope:
    """Immutable binding scope for CEL evaluation.

    The scope is value-typed: equal scopes (same inputs, steps, run,
    workflow, let, clock) compare equal regardless of construction
    order. ``inputs``, ``steps``, and ``let`` are wrapped in
    :class:`types.MappingProxyType` at construction so external mutation
    of the originally-passed dicts cannot leak back into the scope.
    ``run`` and ``workflow`` are frozen dataclasses. The clock
    (``now``) is a callable injected by the caller — typically Dapr
    Workflow's ``current_utc_datetime`` — and is the only sanctioned way
    for an expression to observe wall-clock time.

    To produce a child scope with additional ``let`` bindings (e.g. when
    entering a ``let:`` block), use :meth:`with_let`. The original scope
    is unchanged.
    """

    run: RunInfo
    workflow: WorkflowInfo
    now: Callable[[], datetime]
    inputs: Mapping[str, Any] = field(default_factory=dict)
    steps: Mapping[str, StepBinding] = field(default_factory=dict)
    let: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Wrap mutable mappings as immutable views. Callers can pass plain
        # ``dict`` for ergonomic construction; the scope guarantees the
        # references it stores are read-only afterwards.
        #
        # A value that is already a ``MappingProxyType`` is kept as-is so
        # that scope-derivation paths like :meth:`with_let` — which pass
        # the parent's already-wrapped ``inputs`` / ``steps`` views
        # straight through — don't pay for a redundant dict copy.
        if not isinstance(self.inputs, MappingProxyType):
            object.__setattr__(self, "inputs", MappingProxyType(dict(self.inputs)))
        if not isinstance(self.steps, MappingProxyType):
            object.__setattr__(self, "steps", MappingProxyType(dict(self.steps)))
        if not isinstance(self.let, MappingProxyType):
            object.__setattr__(self, "let", MappingProxyType(dict(self.let)))

    # ----- public API -------------------------------------------------------

    def with_let(self, **overlay: Any) -> BindingScope:
        """Return a new scope with additional ``let`` bindings overlaid.

        ``let`` is per-evaluation: each ``let`` block in a workflow
        expression expands into a fresh overlay on top of the
        surrounding scope. Names already in ``let`` are replaced by the
        overlay; the original scope is unchanged.
        """
        merged: dict[str, Any] = dict(self.let)
        merged.update(overlay)
        # Pass the parent's already-wrapped ``inputs`` / ``steps`` views
        # straight through; ``__post_init__`` keeps existing
        # ``MappingProxyType`` instances as-is, so the new scope shares
        # the same underlying mapping objects rather than re-copying them.
        return BindingScope(
            run=self.run,
            workflow=self.workflow,
            now=self.now,
            inputs=self.inputs,
            steps=self.steps,
            let=merged,
        )

    def resolve(
        self,
        chain: Sequence[str],
        *,
        pos: SourcePosition | None = None,
    ) -> Any:
        """Resolve a dotted name chain.

        ``chain`` is the flattened sequence of identifiers an
        expression names — e.g. ``["inputs", "image"]`` for
        ``inputs.image``, or ``["steps", "scan", "outputs", "critical"]``
        for ``steps.scan.outputs.critical``. The chain must start with
        one of the six allowed roots; anything else raises
        :class:`UnboundNameError` *before* any attribute or item access
        is attempted, so the host Python namespace is structurally
        unreachable.

        Args:
            chain: Non-empty sequence of identifiers.
            pos: Optional source position attached to the resulting
                :class:`UnboundNameError` for error reporting.

        Returns:
            The resolved value. For ``["now"]``, returns the clock
            callable itself; the evaluator (WF-IMPL-006) is responsible
            for invoking it when it encounters ``Call("now", [])``.

        Raises:
            TypeError: If ``chain`` is a ``str`` (a common foot-gun:
                ``str`` is itself a ``Sequence[str]``, so a stray
                ``"inputs.image"`` would otherwise be silently split
                into characters) or if any element of ``chain`` is not
                a ``str``.
            UnboundNameError: If the chain is empty, the root is not
                allowed, a step id is missing, or any item in the walk
                is absent.
        """
        # Reject str explicitly — every str is a Sequence[str] of single
        # characters, so without this guard a caller mistakenly passing
        # ``"inputs.image"`` would receive a confusing UnboundNameError
        # with chain=("i",) rather than a clear type error.
        if isinstance(chain, str):
            raise TypeError(
                "BindingScope.resolve: chain must be a sequence of identifiers, "
                "not a single string; pass e.g. ['inputs', 'image'] instead of "
                f"{chain!r}",
            )
        if not chain:
            raise UnboundNameError(chain, pos=pos, reason="empty name chain")
        for i, element in enumerate(chain):
            if not isinstance(element, str):
                raise TypeError(
                    f"BindingScope.resolve: chain[{i}] must be a str, got {type(element).__name__}",
                )

        head = chain[0]
        tail = list(chain[1:])

        if head not in _ALLOWED_ROOTS:
            raise UnboundNameError(chain, pos=pos, reason=f"unknown root {head!r}")

        if head == "now":
            if tail:
                raise UnboundNameError(chain, pos=pos, reason="'now' has no members")
            return self.now

        if head == "run":
            return _resolve_scalar_root(self.run, _RUN_ATTRS, tail, chain, pos, root_label="run")
        if head == "workflow":
            return _resolve_scalar_root(
                self.workflow, _WORKFLOW_ATTRS, tail, chain, pos, root_label="workflow"
            )
        if head == "inputs":
            return _resolve_mapping_root(self.inputs, tail, chain, pos, root_label="inputs")
        if head == "let":
            return _resolve_mapping_root(self.let, tail, chain, pos, root_label="let")
        # head == "steps"
        return _resolve_steps(self.steps, tail, chain, pos)


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def _resolve_scalar_root(
    obj: object,
    allowed: frozenset[str],
    tail: list[str],
    full_chain: Sequence[str],
    pos: SourcePosition | None,
    *,
    root_label: str,
) -> Any:
    """Resolve a ``run.*`` / ``workflow.*`` chain.

    These roots hold exactly one level of scalar members. Returning the
    root itself, or attempting to descend past a scalar, is a usage
    error in CEL bindings: every reference to ``run`` or ``workflow``
    must name a specific field.
    """
    if not tail:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"'{root_label}' is not a value; pick a member",
        )
    if len(tail) > 1:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"'{root_label}.{tail[0]}' is a scalar; no sub-members",
        )
    attr = tail[0]
    if attr not in allowed:
        raise UnboundNameError(full_chain, pos=pos, reason=f"no such {root_label} field")
    return getattr(obj, attr)


def _resolve_mapping_root(
    root: Mapping[str, Any],
    tail: list[str],
    full_chain: Sequence[str],
    pos: SourcePosition | None,
    *,
    root_label: str,
) -> Any:
    """Resolve ``inputs.*`` / ``let.*`` chains.

    The root itself is not a binding value (the design says ``inputs.*``
    / ``let.<name>``, i.e. members), so an empty tail is a usage error.
    """
    if not tail:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"'{root_label}' is not a value; pick a member",
        )
    current: Any = root
    for i, key in enumerate(tail):
        if not isinstance(current, Mapping):
            walked = ".".join([root_label, *tail[:i]])
            raise UnboundNameError(
                full_chain,
                pos=pos,
                reason=f"'{walked}' is not a mapping",
            )
        if key not in current:
            raise UnboundNameError(full_chain, pos=pos)
        current = current[key]
    return current


def _resolve_steps(
    steps: Mapping[str, StepBinding],
    tail: list[str],
    full_chain: Sequence[str],
    pos: SourcePosition | None,
) -> Any:
    """Resolve ``steps.<id>.outputs.<...>`` chains.

    The shape is strictly ``steps . id . outputs . member [. nested...]``.
    Any deviation — accessing ``steps`` itself, accessing a step's
    non-``outputs`` field, or referencing a missing step id — raises
    :class:`UnboundNameError`. Step ids that aren't valid CEL
    identifiers must reach this resolver via the bracket form
    (``steps["scan-alt"]``) — that's a parser concern; this resolver
    only sees the flattened name chain.
    """
    if not tail:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason="'steps' is not a value; pick a step id",
        )
    step_id = tail[0]
    if step_id not in steps:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"no such step {step_id!r}",
        )
    step = steps[step_id]
    rest = tail[1:]
    if not rest:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"'steps.{step_id}' is not a value; pick '.outputs.<name>'",
        )
    if rest[0] != "outputs":
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"'steps.{step_id}.{rest[0]}' is not exposed; only 'outputs' is",
        )
    after_outputs = rest[1:]
    if not after_outputs:
        raise UnboundNameError(
            full_chain,
            pos=pos,
            reason=f"'steps.{step_id}.outputs' is not a value; pick a member",
        )
    current: Any = step.outputs
    for i, key in enumerate(after_outputs):
        if not isinstance(current, Mapping):
            walked = ".".join(["steps", step_id, "outputs", *after_outputs[:i]])
            raise UnboundNameError(
                full_chain,
                pos=pos,
                reason=f"'{walked}' is not a mapping",
            )
        if key not in current:
            raise UnboundNameError(full_chain, pos=pos)
        current = current[key]
    return current


__all__ = [
    "BindingScope",
    "BindingValue",
    "RunInfo",
    "StepBinding",
    "UnboundNameError",
    "WorkflowInfo",
]
