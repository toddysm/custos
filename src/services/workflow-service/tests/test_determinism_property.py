"""Property-based determinism tests for the Definition Compiler (WF-IMPL-026).

The Workflow Service persists ``to_json(compile(doc, run_meta,
registry))`` on ``Run.compiledGraph`` and re-reads it on every Dapr
Workflow replay. Non-deterministic compilation would surface as a
``expression.divergence`` Dapr non-determinism error at the worst
possible moment — mid-run, after side-effects have already begun —
so this module locks the contract under property-based fuzzing.

The grammar of :func:`_well_typed_workflow_documents` is deliberately
narrow: every generated document is **guaranteed** to type-check
against the seeded :class:`InMemoryActivityTypeRegistry`. Breadth
across step kinds / call-site shapes lives in
:mod:`test_kind_grid` (WF-IMPL-025); this module trades breadth for
the four invariants listed in the WF-IMPL-026 acceptance criteria:

1. **Byte-equal repeats** — ``compile() → to_json()`` produces the
   same bytes ``_BYTE_REPEAT`` times in a row for the same input.
2. **Topological order stability** — shuffling
   ``document.spec.steps`` does not change the compiled
   ``ExecutionGraph.topological_order``.
3. **JSON round-trip** — ``from_json(to_json(g)) == g`` for the
   compiled graph.
4. **Resolved retry-policy stability** — same input → same
   :class:`ResolvedRetryPolicy` per step.

The default seed is locked at ``--hypothesis-seed=0`` for the PR/push
CI job; ``.github/workflows/workflow-service-nightly.yml`` re-runs
this module nightly with a random seed and records any falsifying
example into the Hypothesis database for follow-up triage. Mirrors
the WF-IMPL-010 (#185) pattern used for ``custos_cel``.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime
from typing import Any, Final

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import ExecutionGraph, from_json, to_json

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

#: How many times each ``@given`` example is re-compiled when proving
#: byte-equality across repeats. The WF-IMPL-026 acceptance criterion
#: calls for 100; we keep that number even though Hypothesis runs
#: hundreds of examples so a wall-clock regression (e.g. a stray
#: ``set()`` ordering somewhere in the encoder) cannot hide behind
#: small-n flakiness.
_BYTE_REPEAT: Final[int] = 100

_ACTIVITY_REF: Final[str] = "security/scan@1"
_WORKFLOW_REF: Final[str] = "security/child@1"

#: Seeded registry. ``security/scan@1`` is the only activity ref the
#: generator emits, so a single output schema covers every
#: ``${{ steps.X.outputs.* }}`` reference the grammar can produce.
_ACTIVITY_OUTPUTS: Final[dict[str, Any]] = {
    "type": "object",
    "properties": {
        "critical": {"type": "integer"},
        "findings": {"type": "array", "items": {"type": "string"}},
    },
}


def _registry() -> InMemoryActivityTypeRegistry:
    return InMemoryActivityTypeRegistry({_ACTIVITY_REF: _ACTIVITY_OUTPUTS})


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=datetime(2026, 5, 1, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Document generator
# ---------------------------------------------------------------------------

#: The inputs schema is fixed so the generator can pick CEL
#: expressions whose types are statically known. Every generated
#: ``${{ inputs.X }}`` reference is therefore guaranteed to
#: type-check.
_INPUTS_SCHEMA: Final[dict[str, Any]] = {
    "target": {"type": "string", "required": True},
    "flag": {"type": "boolean", "default": True},
    "count": {"type": "integer", "default": 0},
}

# Step ids are drawn from ``a..z`` so the alphabetical tiebreak the
# topology sorter applies (``custos_workflow.graph.topology.
# topological_sort``) is exercised across multiple zero-in-degree
# frontiers.
_STEP_ID_ALPHABET: Final[str] = "abcdefghijklmnopqrstuvwxyz"


@st.composite
def _step_ids(draw: st.DrawFn) -> list[str]:
    """Draw 1-6 unique step ids from a single lowercase letter each."""

    size = draw(st.integers(min_value=1, max_value=6))
    return draw(
        st.lists(
            st.sampled_from(list(_STEP_ID_ALPHABET)),
            min_size=size,
            max_size=size,
            unique=True,
        )
    )


# Bool / string / int CEL slot pickers — every entry is guaranteed
# to be a well-typed expression against ``_INPUTS_SCHEMA``.
_BOOL_REFS: Final[tuple[str, ...]] = (
    "${{ inputs.flag }}",
    "${{ inputs.count > 0 }}",
    "${{ true }}",
    "${{ false }}",
)
_STRING_REFS: Final[tuple[str, ...]] = (
    "${{ inputs.target }}",
    '${{ "literal" }}',
)


def _activity_body(
    step_id: str, *, with_if: bool, with_needs: str | None, with_data_dep: str | None
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": step_id,
        "activity": _ACTIVITY_REF,
        "connector": "primary",
    }
    if with_if:
        body["if"] = "${{ inputs.flag }}"
    if with_needs is not None:
        body["needs"] = [with_needs]
    if with_data_dep is not None:
        # A data-dep reference on a known integer output of a prior
        # ``security/scan@1`` step. The implicit edge collector turns
        # this into an ``EdgeKind.DATA_DEPENDENCY`` edge.
        body["when"] = f"${{{{ steps.{with_data_dep}.outputs.critical > 0 }}}}"
    return body


def _workflow_body(step_id: str, *, with_if: bool, with_needs: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {"id": step_id, "workflow": _WORKFLOW_REF}
    if with_if:
        body["if"] = "${{ inputs.flag }}"
    if with_needs is not None:
        body["needs"] = [with_needs]
    return body


def _let_body(step_id: str, expr: str) -> dict[str, Any]:
    return {"id": step_id, "let": {"v": expr}}


@st.composite
def _well_typed_workflow_documents(draw: st.DrawFn) -> WorkflowDocument:
    """Generate a :class:`WorkflowDocument` that is guaranteed
    to compile against :func:`_registry` and :func:`_run_meta`.

    The grammar is intentionally narrow — every step kind is
    reachable, every step's CEL slots are filled with expressions
    whose types are statically known against
    :data:`_INPUTS_SCHEMA` and :data:`_ACTIVITY_OUTPUTS`, and any
    ``needs:`` / data-dependency reference points at a strictly
    prior step in the generated order. This guarantees no cycle and
    no forward reference.
    """

    return _build_document(draw, allow_dependencies=True)


@st.composite
def _independent_workflow_documents(draw: st.DrawFn) -> WorkflowDocument:
    """Generate documents whose steps have no inter-step
    dependencies.

    Property 2 (topological-order stability under shuffle) needs
    documents where *every* permutation of ``spec.steps`` is also a
    valid topological order — otherwise the shuffled document is
    rejected by ``validate_step_refs`` with
    ``"references step ... declared later in document order"`` and
    the property is vacuously failed. Removing ``needs:`` /
    data-dependency edges from the generator collapses the
    dependency graph to a discrete set of nodes, making every
    permutation legal and exercising the alphabetical
    no-dependency-frontier tiebreak in
    :func:`~custos_workflow.graph.topology.topological_sort`.
    """

    return _build_document(draw, allow_dependencies=False)


def _build_document(draw: st.DrawFn, *, allow_dependencies: bool) -> WorkflowDocument:
    ids = draw(_step_ids())
    # We build steps in the generated order. ``prior_activity_ids``
    # tracks the subset that are eligible to be referenced by a
    # data-dependency expression (only ``security/scan@1`` activity
    # steps publish the ``critical`` output the generator references).
    steps: list[dict[str, Any]] = []
    prior_activity_ids: list[str] = []

    for idx, sid in enumerate(ids):
        kind: str = draw(st.sampled_from(("activity", "let", "workflow")))
        prior_ids = ids[:idx]
        # Optionally pin this step behind a prior one with ``needs:``.
        attach_needs: bool = draw(st.booleans())
        needs: str | None = (
            draw(st.sampled_from(prior_ids))
            if allow_dependencies and prior_ids and attach_needs
            else None
        )
        if kind == "activity":
            # Optionally weave a data-dependency on a prior activity
            # step so the implicit-edge collector is exercised.
            attach_data_dep: bool = draw(st.booleans())
            data_dep: str | None = (
                draw(st.sampled_from(prior_activity_ids))
                if allow_dependencies and prior_activity_ids and attach_data_dep
                else None
            )
            with_if: bool = draw(st.booleans())
            steps.append(
                _activity_body(
                    sid,
                    with_if=with_if,
                    with_needs=needs,
                    with_data_dep=data_dep,
                )
            )
            prior_activity_ids.append(sid)
        elif kind == "workflow":
            wf_with_if: bool = draw(st.booleans())
            steps.append(
                _workflow_body(
                    sid,
                    with_if=wf_with_if,
                    with_needs=needs,
                )
            )
        else:  # let
            expr: str = draw(st.sampled_from(_BOOL_REFS + _STRING_REFS))
            body = _let_body(sid, expr)
            if needs is not None:
                body["needs"] = [needs]
            steps.append(body)

    return WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "pipeline", "workspace": "security"},
            "spec": {"inputs": _INPUTS_SCHEMA, "steps": steps},
        }
    )


def _compile(doc: WorkflowDocument) -> ExecutionGraph:
    return compile_workflow(doc, _run_meta(), _registry())


def _shuffle_steps(doc: WorkflowDocument, *, rng_seed: int) -> WorkflowDocument:
    """Return a structural clone of ``doc`` with ``spec.steps``
    permuted under ``rng_seed``.

    The permutation is performed on the JSON-mode dump so the
    re-validated document is identical to one the YAML loader would
    have produced — i.e. only the ``spec.steps`` list order
    differs.
    """

    payload = doc.model_dump(mode="json", by_alias=True, exclude_none=True)
    steps = list(payload["spec"]["steps"])
    rng = random.Random(rng_seed)
    rng.shuffle(steps)
    payload["spec"]["steps"] = steps
    return WorkflowDocument.model_validate(payload)


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

# Hypothesis ``filter_too_much`` would fire under unlucky draws (e.g.
# every example yielding an empty body). The grammar above always
# produces at least one step so the filter pressure is zero, but we
# still relax the health check to keep the suite fast under any
# future generator tweak.
_SUITE_SETTINGS: Final[settings] = settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=(HealthCheck.too_slow, HealthCheck.filter_too_much),
)


@_SUITE_SETTINGS
@given(doc=_well_typed_workflow_documents())
def test_byte_equal_across_100_repeats(doc: WorkflowDocument) -> None:
    """Property 1: ``compile() → to_json()`` is byte-stable.

    A regression here means the compiler or the encoder pulled in a
    non-deterministic source (e.g. ``set`` iteration order leaking
    into a list, a clock-derived value sneaking into the envelope,
    or a Pydantic field with a mutable default rebuilt per call).
    """

    first = to_json(_compile(doc))
    for _ in range(_BYTE_REPEAT - 1):
        assert to_json(_compile(doc)) == first


@_SUITE_SETTINGS
@given(
    doc=_independent_workflow_documents(),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_topological_order_stable_under_shuffle(doc: WorkflowDocument, seed: int) -> None:
    """Property 2: shuffling ``spec.steps`` preserves the topo order.

    The :func:`topological_sort` implementation uses Kahn's
    algorithm with an alphabetical tiebreak on step ids
    (``custos_workflow.graph.topology``), so the compiled
    ``topological_order`` must depend only on the dependency graph
    — never on the order steps were authored in.

    This property uses :func:`_independent_workflow_documents` so
    every permutation of ``spec.steps`` is itself a valid
    topological order. Documents that carry ``needs:`` or
    ``${{ steps.X.outputs.* }}`` dependencies cannot be shuffled
    freely because the compiler's
    :func:`~custos_workflow.graph.topology.validate_step_refs`
    stage rejects any document whose CEL references point at a
    step declared later in document order; the broader
    determinism-under-replay contract for those graphs is still
    covered by properties 1, 3, and 4 which use the
    dependency-bearing strategy.
    """

    baseline = _compile(doc)
    shuffled = _compile(_shuffle_steps(doc, rng_seed=seed))
    assert shuffled.topological_order == baseline.topological_order


@_SUITE_SETTINGS
@given(doc=_well_typed_workflow_documents())
def test_json_round_trip(doc: WorkflowDocument) -> None:
    """Property 3: ``from_json(to_json(g)) == g``.

    Every field surfaced on ``ExecutionGraph`` (and its frozen
    children) must survive the JSON envelope unchanged. A failure
    here means either the encoder dropped a field or the decoder
    introduced one the original did not carry.
    """

    graph = _compile(doc)
    blob = to_json(graph)
    restored = from_json(blob)
    assert restored == graph
    # The re-serialised blob must also be byte-identical — proves
    # ``to_json`` is a fixed point of itself across a round-trip.
    assert to_json(restored) == blob


@_SUITE_SETTINGS
@given(doc=_well_typed_workflow_documents())
def test_resolved_retry_policy_stability(doc: WorkflowDocument) -> None:
    """Property 4: ``ResolvedRetryPolicy`` is stable per step.

    Two compiles of the same document must produce
    structurally-equal per-step retry policies. Anything else
    indicates the retry resolver pulled in a non-deterministic
    source (e.g. a default ``datetime.now()``).
    """

    first = _compile(doc)
    second = _compile(doc)
    first_policies = {n.step_id: n.retry_policy for n in first.nodes}
    second_policies = {n.step_id: n.retry_policy for n in second.nodes}
    assert first_policies == second_policies
