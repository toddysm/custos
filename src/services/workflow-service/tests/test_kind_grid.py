"""Exhaustive kind-grid tests (WF-IMPL-025).

This module codifies the public taxonomy surface of
``custos_workflow``: every documented enum member and every
canonical ``compile.*`` error ``kind`` string has at least one
parametrized row, and every grid ends with an exhaustiveness guard
``observed == set(EnumClass)`` so that adding an enum value without
adding a grid row breaks the build.

Mirrors the codification step from WF-IMPL-009 (#184, ``custos_cel``)
and CS-IMPL-020 (#221, catalog-service). The matrices are
intentionally narrow: they prove "this kind exists and round-trips
through ``compile()``", not "this kind has every imaginable feature
wired" — feature breadth lives in the per-stage test modules
(``test_document_models.py``, ``test_callsites.py``,
``test_graph_topology.py``, ``test_retry_resolver.py``,
``test_on_error_compile.py``, ``test_errors.py``).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import (
    BindingsCompileError,
    CallSiteCompileError,
    RetryPolicyCompileError,
    RunMeta,
    TopologyCompileError,
    TypeCheckCompileError,
)
from custos_workflow.compiler import (
    compile as compile_workflow,
)
from custos_workflow.document import WorkflowDocument
from custos_workflow.document.models import (
    BackoffStrategy,
    JitterStrategy,
    OnErrorAction,
)
from custos_workflow.errors import (
    CompileError,
    CompileParseError,
    CompileRetryPolicyError,
    CompileTopologyError,
    CompileTypeError,
)
from custos_workflow.graph import (
    BackoffStrategyTag,
    CallSiteKind,
    Edge,
    EdgeKind,
    JitterStrategyTag,
    OnErrorActionTag,
    PrimitiveHandler,
    StepKind,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _registry() -> InMemoryActivityTypeRegistry:
    """Activity-type registry seeded with every ref referenced below."""

    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                    "findings": {"type": "array", "items": {"type": "string"}},
                },
            },
            "ops/notify@1": {
                "type": "object",
                "properties": {"sent": {"type": "boolean"}},
            },
        }
    )


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=datetime(2026, 5, 1, tzinfo=UTC),
    )


def _doc(steps: Sequence[dict[str, Any]], *, name: str = "pipeline") -> WorkflowDocument:
    return WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": name, "workspace": "security"},
            "spec": {
                "inputs": {
                    "target": {"type": "string", "required": True},
                    "items": {"type": "array", "default": []},
                    "go": {"type": "boolean", "default": True},
                    "skip": {"type": "boolean", "default": False},
                    "filter": {"type": "boolean", "default": True},
                },
                "steps": list(steps),
            },
        }
    )


# ---------------------------------------------------------------------------
# 1. StepKind grid — every step kind compiles + maps to a handler
# ---------------------------------------------------------------------------

#: One minimal step body per :class:`StepKind`. The compiler must
#: accept all four and tag the resulting node with the matching
#: ``kind`` / ``primitive_handler`` pair.
_STEP_KIND_DOCS: dict[StepKind, dict[str, Any]] = {
    StepKind.ACTIVITY: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
    },
    StepKind.LET: {
        "id": "compute",
        "let": {"verdict": "ok"},
    },
    StepKind.WORKFLOW: {
        "id": "child",
        "workflow": "security/child@1",
    },
    StepKind.WAIT: {
        "id": "pause",
        "wait": "PT5S",
    },
    StepKind.APPROVAL: {
        "id": "gate",
        "approval": {"approvers": ["alice"]},
    },
}

#: Documented step-kind → handler dispatch, mirroring
#: design.md § Workflow Schema: Step Kinds Handled.
_STEP_KIND_TO_HANDLER: dict[StepKind, PrimitiveHandler] = {
    StepKind.ACTIVITY: PrimitiveHandler.ACTIVITY_RUNTIME,
    StepKind.LET: PrimitiveHandler.EXPRESSION_INLINE,
    StepKind.WORKFLOW: PrimitiveHandler.SUB_ORCHESTRATION,
    StepKind.WAIT: PrimitiveHandler.RUN_CONTROLLER_TIMER,
    StepKind.APPROVAL: PrimitiveHandler.SUB_ORCHESTRATION,
}


class TestStepKindGrid:
    @pytest.mark.parametrize("kind", list(StepKind))
    def test_every_step_kind_compiles(self, kind: StepKind) -> None:
        body = _STEP_KIND_DOCS[kind]
        graph = compile_workflow(_doc([body]), _run_meta(), _registry())
        assert graph.nodes[0].kind is kind

    @pytest.mark.parametrize("kind", list(StepKind))
    def test_every_step_kind_maps_to_primitive_handler(self, kind: StepKind) -> None:
        body = _STEP_KIND_DOCS[kind]
        graph = compile_workflow(_doc([body]), _run_meta(), _registry())
        assert graph.nodes[0].primitive_handler is _STEP_KIND_TO_HANDLER[kind]

    def test_grid_covers_every_step_kind(self) -> None:
        # Exhaustiveness guard: adding a StepKind member without
        # adding a row to ``_STEP_KIND_DOCS`` breaks here.
        assert set(_STEP_KIND_DOCS) == set(StepKind)
        assert set(_STEP_KIND_TO_HANDLER) == set(StepKind)
        assert set(_STEP_KIND_TO_HANDLER.values()) == set(PrimitiveHandler)


# ---------------------------------------------------------------------------
# 2. CallSiteKind grid — every author-facing call-site slot round-trips
# ---------------------------------------------------------------------------

#: ``PLACEHOLDER`` is reserved for compiler-internal synthetics and
#: is never emitted by the real call-site collector (see the
#: :class:`CallSiteKind.PLACEHOLDER` docstring). The grid below
#: enumerates the remaining seven author-facing slots.
_USER_CALLSITE_KINDS: frozenset[CallSiteKind] = frozenset(
    set(CallSiteKind) - {CallSiteKind.PLACEHOLDER}
)

#: Minimal step body per :class:`CallSiteKind`. Each body produces
#: exactly one node whose ``typed_call_sites`` carries one site
#: tagged with the matching kind.
_CALLSITE_DOCS: dict[CallSiteKind, dict[str, Any]] = {
    CallSiteKind.IF: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
        "if": "${{ inputs.go }}",
    },
    CallSiteKind.WHEN: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
        "when": "${{ inputs.go }}",
    },
    CallSiteKind.UNLESS: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
        "unless": "${{ inputs.skip }}",
    },
    CallSiteKind.WITH: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
        "with": {"image": "${{ inputs.target }}"},
    },
    CallSiteKind.FOR_EACH: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
        "forEach": "${{ inputs.items }}",
    },
    CallSiteKind.WHERE: {
        "id": "scan",
        "activity": "security/scan@1",
        "connector": "primary",
        "where": "${{ inputs.filter }}",
    },
    CallSiteKind.LET: {
        "id": "compute",
        "let": {"verdict": "${{ inputs.go }}"},
    },
}


class TestCallSiteKindGrid:
    @pytest.mark.parametrize("kind", sorted(_USER_CALLSITE_KINDS))
    def test_every_user_call_site_kind_compiles(self, kind: CallSiteKind) -> None:
        body = _CALLSITE_DOCS[kind]
        graph = compile_workflow(_doc([body]), _run_meta(), _registry())
        observed_kinds = {site.kind for site in graph.nodes[0].call_sites.values()}
        assert kind in observed_kinds, (
            f"compile() did not produce a TypedCallSite with kind={kind!r}"
        )

    def test_placeholder_is_reserved_and_never_emitted(self) -> None:
        # The PLACEHOLDER tag is for compiler-internal synthetics
        # only — no author-reachable step shape should produce it.
        for body in _CALLSITE_DOCS.values():
            graph = compile_workflow(_doc([body]), _run_meta(), _registry())
            assert all(
                site.kind is not CallSiteKind.PLACEHOLDER
                for node in graph.nodes
                for site in node.call_sites.values()
            )

    def test_grid_covers_every_user_call_site_kind(self) -> None:
        # Exhaustiveness guard: adding a CallSiteKind member without
        # adding a row breaks here (PLACEHOLDER excluded by design).
        assert set(_CALLSITE_DOCS) == set(_USER_CALLSITE_KINDS)
        assert set(_USER_CALLSITE_KINDS) | {CallSiteKind.PLACEHOLDER} == set(CallSiteKind)


# ---------------------------------------------------------------------------
# 3. EdgeKind grid — every documented edge kind appears in some graph
# ---------------------------------------------------------------------------


class TestEdgeKindGrid:
    def test_explicit_needs_edge(self) -> None:
        # An author-declared ``needs: [X]`` produces an
        # ``EXPLICIT_NEEDS`` edge regardless of any data dependency.
        doc = _doc(
            [
                {"id": "scan", "activity": "security/scan@1", "connector": "primary"},
                {
                    "id": "consumer",
                    "activity": "ops/notify@1",
                    "connector": "primary",
                    "needs": ["scan"],
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        assert any(e.kind is EdgeKind.EXPLICIT_NEEDS for e in graph.edges)

    def test_data_dependency_edge(self) -> None:
        # A ``${{ steps.X.outputs.* }}`` reference with no matching
        # ``needs:`` entry produces a ``DATA_DEPENDENCY`` edge.
        doc = _doc(
            [
                {"id": "scan", "activity": "security/scan@1", "connector": "primary"},
                {
                    "id": "consumer",
                    "activity": "ops/notify@1",
                    "connector": "primary",
                    "if": "${{ steps.scan.outputs.critical > 0 }}",
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        assert any(e.kind is EdgeKind.DATA_DEPENDENCY for e in graph.edges)

    def test_control_flow_edge_constructs(self) -> None:
        # ``CONTROL_FLOW`` is reserved for future use — the v1
        # compiler does not yet fall back to sequential ordering
        # (see :class:`EdgeKind` docstring). The taxonomy slot is
        # still part of the wire surface today, so we cover it by
        # constructing an :class:`Edge` directly and asserting the
        # tag round-trips. Once WF-IMPL-* wires control-flow
        # fallback edges into the compiler, this row promotes to a
        # ``compile_workflow``-driven assertion.
        edge = Edge(from_step="a", to_step="b", kind=EdgeKind.CONTROL_FLOW)
        assert edge.kind is EdgeKind.CONTROL_FLOW

    def test_grid_covers_every_edge_kind(self) -> None:
        # Exhaustiveness guard cross-check: the three assertions
        # above name every member of :class:`EdgeKind`.
        covered = {
            EdgeKind.EXPLICIT_NEEDS,
            EdgeKind.DATA_DEPENDENCY,
            EdgeKind.CONTROL_FLOW,
        }
        assert covered == set(EdgeKind)


# ---------------------------------------------------------------------------
# 4. Backoff / Jitter / OnError enum grids — document <-> compiled tag
# ---------------------------------------------------------------------------

#: Document enum -> compiled tag, mirroring the document/graph
#: enum alignment locked in WF-IMPL-018.
_BACKOFF_MAP: dict[BackoffStrategy, BackoffStrategyTag] = {
    BackoffStrategy.CONSTANT: BackoffStrategyTag.CONSTANT,
    BackoffStrategy.LINEAR: BackoffStrategyTag.LINEAR,
    BackoffStrategy.EXPONENTIAL: BackoffStrategyTag.EXPONENTIAL,
}

_JITTER_MAP: dict[JitterStrategy, JitterStrategyTag] = {
    JitterStrategy.NONE: JitterStrategyTag.NONE,
    JitterStrategy.FULL: JitterStrategyTag.FULL,
    JitterStrategy.EQUAL: JitterStrategyTag.EQUAL,
    JitterStrategy.DECORRELATED: JitterStrategyTag.DECORRELATED,
}

_ON_ERROR_MAP: dict[OnErrorAction, OnErrorActionTag] = {
    OnErrorAction.SKIP: OnErrorActionTag.SKIP,
    OnErrorAction.RETRY: OnErrorActionTag.RETRY,
    OnErrorAction.FAIL: OnErrorActionTag.FAIL,
}


class TestRetryEnumGrid:
    @pytest.mark.parametrize("doc_enum,tag", list(_BACKOFF_MAP.items()))
    def test_every_backoff_strategy_round_trips(
        self, doc_enum: BackoffStrategy, tag: BackoffStrategyTag
    ) -> None:
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "retry": {
                        "maxAttempts": 3,
                        "backoff": {
                            "strategy": doc_enum.value,
                            "initialDelay": "PT1S",
                            "maxDelay": "PT1M",
                        },
                    },
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        policy = graph.nodes[0].retry_policy
        assert policy is not None
        assert policy.backoff.strategy is tag

    @pytest.mark.parametrize("doc_enum,tag", list(_JITTER_MAP.items()))
    def test_every_jitter_strategy_round_trips(
        self, doc_enum: JitterStrategy, tag: JitterStrategyTag
    ) -> None:
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "retry": {"maxAttempts": 3, "jitter": doc_enum.value},
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        policy = graph.nodes[0].retry_policy
        assert policy is not None
        assert policy.jitter is tag

    def test_backoff_grid_covers_every_member(self) -> None:
        assert set(_BACKOFF_MAP) == set(BackoffStrategy)
        assert set(_BACKOFF_MAP.values()) == set(BackoffStrategyTag)

    def test_jitter_grid_covers_every_member(self) -> None:
        assert set(_JITTER_MAP) == set(JitterStrategy)
        assert set(_JITTER_MAP.values()) == set(JitterStrategyTag)


class TestOnErrorActionGrid:
    @pytest.mark.parametrize("doc_enum,tag", list(_ON_ERROR_MAP.items()))
    def test_every_on_error_action_round_trips(
        self, doc_enum: OnErrorAction, tag: OnErrorActionTag
    ) -> None:
        # ``retry`` actions also need a structured retry block so
        # the resolver has something to layer; ``skip`` / ``fail``
        # do not.
        arm: dict[str, Any] = {
            "match": {"code": f"E_{doc_enum.value.upper()}"},
            "do": doc_enum.value,
        }
        if doc_enum is OnErrorAction.RETRY:
            arm["retry"] = {"maxAttempts": 4}
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "on_error": [arm],
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        actions = {route.action for route in graph.nodes[0].on_error_routes}
        assert tag in actions, f"compile() did not surface action={tag!r}"

    def test_grid_covers_every_on_error_action(self) -> None:
        assert set(_ON_ERROR_MAP) == set(OnErrorAction)
        assert set(_ON_ERROR_MAP.values()) == set(OnErrorActionTag)


# ---------------------------------------------------------------------------
# 5. Error-kind grid — every canonical ``compile.*`` kind has a trigger
# ---------------------------------------------------------------------------

#: The full, locked set of compile-time error ``kind`` strings.
#: The four canonical buckets live on the public taxonomy
#: (:mod:`custos_workflow.errors`); ``compile.bindings_error`` is the
#: non-canonical 5th kind documented as "outside the four canonical
#: buckets" on :class:`BindingsCompileError`.
_LOCKED_COMPILE_ERROR_KINDS: frozenset[str] = frozenset(
    {
        "compile.parse_error",
        "compile.type_error",
        "compile.topology_error",
        "compile.retry_policy_error",
        "compile.bindings_error",
    }
)


def _trigger_parse_error() -> WorkflowDocument:
    # Malformed CEL inside a well-formed ``${{ ... }}`` placeholder.
    return _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "with": {"image": "${{ 1 + }}"},
            },
        ]
    )


def _trigger_type_error() -> WorkflowDocument:
    # ``inputs.nope`` is not in the inputs schema.
    return _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "if": "${{ inputs.nope }}",
            },
        ]
    )


def _trigger_topology_error() -> WorkflowDocument:
    # Forward ``needs:`` reference — caught by
    # ``collect_explicit_edges`` and surfaced as a topology error.
    return _doc(
        [
            {
                "id": "a",
                "activity": "security/scan@1",
                "connector": "primary",
                "needs": ["b"],
            },
            {"id": "b", "activity": "security/scan@1", "connector": "primary"},
        ]
    )


def _trigger_retry_policy_error() -> WorkflowDocument:
    # On-error arm with both ``maxAttempts`` shorthand and a
    # structured ``retry`` block whose values disagree — the
    # resolver rejects the conflict.
    return _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "on_error": [
                    {
                        "match": {"code": "E_BOOM"},
                        "do": "retry",
                        "maxAttempts": 7,
                        "retry": {"maxAttempts": 9},
                    },
                ],
            },
        ]
    )


def _trigger_bindings_error() -> WorkflowDocument:
    # Activity ref the registry has never heard of.
    return _doc(
        [
            {
                "id": "scan",
                "activity": "missing/activity@9",
                "connector": "primary",
            },
        ]
    )


#: ``kind`` string → (doc-factory, expected-exception-class).
#: Every row's exception MUST be a :class:`CompileError` subclass
#: (the public family) so callers can branch on a single ``except``.
_ERROR_KIND_TRIGGERS: dict[str, tuple[Any, type[CompileError]]] = {
    "compile.parse_error": (_trigger_parse_error, CallSiteCompileError),
    "compile.type_error": (_trigger_type_error, TypeCheckCompileError),
    "compile.topology_error": (_trigger_topology_error, TopologyCompileError),
    "compile.retry_policy_error": (_trigger_retry_policy_error, RetryPolicyCompileError),
    "compile.bindings_error": (_trigger_bindings_error, BindingsCompileError),
}


class TestCompileErrorKindGrid:
    @pytest.mark.parametrize("kind", sorted(_LOCKED_COMPILE_ERROR_KINDS))
    def test_every_compile_kind_has_a_trigger(self, kind: str) -> None:
        factory, exc_class = _ERROR_KIND_TRIGGERS[kind]
        with pytest.raises(exc_class) as ei:
            compile_workflow(factory(), _run_meta(), _registry())
        # The raised exception belongs to the public family AND
        # serializes with the documented ``kind`` string.
        assert isinstance(ei.value, CompileError)
        envelope = ei.value.to_dict()
        assert envelope["kind"] == kind

    @pytest.mark.parametrize(
        "kind,base",
        [
            ("compile.parse_error", CompileParseError),
            ("compile.type_error", CompileTypeError),
            ("compile.topology_error", CompileTopologyError),
            ("compile.retry_policy_error", CompileRetryPolicyError),
        ],
    )
    def test_canonical_kinds_inherit_from_public_base(
        self, kind: str, base: type[CompileError]
    ) -> None:
        # The four canonical buckets MUST be subclasses of their
        # public :mod:`custos_workflow.errors` base so external
        # callers can ``except CompileParseError`` and catch every
        # parse-stage failure (regardless of the compiler-internal
        # specialization that actually fired).
        factory, _ = _ERROR_KIND_TRIGGERS[kind]
        with pytest.raises(base):
            compile_workflow(factory(), _run_meta(), _registry())

    def test_grid_covers_every_locked_kind(self) -> None:
        # Exhaustiveness guard: adding a new ``compile.*`` kind to
        # the taxonomy without adding a trigger row breaks here.
        assert set(_ERROR_KIND_TRIGGERS) == set(_LOCKED_COMPILE_ERROR_KINDS)
