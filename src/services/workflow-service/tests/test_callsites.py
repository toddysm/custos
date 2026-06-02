"""Tests for :mod:`custos_workflow.callsites` (WF-IMPL-020)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from custos_workflow.callsites import (
    CallSite,
    CallSiteParseError,
    PlaceholderSegment,
    SourcePosition,
    collect_call_sites,
    extract_placeholders,
)
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph.model import CallSiteKind

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _doc(steps: Sequence[dict[str, Any]]) -> WorkflowDocument:
    return WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "pipeline"},
            "spec": {"steps": list(steps)},
        }
    )


def _step(extras: dict[str, Any] | None = None, *, sid: str = "scan") -> dict[str, Any]:
    """Build an activity step dict for fixture use.

    ``extras`` is merged into the result and overrides defaults so a
    caller can replace ``activity`` or pass arbitrary modifier
    fields (``if`` / ``when`` / ``with`` / ``forEach`` / etc.)
    without fighting mypy's keyword-arg inference on nested dicts.
    Pydantic's discriminated step union performs the actual schema
    enforcement via :meth:`WorkflowDocument.model_validate`.
    """
    base: dict[str, Any] = {"id": sid, "activity": "ns/scan@1"}
    if extras is not None:
        base.update(extras)
    return base


def _scan(**extras: Any) -> dict[str, Any]:
    """Convenience for activity steps without nested dict fields."""
    return _step(extras=extras)


# ===========================================================================
# extract_placeholders
# ===========================================================================


class TestExtractPlaceholders:
    def test_empty_string(self) -> None:
        assert extract_placeholders("") == []

    def test_plain_literal_no_placeholder(self) -> None:
        assert extract_placeholders("just a normal string") == []

    def test_single_placeholder(self) -> None:
        segments = extract_placeholders("${{ inputs.target }}")
        assert segments == [
            PlaceholderSegment(
                token="${{ inputs.target }}",
                inner=" inputs.target ",
                start=0,
                end=20,
            ),
        ]

    def test_placeholder_with_surrounding_literal(self) -> None:
        segments = extract_placeholders("prefix-${{ inputs.x }}-suffix")
        assert len(segments) == 1
        s = segments[0]
        assert s.token == "${{ inputs.x }}"
        assert s.inner == " inputs.x "
        assert s.start == 7
        assert s.end == 22

    def test_multiple_placeholders(self) -> None:
        segments = extract_placeholders("${{ a }}-mid-${{ b }}")
        assert len(segments) == 2
        assert segments[0].inner == " a "
        assert segments[1].inner == " b "
        assert segments[0].start == 0
        assert segments[1].start == 13
        # Source order is preserved.
        assert segments[0].end <= segments[1].start

    def test_escaped_placeholder_is_skipped(self) -> None:
        # The leading backslash escapes the opening "$" — no segment
        # should be emitted.
        segments = extract_placeholders(r"literal \${{ not a placeholder }}")
        assert segments == []

    def test_escaped_and_real_placeholder_coexist(self) -> None:
        segments = extract_placeholders(r"\${{ escaped }} but ${{ real }}")
        assert len(segments) == 1
        assert segments[0].inner == " real "

    def test_map_literal_inside_placeholder(self) -> None:
        # ``${{ {'a': 1, 'b': 2} }}`` — the inner braces must not close
        # the placeholder prematurely.
        text = "${{ {'a': 1, 'b': 2} }}"
        segments = extract_placeholders(text)
        assert len(segments) == 1
        assert segments[0].token == text
        assert segments[0].inner == " {'a': 1, 'b': 2} "

    def test_closing_braces_inside_string_literal(self) -> None:
        # A ``}}`` inside a CEL string literal must NOT close the
        # placeholder.
        text = "${{ 'literal }} chars' + ' tail' }}"
        segments = extract_placeholders(text)
        assert len(segments) == 1
        assert segments[0].token == text

    def test_string_with_escape_inside_placeholder(self) -> None:
        # A backslash escape inside a CEL string literal does not
        # confuse the scanner.
        text = "${{ 'with \\' apos' + 'end' }}"
        segments = extract_placeholders(text)
        assert len(segments) == 1

    def test_unterminated_placeholder_raises(self) -> None:
        with pytest.raises(ValueError, match="unterminated"):
            extract_placeholders("prefix-${{ inputs.x")

    def test_unterminated_due_to_unbalanced_braces(self) -> None:
        # Open ``{`` raises depth; without a matching close the
        # placeholder never terminates.
        with pytest.raises(ValueError, match="unterminated"):
            extract_placeholders("${{ {'a': 1 }")

    def test_single_close_brace_does_not_terminate(self) -> None:
        # A bare ``}`` at depth zero is structurally invalid but the
        # scanner only fires on the doubled form; the placeholder
        # therefore remains open and the call site is reported as
        # unterminated.
        with pytest.raises(ValueError, match="unterminated"):
            extract_placeholders("${{ foo } bar")

    def test_double_quoted_string_inside(self) -> None:
        text = '${{ "double }} quoted" + "tail" }}'
        segments = extract_placeholders(text)
        assert len(segments) == 1
        assert segments[0].token == text


# ===========================================================================
# collect_call_sites — direct CEL slots on _StepCommon
# ===========================================================================


class TestCollectCommonSlots:
    def test_if_when_unless_for_each_where(self) -> None:
        doc = _doc(
            [
                _step(
                    {
                        "if": "${{ inputs.go }}",
                        "when": "${{ inputs.cond1 }}",
                        "unless": "${{ inputs.skip }}",
                        "forEach": "${{ inputs.items }}",
                        "where": "${{ inputs.filter }}",
                    },
                    sid="scan",
                ),
            ]
        )
        sites = collect_call_sites(doc)["scan"]
        kinds = {s.kind for s in sites}
        assert kinds == {
            CallSiteKind.IF,
            CallSiteKind.WHEN,
            CallSiteKind.UNLESS,
            CallSiteKind.FOR_EACH,
            CallSiteKind.WHERE,
        }
        # Every common slot site keeps the verbatim source and a
        # zero text offset.
        for site in sites:
            assert site.source.startswith("${{")
            assert site.source.endswith("}}")
            assert site.position.text_offset == 0
            assert site.position.document_path.startswith("spec.steps[0].")

    def test_none_when_slot_omitted(self) -> None:
        doc = _doc([_step(sid="scan")])
        assert collect_call_sites(doc)["scan"] == []

    def test_paths_use_wire_names(self) -> None:
        # ``forEach`` is the wire name (Python attribute is
        # ``for_each``); ``if`` is the wire name (Python attribute is
        # ``if_``). The breadcrumb must use the wire form.
        doc = _doc(
            [
                _step(
                    {
                        "forEach": "${{ inputs.items }}",
                        "if": "${{ inputs.go }}",
                    },
                    sid="loop",
                ),
            ]
        )
        paths = {s.path for s in collect_call_sites(doc)["loop"]}
        assert paths == {"forEach", "if"}
        breadcrumbs = {s.position.document_path for s in collect_call_sites(doc)["loop"]}
        assert "spec.steps[0].forEach" in breadcrumbs
        assert "spec.steps[0].if" in breadcrumbs


# ===========================================================================
# collect_call_sites — waitFor slots
# ===========================================================================


class TestCollectWaitFor:
    def test_event_key_only(self) -> None:
        doc = _doc(
            [{"id": "await-event", "waitFor": {"eventKey": "${{ inputs.key }}"}}],
        )
        sites = collect_call_sites(doc)["await-event"]
        assert len(sites) == 1
        site = sites[0]
        assert site.kind is CallSiteKind.WAIT_FOR_EVENT_KEY
        assert site.path == "waitFor.eventKey"
        assert site.source == "${{ inputs.key }}"
        assert site.position.document_path == "spec.steps[0].waitFor.eventKey"
        assert site.position.text_offset == 0

    def test_event_key_and_selector(self) -> None:
        doc = _doc(
            [
                {
                    "id": "await-event",
                    "waitFor": {
                        "eventKey": "${{ inputs.key }}",
                        "selector": "${{ event.ok }}",
                    },
                }
            ],
        )
        sites = collect_call_sites(doc)["await-event"]
        by_kind = {s.kind: s for s in sites}
        assert set(by_kind) == {
            CallSiteKind.WAIT_FOR_EVENT_KEY,
            CallSiteKind.WAIT_FOR_SELECTOR,
        }
        assert by_kind[CallSiteKind.WAIT_FOR_SELECTOR].path == "waitFor.selector"
        assert (
            by_kind[CallSiteKind.WAIT_FOR_SELECTOR].position.document_path
            == "spec.steps[0].waitFor.selector"
        )

    def test_ttl_is_not_a_call_site(self) -> None:
        doc = _doc(
            [
                {
                    "id": "await-event",
                    "waitFor": {"eventKey": "${{ inputs.key }}", "ttl": "PT2H"},
                }
            ],
        )
        sites = collect_call_sites(doc)["await-event"]
        assert [s.kind for s in sites] == [CallSiteKind.WAIT_FOR_EVENT_KEY]


# ===========================================================================
# collect_call_sites — let bindings
# ===========================================================================


class TestCollectLetBindings:
    def test_each_let_value_yields_one_call_site(self) -> None:
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "a": "${{ inputs.target }}",
                        "b": "${{ inputs.threshold }}",
                    },
                },
            ]
        )
        sites = collect_call_sites(doc)["derive"]
        assert len(sites) == 2
        assert {s.kind for s in sites} == {CallSiteKind.LET}
        assert {s.path for s in sites} == {"let.a", "let.b"}

    def test_let_literal_value_not_treated_as_call_site(self) -> None:
        # ``let`` can hold pure-data values; only ``${{ ... }}`` tokens
        # are call sites.
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "literal": "plain string",
                        "expr": "${{ inputs.target }}",
                    },
                },
            ]
        )
        sites = collect_call_sites(doc)["derive"]
        assert [s.path for s in sites] == ["let.expr"]

    def test_let_non_string_value_skipped(self) -> None:
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "num": 42,
                        "nested": {"a": 1},
                        "expr": "${{ inputs.target }}",
                    },
                },
            ]
        )
        sites = collect_call_sites(doc)["derive"]
        assert [s.path for s in sites] == ["let.expr"]

    def test_let_string_without_close_wrapper_raises(self) -> None:
        # A string that opens with ``${{`` but never closes is now
        # treated as a typo rather than literal data: the document
        # model carries it through with no validation, so the
        # collector eagerly raises ``CallSiteParseError`` with a
        # locator that points at the offending ``let`` binding.
        # (Pre-fix behaviour was to silently treat it as literal;
        # WF-IMPL-020 review feedback flagged that as too
        # forgiving.)
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "almost": "${{ but no close",
                    },
                },
            ]
        )
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.step_id == "derive"
        assert ei.value.path == "let.almost"

    def test_let_mixed_content_string_rejected(self) -> None:
        # ``"${{ a }}-${{ b }}"`` starts with ``${{`` and ends with
        # ``}}`` but is actually two distinct placeholders. The
        # simpler "starts and ends with wrapper" check would
        # false-positively treat this as a single CEL expression
        # and feed ``" a }}-${{ b "`` to ``custos_cel.parse`` —
        # producing a confusing diagnostic. The collector now
        # validates that the single matched segment covers the
        # whole value before accepting the binding as CEL.
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "mixed": "${{ a }}-${{ b }}",
                    },
                },
            ]
        )
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.path == "let.mixed"
        assert "mixed-content" in str(ei.value)

    def test_let_literal_with_embedded_placeholder_rejected(self) -> None:
        # A single placeholder surrounded by literal text is also
        # mixed-content under ``let:`` and is rejected with the
        # same diagnostic.
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "decorated": "prefix-${{ x }}-suffix",
                    },
                },
            ]
        )
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.path == "let.decorated"
        assert "mixed-content" in str(ei.value)

    def test_let_accepts_surrounding_whitespace(self) -> None:
        # Whitespace around the placeholder is tolerated since CEL
        # is whitespace-insensitive and authors legitimately may
        # insert it for readability.
        doc = _doc(
            [
                {
                    "id": "derive",
                    "let": {
                        "padded": "   ${{ inputs.target }}   ",
                    },
                },
            ]
        )
        sites = collect_call_sites(doc)["derive"]
        assert [s.path for s in sites] == ["let.padded"]
        assert sites[0].kind is CallSiteKind.LET


# ===========================================================================
# collect_call_sites — with block placeholders
# ===========================================================================


class TestCollectWithBlock:
    def test_with_full_token_yields_one_site(self) -> None:
        doc = _doc(
            [
                _step(
                    {"with": {"image": "${{ inputs.target }}"}},
                    sid="scan",
                ),
            ]
        )
        sites = collect_call_sites(doc)["scan"]
        assert len(sites) == 1
        s = sites[0]
        assert s.kind is CallSiteKind.WITH
        assert s.path == "with.image"
        assert s.source == "${{ inputs.target }}"
        assert s.position.document_path == "spec.steps[0].with.image"
        assert s.position.text_offset == 0

    def test_with_no_placeholder_emits_nothing(self) -> None:
        doc = _doc(
            [_step({"with": {"image": "literal-string"}}, sid="scan")],
        )
        assert collect_call_sites(doc)["scan"] == []

    def test_with_multiple_placeholders_yields_multiple_sites(self) -> None:
        doc = _doc(
            [
                _step(
                    {
                        "with": {
                            "tag": "registry/${{ inputs.target }}:${{ inputs.threshold }}",
                        },
                    },
                    sid="scan",
                ),
            ]
        )
        sites = collect_call_sites(doc)["scan"]
        assert len(sites) == 2
        assert all(s.kind is CallSiteKind.WITH for s in sites)
        # Each is suffixed with its 0-based index when more than one
        # placeholder shares the same with-key.
        assert [s.path for s in sites] == ["with.tag[0]", "with.tag[1]"]
        # Text offsets advance through the field.
        assert sites[0].position.text_offset < sites[1].position.text_offset
        # Sources are the individual segments, not the full string.
        assert sites[0].source == "${{ inputs.target }}"
        assert sites[1].source == "${{ inputs.threshold }}"

    def test_with_non_string_value_passes_through(self) -> None:
        # Numeric / dict / list values under ``with:`` carry data, not
        # CEL; the collector must skip them silently.
        doc = _doc(
            [
                _step(
                    {
                        "with": {
                            "count": 5,
                            "flags": ["a", "b"],
                            "image": "${{ inputs.target }}",
                        },
                    },
                    sid="scan",
                ),
            ]
        )
        sites = collect_call_sites(doc)["scan"]
        assert [s.path for s in sites] == ["with.image"]

    def test_with_block_absent_emits_nothing(self) -> None:
        doc = _doc([_step(sid="scan")])
        assert collect_call_sites(doc)["scan"] == []

    def test_with_escaped_placeholder_not_extracted(self) -> None:
        doc = _doc(
            [
                _step(
                    {"with": {"image": r"literal \${{ not a placeholder }}"}},
                    sid="scan",
                ),
            ]
        )
        assert collect_call_sites(doc)["scan"] == []

    def test_with_unterminated_placeholder_raises(self) -> None:
        doc = _doc(
            [
                _step(
                    {"with": {"image": "prefix-${{ inputs.x"}},
                    sid="scan",
                ),
            ]
        )
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.step_id == "scan"
        assert ei.value.path == "with.image"


# ===========================================================================
# collect_call_sites — workflow-step with block + multi-step ordering
# ===========================================================================


class TestCollectWorkflowSteps:
    def test_workflow_step_with_block_walked(self) -> None:
        doc = _doc(
            [
                {
                    "id": "sub",
                    "workflow": "ws/wf@1",
                    "with": {"param": "${{ inputs.target }}"},
                },
            ]
        )
        sites = collect_call_sites(doc)["sub"]
        assert [s.kind for s in sites] == [CallSiteKind.WITH]
        assert sites[0].path == "with.param"

    def test_every_step_keyed_in_result(self) -> None:
        doc = _doc(
            [
                _step(sid="a"),
                _step({"if": "${{ inputs.cond }}"}, sid="b"),
                {"id": "c", "let": {"x": "${{ inputs.target }}"}},
            ]
        )
        result = collect_call_sites(doc)
        # All step ids appear in the mapping, even those without CEL.
        assert set(result.keys()) == {"a", "b", "c"}
        assert result["a"] == []
        assert {s.kind for s in result["b"]} == {CallSiteKind.IF}
        assert {s.kind for s in result["c"]} == {CallSiteKind.LET}


# ===========================================================================
# collect_call_sites — parse failures
# ===========================================================================


class TestParseFailures:
    def test_invalid_cel_in_if_raises_with_context(self) -> None:
        # A syntactically broken expression — bare ``+`` operator.
        doc = _doc([_step({"if": "${{ + }}"}, sid="scan")])
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.step_id == "scan"
        assert ei.value.path == "if"
        # The original CEL ParseError is preserved as __cause__.
        assert ei.value.__cause__ is not None

    def test_invalid_cel_in_with_includes_with_key(self) -> None:
        doc = _doc(
            [
                _step(
                    {"with": {"image": "prefix-${{ + }}-suffix"}},
                    sid="scan",
                ),
            ]
        )
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.path == "with.image"

    def test_invalid_cel_in_let_includes_binding_name(self) -> None:
        doc = _doc([{"id": "derive", "let": {"broken": "${{ + }}"}}])
        with pytest.raises(CallSiteParseError) as ei:
            collect_call_sites(doc)
        assert ei.value.path == "let.broken"


# ===========================================================================
# Smoke: returned records are CallSite dataclass instances
# ===========================================================================


class TestRecordShape:
    def test_call_site_is_frozen_dataclass(self) -> None:
        doc = _doc([_step({"if": "${{ inputs.go }}"}, sid="scan")])
        site = collect_call_sites(doc)["scan"][0]
        assert isinstance(site, CallSite)
        assert isinstance(site.position, SourcePosition)
        # frozen=True ⇒ assignment raises.
        with pytest.raises(Exception):  # noqa: B017 - dataclasses raise FrozenInstanceError; we don't import it
            site.path = "other"  # type: ignore[misc]
