"""Doc-example self-test for ``docs/developers/trigger-api.md`` (TS-IMPL-021).

Every fenced ``json`` / ``yaml`` / ``cel`` block in the developer guide is
validated against the real code so the document can never drift:

* JSON blocks tagged with a ``<!-- doctest: <Model> -->`` directive are parsed
  through that pydantic model (``extra="forbid"`` catches stray fields).
* ``cel`` blocks are compiled by the real :class:`SelectorEvaluator`, so an
  example that the v1 CEL subset rejects fails the suite.
* ``<!-- doctest: desugar field=.. match=.. value=.. -->`` blocks are pinned to
  the real :func:`desugar_legacy_selector` output.
* The event-taxonomy table is checked for completeness against the canonical
  registry — a new platform kind cannot be added in code without appearing in
  the doc.

All other fenced blocks are still checked for basic well-formedness.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel

from custos_trigger.events import NormalizedEvent
from custos_trigger.models import (
    CancelResumeRequest,
    ManualFireRequest,
    ManualFireResult,
    RegisterResumeRequest,
    RegisterResumeResponse,
    SelectorMatchType,
    Subscription,
    SubscriptionCreate,
    SubscriptionPatch,
)
from custos_trigger.selector import SelectorEvaluator, desugar_legacy_selector
from custos_trigger.taxonomy import CANONICAL_EVENT_KINDS, PLATFORM_DOMAINS

# ---------------------------------------------------------------------------
# Locate the doc. The trigger-service lives in ``src/services/trigger-service``
# and the docs in ``docs/`` at the repo root, so walk up to find it.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_REPO_ROOT = next(p for p in _HERE.parents if (p / "docs").is_dir())
_DOC_PATH = _REPO_ROOT / "docs" / "developers" / "trigger-api.md"

_MODELS: dict[str, type[BaseModel]] = {
    "SubscriptionCreate": SubscriptionCreate,
    "SubscriptionPatch": SubscriptionPatch,
    "ManualFireRequest": ManualFireRequest,
    "ManualFireResult": ManualFireResult,
    "RegisterResumeRequest": RegisterResumeRequest,
    "RegisterResumeResponse": RegisterResumeResponse,
    "CancelResumeRequest": CancelResumeRequest,
    "Subscription": Subscription,
    "NormalizedEvent": NormalizedEvent,
}

_FENCE = re.compile(r"```(?P<lang>\w+)\n(?P<body>.*?)\n```", re.DOTALL)
_DIRECTIVE = re.compile(r"[ \t]*<!--\s*doctest:\s*(?P<body>.+?)\s*-->[ \t]*")


def _doc_text() -> str:
    return _DOC_PATH.read_text(encoding="utf-8")


def _fences() -> list[tuple[int, str, str, list[str]]]:
    """Return ``(line, lang, body, directives)`` for every fenced block.

    ``directives`` are the contiguous ``<!-- doctest: ... -->`` comment lines
    immediately preceding the fence (blank lines allowed between them).
    """
    text = _doc_text()
    lines = text.splitlines()
    # Map a character offset to the directives that precede that fence.
    out: list[tuple[int, str, str, list[str]]] = []
    for match in _FENCE.finditer(text):
        start_line = text[: match.start()].count("\n")  # 0-based index of fence line
        directives: list[str] = []
        cursor = start_line - 1
        while cursor >= 0:
            stripped = lines[cursor].strip()
            if stripped == "":
                cursor -= 1
                continue
            dmatch = _DIRECTIVE.fullmatch(lines[cursor])
            if dmatch is None:
                break
            directives.insert(0, dmatch.group("body").strip())
            cursor -= 1
        out.append((start_line + 1, match.group("lang"), match.group("body"), directives))
    return out


def _id(item: tuple[int, str, str, list[str]]) -> str:
    return f"L{item[0]}-{item[1]}"


# ---------------------------------------------------------------------------
# Structural sanity
# ---------------------------------------------------------------------------


def test_trigger_api_doc_exists() -> None:
    assert _DOC_PATH.is_file(), f"missing developer guide: {_DOC_PATH}"


@pytest.mark.parametrize("fence", _fences(), ids=_id)
def test_json_and_yaml_blocks_are_well_formed(fence: tuple[int, str, str, list[str]]) -> None:
    _line, lang, body, _directives = fence
    if lang == "json":
        json.loads(body)
    elif lang == "yaml":
        yaml.safe_load(body)


# ---------------------------------------------------------------------------
# Directive-routed validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fence", _fences(), ids=_id)
def test_tagged_blocks_match_the_code(fence: tuple[int, str, str, list[str]]) -> None:
    line, lang, body, directives = fence
    if not directives:
        pytest.skip(f"L{line}: untagged block")

    for directive in directives:
        if directive in _MODELS:
            assert lang == "json", f"L{line}: model block must be json"
            _MODELS[directive].model_validate_json(body)
        elif directive == "cel":
            assert lang == "cel", f"L{line}: cel block must use the ```cel fence"
            SelectorEvaluator().compile(body.strip(), subscription_id="doc")
        elif directive.startswith("desugar"):
            fields = dict(token.split("=", 1) for token in directive.split()[1:])
            produced = desugar_legacy_selector(
                field=fields["field"],
                match_type=SelectorMatchType(fields["match"]),
                value=fields["value"],
            )
            assert produced == body.strip(), (
                f"L{line}: documented desugar output is stale; "
                f"desugar_legacy_selector produced {produced!r}"
            )
        else:
            pytest.fail(f"L{line}: unknown doctest directive {directive!r}")


# ---------------------------------------------------------------------------
# Taxonomy completeness — the doc table cannot omit a canonical kind or domain.
# ---------------------------------------------------------------------------


def test_taxonomy_table_lists_every_canonical_kind() -> None:
    text = _doc_text()
    missing = sorted(kind for kind in CANONICAL_EVENT_KINDS if f"`{kind}`" not in text)
    assert not missing, f"taxonomy table is missing canonical kinds: {missing}"


def test_taxonomy_table_lists_every_platform_domain() -> None:
    text = _doc_text()
    missing = sorted(domain for domain in PLATFORM_DOMAINS if f"| `{domain}` |" not in text)
    assert not missing, f"taxonomy table is missing platform domains: {missing}"
