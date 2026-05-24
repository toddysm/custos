"""Doc-example self-test for ``docs/developers/catalog-api.md``.

Parses every fenced ``yaml`` block in the catalog-api developer
guide and routes documents that carry an ``apiVersion`` + ``kind``
pair through the appropriate structural validator
(``validate_workflow`` / ``validate_template``). Blocks that are
clearly partial (request bodies, placeholder snippets, etc.) are
skipped — the document is the source of truth for the on-wire shape
of full Workflow and WorkflowTemplate documents only.

This guarantees that every full example in the developer guide stays
in lockstep with the JSON Schema as the catalog evolves.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from custos_catalog.schema import validate_template, validate_workflow

# ---------------------------------------------------------------------------
# Locate the doc relative to this test file. The catalog-service lives in
# ``src/services/catalog-service`` and the docs in ``docs/`` at the repo
# root, so we walk up to find it.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_REPO_ROOT = next(p for p in _HERE.parents if (p / "docs").is_dir())
_DOC_PATH = _REPO_ROOT / "docs" / "developers" / "catalog-api.md"

_YAML_FENCE = re.compile(r"```yaml\n(?P<body>.*?)\n```", re.DOTALL)


def _yaml_blocks() -> list[tuple[int, str]]:
    text = _DOC_PATH.read_text(encoding="utf-8")
    blocks: list[tuple[int, str]] = []
    for match in _YAML_FENCE.finditer(text):
        line = text[: match.start()].count("\n") + 1
        blocks.append((line, match.group("body")))
    return blocks


def _id(item: tuple[int, str]) -> str:
    return f"L{item[0]}"


def test_catalog_api_doc_exists() -> None:
    """Sanity check: the developer guide is checked in at the expected path."""
    assert _DOC_PATH.is_file(), f"missing developer guide: {_DOC_PATH}"


@pytest.mark.parametrize("block", _yaml_blocks(), ids=_id)
def test_yaml_block_is_well_formed(block: tuple[int, str]) -> None:
    """Every fenced YAML block in the doc must at minimum parse."""
    _line, body = block
    yaml.safe_load(body)


@pytest.mark.parametrize("block", _yaml_blocks(), ids=_id)
def test_full_documents_pass_structural_validation(
    block: tuple[int, str],
) -> None:
    """Every fenced YAML block that declares ``apiVersion`` + ``kind`` of a
    Workflow / WorkflowTemplate MUST validate against the structural
    schema. Other documents (ActivityManifest, ConnectorManifest) and
    partial snippets (request bodies, placeholder lists) are skipped.
    """
    line, body = block
    doc = yaml.safe_load(body)
    if not isinstance(doc, dict):
        pytest.skip(f"L{line}: non-mapping YAML block (illustrative)")
    api_version = doc.get("apiVersion")
    kind = doc.get("kind")
    if not api_version or not kind:
        pytest.skip(f"L{line}: partial snippet (no apiVersion/kind)")
    if kind == "Workflow":
        validate_workflow(doc)
    elif kind == "WorkflowTemplate":
        validate_template(doc)
    else:
        # ActivityManifest / ConnectorManifest schemas live in their
        # owning services; the catalog-service doc only needs to
        # surface them as illustrative.
        pytest.skip(f"L{line}: {kind} validated elsewhere")
