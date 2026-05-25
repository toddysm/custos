"""Doc-example self-test for ``docs/developers/auth-api.md``.

Parses every fenced ``yaml`` block in the auth-api developer guide
and asserts that it round-trips through ``yaml.safe_load``.

The auth-service public surface is JSON-on-the-wire, but the
developer guide uses YAML fences for request/response examples
(JSON is a strict subset of YAML so parsers accept both, and YAML
is easier to read in long-form documentation). Every fenced YAML
block in the guide is treated as a contract: if it stops parsing,
something in the doc has drifted out of shape and the test fails.

Each block is rendered as an individual parametrized test case so a
failure points at the exact starting line of the offending fence in
the source document.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Locate the doc relative to this test file. The auth-service lives in
# ``src/services/auth-service`` and the docs in ``docs/`` at the repo
# root, so we walk up to find it.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_REPO_ROOT = next(p for p in _HERE.parents if (p / "docs").is_dir())
_DOC_PATH = _REPO_ROOT / "docs" / "developers" / "auth-api.md"

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


def test_auth_api_doc_exists() -> None:
    """Sanity check: the developer guide is checked in at the expected path."""
    assert _DOC_PATH.is_file(), f"missing developer guide: {_DOC_PATH}"


def test_auth_api_doc_has_yaml_blocks() -> None:
    """The guide must contain at least one fenced YAML block, otherwise
    this test file would silently pass with no parametrized cases."""
    blocks = _yaml_blocks()
    assert blocks, f"no ```yaml fences found in {_DOC_PATH}"


@pytest.mark.parametrize("block", _yaml_blocks(), ids=_id)
def test_yaml_block_is_well_formed(block: tuple[int, str]) -> None:
    """Every fenced YAML block in the doc must at minimum parse."""
    _line, body = block
    yaml.safe_load(body)
