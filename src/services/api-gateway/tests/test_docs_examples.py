"""Doc-example self-test for ``docs/developers/api-gateway.md`` (AGW-IMPL-021).

The developer guide is pinned to the running code so it can never silently
drift:

* JSON blocks tagged with a ``<!-- doctest: <Model> -->`` directive are parsed
  through that pydantic model.
* The error-taxonomy table is checked for completeness against the locked
  :data:`~custos_gateway.errors.LOCKED_CODE_TO_STATUS` map — every code must
  appear with its real HTTP status, and no extra code may be invented.
* The route-registry section must mention every distinct permission the
  :data:`~custos_gateway.routes.registry.M1_ROUTE_REGISTRY` enforces and every
  owning component's Dapr app-id.
* The configuration table must list every ``CUSTOS_GATEWAY_*`` / Dapr / operational
  environment variable the settings loader reads.

All other fenced blocks are still checked for basic well-formedness.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pydantic import BaseModel

from custos_gateway import settings as gw_settings
from custos_gateway.clients.auth import AUTH_APP_ID
from custos_gateway.errors import LOCKED_CODE_TO_STATUS, ProblemDetail
from custos_gateway.routes.registry import (
    CATALOG_APP_ID,
    CONNECTOR_APP_ID,
    OBSERVABILITY_APP_ID,
    TRIGGER_APP_ID,
    WORKFLOW_APP_ID,
    registry_required_permissions,
)

# ---------------------------------------------------------------------------
# Locate the doc. The gateway lives in ``src/services/api-gateway`` and the docs
# in ``docs/`` at the repo root, so walk up to find it.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_REPO_ROOT = next(p for p in _HERE.parents if (p / "docs").is_dir())
_DOC_PATH = _REPO_ROOT / "docs" / "developers" / "api-gateway.md"

_MODELS: dict[str, type[BaseModel]] = {
    "ProblemDetail": ProblemDetail,
}

_FENCE = re.compile(r"```(?P<lang>\w*)\n(?P<body>.*?)\n```", re.DOTALL)
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


def test_api_gateway_doc_exists() -> None:
    assert _DOC_PATH.is_file(), f"missing developer guide: {_DOC_PATH}"


def test_every_fence_marker_is_paired() -> None:
    """The fence parser only sees *closed* blocks, so guard against a stray fence.

    A malformed or unclosed triple-backtick fence would otherwise be silently
    skipped, letting an example break (or drift) without failing this suite. An
    odd number of fence markers means at least one block is unterminated.
    """
    markers = re.findall(r"^```", _doc_text(), re.MULTILINE)
    assert len(markers) % 2 == 0, (
        f"unbalanced code-fence markers in {_DOC_PATH.name}: found {len(markers)} "
        "``` markers (an unclosed fence hides blocks from the doc-example checks)"
    )
    # Every opening fence the parser recognises must round-trip back to a closed
    # block, so the count of parsed blocks equals half the marker count.
    assert len(_fences()) == len(markers) // 2


@pytest.mark.parametrize("fence", _fences(), ids=_id)
def test_json_blocks_are_well_formed(fence: tuple[int, str, str, list[str]]) -> None:
    _line, lang, body, _directives = fence
    if lang == "json":
        json.loads(body)


# ---------------------------------------------------------------------------
# Directive-routed validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fence", _fences(), ids=_id)
def test_tagged_blocks_match_the_code(fence: tuple[int, str, str, list[str]]) -> None:
    line, lang, body, directives = fence
    if not directives:
        return
    for directive in directives:
        if directive in _MODELS:
            assert lang == "json", f"L{line}: model block must be json"
            _MODELS[directive].model_validate_json(body)
        else:
            pytest.fail(f"L{line}: unknown doctest directive {directive!r}")


# ---------------------------------------------------------------------------
# Error taxonomy completeness — the doc table cannot omit a locked code, invent
# a code, or document a wrong HTTP status.
# ---------------------------------------------------------------------------

_ERROR_TABLE_MARKER = "The closed code → HTTP-status set:"
_ERROR_ROW = re.compile(r"^\| `(?P<code>[a-z-]+)` \| (?P<status>\d+) \|", re.MULTILINE)


def _error_table_region() -> str:
    text = _doc_text()
    start = text.index(_ERROR_TABLE_MARKER)
    rest = text[start:]
    end = rest.index("\n---", rest.index("|"))
    return rest[:end]


def test_error_table_lists_every_locked_code_with_its_status() -> None:
    region = _error_table_region()
    documented = {m.group("code"): int(m.group("status")) for m in _ERROR_ROW.finditer(region)}
    expected = {code.value: status for code, status in LOCKED_CODE_TO_STATUS.items()}
    assert documented == expected, (
        "error-taxonomy table is out of sync with LOCKED_CODE_TO_STATUS; "
        f"documented={documented!r} expected={expected!r}"
    )


# ---------------------------------------------------------------------------
# Route-registry completeness — every permission and every owning app-id the
# registry references must be documented.
# ---------------------------------------------------------------------------


def test_doc_mentions_every_registry_permission() -> None:
    text = _doc_text()
    missing = sorted(p for p in registry_required_permissions() if f"`{p}`" not in text)
    assert not missing, f"route-registry doc is missing permissions: {missing}"


def test_doc_mentions_every_downstream_app_id() -> None:
    text = _doc_text()
    app_ids = {
        AUTH_APP_ID,
        CATALOG_APP_ID,
        WORKFLOW_APP_ID,
        TRIGGER_APP_ID,
        CONNECTOR_APP_ID,
        OBSERVABILITY_APP_ID,
    }
    missing = sorted(app_id for app_id in app_ids if f"`{app_id}`" not in text)
    assert not missing, f"route-registry doc is missing app-ids: {missing}"


# ---------------------------------------------------------------------------
# Configuration completeness — every env var the settings loader reads must be
# listed in the configuration table.
# ---------------------------------------------------------------------------


def _config_env_vars() -> list[str]:
    return [
        value
        for name, value in vars(gw_settings).items()
        if name.startswith("ENV_") and isinstance(value, str)
    ]


def test_config_table_lists_every_environment_variable() -> None:
    text = _doc_text()
    missing = sorted(var for var in _config_env_vars() if f"`{var}`" not in text)
    assert not missing, f"configuration table is missing env vars: {missing}"
