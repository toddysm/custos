"""Doc-example self-test for ``docs/developers/observability-api.md`` (OBS-IMPL-017).

Every fenced ``json`` / ``yaml`` block in the developer guide is validated
against the real code so the document can never drift:

* JSON blocks tagged with a ``<!-- doctest: <Model> -->`` directive are parsed
  through that pydantic wire model.
* a ``yaml`` block tagged ``<!-- doctest: alert-rules -->`` is loaded by the real
  :func:`load_alert_rules`, so an example the DSL rejects fails the suite.
* the ``<!-- doctest: exporter-base -->`` and ``<!-- doctest: exporter-customer -->``
  blocks are merged by the real :func:`merge_collector_config`.
* the error / event / config tables are checked for completeness against the
  locked taxonomies and the settings module — a new error kind, ``obs.*`` event,
  or ``CUSTOS_*`` variable cannot be added in code without appearing in the doc.

All other fenced blocks are still checked for basic well-formedness.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel

from custos_obs import settings as settings_module
from custos_obs.alerting import load_alert_rules
from custos_obs.api.models import (
    AuditEventModel,
    AuditEventPageModel,
    LogPageModel,
    LogRecordModel,
    MetricSeriesModel,
)
from custos_obs.errors import LOCKED_OBS_ERROR_KINDS
from custos_obs.events import LOCKED_OBS_EVENT_NAMES
from custos_obs.exporters import merge_collector_config

# ---------------------------------------------------------------------------
# Locate the doc. The service lives in ``src/services/observability-audit-service``
# and the docs in ``docs/`` at the repo root, so walk up to find it.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_REPO_ROOT = next(p for p in _HERE.parents if (p / "docs").is_dir())
_DOC_PATH = _REPO_ROOT / "docs" / "developers" / "observability-api.md"

_MODELS: dict[str, type[BaseModel]] = {
    "LogRecordModel": LogRecordModel,
    "LogPageModel": LogPageModel,
    "MetricSeriesModel": MetricSeriesModel,
    "AuditEventModel": AuditEventModel,
    "AuditEventPageModel": AuditEventPageModel,
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


def _block_for(directive: str) -> str:
    """Return the body of the single fenced block tagged with ``directive``."""
    matches = [body for _line, _lang, body, directives in _fences() if directive in directives]
    assert len(matches) == 1, f"expected exactly one {directive!r} block, found {len(matches)}"
    return matches[0]


# ---------------------------------------------------------------------------
# Structural sanity
# ---------------------------------------------------------------------------


def test_observability_api_doc_exists() -> None:
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
        # Untagged blocks (the auth envelope, the Problem body, SSE frames, the
        # Prometheus exposition snippet) are only structurally checked.
        return

    for directive in directives:
        if directive in _MODELS:
            assert lang == "json", f"L{line}: model block must be json"
            _MODELS[directive].model_validate_json(body)
        elif directive == "alert-rules":
            assert lang == "yaml", f"L{line}: alert-rules block must be yaml"
            ruleset = load_alert_rules(body)
            assert ruleset.rules, f"L{line}: alert-rules block produced no rules"
        elif directive in {"exporter-base", "exporter-customer"}:
            assert lang == "yaml", f"L{line}: exporter block must be yaml"
            # Validated jointly in test_exporter_blocks_merge below.
        else:
            pytest.fail(f"L{line}: unknown doctest directive {directive!r}")


def test_exporter_blocks_merge() -> None:
    """The documented base + customer exporter blocks merge cleanly."""
    base = _block_for("exporter-base")
    customer = _block_for("exporter-customer")
    result = merge_collector_config(base, customer)
    assert "loki/customer" in result.exporter_names


# ---------------------------------------------------------------------------
# Taxonomy + config completeness — the doc tables cannot omit a locked member.
# ---------------------------------------------------------------------------


def _table_after(marker: str) -> str:
    """Return the first markdown table region following ``marker``."""
    text = _doc_text()
    start = text.index(marker)
    rest = text[start:]
    table_start = rest.index("|")
    end = rest.index("\n\n", table_start)
    return rest[:end]


def test_error_taxonomy_table_lists_every_locked_kind() -> None:
    table = _table_after("The locked Problem Details taxonomy:")
    missing = sorted(kind for kind in LOCKED_OBS_ERROR_KINDS if f"`{kind}`" not in table)
    assert not missing, f"error taxonomy table is missing locked kinds: {missing}"


def test_event_taxonomy_table_lists_every_locked_event() -> None:
    table = _table_after("The locked `obs.*` event taxonomy:")
    missing = sorted(name for name in LOCKED_OBS_EVENT_NAMES if f"`{name}`" not in table)
    assert not missing, f"event taxonomy table is missing locked events: {missing}"


def test_configuration_table_lists_every_env_var() -> None:
    table = _table_after("| Environment variable | Default | Notes |")
    env_vars = sorted(
        value
        for name, value in vars(settings_module).items()
        if name.startswith("ENV_") and isinstance(value, str)
    )
    assert env_vars, "no ENV_* constants discovered in the settings module"
    missing = [var for var in env_vars if f"`{var}`" not in table]
    assert not missing, f"configuration table is missing variables: {missing}"
