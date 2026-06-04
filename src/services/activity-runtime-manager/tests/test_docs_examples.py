"""Doc-example self-test for the Activity Runtime Manager (ARM-IMPL-022).

Pins three things so the docs cannot silently drift from the code:

#. every fenced ``yaml`` ``ActivityManifest`` in
   ``docs/developers/activity-author.md`` parses through
   :func:`~custos_arm.manifest.parse_manifest`;
#. every fenced ``json`` block in that guide is well-formed JSON;
#. the service ``README.md`` § Configuration table documents **exactly** the
   ``ARM_*`` / ``ENVIRONMENT`` / ``HOST`` / ``PORT`` env vars the loader
   recognizes (no orphan rows, no undocumented vars), and every ``DEFAULT_*``
   value from :mod:`custos_arm.config` appears in the table.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from custos_arm import config
from custos_arm.manifest import parse_manifest

# ---------------------------------------------------------------------------
# Locate the repo-root docs + this service's README.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_REPO_ROOT = next(p for p in _HERE.parents if (p / "docs").is_dir())
_GUIDE_PATH = _REPO_ROOT / "docs" / "developers" / "activity-author.md"
_README_PATH = _REPO_ROOT / "src" / "services" / "activity-runtime-manager" / "README.md"

_YAML_FENCE = re.compile(r"```yaml\n(?P<body>.*?)\n```", re.DOTALL)
_JSON_FENCE = re.compile(r"```json\n(?P<body>.*?)\n```", re.DOTALL)


def _fences(pattern: re.Pattern[str], text: str) -> list[tuple[int, str]]:
    blocks: list[tuple[int, str]] = []
    for match in pattern.finditer(text):
        line = text[: match.start()].count("\n") + 1
        blocks.append((line, match.group("body")))
    return blocks


def _guide_text() -> str:
    return _GUIDE_PATH.read_text(encoding="utf-8")


def _yaml_blocks() -> list[tuple[int, str]]:
    return _fences(_YAML_FENCE, _guide_text())


def _json_blocks() -> list[tuple[int, str]]:
    return _fences(_JSON_FENCE, _guide_text())


def _id(item: tuple[int, str]) -> str:
    return f"L{item[0]}"


# ---------------------------------------------------------------------------
# Existence + cross-linking
# ---------------------------------------------------------------------------


def test_guide_exists_and_cross_links_the_design_doc() -> None:
    assert _GUIDE_PATH.is_file(), f"missing developer guide: {_GUIDE_PATH}"
    text = _guide_text()
    assert "design/components/activity-runtime-manager/design.md" in text


# ---------------------------------------------------------------------------
# Fenced examples
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("block", _yaml_blocks(), ids=_id)
def test_yaml_manifest_blocks_parse(block: tuple[int, str]) -> None:
    """Every ``ActivityManifest`` YAML block must validate via the parser."""
    line, body = block
    doc = yaml.safe_load(body)
    if not isinstance(doc, dict) or doc.get("kind") != "ActivityManifest":
        pytest.skip(f"L{line}: not a full ActivityManifest block")
    manifest = parse_manifest(doc)
    assert manifest.metadata.type
    assert manifest.spec.runtime.digest


@pytest.mark.parametrize("block", _json_blocks(), ids=_id)
def test_json_blocks_are_well_formed(block: tuple[int, str]) -> None:
    """Every fenced JSON envelope example must parse."""
    _line, body = block
    json.loads(body)


def test_guide_ships_a_manifest_and_envelope_examples() -> None:
    """Guard against the fences being deleted or renamed away."""
    assert any(
        isinstance(yaml.safe_load(body), dict)
        and yaml.safe_load(body).get("kind") == "ActivityManifest"
        for _line, body in _yaml_blocks()
    ), "the guide must ship at least one ActivityManifest example"
    assert len(_json_blocks()) >= 3, "the guide must ship the inputs/outputs envelopes"


# ---------------------------------------------------------------------------
# README configuration table
# ---------------------------------------------------------------------------


def _recognized_env_vars() -> set[str]:
    """Every env-var name the loader reads (the ``ENV_*`` string constants)."""
    return {
        value
        for name, value in vars(config).items()
        if name.startswith("ENV_") and isinstance(value, str)
    }


def _documented_env_vars(readme: str) -> set[str]:
    """Backtick-wrapped ``ARM_*`` / ``ENVIRONMENT`` / ``HOST`` / ``PORT`` tokens."""
    tokens = set(re.findall(r"`([A-Z][A-Z0-9_]*)`", readme))
    return {t for t in tokens if t.startswith("ARM_") or t in {"ENVIRONMENT", "HOST", "PORT"}}


def test_readme_config_table_matches_the_loader() -> None:
    """The README must document exactly the env vars the loader recognizes."""
    readme = _README_PATH.read_text(encoding="utf-8")
    recognized = _recognized_env_vars()
    documented = _documented_env_vars(readme)

    missing = recognized - documented
    orphan = documented - recognized
    assert not missing, f"README is missing env vars: {sorted(missing)}"
    assert not orphan, f"README documents unknown env vars: {sorted(orphan)}"


def test_readme_documents_every_default() -> None:
    """Each ``DEFAULT_*`` value from the loader must appear in the README table."""
    readme = _README_PATH.read_text(encoding="utf-8")
    for name, value in vars(config).items():
        if not name.startswith("DEFAULT_"):
            continue
        assert str(value) in readme, f"README omits default {name}={value!r}"
