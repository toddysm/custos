"""Drift guard: the packaged schema MUST equal the design source-of-truth.

The ConnectorManifest v1 schema lives in two places by design:

1. ``design/components/connector-service/schemas/connector-manifest.v1.schema.json``
   — the authoritative source-of-truth tracked by change-record proposals.
2. ``src/services/connector-service/src/custos_connector/manifest/_schemas/
   connector-manifest.v1.schema.json``
   — packaged with the wheel so :mod:`custos_connector.manifest.validator`
   can load it via :mod:`importlib.resources` at runtime.

This test pins the two copies bytes-equal so a change to the design
file without a synchronised package update (or vice-versa) fails the
pre-merge CI gate. Operators who legitimately need to evolve the
schema must update both files in the same commit.
"""

from __future__ import annotations

from pathlib import Path

from custos_connector.manifest.validator import _SCHEMA_RESOURCE


def _repo_root() -> Path:
    """Walk up from this file until we hit the repo top-level."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "design").is_dir() and (parent / "src").is_dir():
            return parent
    raise RuntimeError("could not locate repository root from tests/")


def test_packaged_schema_matches_design_source_of_truth() -> None:
    repo = _repo_root()
    design_copy = (
        repo / "design" / "components" / "connector-service" / "schemas" / _SCHEMA_RESOURCE
    )
    packaged_copy = (
        repo
        / "src"
        / "services"
        / "connector-service"
        / "src"
        / "custos_connector"
        / "manifest"
        / "_schemas"
        / _SCHEMA_RESOURCE
    )
    assert design_copy.is_file(), f"design schema missing: {design_copy}"
    assert packaged_copy.is_file(), f"packaged schema missing: {packaged_copy}"
    design_bytes = design_copy.read_bytes()
    packaged_bytes = packaged_copy.read_bytes()
    assert design_bytes == packaged_bytes, (
        "packaged ConnectorManifest v1 schema has drifted from the design "
        "source-of-truth; run `cp design/components/connector-service/"
        "schemas/connector-manifest.v1.schema.json "
        "src/services/connector-service/src/custos_connector/manifest/_schemas/`"
    )
