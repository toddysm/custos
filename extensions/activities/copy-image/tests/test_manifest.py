"""Structural validation of ``activity-manifest.yaml`` against the
ActivityManifest v1 contract.

The activity is decoupled — it does not import the platform's ARM manifest
models — so this test asserts the same structural constraints those models
enforce (see ``docs/developers/activity-author.md`` and the ARM
``custos_arm.manifest.models`` reference). Keeping the check here means a
manifest typo fails the activity's own CI before the image is built.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_MANIFEST = Path(__file__).resolve().parents[1] / "activity-manifest.yaml"

# These mirror the platform validators exactly (custos_arm.manifest._base,
# custos_arm.contract._base) so this decoupled CI gate stays aligned with
# what ARM/Catalog actually enforce.
_CAPABILITY_RE = re.compile(r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+$")
_SEMVER_RE = re.compile(r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
_DURATION_RE = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)
_ERROR_CLASSES = {"permanent", "retryable", "cancelled"}
_ISOLATION_TIERS = {"process", "vm", "microvm"}
_DETERMINISM = {"pure", "side-effecting"}
_IDEMPOTENCY = {"by-input-hash", "none"}


def _is_capability(token: str) -> bool:
    """Mirror ``custos_arm.manifest._base.is_capability_token``: dot-namespaced
    lowercase grammar, and ``event.*`` verbs are rejected."""
    if token.startswith("event."):
        return False
    return _CAPABILITY_RE.match(token) is not None


def _is_duration(value: str) -> bool:
    """Mirror ``custos_arm.contract._base.is_iso8601_duration``: matches the
    grammar AND carries at least one component (empty ``P``/``PT`` rejected)."""
    match = _DURATION_RE.match(value)
    if match is None:
        return False
    return any(match.group(p) for p in ("weeks", "days", "hours", "minutes", "seconds"))


def _load() -> dict[str, Any]:
    data = yaml.safe_load(_MANIFEST.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def test_top_level_identity() -> None:
    m = _load()
    assert m["apiVersion"] == "custos.dev/v1"
    assert m["kind"] == "ActivityManifest"


def test_metadata() -> None:
    md = _load()["metadata"]
    assert md["type"] == "copy-image"
    assert _SEMVER_RE.match(md["version"]), md["version"]
    assert md["namespace"] == "custos.builtin"
    assert md["description"]
    assert md["owner"]
    assert md.get("labels", {}).get("category") == "registry"


def test_runtime() -> None:
    rt = _load()["spec"]["runtime"]
    assert rt["kind"] == "oci-container"
    assert rt["image"]
    # Repo image-naming convention: ghcr.io/toddysm/custos/<name>.
    assert rt["image"].startswith("ghcr.io/toddysm/custos/")
    assert str(rt["digest"]).startswith("sha256:")
    assert rt.get("isolation", {}).get("minTier") in _ISOLATION_TIERS


def test_contract_version_is_1() -> None:
    assert _load()["spec"]["contractVersion"] == "1"


def test_inputs_and_outputs_schemas() -> None:
    spec = _load()["spec"]
    for key in ("inputs", "outputs"):
        schema = spec[key]["schema"]
        assert isinstance(schema, dict)
        assert schema.get("$schema", "").endswith("draft/2020-12/schema")
        assert schema.get("type") == "object"
    # Headline inputs/outputs are declared.
    in_props = spec["inputs"]["schema"]["properties"]
    assert {"source", "destination", "copyReferrers", "allPlatforms"} <= set(in_props)
    out_props = spec["outputs"]["schema"]["properties"]
    assert {"destinationRef", "digest"} <= set(out_props)


def test_output_artifacts_unique_and_well_formed() -> None:
    artifacts = _load()["spec"]["outputs"].get("artifacts", [])
    names = [a["name"] for a in artifacts]
    assert len(names) == len(set(names)), "artifact names must be unique"
    for a in artifacts:
        assert a["name"]
        assert a["mediaType"]
        assert isinstance(a["required"], bool)
    assert "copy-report" in names


def test_connector_slots() -> None:
    connectors = _load()["spec"]["connectors"]
    by_name = {c["name"]: c for c in connectors}
    assert {"source", "dest"} == set(by_name)
    for c in connectors:
        assert c["type"] == "oci-registry"
        assert isinstance(c["required"], bool)
        for token in c["capabilities"]:
            assert _is_capability(token), token
    assert "oci.pull" in by_name["source"]["capabilities"]
    assert "oci.push" in by_name["dest"]["capabilities"]


def test_resources() -> None:
    res = _load()["spec"]["resources"]
    assert _is_duration(res["timeout"]), res["timeout"]
    assert res["ephemeralStorage"]["limit"]


def test_errors_use_known_classes() -> None:
    errors = _load()["spec"]["errors"]
    codes = {e["code"] for e in errors}
    assert {
        "source.unauthorized",
        "dest.unauthorized",
        "source.not_found",
        "dest.push_failed",
        "copy.manifest_mismatch",
    } <= codes
    for e in errors:
        assert e["class"] in _ERROR_CLASSES, e


def test_determinism_and_idempotency() -> None:
    spec = _load()["spec"]
    assert spec["determinism"] in _DETERMINISM
    assert spec["idempotency"] in _IDEMPOTENCY
    assert spec["idempotency"] == "by-input-hash"
