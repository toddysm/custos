"""Unit + property tests for :mod:`custos_connector.manifest.normalizer`.

Covers CONN-IMPL-006 acceptance criteria:

* Hypothesis property: permuting dict key insertion order in any input
  produces the SAME digest.
* Hypothesis property: any structural mutation (added/removed/changed
  field) produces a DIFFERENT digest.
* Bytes-exact match against a fixture vector committed to the repo.
* OCI digest format: ``sha256:<64 lowercase hex>``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from custos_connector.manifest import (
    canonical_bytes,
    canonical_json,
    compute_digest,
    normalize_manifest,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "design").is_dir() and (parent / "src").is_dir():
            return parent
    raise RuntimeError("could not locate repository root from tests/")


def _load_example(name: str) -> dict[str, Any]:
    path = _repo_root() / "design" / "components" / "connector-service" / "examples" / name
    parsed: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return parsed


def _permute_keys(payload: Any) -> Any:
    """Return a structurally identical copy with every dict's key
    insertion order reversed at every nesting level.

    Two such permutations of the same payload differ only in dict
    iteration order, never in content. The canonical encoder MUST be
    insensitive to this difference.
    """
    if isinstance(payload, dict):
        items = list(payload.items())
        items.reverse()
        return {k: _permute_keys(v) for k, v in items}
    if isinstance(payload, list):
        return [_permute_keys(item) for item in payload]
    return payload


# ---------------------------------------------------------------------------
# Bytes-exact fixture vector
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "manifest_canonical"


def _read_fixture(stem: str, ext: str) -> str:
    return (FIXTURE_DIR / f"{stem}.{ext}").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "stem",
    [
        "oci-registry-akv-secrets",
        "amazon-s3-bucket-kms",
    ],
)
def test_canonical_output_matches_fixture_bytes(stem: str) -> None:
    """The canonical byte string MUST equal the committed fixture vector.

    A bytes-equal mismatch indicates the canonical encoder or its
    dependencies (json module behaviour change, recursion structure)
    has drifted; the digest will no longer be content-addressable and
    every previously stored ``ConflictDigest`` row becomes ambiguous.
    """
    input_doc = json.loads(_read_fixture(stem, "input.json"))
    expected_canonical = _read_fixture(stem, "canonical.json").rstrip("\n")
    expected_digest = _read_fixture(stem, "digest.txt").strip()

    assert canonical_json(input_doc) == expected_canonical
    _, digest = compute_digest(input_doc)
    assert digest == expected_digest


# ---------------------------------------------------------------------------
# Permutation invariance
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "oci-registry-azure-key-vault-secrets.manifest.json",
        "oci-registry-amazon-kms-secrets.manifest.json",
        "azure-blob-storage-kms.manifest.json",
        "amazon-s3-bucket-amazon-kms.manifest.json",
    ],
)
def test_permutation_invariance_on_real_examples(name: str) -> None:
    payload = _load_example(name)
    permuted = _permute_keys(payload)
    _, digest_a = compute_digest(payload)
    _, digest_b = compute_digest(permuted)
    assert digest_a == digest_b


# ---------------------------------------------------------------------------
# Hypothesis: arbitrary JSON-shaped object permutation invariance
# ---------------------------------------------------------------------------


def _json_value() -> st.SearchStrategy[Any]:
    """A bounded JSON-shaped value strategy.

    Hypothesis's ``recursive`` strategy keeps the depth bounded so the
    tests stay fast; we cap children to keep digest computation cheap.
    """
    return st.recursive(
        st.one_of(
            st.none(),
            st.booleans(),
            st.integers(min_value=-1000, max_value=1000),
            st.floats(allow_nan=False, allow_infinity=False, width=32),
            st.text(min_size=0, max_size=16),
        ),
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            st.dictionaries(
                # ASCII printable, sized to keep generated examples
                # compact + readable in test failure output.
                st.text(
                    alphabet=st.characters(min_codepoint=33, max_codepoint=126),
                    min_size=1,
                    max_size=8,
                ),
                children,
                max_size=4,
            ),
        ),
        max_leaves=8,
    )


def _json_object() -> st.SearchStrategy[dict[str, Any]]:
    return st.dictionaries(
        st.text(
            alphabet=st.characters(min_codepoint=33, max_codepoint=126),
            min_size=1,
            max_size=8,
        ),
        _json_value(),
        min_size=1,
        max_size=5,
    )


@given(payload=_json_object())
@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_canonical_form_is_insensitive_to_key_order(payload: dict[str, Any]) -> None:
    permuted = _permute_keys(payload)
    assert canonical_json(payload) == canonical_json(permuted)
    assert canonical_bytes(payload) == canonical_bytes(permuted)
    _, digest_a = compute_digest(payload)
    _, digest_b = compute_digest(permuted)
    assert digest_a == digest_b


# ---------------------------------------------------------------------------
# Hypothesis: mutation sensitivity — different structure ⇒ different digest
# ---------------------------------------------------------------------------


@given(payload=_json_object(), extra_key=st.text(min_size=1, max_size=8))
@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_adding_a_field_changes_the_digest(payload: dict[str, Any], extra_key: str) -> None:
    # Make sure the extra key is not already in the payload (else it
    # would be a value mutation, which the test below covers
    # separately).
    if extra_key in payload:
        return
    mutated = dict(payload)
    mutated[extra_key] = "sentinel"
    _, digest_a = compute_digest(payload)
    _, digest_b = compute_digest(mutated)
    assert digest_a != digest_b


@given(payload=_json_object())
@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_changing_a_value_changes_the_digest(payload: dict[str, Any]) -> None:
    """Replacing the value at any top-level key produces a new digest.

    We pick the first key (sorted, for stability) and overwrite the
    value with a sentinel that no random Hypothesis-generated value
    would equal byte-for-byte.
    """
    if not payload:
        return  # _json_object enforces min_size=1; defensive only.
    sentinel = "__custos_mutation_sentinel__"
    first_key = sorted(payload)[0]
    if payload[first_key] == sentinel:
        return  # impossible in practice but defensive.
    mutated = dict(payload)
    mutated[first_key] = sentinel
    _, digest_a = compute_digest(payload)
    _, digest_b = compute_digest(mutated)
    assert digest_a != digest_b


# ---------------------------------------------------------------------------
# OCI digest format
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["oci-registry-azure-key-vault-secrets.manifest.json"])
def test_digest_has_oci_format(name: str) -> None:
    payload = _load_example(name)
    _, digest = compute_digest(payload)
    assert digest.startswith("sha256:")
    hex_part = digest.removeprefix("sha256:")
    assert len(hex_part) == 64
    assert all(c in "0123456789abcdef" for c in hex_part)


def test_normalize_returns_sorted_keys() -> None:
    payload = {"z": 1, "a": 2, "m": {"y": 3, "b": 4}}
    out = normalize_manifest(payload)
    assert list(out.keys()) == sorted(out.keys())
    inner = out["m"]
    assert isinstance(inner, dict)
    assert list(inner.keys()) == sorted(inner.keys())


def test_normalize_preserves_array_order() -> None:
    payload = {"items": [3, 1, 2]}
    out = normalize_manifest(payload)
    assert out["items"] == [3, 1, 2]


def test_normalize_returns_fresh_dict() -> None:
    payload = {"a": 1}
    out = normalize_manifest(payload)
    out["a"] = 999
    assert payload["a"] == 1


def test_canonical_json_uses_tight_separators() -> None:
    payload = {"a": 1, "b": [1, 2]}
    rendered = canonical_json(payload)
    assert " " not in rendered  # no whitespace between tokens
    assert ":" in rendered and "," in rendered


def test_canonical_json_emits_utf8_unicode_directly() -> None:
    """ensure_ascii=False so non-ASCII runes are not \\uXXXX-escaped."""
    payload = {"label": "café"}
    rendered = canonical_json(payload)
    assert "café" in rendered
    assert "\\u" not in rendered
