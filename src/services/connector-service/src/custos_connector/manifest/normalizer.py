"""Connector manifest normalizer + sha256 digest (CONN-IMPL-006, #289).

Mirrors the catalog-service workflow normalizer in
``src/services/catalog-service/src/custos_catalog/normalize.py`` (CS-IMPL-006):
both modules implement the same canonical-JSON contract — lexicographic
key sort, array order preserved, tight separators, UTF-8 byte
representation hashed with SHA-256.

The digest is the content-address used by
``CatalogStoreProvider.put_connector_type_version(digest=...)`` for the
``ConflictDigest`` 409 guarantee. Two manifests with identical
normalized form MUST hash to the same digest; any structural
difference MUST produce a different digest.

This implementation is intentionally inlined inside connector-service
rather than re-exported from ``custos-common`` because that library is
not yet bootstrapped (only ``.gitkeep`` lives there). When it is, the
``canonical_json`` + ``compute_digest`` functions here move to
``custos_common.canonical`` and both connector-service and
catalog-service re-export from there — that's a follow-up ticket.

Wire format note: the OCI distribution spec uses ``<alg>:<hex>`` for
content digests. We emit ``sha256:<hex>`` so the digest can be passed
verbatim into OCI manifest descriptors and the fallback-tag derivation
in :mod:`custos_connector.manifest.discovery`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, Final

#: OCI digest algorithm prefix. v1 supports sha256 only; bumping this
#: also requires the discovery module to widen its acceptance set in
#: :func:`custos_connector.manifest.discovery.fallback_tag_for_digest`.
_SHA256_PREFIX: Final[str] = "sha256:"


def _sort_key(key: Any) -> tuple[str, str]:
    """Total-ordering sort key tolerating heterogeneous dict-key types.

    Plain ``sorted(dict.keys())`` raises :class:`TypeError` when a
    mapping mixes key types. Connector manifests that have passed the
    schema gate cannot have non-string keys, but we mirror the
    catalog-service normalizer's totality property so this function
    can be safely called on partially-validated inputs without
    short-circuiting downstream error reporting with a ``TypeError``.
    """
    return (type(key).__name__, str(key))


def _canonicalize(node: Any) -> Any:
    """Recursively sort dict keys at every level; preserve list order."""
    if isinstance(node, dict):
        return {key: _canonicalize(node[key]) for key in sorted(node, key=_sort_key)}
    if isinstance(node, list):
        return [_canonicalize(item) for item in node]
    return node


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Render ``payload`` as canonical JSON.

    Canonical here means: lexicographic key order at every nesting
    level, list order preserved, tight ``(",", ":")`` separators, no
    trailing newline, ``ensure_ascii=False`` so the byte stream is true
    UTF-8 (the OCI digest is over the UTF-8 byte stream of the
    canonical form).
    """
    return json.dumps(
        _canonicalize(dict(payload)),
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    """Return the UTF-8 byte representation of :func:`canonical_json`."""
    return canonical_json(payload).encode("utf-8")


def normalize_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical (key-sorted) form of ``payload`` as a fresh dict.

    The returned mapping is independent of the input; mutating it does
    not affect the caller's payload. List order is preserved because
    the ``events.delivery`` / ``capabilities`` arrays in the schema
    declare ``uniqueItems: true`` (membership matters, not order),
    while ``events.produced`` author-declared event-type catalog order
    is meaningful and must round-trip unchanged.
    """
    canonical: Any = _canonicalize(dict(payload))
    if not isinstance(canonical, dict):
        # _canonicalize is total over JSON types and returns dict-for-dict
        # input, but the type system can't see that; assert and fall back.
        raise TypeError(  # pragma: no cover - defensive
            f"normalize_manifest expected a dict-shaped payload, got {type(payload).__name__}"
        )
    return canonical


def compute_digest(payload: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    """Return ``(normalized_payload, digest)`` for ``payload``.

    The digest is the OCI-format ``sha256:<hex>`` string suitable for
    direct insertion into a manifest descriptor and for the fallback-tag
    derivation rule in :mod:`custos_connector.manifest.discovery`.

    Property guarantees (covered by Hypothesis tests):

    1. **Permutation invariance.** Two ``payload`` inputs that differ
       only in dict key insertion order produce byte-identical
       canonical forms and therefore byte-identical digests.
    2. **Mutation sensitivity.** Any structural change — added,
       removed, or modified field — produces a different digest.
    """
    normalized = normalize_manifest(payload)
    encoded = canonical_bytes(normalized)
    digest_hex = hashlib.sha256(encoded).hexdigest()
    return normalized, f"{_SHA256_PREFIX}{digest_hex}"


__all__ = [
    "canonical_bytes",
    "canonical_json",
    "compute_digest",
    "normalize_manifest",
]
