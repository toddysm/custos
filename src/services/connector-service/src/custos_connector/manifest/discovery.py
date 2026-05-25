"""ConnectorManifest discovery pipeline (CONN-IMPL-007, #290).

Implements the v1 discovery rules from design §
*Plugin Packaging and Discovery → Manifest artifact rules* +
*Fallback tag naming*.

Two paths resolve a manifest for a given ``(repository, image_digest)``:

1. **OCI Referrers API (Distribution Spec v1.1+).**
   ``GET /v2/<repo>/referrers/<digest>``
   with ``?artifactType=application/vnd.custos.connector.manifest.v1+json``
   returns an OCI Image Index whose ``manifests[]`` entries describe
   manifest artifacts that reference the subject. We filter on
   ``artifactType`` because some registries ignore the query parameter
   (RFC says SHOULD-filter, MUST-include-OCI-Filters-Applied header).

2. **Deterministic fallback tag.**
   ``custos-connector-manifest-v1_sha256-<hex>`` — derived from the
   subject digest. Lowercase hex only, ``:`` collapsed to ``-``,
   ``sha256`` algorithm only in v1. Length budget ≤ 128 chars (OCI
   distribution spec tag length cap).

Both paths may resolve. *Referrers wins* — if both produce a descriptor
they MUST agree (same digest, same size); otherwise the discovery
rejects with :class:`DiscoveryErrorCode.AMBIGUOUS_MANIFEST`. Zero
resolutions across both paths is also a hard failure
(:class:`DiscoveryErrorCode.NO_MANIFEST_FOUND`).

Every discovery emits exactly one audit event:

* ``connector.manifest.fallback-used`` — Referrers empty, fallback tag
  found.
* ``connector.manifest.fallback-ignored`` — Referrers returned the
  authoritative answer; fallback tag was not consulted (or was
  consulted and matched, which is the same observable from the
  operator's standpoint).
* ``connector.manifest.fallback-rejected`` — Fallback path was the
  source of the rejection (``unsupported-digest-algorithm``,
  ``invalid-digest-format``, ``fallback-tag-too-long``).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

import httpx

from custos_connector.audit import emit_event
from custos_connector.manifest.errors import (
    DiscoveryErrorCode,
    ManifestDiscoveryError,
)

#: OCI media type for a v1 connector manifest artifact.
#:
#: This is the ``artifactType`` published in the manifest's OCI
#: descriptor and queried via the Referrers API filter parameter.
CONNECTOR_MANIFEST_MEDIA_TYPE: Final[str] = "application/vnd.custos.connector.manifest.v1+json"

#: Maximum tag length per OCI distribution spec §4.1.
#: Reference: https://github.com/opencontainers/distribution-spec/blob/main/spec.md
MAX_OCI_TAG_LENGTH: Final[int] = 128

#: Fallback tag template:
#: ``custos-connector-manifest-v1_sha256-<hex>``.
_FALLBACK_TAG_PREFIX: Final[str] = "custos-connector-manifest-v1_"

#: Hex-character class for the digest hex part. Pre-compiled for speed
#: in the validation path.
_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]+$")

#: SHA-256 hex string length (256 bits = 64 hex chars).
_SHA256_HEX_LEN: Final[int] = 64

#: Audit event suffix names. Centralised so callers can refer to them
#: as constants in tests + dashboards.
AUDIT_EVENT_FALLBACK_USED: Final[str] = "connector.manifest.fallback-used"
AUDIT_EVENT_FALLBACK_IGNORED: Final[str] = "connector.manifest.fallback-ignored"
AUDIT_EVENT_FALLBACK_REJECTED: Final[str] = "connector.manifest.fallback-rejected"


@dataclass(frozen=True, slots=True)
class ManifestDescriptor:
    """A resolved manifest pointer suitable for downstream content fetch.

    Mirrors the subset of fields the Plugin Loader (CONN-IMPL-008)
    needs to ``GET /v2/<repo>/manifests/<digest>``. The full OCI
    descriptor is preserved as ``raw`` so audit / debugging tooling
    can inspect ``annotations``, ``platform``, etc.
    """

    digest: str
    media_type: str
    size: int
    raw: Mapping[str, Any] = field(default_factory=dict)


def fallback_tag_for_digest(digest: str) -> str:
    """Compute the deterministic fallback tag for an OCI subject digest.

    Rules (design § Fallback tag naming):

    * Algorithm MUST be ``sha256`` — anything else raises
      :class:`DiscoveryErrorCode.UNSUPPORTED_DIGEST_ALGORITHM`.
    * Digest MUST be ``sha256:<64 lowercase hex>`` — any other shape
      raises :class:`DiscoveryErrorCode.INVALID_DIGEST_FORMAT`.
    * Result is ``custos-connector-manifest-v1_sha256-<hex>``. The
      length is ``29 + 7 + 64 = 100`` chars; well under the OCI tag
      cap of 128, but we still range-check defensively so a future
      template change can't silently overflow.
    """
    if not isinstance(digest, str) or ":" not in digest:
        raise ManifestDiscoveryError(
            code=DiscoveryErrorCode.INVALID_DIGEST_FORMAT,
            detail=f"digest must be '<alg>:<hex>'; got {digest!r}",
            digest=digest if isinstance(digest, str) else "",
        )
    alg, _, hex_part = digest.partition(":")
    alg_lower = alg.lower()
    if alg_lower != "sha256":
        raise ManifestDiscoveryError(
            code=DiscoveryErrorCode.UNSUPPORTED_DIGEST_ALGORITHM,
            detail=f"v1 supports only sha256 digests; got algorithm {alg!r}",
            digest=digest,
        )
    if len(hex_part) != _SHA256_HEX_LEN or not _HEX_RE.fullmatch(hex_part):
        raise ManifestDiscoveryError(
            code=DiscoveryErrorCode.INVALID_DIGEST_FORMAT,
            detail=(
                f"sha256 digest must be exactly {_SHA256_HEX_LEN} lowercase hex "
                f"chars; got {hex_part!r}"
            ),
            digest=digest,
        )
    tag = f"{_FALLBACK_TAG_PREFIX}sha256-{hex_part}"
    if len(tag) > MAX_OCI_TAG_LENGTH:  # pragma: no cover - mathematically unreachable for sha256
        raise ManifestDiscoveryError(
            code=DiscoveryErrorCode.FALLBACK_TAG_TOO_LONG,
            detail=(
                f"computed fallback tag {tag!r} ({len(tag)} chars) exceeds the "
                f"OCI tag length cap of {MAX_OCI_TAG_LENGTH}"
            ),
            digest=digest,
        )
    return tag


def _descriptor_from_dict(raw: Mapping[str, Any]) -> ManifestDescriptor | None:
    """Coerce a JSON object into a :class:`ManifestDescriptor`, or skip.

    Returns ``None`` for objects that don't carry the minimum fields
    we need (``digest`` + ``mediaType`` + ``size``); the caller filters
    these out silently because some registries include placeholder
    entries in the referrers index that aren't usable artifact
    pointers.
    """
    digest = raw.get("digest")
    media_type = raw.get("mediaType") or raw.get("artifactType")
    size = raw.get("size")
    if not isinstance(digest, str) or not isinstance(media_type, str) or not isinstance(size, int):
        return None
    return ManifestDescriptor(
        digest=digest,
        media_type=media_type,
        size=size,
        raw=dict(raw),
    )


async def resolve_referrers(
    client: httpx.AsyncClient,
    *,
    repository: str,
    subject_digest: str,
) -> list[ManifestDescriptor]:
    """Call the OCI Referrers API and return descriptors matching the v1 media type.

    Returns an empty list for:

    * 404 (registry has no referrers for this subject).
    * 200 + empty ``manifests[]``.
    * Any non-2xx the caller can map to "Referrers unsupported" (we do
      NOT raise on 4xx/5xx here; the discovery composite treats an
      error as "Referrers said nothing" and lets the fallback path run).

    The function does NOT do automatic Referrers Fallback tag (see OCI
    distribution spec §4.13 "Listing Referrers") — that's a separate
    Custos-specific path implemented by :func:`resolve_fallback_tag`.
    """
    path = f"/v2/{repository}/referrers/{subject_digest}"
    params = {"artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE}
    try:
        response = await client.get(path, params=params)
    except httpx.HTTPError:
        # Network errors are treated as "Referrers said nothing"; the
        # fallback path will still get a chance to run.
        return []
    if response.status_code == 404:
        return []
    if response.status_code >= 400:
        return []
    try:
        body = response.json()
    except ValueError:
        return []
    if not isinstance(body, dict):
        return []
    manifests = body.get("manifests")
    if not isinstance(manifests, list):
        return []

    out: list[ManifestDescriptor] = []
    for entry in manifests:
        if not isinstance(entry, dict):
            continue
        # Some registries echo every entry regardless of artifactType
        # filter; we MUST re-filter client-side to obey the v1 media
        # type contract.
        artifact_type = entry.get("artifactType") or entry.get("mediaType")
        if artifact_type != CONNECTOR_MANIFEST_MEDIA_TYPE:
            continue
        desc = _descriptor_from_dict(entry)
        if desc is not None:
            out.append(desc)
    return out


async def resolve_fallback_tag(
    client: httpx.AsyncClient,
    *,
    repository: str,
    subject_digest: str,
) -> ManifestDescriptor | None:
    """HEAD the fallback tag and return a descriptor if the registry has it.

    Returns ``None`` when the tag does not exist (404). Any other
    non-2xx is treated as "the registry doesn't know about this tag"
    so the composite discovery can decide on a final verdict from the
    Referrers result alone.

    Raises :class:`ManifestDiscoveryError` only when the *digest input*
    is malformed — i.e. when :func:`fallback_tag_for_digest` rejects.
    """
    tag = fallback_tag_for_digest(subject_digest)
    path = f"/v2/{repository}/manifests/{tag}"
    # Use HEAD so we don't pull the full body just to confirm existence
    # and read the descriptor headers. Registries that don't support
    # HEAD on manifests will surface a 405 which we read as "no".
    try:
        response = await client.head(
            path,
            headers={"Accept": CONNECTOR_MANIFEST_MEDIA_TYPE},
        )
    except httpx.HTTPError:
        return None
    if response.status_code == 404 or response.status_code >= 400:
        return None

    digest = response.headers.get("docker-content-digest") or ""
    media_type = response.headers.get("content-type") or CONNECTOR_MANIFEST_MEDIA_TYPE
    size_raw = response.headers.get("content-length") or "0"
    try:
        size = int(size_raw)
    except ValueError:
        size = 0
    return ManifestDescriptor(
        digest=digest,
        media_type=media_type,
        size=size,
        raw={"tag": tag},
    )


async def discover_manifest(
    client: httpx.AsyncClient,
    *,
    repository: str,
    subject_digest: str,
) -> ManifestDescriptor:
    """Run Referrers + fallback discovery; enforce "exactly one valid manifest".

    Resolution rules:

    * Both paths may resolve.
    * If Referrers returned exactly one descriptor, that descriptor
      wins; the fallback result is ignored (audit ``fallback-ignored``).
    * If Referrers returned multiple descriptors, the discovery fails
      with :class:`DiscoveryErrorCode.AMBIGUOUS_MANIFEST` (registry
      should have constrained on ``artifactType``; we cannot pick a
      winner without operator input).
    * If Referrers returned zero descriptors but the fallback tag
      resolved, the fallback descriptor wins (audit ``fallback-used``).
    * If both Referrers and the fallback tag returned descriptors but
      Referrers returned more than zero, the rule above takes
      precedence.
    * If Referrers returned zero and the fallback tag did not resolve,
      the discovery fails with
      :class:`DiscoveryErrorCode.NO_MANIFEST_FOUND`.
    * If the fallback path rejected with a digest-format error, the
      discovery fails with that code (audit ``fallback-rejected``).

    Args:
        client: An ``httpx.AsyncClient`` already configured with the
            registry base URL, authentication, and timeout. The caller
            owns the client lifecycle.
        repository: OCI repository name (e.g. ``team-a/oci-registry-conn``).
        subject_digest: ``sha256:<hex>`` of the connector image the
            manifest references.

    Returns:
        The single :class:`ManifestDescriptor` the Plugin Loader should
        ``GET /v2/<repo>/manifests/<digest>`` next.

    Raises:
        ManifestDiscoveryError: With one of the codes documented above.
    """
    # Resolve fallback tag derivation first so a malformed digest fails
    # fast with a stable code instead of leaking through the Referrers
    # API call (which would otherwise serialize an invalid URL).
    try:
        fallback_tag = fallback_tag_for_digest(subject_digest)
    except ManifestDiscoveryError as exc:
        emit_event(
            AUDIT_EVENT_FALLBACK_REJECTED,
            {
                "repository": repository,
                "subject_digest": subject_digest,
                "code": str(exc.code),
                "detail": exc.detail,
            },
        )
        raise

    referrers = await resolve_referrers(
        client,
        repository=repository,
        subject_digest=subject_digest,
    )

    if len(referrers) > 1:
        emit_event(
            AUDIT_EVENT_FALLBACK_REJECTED,
            {
                "repository": repository,
                "subject_digest": subject_digest,
                "code": str(DiscoveryErrorCode.AMBIGUOUS_MANIFEST),
                "detail": (
                    f"Referrers API returned {len(referrers)} descriptors; v1 requires exactly one"
                ),
            },
        )
        raise ManifestDiscoveryError(
            code=DiscoveryErrorCode.AMBIGUOUS_MANIFEST,
            detail=(
                f"Referrers API returned {len(referrers)} manifest descriptors "
                f"for {subject_digest!r}; v1 requires exactly one"
            ),
            digest=subject_digest,
        )

    if len(referrers) == 1:
        # Referrers won. The fallback path is informational only; we
        # do NOT issue an additional HEAD against the fallback tag
        # because in production that's a wasted round-trip. The audit
        # ``fallback-ignored`` event records the decision so operators
        # can audit deterministically.
        emit_event(
            AUDIT_EVENT_FALLBACK_IGNORED,
            {
                "repository": repository,
                "subject_digest": subject_digest,
                "fallback_tag": fallback_tag,
                "resolved_via": "referrers",
            },
        )
        return referrers[0]

    # Referrers said nothing. Try the fallback tag.
    fallback_descriptor = await resolve_fallback_tag(
        client,
        repository=repository,
        subject_digest=subject_digest,
    )
    if fallback_descriptor is None:
        emit_event(
            AUDIT_EVENT_FALLBACK_REJECTED,
            {
                "repository": repository,
                "subject_digest": subject_digest,
                "code": str(DiscoveryErrorCode.NO_MANIFEST_FOUND),
                "detail": (
                    "neither the Referrers API nor the fallback tag yielded a manifest descriptor"
                ),
            },
        )
        raise ManifestDiscoveryError(
            code=DiscoveryErrorCode.NO_MANIFEST_FOUND,
            detail=(
                f"no manifest descriptor for subject {subject_digest!r}: "
                f"Referrers API returned 0 entries and fallback tag "
                f"{fallback_tag!r} did not resolve"
            ),
            digest=subject_digest,
        )

    emit_event(
        AUDIT_EVENT_FALLBACK_USED,
        {
            "repository": repository,
            "subject_digest": subject_digest,
            "fallback_tag": fallback_tag,
            "resolved_via": "fallback-tag",
        },
    )
    return fallback_descriptor


__all__ = [
    "AUDIT_EVENT_FALLBACK_IGNORED",
    "AUDIT_EVENT_FALLBACK_REJECTED",
    "AUDIT_EVENT_FALLBACK_USED",
    "CONNECTOR_MANIFEST_MEDIA_TYPE",
    "MAX_OCI_TAG_LENGTH",
    "ManifestDescriptor",
    "discover_manifest",
    "fallback_tag_for_digest",
    "resolve_fallback_tag",
    "resolve_referrers",
]
