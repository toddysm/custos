"""Parse and canonicalize Activity Manifest v1 documents."""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from custos_arm.manifest.models import ActivityManifest


class ManifestError(ValueError):
    """Raised when a manifest document is malformed or invalid."""


def parse_manifest(data: str | bytes | dict[str, Any]) -> ActivityManifest:
    """Parse a manifest from JSON text/bytes or an already-decoded mapping.

    Raises :class:`ManifestError` when the payload is not valid JSON or does
    not satisfy the Activity Manifest v1 schema.
    """
    if isinstance(data, (str, bytes)):
        try:
            decoded = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
    else:
        decoded = data

    if not isinstance(decoded, dict):
        raise ManifestError("manifest must be a JSON object")

    try:
        return ActivityManifest.model_validate(decoded)
    except ValidationError as exc:
        raise ManifestError(f"manifest failed validation: {exc}") from exc


def to_canonical_json(manifest: ActivityManifest) -> str:
    """Serialize a manifest to its canonical JSON form.

    Canonical form uses the wire aliases, omits fields whose value is
    ``None``, sorts object keys, and uses compact separators so two
    semantically-equal manifests produce byte-identical output (suitable for
    hashing).
    """
    payload = manifest.model_dump(by_alias=True, exclude_none=True)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
