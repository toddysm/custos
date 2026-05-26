"""ConnectorManifest v1 strict validator (CONN-IMPL-005, #288).

Pipeline (Connector Service publish-time gate, also reachable from
Catalog Service ``connector-types`` register per CONN-IMPL-034):

    raw payload  ->  validate_manifest()  ->  normalize_manifest()
                                          ->  put_connector_type_version(digest=...)

The validator runs the JSON Schema at
``design/components/connector-service/schemas/connector-manifest.v1.schema.json``
(packaged with the wheel under ``custos_connector/manifest/_schemas/``)
and adds three layers of post-schema enforcement that the schema alone
can't express with stable error codes:

1. **Per-kind ``target.config`` field shape.** The schema uses
   ``allOf`` + ``if/then`` branches; that yields ``oneOf``-style errors
   that don't map cleanly to operator-actionable codes. We re-check
   the per-kind required fields here.
2. **Authentication type vendor extension.** ``credentials.authenticationType``
   accepts a closed enum **or** an ``x-<vendor>`` extension token; the
   ``anyOf`` rejection message is opaque, so we re-check explicitly.
3. **Capability token namespace separation.** Tokens in
   ``spec.capabilities`` MUST NOT use the reserved ``event.*`` prefix
   (event-delivery verbs belong in ``spec.events``). The schema's
   ``not`` keyword surfaces a misleading error message; we re-check.

The validator never raises bare :class:`jsonschema.ValidationError` —
every rejection becomes a :class:`ManifestValidationError` carrying a
stable :class:`ValidationErrorCode`.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from importlib import resources
from typing import Any, Final

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from custos_connector.manifest.capabilities import (
    classify_capability_token,
    extract_capability_name,
)
from custos_connector.manifest.errors import (
    ManifestValidationError,
    ValidationErrorCode,
)

#: Packaged resource path for the v1 schema. Kept in sync with the
#: design source-of-truth via ``tests/test_manifest_schema_drift.py``.
_SCHEMA_RESOURCE: Final[str] = "connector-manifest.v1.schema.json"


def _load_schema() -> dict[str, Any]:
    """Load and return the v1 JSON Schema as a plain dict."""
    pkg = resources.files("custos_connector.manifest._schemas")
    raw = (pkg / _SCHEMA_RESOURCE).read_text(encoding="utf-8")
    parsed: Any = json.loads(raw)
    if not isinstance(parsed, dict):  # pragma: no cover - defensive
        # A malformed packaged schema would brick the service; this is
        # guarded only so the failure mode is a clean RuntimeError
        # instead of a misleading TypeError deeper in jsonschema.
        raise RuntimeError(f"packaged schema {_SCHEMA_RESOURCE} is not a JSON object")
    return parsed


CONNECTOR_MANIFEST_V1_SCHEMA: Final[dict[str, Any]] = _load_schema()

_VALIDATOR: Final[Draft202012Validator] = Draft202012Validator(CONNECTOR_MANIFEST_V1_SCHEMA)

#: SemVer 2.0 regex (mirrors the schema's ``metadata.version`` pattern).
_SEMVER_RE: Final[re.Pattern[str]] = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)

#: Dot-delimited lowercase token (capabilities + events) — at least one dot.
_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]*(?:\.[a-z0-9][a-z0-9-]*)+$")

#: ``x-<vendor>`` authentication-type extension pattern.
_VENDOR_AUTH_RE: Final[re.Pattern[str]] = re.compile(r"^x-[a-z0-9][a-z0-9.-]{1,63}$")

#: Closed set of v1 authentication-type tokens (schema-anchored).
_KNOWN_AUTH_TYPES: Final[frozenset[str]] = frozenset(
    {
        "azure-key-vault",
        "amazon-kms",
        "azure-managed-identity",
        "oidc",
    }
)

#: Per-kind required ``target.config`` fields. Order is significant for
#: the "first missing field" error message; matches the schema's required
#: array order.
_TARGET_CONFIG_REQUIRED: Final[dict[str, tuple[str, ...]]] = {
    "oci-registry": ("repositoryNamespace",),
    "azure-blob-storage": ("storageAccount", "container"),
    "amazon-s3-bucket": ("bucket", "region"),
}

#: Valid ``events.delivery`` modes.
_VALID_DELIVERY: Final[frozenset[str]] = frozenset({"push", "pull"})


def _format_path(path: tuple[Any, ...]) -> str:
    """Render a ``jsonschema`` ``absolute_path`` as a JSON-Pointer string."""
    parts: list[str] = []
    for segment in path:
        seg = str(segment)
        seg = seg.replace("~", "~0").replace("/", "~1")
        parts.append(seg)
    return "/" + "/".join(parts) if parts else ""


def _classify_schema_error(err: ValidationError) -> ValidationErrorCode:
    """Map a generic jsonschema error to a stable validator code.

    For most schema violations we surface :attr:`ValidationErrorCode.SCHEMA_VIOLATION`;
    a few well-known violations get more specific codes so operators
    don't need to read JSON-Pointer paths to triage them.
    """
    path = list(err.absolute_path)
    # metadata.contractVersion const violation -> stable code.
    if path == ["metadata", "contractVersion"] and err.validator == "const":
        return ValidationErrorCode.UNSUPPORTED_CONTRACT_VERSION
    # metadata.version pattern (SemVer) violation -> stable code.
    if path == ["metadata", "version"] and err.validator == "pattern":
        return ValidationErrorCode.INVALID_SEMVER
    return ValidationErrorCode.SCHEMA_VIOLATION


def _check_target_config(manifest: Mapping[str, Any]) -> None:
    """Enforce per-kind ``target.config`` shape with a stable error code.

    All ``isinstance(...)`` guards in this helper are defensive: by the
    time we reach this function the JSON Schema pass in
    :func:`validate_manifest` has already rejected any payload whose
    ``spec`` / ``target`` / ``config`` is not the right shape. The
    guards exist so a direct caller (or a future refactor that
    re-orders the post-checks) gets a clean no-op instead of an
    AttributeError. The branches are excluded from coverage.
    """
    spec = manifest.get("spec")
    if not isinstance(spec, dict):  # pragma: no cover - defensive
        return
    target = spec.get("target")
    if not isinstance(target, dict):  # pragma: no cover - defensive
        return
    kind = target.get("kind")
    if (
        not isinstance(kind, str) or kind not in _TARGET_CONFIG_REQUIRED
    ):  # pragma: no cover - defensive
        return
    config = target.get("config")
    if not isinstance(config, dict):  # pragma: no cover - defensive
        # The schema requires config to be an object; a non-object is
        # already a SCHEMA_VIOLATION caught above.
        return
    required = _TARGET_CONFIG_REQUIRED[kind]
    for field_name in required:
        if field_name not in config or config[field_name] in (None, ""):
            raise ManifestValidationError(
                code=ValidationErrorCode.MISSING_TARGET_CONFIG_FIELD,
                detail=(
                    f"target.kind={kind!r} requires config.{field_name} to be set; "
                    f"got {config.get(field_name)!r}"
                ),
                path=f"/spec/target/config/{field_name}",
            )


def _check_authentication_type(manifest: Mapping[str, Any]) -> None:
    """Reject ``credentials.authenticationType`` values outside the v1 set.

    Accepts either the closed enum or an ``x-<vendor>`` extension token.
    Defensive ``isinstance`` guards (same rationale as
    :func:`_check_target_config`) are excluded from coverage.
    """
    spec = manifest.get("spec")
    if not isinstance(spec, dict):  # pragma: no cover - defensive
        return
    credentials = spec.get("credentials")
    if not isinstance(credentials, dict):  # pragma: no cover - defensive
        return
    auth_type = credentials.get("authenticationType")
    if not isinstance(auth_type, str):  # pragma: no cover - defensive
        # Already a schema violation; nothing more to add.
        return
    if auth_type in _KNOWN_AUTH_TYPES:
        return
    if _VENDOR_AUTH_RE.fullmatch(auth_type):
        return
    raise ManifestValidationError(
        code=ValidationErrorCode.UNKNOWN_AUTHENTICATION_TYPE,
        detail=(
            f"credentials.authenticationType={auth_type!r} is neither a known v1 "
            f"value ({sorted(_KNOWN_AUTH_TYPES)}) nor a valid x-<vendor> token"
        ),
        path="/spec/credentials/authenticationType",
    )


def _check_capability_tokens(manifest: Mapping[str, Any]) -> None:
    """Enforce capability namespace governance with stable error codes.

    Per design § Namespace governance (CONN-IMPL-009):

    * ``event.*`` tokens MUST NOT appear in ``capabilities`` (their
      verbs belong in ``spec.events``) → :attr:`EVENT_TOKEN_IN_CAPABILITIES`.
    * Tokens whose first dot-segment is a reserved Tier 1 prefix
      (``oci``, ``s3``, ``blob``, ``http``, ``sql``, ``notification``)
      MUST be in the curated Tier 1 registry →
      :attr:`UNKNOWN_CORE_CAPABILITY`.
    * Tokens outside the Tier 1 registry MUST match the Tier 2 vendor
      pattern ``x-<vendor>.<verb>`` → :attr:`INVALID_CAPABILITY_SYNTAX`.
    * Duplicate names (any combination of string and object form) →
      :attr:`SCHEMA_VIOLATION` with a duplicate-name detail (the
      schema's ``uniqueItems`` works on deep equality and so cannot
      catch a mixed-form duplicate).

    Defensive ``isinstance`` guards excluded from coverage; see
    :func:`_check_target_config`.
    """
    spec = manifest.get("spec")
    if not isinstance(spec, dict):  # pragma: no cover - defensive
        return
    capabilities = spec.get("capabilities")
    if not isinstance(capabilities, list):  # pragma: no cover - defensive
        return

    seen_names: set[str] = set()
    for idx, cap in enumerate(capabilities):
        # Extract the canonical name from string or object form. The
        # schema already enforced the union; defensive guard is in
        # extract_capability_name itself.
        name = extract_capability_name(cap)
        path = f"/spec/capabilities/{idx}"

        if name in seen_names:
            raise ManifestValidationError(
                code=ValidationErrorCode.SCHEMA_VIOLATION,
                detail=(
                    f"capabilities[{idx}] duplicates capability {name!r}; "
                    f"each capability token may appear at most once "
                    f"(across string and object entry forms)"
                ),
                path=path,
            )
        seen_names.add(name)

        # The schema's ``not: ^event\.`` catches event.* before we get
        # here, but we re-check explicitly so direct callers of
        # _check_capability_tokens (and schema-relaxation scenarios)
        # still emit a stable EVENT_TOKEN_IN_CAPABILITIES code.
        if name.startswith("event.") or name == "event":
            raise ManifestValidationError(
                code=ValidationErrorCode.EVENT_TOKEN_IN_CAPABILITIES,
                detail=(
                    f"capabilities[{idx}]={name!r} uses the reserved 'event.*' "
                    f"namespace; event-delivery verbs belong in spec.events"
                ),
                path=path,
            )

        try:
            classify_capability_token(name)
        except ManifestValidationError as err:
            # Re-raise with the per-index path attached so operators
            # can pinpoint the offending entry.
            raise ManifestValidationError(
                code=err.code,
                detail=err.detail,
                path=path,
            ) from None


def _check_events(manifest: Mapping[str, Any]) -> None:
    """Enforce ``events`` invariants with stable error codes.

    The schema already requires ``events.delivery`` and ``events.produced``
    when ``events`` is present; we add stable codes for the
    delivery-value and empty-produced cases that operators hit most.
    Defensive ``isinstance`` guards excluded from coverage; see
    :func:`_check_target_config`.
    """
    spec = manifest.get("spec")
    if not isinstance(spec, dict):  # pragma: no cover - defensive
        return
    events = spec.get("events")
    if not isinstance(events, dict):
        return

    delivery = events.get("delivery")
    if isinstance(delivery, list):
        for idx, mode in enumerate(delivery):
            if not isinstance(mode, str) or mode not in _VALID_DELIVERY:
                raise ManifestValidationError(
                    code=ValidationErrorCode.INVALID_EVENT_DELIVERY,
                    detail=(
                        f"events.delivery[{idx}]={mode!r} is not one of {sorted(_VALID_DELIVERY)}"
                    ),
                    path=f"/spec/events/delivery/{idx}",
                )

    produced = events.get("produced")
    if produced is None or (isinstance(produced, list) and len(produced) == 0):
        raise ManifestValidationError(
            code=ValidationErrorCode.EMPTY_EVENT_PRODUCED,
            detail=(
                "spec.events.produced must list at least one normalized event "
                "type when spec.events is present"
            ),
            path="/spec/events/produced",
        )
    if isinstance(produced, list):
        for idx, evt in enumerate(produced):
            if not isinstance(evt, str) or not _TOKEN_RE.fullmatch(
                evt
            ):  # pragma: no cover - defensive
                raise ManifestValidationError(
                    code=ValidationErrorCode.INVALID_TOKEN_SYNTAX,
                    detail=(
                        f"events.produced[{idx}]={evt!r} is not a valid "
                        f"dot-delimited lowercase token"
                    ),
                    path=f"/spec/events/produced/{idx}",
                )


def _check_semver(manifest: Mapping[str, Any]) -> None:
    """Reject ``metadata.version`` strings that don't match SemVer 2.0.

    Duplicates the schema's pattern check but with a stable error code
    and a more operator-actionable message. Runs even if the schema
    pass was successful (the schema regex and our regex are identical;
    this is a defensive belt-and-suspenders check, with the same
    ``isinstance`` guard rationale as :func:`_check_target_config`).
    """
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict):  # pragma: no cover - defensive
        return
    version = metadata.get("version")
    if not isinstance(version, str):  # pragma: no cover - defensive
        return
    if not _SEMVER_RE.fullmatch(version):  # pragma: no cover - schema catches first
        raise ManifestValidationError(
            code=ValidationErrorCode.INVALID_SEMVER,
            detail=f"metadata.version={version!r} is not a SemVer 2.0 string",
            path="/metadata/version",
        )


def validate_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Strict v1 validation pass. Returns the payload (unchanged) on success.

    The return type is a plain ``dict[str, Any]`` (deep-copied) so
    downstream callers can hand it to
    :func:`custos_connector.manifest.normalizer.normalize_manifest`
    without risking mutation of the caller's payload.

    Args:
        payload: The parsed manifest payload (e.g. ``json.loads(bytes)``).

    Returns:
        The validated payload as a fresh ``dict[str, Any]`` (deep-copied
        so the caller's input is never mutated by the normalizer or any
        downstream consumer).

    Raises:
        ManifestValidationError: First hard rejection encountered.
            The schema pass runs first and emits the most general code
            (``SCHEMA_VIOLATION``) for any rejection not classified by
            :func:`_classify_schema_error`. Then the per-kind /
            authentication / capability / events post-checks run and
            may upgrade the code to a more specific one.
    """
    # Schema pass — first error wins. We re-classify the error code so
    # well-known violations get stable codes; everything else is
    # SCHEMA_VIOLATION with the underlying jsonschema message preserved
    # in `detail`.
    for err in _VALIDATOR.iter_errors(payload):
        code = _classify_schema_error(err)
        raise ManifestValidationError(
            code=code,
            detail=err.message,
            path=_format_path(tuple(err.absolute_path)),
        )

    # Post-checks (run in publishing-priority order).
    _check_semver(payload)
    _check_target_config(payload)
    _check_authentication_type(payload)
    _check_capability_tokens(payload)
    _check_events(payload)

    # Deep-copy via JSON round-trip so the caller's mapping is never
    # mutated by downstream normalization. The payload has already
    # passed schema validation so it's guaranteed JSON-serialisable.
    return json.loads(json.dumps(dict(payload)))  # type: ignore[no-any-return]


__all__ = [
    "CONNECTOR_MANIFEST_V1_SCHEMA",
    "validate_manifest",
]
