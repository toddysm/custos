"""`InstanceConfigValidator` — create-time gate for connector instances.

CONN-IMPL-012 (#295) is the second of two validation gates in the
connector pipeline:

1. **Manifest validator** (CONN-IMPL-005, see
   :mod:`custos_connector.manifest.validator`): runs at publish-time
   and gates what can be registered as a ``ConnectorTypeVersion``.
2. **Instance config validator** (this module): runs at create-time
   and gates what the operator can pass as ``target_config``,
   ``credentials_authentication``, and ``used_capabilities`` when
   binding an instance to a specific ``(type, version)`` row.

Validation rules
----------------

For each create call, we resolve the referenced ``ConnectorTypeVersion``
manifest and check the operator-supplied payload against three rule
families:

* **Per-kind ``target_config``**. The manifest's ``target.kind``
  picks a per-kind required-field tuple (mirrors the publish-time
  table in :mod:`custos_connector.manifest.validator`). The validator
  merges ``manifest.target.config`` (publisher-supplied defaults)
  with the operator's ``target_config`` (deploy-time overrides) and
  checks every required field is present in the merge. Missing keys
  emit a :attr:`InstanceConfigCode.MISSING_TARGET_CONFIG_FIELD`
  failure each — we collect every missing field rather than
  short-circuit on the first so the operator can fix all of them in
  one round-trip.
* **Per-auth ``credentials_authentication``**. The manifest's
  ``credentials.authenticationType`` picks a per-auth required-field
  tuple. Same merge-then-check pattern as target config. Vendor
  ``x-<vendor>`` auth types are accepted with no additional field
  checks: the plugin author owns the contract for their extension
  and the platform has no way to know which keys are required.
* **Capability availability**. Every token in
  ``used_capabilities`` MUST appear in ``manifest.capabilities``.
  This is the inverse of the manifest's "advertised superset" — the
  operator pins which subset they actually intend to grant.
  Unknown tokens emit
  :attr:`InstanceConfigCode.UNKNOWN_CAPABILITY_ON_INSTANCE` failures.

Failure-aggregation policy
--------------------------

The validator collects **all** issues into a single
:class:`InstanceConfigValidationError` rather than short-circuiting.
This matches the manifest-validator UX precedent (CONN-IMPL-005) and
lets the API surface a single 400 response carrying every problem.

Failure modes that are NOT in scope here:

* ``lease.ttl ≤ credentials.maxLeaseTtl``. The v1 connector-manifest
  schema does not yet carry ``credentials.maxLeaseTtl`` — the only
  TTL bound today is the platform-wide ceiling enforced in
  :class:`custos_connector.instances.service.InstanceService`.
  Manifest-derived per-type ceilings land with CONN-IMPL-013.
* Capability *governance* (Tier 1 namespace, vendor extension shape):
  already enforced at publish-time, so by the time we reach instance
  create the manifest's capability list is guaranteed well-formed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final


class InstanceConfigCode(StrEnum):
    """Stable rejection codes emitted by the instance config validator.

    Codes are part of the connector REST contract; renaming any of
    these is a breaking change.
    """

    #: An operator-supplied ``target_config`` is missing a required
    #: field for the manifest's ``target.kind`` (e.g.
    #: ``repositoryNamespace`` for ``oci-registry``). One issue is
    #: emitted per missing key; the :attr:`InstanceConfigIssue.path`
    #: identifies the offending key.
    MISSING_TARGET_CONFIG_FIELD = "missing-target-config-field"

    #: The manifest declares a ``target.kind`` outside the known
    #: v1 set (``oci-registry``, ``azure-blob-storage``,
    #: ``amazon-s3-bucket``). Indicates manifest/validator drift —
    #: a manifest that passed CONN-IMPL-005 should never trip this.
    UNKNOWN_TARGET_KIND = "unknown-target-kind"

    #: An operator-supplied ``credentials_authentication`` is missing
    #: a required field for the manifest's
    #: ``credentials.authenticationType``. Vendor ``x-<vendor>`` auth
    #: types don't trigger this code (no platform-side field table).
    MISSING_AUTHENTICATION_FIELD = "missing-authentication-field"

    #: A token in ``used_capabilities`` is not present in the
    #: manifest's ``capabilities`` array. The operator can only pin
    #: a subset of what the connector type advertises.
    UNKNOWN_CAPABILITY_ON_INSTANCE = "unknown-capability-on-instance"


#: Per-kind required fields for the merged ``target_config``. Mirrors
#: :data:`custos_connector.manifest.validator._TARGET_CONFIG_REQUIRED`
#: — duplicated here on purpose so the two validators evolve
#: independently (the publish-time rule is "manifest provides all
#: required fields" while the deploy-time rule is "manifest + operator
#: overrides provide all required fields").
_TARGET_CONFIG_REQUIRED: Final[dict[str, tuple[str, ...]]] = {
    "oci-registry": ("repositoryNamespace",),
    "azure-blob-storage": ("storageAccount", "container"),
    "amazon-s3-bucket": ("bucket", "region"),
}

#: Per-auth required fields for the merged
#: ``credentials_authentication``. The manifest schema itself only
#: requires the bag be a non-empty object; this table is the
#: deploy-time complement that names the keys an operator must
#: supply (potentially via the manifest's defaults) for the platform
#: to wire the auth flow. Vendor ``x-<vendor>`` types skip this table.
_AUTH_REQUIRED: Final[dict[str, tuple[str, ...]]] = {
    "azure-key-vault": ("vaultUri", "secretName"),
    "amazon-kms": ("keyId",),
    # Azure managed identity may be either system-assigned (no extra
    # fields) or user-assigned (``clientId``). We do not require
    # ``clientId`` here so the system-assigned case is admissible
    # with an empty operator override.
    "azure-managed-identity": (),
    "oidc": ("issuerUri", "audience"),
}


@dataclass(frozen=True, slots=True)
class InstanceConfigIssue:
    """One issue surfaced by :class:`InstanceConfigValidator`.

    Attributes:
        code: A :class:`InstanceConfigCode` value (compared as a
            plain string in the wire payload).
        detail: Human-readable explanation. Safe to surface in an
            HTTP 400 body; carries no secrets (operator paths name
            structural keys, not their values).
        path: JSON-Pointer-style ``"/"``-joined path to the offending
            element (e.g. ``"target_config/repositoryNamespace"`` for
            a missing field, ``"used_capabilities/0"`` for the first
            bad capability token).
    """

    code: InstanceConfigCode
    detail: str
    path: str = ""


@dataclass(frozen=True, slots=True)
class InstanceConfigValidationError(Exception):
    """Raised by :func:`validate_instance_config` when issues exist.

    Carries the **complete** list of issues — the validator never
    short-circuits. Callers in :class:`InstanceService` map this
    to a single HTTP 400 response listing every problem.

    Attributes:
        issues: Non-empty tuple of :class:`InstanceConfigIssue`
            entries. The validator only raises when ``len(issues) > 0``.
    """

    issues: tuple[InstanceConfigIssue, ...] = field(default_factory=tuple)

    def __str__(self) -> str:  # pragma: no cover - trivial
        if not self.issues:
            return "instance config validation failed"
        rendered = "; ".join(
            f"[{i.code}] {i.path}: {i.detail}" if i.path else f"[{i.code}] {i.detail}"
            for i in self.issues
        )
        return f"instance config validation failed: {rendered}"


def _is_vendor_auth_type(auth_type: str) -> bool:
    """Return ``True`` if ``auth_type`` is an ``x-<vendor>`` token.

    The publish-time validator already enforced the
    ``^x-[a-z0-9][a-z0-9.-]{1,63}$`` grammar on the manifest, so a
    cheap prefix check is sufficient here.
    """
    return auth_type.startswith("x-")


def validate_instance_config(
    *,
    manifest: Mapping[str, Any],
    target_config: Mapping[str, Any],
    credentials_authentication: Mapping[str, Any],
    used_capabilities: Sequence[str] | None,
) -> None:
    """Validate an operator-supplied instance payload against the
    referenced ``ConnectorTypeVersion`` manifest.

    All issues are collected; the function either returns ``None``
    (no issues) or raises :class:`InstanceConfigValidationError`
    carrying the full list.

    Args:
        manifest: The normalized manifest dict stored on the
            ``ConnectorTypeVersion`` row (see CONN-IMPL-008). Must
            contain ``target.kind``, ``target.config``,
            ``credentials.authenticationType``,
            ``credentials.authentication``, and ``capabilities``
            keys — all guaranteed by CONN-IMPL-005's publish-time
            validation.
        target_config: Operator-supplied target config overrides.
            Merged on top of ``manifest.target.config`` before the
            per-kind required-field check.
        credentials_authentication: Operator-supplied authentication
            field overrides. Merged on top of
            ``manifest.credentials.authentication`` before the
            per-auth required-field check.
        used_capabilities: Operator-pinned capability subset. ``None``
            means "leave the catalog superset in place" and skips the
            availability check entirely. An empty tuple is a valid
            pin (operator advertises no capabilities) and still
            passes.

    Raises:
        InstanceConfigValidationError: When any issue was collected.
    """
    issues: list[InstanceConfigIssue] = []

    target_block = manifest.get("target", {})
    kind = target_block.get("kind") if isinstance(target_block, Mapping) else None
    manifest_target_config: Mapping[str, Any] = (
        target_block.get("config", {})
        if isinstance(target_block, Mapping) and isinstance(target_block.get("config"), Mapping)
        else {}
    )
    if not isinstance(kind, str) or kind not in _TARGET_CONFIG_REQUIRED:
        issues.append(
            InstanceConfigIssue(
                code=InstanceConfigCode.UNKNOWN_TARGET_KIND,
                detail=(
                    f"manifest target.kind={kind!r} is not a known v1 kind; "
                    "expected one of: " + ", ".join(sorted(_TARGET_CONFIG_REQUIRED))
                ),
                path="target/kind",
            )
        )
    else:
        merged_target = {**dict(manifest_target_config), **dict(target_config)}
        for required_key in _TARGET_CONFIG_REQUIRED[kind]:
            value = merged_target.get(required_key)
            # Treat ``None`` and empty string as missing — both shapes
            # surface as "operator forgot to provide a value".
            if value is None or (isinstance(value, str) and not value.strip()):
                issues.append(
                    InstanceConfigIssue(
                        code=InstanceConfigCode.MISSING_TARGET_CONFIG_FIELD,
                        detail=(
                            f"target.kind={kind!r} requires "
                            f"target_config.{required_key!r} to be provided"
                        ),
                        path=f"target_config/{required_key}",
                    )
                )

    credentials_block = manifest.get("credentials", {})
    auth_type = (
        credentials_block.get("authenticationType")
        if isinstance(credentials_block, Mapping)
        else None
    )
    manifest_auth: Mapping[str, Any] = (
        credentials_block.get("authentication", {})
        if isinstance(credentials_block, Mapping)
        and isinstance(credentials_block.get("authentication"), Mapping)
        else {}
    )
    if isinstance(auth_type, str) and not _is_vendor_auth_type(auth_type):
        required = _AUTH_REQUIRED.get(auth_type, ())
        merged_auth = {**dict(manifest_auth), **dict(credentials_authentication)}
        for required_key in required:
            value = merged_auth.get(required_key)
            if value is None or (isinstance(value, str) and not value.strip()):
                issues.append(
                    InstanceConfigIssue(
                        code=InstanceConfigCode.MISSING_AUTHENTICATION_FIELD,
                        detail=(
                            f"credentials.authenticationType={auth_type!r} requires "
                            f"credentials_authentication.{required_key!r} to be provided"
                        ),
                        path=f"credentials_authentication/{required_key}",
                    )
                )

    if used_capabilities is not None:
        spec_block = manifest.get("spec", {}) if isinstance(manifest, Mapping) else {}
        advertised_raw = (
            spec_block.get("capabilities", ())
            if isinstance(spec_block, Mapping)
            else ()
        )
        advertised: frozenset[str] = (
            frozenset(
                name
                for capability in advertised_raw
                for name in (extract_capability_name(capability),)
                if isinstance(name, str)
            )
            if isinstance(advertised_raw, (list, tuple))
            else frozenset()
        )
        for index, token in enumerate(used_capabilities):
            if token not in advertised:
                issues.append(
                    InstanceConfigIssue(
                        code=InstanceConfigCode.UNKNOWN_CAPABILITY_ON_INSTANCE,
                        detail=(
                            f"capability {token!r} is not advertised by "
                            "the referenced connector type version "
                            f"(advertised: {sorted(advertised)!r})"
                        ),
                        path=f"used_capabilities/{index}",
                    )
                )

    if issues:
        raise InstanceConfigValidationError(issues=tuple(issues))


__all__ = [
    "InstanceConfigCode",
    "InstanceConfigIssue",
    "InstanceConfigValidationError",
    "validate_instance_config",
]
