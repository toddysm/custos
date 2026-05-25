"""OIDC issuer configuration schema (AS-IMPL-020).

Auth Service is configured through ``CUSTOS_AUTH_OIDC_ISSUERS`` — a
JSON document enumerating every external OIDC issuer the platform
trusts. Each issuer entry carries enough information for the generic
verifier (issuer URL, JWKS URI, audiences, algorithm allow-list) and
for the provisioning policy that links ``(issuer, subject)`` to an
internal user.

Schema (closes the design TODO "Specify the OIDC issuer config schema
for ``CUSTOS_AUTH_OIDC_ISSUERS``")::

    {
      "issuers": [
        {
          "id": "github-actions",
          "preset": "github" | "entra" | null,
          "issuer_url": "https://token.actions.githubusercontent.com",
          "jwks_uri": "https://token.actions.githubusercontent.com/.well-known/jwks",
          "audiences": ["custos"],
          "algorithms": ["RS256"],
          "subject_claim": "sub",
          "provisioning_policy": "zero-binding",
          "group_claim": null,
          "group_bindings": [
            {"claim_value": "engineering",
             "role": "workspace.author",
             "workspace_id": "ws-1"}
          ],
          "token_endpoint": "https://github.com/login/oauth/access_token",
          "client_id": "Iv1.xxxxxxxxxxxxxxxx",
          "client_secret_env": "CUSTOS_AUTH_OIDC_GITHUB_CLIENT_SECRET"
        }
      ]
    }

Top-level shape is ``{"issuers": [<issuer>, ...]}``; the wrapper
exists so future top-level config (e.g. global defaults, signing-key
overrides) can be added without breaking the wire format. Empty issuer
list is legal — that is the M1 default deployment shape (OIDC verifier
present but no issuers configured = ``oidc_not_enabled`` from the
callback).

The schema is **closed**: ``parse_issuers_config`` raises
:class:`OidcConfigError` on unknown keys so a misconfigured operator
gets a fail-loud diagnostic at startup instead of a silent default.

Preset handling: when ``preset`` is set, the issuer entry can omit
fields that the preset supplies a default for (e.g. ``issuer_url`` and
``jwks_uri`` for the ``github`` preset). Explicit fields always win
over preset defaults — overrides are how operators pin a specific
issuer URL when the preset's default would route traffic to the wrong
endpoint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import Final, Literal

#: Default JWT signing algorithms accepted by the verifier when an
#: issuer entry does not specify ``algorithms``. ``RS256`` is the de-
#: facto OIDC default (Google, Microsoft, GitHub all use it), so the
#: baseline default remains ``RS256`` unless an issuer explicitly
#: configures a broader allow-list.
DEFAULT_ALGORITHMS: Final[tuple[str, ...]] = ("RS256",)

#: Default subject claim — the standard OIDC ``sub`` claim. Presets
#: that publish a different stable subject (e.g. Entra's ``oid``) can
#: override this per-issuer.
DEFAULT_SUBJECT_CLAIM: Final[str] = "sub"

#: Default provisioning policy. Per design § Identity Sources, a
#: first-time ``(issuer, subject)`` triggers a User row with zero
#: workspace bindings — least-surprising, no implicit grants.
DEFAULT_PROVISIONING_POLICY: Final[str] = "zero-binding"

#: Names of known presets. Used both for validation
#: (``parse_issuers_config`` rejects unknown preset names early) and
#: for the runtime preset registry that resolves a preset id to its
#: default-config function.
KNOWN_PRESETS: Final[frozenset[str]] = frozenset({"github", "entra"})

ProvisioningPolicy = Literal["zero-binding"]


class OidcConfigError(ValueError):
    """Raised when ``CUSTOS_AUTH_OIDC_ISSUERS`` is malformed.

    The auth-service refuses to start on a bad OIDC config so the
    operator sees the problem at deploy time rather than at first
    user login. Mirrors the strict-by-default posture of every other
    settings parser in :mod:`custos_auth.settings`.
    """


@dataclass(frozen=True, slots=True)
class GroupBinding:
    """A single ``group claim → role binding`` mapping rule.

    Used by presets that publish a group membership claim (Entra's
    ``groups``, Okta's ``groups``, etc.) to auto-grant a workspace
    role at first sign-in. ``claim_value`` is matched against the
    string values in the configured ``group_claim`` array; a single
    match auto-grants ``role`` at ``workspace_id`` scope.

    Rebinding (claim removed → role revoked) is **not** automatic in
    v1 — these are one-shot grants applied at first OIDC link. Phase
    H ships the data shape and link-time application; M3+ adds the
    revocation-on-claim-removal loop.
    """

    claim_value: str
    role: str
    workspace_id: str


@dataclass(frozen=True, slots=True)
class OidcIssuerConfig:
    """A single OIDC issuer entry resolved against any preset defaults.

    After :func:`parse_issuers_config` returns, every field is fully
    populated — preset defaults have been merged in, optional keys
    have been normalised, and the verifier can consume the entry
    without re-running the preset machinery.
    """

    id: str
    """Stable identifier the operator picks. Used in audit rows and
    in the ``POST /v1/auth/login/oidc/callback`` ``issuer`` field
    so multi-issuer deployments can route the exchange to the right
    verifier.
    """

    preset: str | None
    """``"github"`` / ``"entra"`` / ``None``. Captured so the
    verifier can apply preset-specific claim parsing (e.g. GitHub's
    ``repository`` claim or Entra's tenant audience handling).
    """

    issuer_url: str
    """The exact ``iss`` claim the verifier requires."""

    jwks_uri: str
    """URL the JWKS cache fetches public keys from."""

    audiences: tuple[str, ...]
    """Acceptable ``aud`` values. The verifier requires the token's
    ``aud`` claim (string or array) to intersect this set.
    """

    algorithms: tuple[str, ...]
    """Acceptable signature algorithms. Defaults to
    :data:`DEFAULT_ALGORITHMS`. The verifier refuses tokens signed
    with an algorithm outside this set even if the JWKS happens to
    return a matching key.
    """

    subject_claim: str
    """Claim used as the OIDC subject when linking the identity.
    Defaults to :data:`DEFAULT_SUBJECT_CLAIM`.
    """

    provisioning_policy: ProvisioningPolicy
    """How to handle a first-time ``(issuer, subject)``. Only
    ``"zero-binding"`` is supported in v1 — admin grants the first
    role binding manually.
    """

    group_claim: str | None
    """Optional claim carrying group / role memberships. Presets
    that auto-map groups to role bindings (Entra) populate this.
    """

    group_bindings: tuple[GroupBinding, ...]
    """Optional list of ``group claim → role binding`` rules applied
    at first OIDC link. Empty tuple disables group mapping.
    """

    token_endpoint: str | None
    """OAuth 2.0 token endpoint used by the
    ``POST /v1/auth/login/oidc/callback`` handler to exchange the
    authorization ``code`` for an ID token. ``None`` means the
    handler cannot perform the exchange — useful for workload-token
    issuers (e.g. GitHub Actions) where there is no code flow.
    """

    client_id: str | None
    """OAuth client id used in the code exchange. ``None`` is legal
    when ``token_endpoint`` is also ``None``.
    """

    client_secret_env: str | None
    """Name of the env var carrying the OAuth client secret. The
    Helm chart projects the secret value via an ExternalSecret so
    it never appears in plaintext on disk. ``None`` is legal when
    no code exchange is required.
    """


@dataclass(frozen=True, slots=True)
class IssuersConfig:
    """Container for the parsed issuer list.

    A plain tuple would also work, but wrapping it in a dataclass
    lets us add helper methods (``by_id``) without touching every
    caller and keeps room for future top-level fields (global
    defaults, signing-key overrides) without changing the public
    type.
    """

    issuers: tuple[OidcIssuerConfig, ...] = ()

    def by_id(self, issuer_id: str) -> OidcIssuerConfig | None:
        """Resolve an entry by ``issuer_id`` (the callback ``issuer`` field)."""
        for entry in self.issuers:
            if entry.id == issuer_id:
                return entry
        return None

    def __bool__(self) -> bool:  # pragma: no cover — trivial
        return bool(self.issuers)


_ALLOWED_TOP_LEVEL: Final[frozenset[str]] = frozenset({"issuers"})

_ALLOWED_ISSUER_KEYS: Final[frozenset[str]] = frozenset(
    {
        "id",
        "preset",
        "issuer_url",
        "jwks_uri",
        "audiences",
        "algorithms",
        "subject_claim",
        "provisioning_policy",
        "group_claim",
        "group_bindings",
        "token_endpoint",
        "client_id",
        "client_secret_env",
    }
)

_ALLOWED_GROUP_BINDING_KEYS: Final[frozenset[str]] = frozenset(
    {"claim_value", "role", "workspace_id"}
)


def _require_str(entry: dict[str, object], key: str, *, where: str) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value:
        raise OidcConfigError(
            f"OIDC issuer config: {where} field {key!r} must be a non-empty string"
        )
    return value


def _optional_str(entry: dict[str, object], key: str, *, where: str) -> str | None:
    value = entry.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise OidcConfigError(
            f"OIDC issuer config: {where} field {key!r} must be a non-empty string when set"
        )
    return value


def _string_tuple(
    entry: dict[str, object], key: str, *, where: str, required: bool
) -> tuple[str, ...]:
    raw = entry.get(key)
    if raw is None:
        if required:
            raise OidcConfigError(f"OIDC issuer config: {where} field {key!r} is required")
        return ()
    if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
        raise OidcConfigError(
            f"OIDC issuer config: {where} field {key!r} must be a list of non-empty strings"
        )
    return tuple(raw)


def _parse_group_binding(raw: object, *, where: str) -> GroupBinding:
    if not isinstance(raw, dict):
        raise OidcConfigError(f"OIDC issuer config: {where} group_bindings entry must be an object")
    unknown = set(raw.keys()) - _ALLOWED_GROUP_BINDING_KEYS
    if unknown:
        raise OidcConfigError(
            f"OIDC issuer config: {where} group_bindings entry has unknown keys: "
            f"{sorted(unknown)}; allowed: {sorted(_ALLOWED_GROUP_BINDING_KEYS)}"
        )
    return GroupBinding(
        claim_value=_require_str(raw, "claim_value", where=f"{where} group_bindings"),
        role=_require_str(raw, "role", where=f"{where} group_bindings"),
        workspace_id=_require_str(raw, "workspace_id", where=f"{where} group_bindings"),
    )


def _parse_issuer(raw: object, *, index: int) -> OidcIssuerConfig:
    where = f"issuers[{index}]"
    if not isinstance(raw, dict):
        raise OidcConfigError(f"OIDC issuer config: {where} must be an object")
    unknown = set(raw.keys()) - _ALLOWED_ISSUER_KEYS
    if unknown:
        raise OidcConfigError(
            f"OIDC issuer config: {where} has unknown keys: {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_ISSUER_KEYS)}"
        )

    preset_raw = raw.get("preset")
    if preset_raw is not None:
        if not isinstance(preset_raw, str):
            raise OidcConfigError(
                f"OIDC issuer config: {where} field 'preset' must be a string when set"
            )
        if preset_raw not in KNOWN_PRESETS:
            raise OidcConfigError(
                f"OIDC issuer config: {where} unknown preset {preset_raw!r}; "
                f"known presets: {sorted(KNOWN_PRESETS)}"
            )
    preset = preset_raw

    # Defer to the preset for default issuer_url / jwks_uri / algorithms
    # when the operator omits them. The defaults are resolved by
    # custos_auth.oidc.presets.apply_preset_defaults — imported lazily
    # to avoid a circular dependency between presets and config.
    from custos_auth.oidc.presets import apply_preset_defaults

    # First pass: assemble a raw entry honouring only explicit fields.
    issuer_id = _require_str(raw, "id", where=where)

    raw_bindings = raw.get("group_bindings")
    bindings: tuple[GroupBinding, ...]
    if raw_bindings is None:
        bindings = ()
    else:
        if not isinstance(raw_bindings, list):
            raise OidcConfigError(
                f"OIDC issuer config: {where} field 'group_bindings' must be a list when set"
            )
        bindings = tuple(_parse_group_binding(item, where=where) for item in raw_bindings)

    provisioning_raw = raw.get("provisioning_policy", DEFAULT_PROVISIONING_POLICY)
    if provisioning_raw != "zero-binding":
        raise OidcConfigError(
            f"OIDC issuer config: {where} field 'provisioning_policy' must be 'zero-binding' "
            f"(only value supported in v1); got {provisioning_raw!r}"
        )

    explicit = OidcIssuerConfig(
        id=issuer_id,
        preset=preset,
        # Sentinel empty strings — apply_preset_defaults fills them
        # in when a preset is set; we re-validate after the merge.
        issuer_url=_optional_str(raw, "issuer_url", where=where) or "",
        jwks_uri=_optional_str(raw, "jwks_uri", where=where) or "",
        audiences=_string_tuple(raw, "audiences", where=where, required=False),
        algorithms=_string_tuple(raw, "algorithms", where=where, required=False),
        subject_claim=_optional_str(raw, "subject_claim", where=where) or "",
        provisioning_policy="zero-binding",
        group_claim=_optional_str(raw, "group_claim", where=where),
        group_bindings=bindings,
        token_endpoint=_optional_str(raw, "token_endpoint", where=where),
        client_id=_optional_str(raw, "client_id", where=where),
        client_secret_env=_optional_str(raw, "client_secret_env", where=where),
    )

    merged = apply_preset_defaults(explicit)

    # Post-merge validation: required fields must now be present.
    if not merged.issuer_url:
        raise OidcConfigError(
            f"OIDC issuer config: {where} field 'issuer_url' is required "
            f"(no preset supplied a default)"
        )
    if not merged.jwks_uri:
        raise OidcConfigError(
            f"OIDC issuer config: {where} field 'jwks_uri' is required "
            f"(no preset supplied a default)"
        )
    if not merged.audiences:
        raise OidcConfigError(
            f"OIDC issuer config: {where} field 'audiences' is required "
            "and must list at least one value"
        )
    final = merged
    if not final.algorithms:
        final = replace(final, algorithms=DEFAULT_ALGORITHMS)
    if not final.subject_claim:
        final = replace(final, subject_claim=DEFAULT_SUBJECT_CLAIM)
    return final


def parse_issuers_config(raw: str) -> IssuersConfig:
    """Parse a ``CUSTOS_AUTH_OIDC_ISSUERS`` JSON document.

    Empty / whitespace-only input returns an empty :class:`IssuersConfig`
    so the M1 default deployment (no issuers configured) is legal.
    Any other malformed input raises :class:`OidcConfigError`.

    The parser is **strict**: unknown top-level keys, unknown issuer
    keys, unknown preset names, and unsupported provisioning policies
    all raise. The auth-service refuses to start on the resulting
    error — operators see the misconfiguration at deploy time.
    """
    raw = raw.strip()
    if raw == "":
        return IssuersConfig()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise OidcConfigError(
            f"OIDC issuer config: invalid JSON: {exc.msg} at line {exc.lineno} col {exc.colno}"
        ) from exc

    if not isinstance(parsed, dict):
        raise OidcConfigError(
            "OIDC issuer config: top-level value must be an object (expected {'issuers': [...]})"
        )

    unknown_top = set(parsed.keys()) - _ALLOWED_TOP_LEVEL
    if unknown_top:
        raise OidcConfigError(
            f"OIDC issuer config: unknown top-level keys: {sorted(unknown_top)}; "
            f"allowed: {sorted(_ALLOWED_TOP_LEVEL)}"
        )

    issuers_raw = parsed.get("issuers", [])
    if not isinstance(issuers_raw, list):
        raise OidcConfigError("OIDC issuer config: 'issuers' must be a list")

    issuers = tuple(_parse_issuer(item, index=i) for i, item in enumerate(issuers_raw))

    # Reject duplicate ids early — the verifier indexes on id and a
    # collision would silently mask the second entry.
    seen: set[str] = set()
    for entry in issuers:
        if entry.id in seen:
            raise OidcConfigError(f"OIDC issuer config: duplicate issuer id {entry.id!r}")
        seen.add(entry.id)

    return IssuersConfig(issuers=issuers)


__all__ = [
    "DEFAULT_ALGORITHMS",
    "DEFAULT_PROVISIONING_POLICY",
    "DEFAULT_SUBJECT_CLAIM",
    "KNOWN_PRESETS",
    "GroupBinding",
    "IssuersConfig",
    "OidcConfigError",
    "OidcIssuerConfig",
    "ProvisioningPolicy",
    "parse_issuers_config",
]


# Make ``field`` available even though only the parser uses it via the
# dataclass field defaults. Suppress unused-import lint by exporting.
_ = field
