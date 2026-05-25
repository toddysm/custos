"""OIDC preset registry (AS-IMPL-021, AS-IMPL-022).

A preset is a small bundle of default-config values + claim-extraction
helpers for a specific OIDC provider. Operators pick a preset by name
in ``CUSTOS_AUTH_OIDC_ISSUERS``; the preset's defaults are merged into
the explicit issuer entry so the operator only has to specify the
fields that diverge from the provider's well-known defaults.

Each preset module exposes:

* ``defaults() -> dict[str, object]`` — partial issuer-config fields
  the parser merges with the operator's explicit entry.
* ``extract_subject(claims: Mapping[str, Any]) -> str`` — preset-
  specific subject extraction (e.g. GitHub Actions parses
  ``repo:<org>/<repo>:ref:...`` out of the ``sub`` claim).
* ``extra_audit_payload(claims) -> dict`` — preset-specific extra
  claims surfaced on ``authn.success`` / ``oidc.identity-linked``
  audit rows (``repository`` / ``workflow`` for GitHub; ``tid`` /
  ``oid`` for Entra).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from custos_auth.oidc.config import OidcIssuerConfig


class OidcPreset(Protocol):
    """Structural type implemented by every preset module."""

    name: str

    def defaults(self) -> dict[str, object]:  # pragma: no cover — protocol
        ...

    def extract_subject(self, claims: Mapping[str, Any]) -> str:  # pragma: no cover — protocol
        ...

    def extra_audit_payload(
        self, claims: Mapping[str, Any]
    ) -> dict[str, str]:  # pragma: no cover — protocol
        ...


def get_preset(name: str) -> OidcPreset:
    """Resolve a preset module by name.

    Imports are deferred so unused presets do not contribute to the
    package's import-time cost; the registry stays tiny (two entries
    for v1, plus the upstream cap from
    :data:`custos_auth.oidc.config.KNOWN_PRESETS`).
    """
    if name == "github":
        from custos_auth.oidc.presets import github as github_module

        preset_module: OidcPreset = github_module
    elif name == "entra":
        from custos_auth.oidc.presets import entra as entra_module

        preset_module = entra_module
    else:  # pragma: no cover — guarded by config parser
        raise ValueError(f"unknown OIDC preset {name!r}")
    return preset_module


def apply_preset_defaults(entry: OidcIssuerConfig) -> OidcIssuerConfig:
    """Merge ``entry.preset``'s defaults into ``entry``.

    Returns the input unchanged when ``entry.preset is None``.
    Explicit fields always win — preset defaults only fill in fields
    the operator left at their dataclass sentinel (empty string for
    URLs / subject claim, empty tuple for audiences / algorithms,
    ``None`` for the optional code-flow fields).
    """
    if entry.preset is None:
        return entry
    preset = get_preset(entry.preset)
    defaults = preset.defaults()

    def _pick(name: str, current: object) -> object:
        """Return the preset default only when the explicit value is empty."""
        if name not in defaults:
            return current
        if current in ("", (), None):
            return defaults[name]
        return current

    return replace(
        entry,
        issuer_url=_pick("issuer_url", entry.issuer_url),  # type: ignore[arg-type]
        jwks_uri=_pick("jwks_uri", entry.jwks_uri),  # type: ignore[arg-type]
        audiences=_pick("audiences", entry.audiences),  # type: ignore[arg-type]
        algorithms=_pick("algorithms", entry.algorithms),  # type: ignore[arg-type]
        subject_claim=_pick("subject_claim", entry.subject_claim),  # type: ignore[arg-type]
        group_claim=_pick("group_claim", entry.group_claim),  # type: ignore[arg-type]
        token_endpoint=_pick("token_endpoint", entry.token_endpoint),  # type: ignore[arg-type]
    )


__all__ = [
    "OidcPreset",
    "apply_preset_defaults",
    "get_preset",
]
