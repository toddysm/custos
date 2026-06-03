"""Shared pydantic base and semver / namespace helpers for the manifest.

Every manifest model uses :class:`ManifestModel` so the wire surface is
consistent: camelCase JSON aliases, populate-by-field-name for ergonomic
construction in Python, and ``extra="forbid"`` so a typo'd field is a loud
validation error rather than silently dropped.
"""

from __future__ import annotations

import re
from typing import Final, NamedTuple

from pydantic import BaseModel, ConfigDict

#: A full ``MAJOR.MINOR.PATCH`` semver triple (no pre-release / build metadata
#: in v1 — the manifest version is always a concrete release).
_SEMVER_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$"
)

#: Reserved namespace prefixes only the platform may publish into. A manifest
#: namespace whose first dot-delimited segment is one of these belongs to the
#: platform tier (design § Namespace model).
RESERVED_NAMESPACE_PREFIXES: Final[frozenset[str]] = frozenset(
    {"custos", "system", "platform", "builtin"}
)

#: A dot-delimited, lowercase capability token (e.g. ``oci.pull``). Bare tokens
#: (no dot) and ``event.*`` verbs are rejected (design § spec.connectors[]).
_CAPABILITY_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+$")


class SemVer(NamedTuple):
    """A parsed ``MAJOR.MINOR.PATCH`` version triple."""

    major: int
    minor: int
    patch: int


def parse_semver(value: str) -> SemVer:
    """Parse a ``MAJOR.MINOR.PATCH`` string into a :class:`SemVer`.

    Raises :class:`ValueError` when ``value`` is not a full three-component
    semver triple.
    """
    match = _SEMVER_PATTERN.match(value)
    if match is None:
        raise ValueError(f"version {value!r} is not a MAJOR.MINOR.PATCH semver string")
    return SemVer(int(match["major"]), int(match["minor"]), int(match["patch"]))


def reserved_namespace_prefix(namespace: str) -> str | None:
    """Return the reserved prefix a namespace falls under, or ``None``.

    The first dot-delimited segment of ``namespace`` is compared against
    :data:`RESERVED_NAMESPACE_PREFIXES`; e.g. ``custos.builtin`` →
    ``"custos"``.
    """
    head = namespace.split(".", 1)[0]
    return head if head in RESERVED_NAMESPACE_PREFIXES else None


def is_capability_token(value: str) -> bool:
    """Return ``True`` when ``value`` is a valid dot-namespaced capability.

    Bare tokens (e.g. ``pull``) and any ``event.*`` verb are rejected.
    """
    if value.startswith("event."):
        return False
    return _CAPABILITY_PATTERN.match(value) is not None


class ManifestModel(BaseModel):
    """Base for every Activity Manifest v1 model."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")
