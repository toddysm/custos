"""Shared pydantic base + ISO-8601 helpers for the Activity Contract v1.

Every contract model uses :class:`ContractModel` so the wire surface is
consistent: camelCase JSON aliases, populate-by-field-name for ergonomic
construction in Python, and ``extra="forbid"`` so a typo'd field is a loud
validation error rather than silently dropped.
"""

from __future__ import annotations

import re
from typing import Final

from pydantic import BaseModel, ConfigDict

#: ISO-8601 duration grammar — the ``P[nD]T[nH][nM][nS]`` / ``PnW`` subset
#: shared across the platform (Workflow Service, ARM config). Months /
#: years are rejected because they are calendar-dependent.
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)


def is_iso8601_duration(value: str) -> bool:
    """Return ``True`` when ``value`` is a non-empty ISO-8601 duration.

    Empty shapes (``P``, ``PT``) are rejected: they match the grammar but
    carry no component and so cannot describe a real duration.
    """
    match = _ISO8601_DURATION_PATTERN.match(value)
    if match is None:
        return False
    return any(match.group(part) for part in ("weeks", "days", "hours", "minutes", "seconds"))


class ContractModel(BaseModel):
    """Base model for every Activity Contract type."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


__all__ = ["ContractModel", "is_iso8601_duration"]
