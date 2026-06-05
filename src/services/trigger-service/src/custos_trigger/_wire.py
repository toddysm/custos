"""Shared pydantic base for the Trigger Service wire surface.

Every wire/domain model uses :class:`WireModel` so the JSON surface is
consistent: snake_case Python fields serialize as ``camelCase`` on the wire
via the :func:`pydantic.alias_generators.to_camel` alias generator (so no field
has to hand-roll its alias), populate-by-field-name lets callers construct with
either spelling, and ``extra="forbid"`` makes a typo'd field a loud validation
error rather than a silently dropped key. Mirrors workflow-service's
``_CamelModel`` convention.

Whitespace is intentionally *not* stripped: ``EventRaw.body`` carries the raw
source payload verbatim for audit, so a global ``str_strip_whitespace`` would
corrupt it.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class WireModel(BaseModel):
    """Base model for every Trigger Service wire/domain type."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )


__all__ = ["WireModel"]
