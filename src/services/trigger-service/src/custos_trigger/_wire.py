"""Shared pydantic base for the Trigger Service wire surface.

Every wire/domain model uses :class:`WireModel` so the JSON surface is
consistent: ``camelCase`` aliases, populate-by-field-name for ergonomic
construction in Python, and ``extra="forbid"`` so a typo'd field is a loud
validation error rather than a silently dropped key. This mirrors the Activity
Contract's ``ContractModel`` convention (ARM ``contract/_base.py``).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class WireModel(BaseModel):
    """Base model for every Trigger Service wire/domain type."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


__all__ = ["WireModel"]
