"""Tests for :mod:`custos_workflow.bindings.registry`."""

from __future__ import annotations

import pytest

from custos_workflow.bindings import (
    ActivityTypeNotFoundError,
    InMemoryActivityTypeRegistry,
)


class TestInMemoryActivityTypeRegistry:
    def test_lookup_hit(self) -> None:
        reg = InMemoryActivityTypeRegistry(
            {
                "security/scan@1": {
                    "type": "object",
                    "properties": {"critical": {"type": "integer"}},
                }
            }
        )
        schema = reg.get_outputs_schema("security/scan@1")
        assert schema["properties"]["critical"]["type"] == "integer"

    def test_lookup_miss_raises(self) -> None:
        reg = InMemoryActivityTypeRegistry({})
        with pytest.raises(ActivityTypeNotFoundError):
            reg.get_outputs_schema("missing/activity@1")

    def test_construction_copies_outer_dict(self) -> None:
        # Outer dict mutation after construction must not leak in.
        seed: dict[str, dict[str, str]] = {"a/b@1": {"type": "object"}}
        reg = InMemoryActivityTypeRegistry(seed)
        seed.pop("a/b@1")
        assert reg.get_outputs_schema("a/b@1")["type"] == "object"
