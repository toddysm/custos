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
        with pytest.raises(ActivityTypeNotFoundError) as exc_info:
            reg.get_outputs_schema("missing/activity@1")
        # ``args[0]`` MUST be the raw activity reference.
        assert exc_info.value.args[0] == "missing/activity@1"
        assert exc_info.value.activity_ref == "missing/activity@1"
        assert str(exc_info.value) == "missing/activity@1"

    def test_construction_copies_outer_dict(self) -> None:
        # Outer dict mutation after construction must not leak in.
        seed: dict[str, dict[str, str]] = {"a/b@1": {"type": "object"}}
        reg = InMemoryActivityTypeRegistry(seed)
        seed.pop("a/b@1")
        assert reg.get_outputs_schema("a/b@1")["type"] == "object"


class TestActivityTypeNotFoundError:
    def test_args_zero_is_raw_ref_even_with_message(self) -> None:
        exc = ActivityTypeNotFoundError(
            "ns/type@1", message="step 'scan': activity reference not registered"
        )
        # Machine-readable handle: ``args[0]`` + ``.activity_ref``.
        assert exc.args[0] == "ns/type@1"
        assert exc.activity_ref == "ns/type@1"
        # Human-readable form: ``str(exc)`` uses the message when given.
        assert str(exc) == "step 'scan': activity reference not registered"

    def test_default_str_falls_back_to_ref(self) -> None:
        exc = ActivityTypeNotFoundError("ns/type@1")
        assert str(exc) == "ns/type@1"
