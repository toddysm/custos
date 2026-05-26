"""Unit tests for :mod:`custos_sidecar.context_registry`."""

from __future__ import annotations

import pytest

from custos_sidecar.context_registry import ContextRegistry, SlotContext
from custos_sidecar.errors import SidecarError, SidecarErrorCode


def _ctx(slot: str = "primary", *, caps: tuple[str, ...] = ("read",)) -> SlotContext:
    return SlotContext(
        slot=slot,
        connector_instance_id=f"ci_{slot}",
        capabilities=caps,
        endpoint=f"https://example/{slot}",
        token_type="Bearer",
        extras={},
    )


def test_resolve_happy_path():
    reg = ContextRegistry([_ctx(caps=("read", "write"))])
    ctx = reg.resolve("primary", purpose="read")
    assert ctx.slot == "primary"
    assert ctx.connector_instance_id == "ci_primary"


def test_unknown_slot_raises_slot_not_found():
    reg = ContextRegistry([_ctx()])
    with pytest.raises(SidecarError) as info:
        reg.resolve("missing", purpose="read")
    assert info.value.code is SidecarErrorCode.SLOT_NOT_FOUND


def test_unknown_purpose_raises_capability_forbidden():
    reg = ContextRegistry([_ctx(caps=("read",))])
    with pytest.raises(SidecarError) as info:
        reg.resolve("primary", purpose="write")
    assert info.value.code is SidecarErrorCode.CAPABILITY_FORBIDDEN


def test_duplicate_slot_rejected():
    with pytest.raises(ValueError):
        ContextRegistry([_ctx("a"), _ctx("a")])


def test_empty_slot_name_rejected():
    with pytest.raises(ValueError):
        ContextRegistry(
            [
                SlotContext(
                    slot="",
                    connector_instance_id="ci",
                    capabilities=("read",),
                    endpoint="https://x",
                    token_type="Bearer",
                    extras={},
                )
            ]
        )


def test_from_wire_round_trip():
    reg = ContextRegistry.from_wire(
        [
            {
                "slot": "primary",
                "connectorInstanceId": "ci_p",
                "capabilities": ["read", "write"],
                "endpoint": "https://example/p",
                "tokenType": "Bearer",
                "extras": {"region": "us-east-1"},
            }
        ]
    )
    ctx = reg.resolve("primary", purpose="write")
    assert ctx.endpoint == "https://example/p"
    assert ctx.extras == {"region": "us-east-1"}


def test_slot_names_preserves_insertion_order():
    reg = ContextRegistry([_ctx("a"), _ctx("b"), _ctx("c")])
    assert reg.slot_names() == ("a", "b", "c")
