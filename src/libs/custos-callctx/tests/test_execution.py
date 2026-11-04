"""Stable execution identity excludes credentials and cannot follow a changed account default."""

import json

import pytest

from custos_callctx.execution import (
    ExecutionBinding,
    ExecutionBindings,
    ExecutionIdentity,
    load_execution_bindings,
)


def test_reference_round_trip_and_closed_fields() -> None:
    """Only non-secret scope and identity coordinates are accepted by the durable model."""
    identity = ExecutionIdentity("workspace", "principal", "binding")
    assert ExecutionIdentity.from_dict(identity.to_dict()) == identity
    with pytest.raises(ValueError):
        ExecutionIdentity.from_dict({**identity.to_dict(), "token": "synthetic-private"})
    with pytest.raises(ValueError):
        ExecutionIdentity("workspace", " ", "binding")


def test_default_changes_never_retarget_original_identity() -> None:
    """New selections may use a new account while retained runs keep the exact original binding."""
    first = ExecutionBinding(ExecutionIdentity("ws", "first", "a"), "first-token", "token")
    second = ExecutionBinding(ExecutionIdentity("ws", "second", "b"), "second-token", "token")
    old = ExecutionBindings((first, second), {"ws": "a"})
    selected = old.select("ws")
    changed = ExecutionBindings((first, second), {"ws": "b"})
    assert changed.select("ws") == second.identity
    assert changed.resolve(selected) == first
    with pytest.raises(ValueError, match="unavailable"):
        ExecutionBindings((second,), {"ws": "b"}).resolve(selected)
    with pytest.raises(ValueError, match="unavailable"):
        changed.resolve(ExecutionIdentity("ws", "second", "a"))
    with pytest.raises(ValueError):
        changed.select("another-workspace")


def test_configuration_is_bounded_and_contains_references_only() -> None:
    """Reject duplicate, cross-workspace and credential-bearing configuration."""
    entry = {
        "workspaceId": "ws",
        "principalId": "account",
        "bindingId": "default",
        "secretRef": "execution-token",
        "secretKey": "token",
    }
    configured = ExecutionBindings.from_json(
        json.dumps(
            {
                "bindings": [entry],
                "defaults": {"ws": "default"},
            }
        )
    )
    assert configured.resolve(configured.select("ws")).secret_ref == "execution-token"
    for value in (
        {"bindings": [entry, entry], "defaults": {"ws": "default"}},
        {"bindings": [entry], "defaults": {"other": "default"}},
        {"bindings": [{**entry, "token": "private"}], "defaults": {}},
        {"bindings": [], "defaults": {}, "extra": True},
    ):
        with pytest.raises(ValueError):
            ExecutionBindings.from_json(json.dumps(value))
    with pytest.raises(ValueError, match="64 KiB"):
        ExecutionBindings.from_json(" " * 65_537)
    with pytest.raises(ValueError, match="duplicate"):
        ExecutionBindings.from_json('{"bindings":[],"bindings":[],"defaults":{}}')


def test_execution_configuration_is_explicitly_enabled() -> None:
    """Missing or malformed gates never turn new actor configuration into an implicit default."""
    assert load_execution_bindings({}) is None
    for env in (
        {"CUSTOS_EXECUTION_IDENTITY_ENABLED": "maybe"},
        {"CUSTOS_EXECUTION_IDENTITY_ENABLED": "true"},
    ):
        with pytest.raises(ValueError):
            load_execution_bindings(env)
