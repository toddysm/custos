"""Unit tests for the binding-changed event types and publishers."""

from __future__ import annotations

import pytest
from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    RoleId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    TenantScope,
    WorkspaceScope,
)

from custos_auth.binding_events import (
    BindingChangedEvent,
    NoOpBindingChangedPublisher,
    RecordingBindingChangedPublisher,
)


def _event(scope: object) -> BindingChangedEvent:
    return BindingChangedEvent(
        principal_id=PrincipalId("user-1"),
        role_id=RoleId("role:workspace.viewer"),
        scope=scope,  # type: ignore[arg-type]
        action="granted",
        binding_id=RoleBindingId("b-1"),
    )


def test_scope_kind_workspace() -> None:
    event = _event(WorkspaceScope(workspace_id=WorkspaceId("w")))
    assert event.scope_kind == "workspace"


def test_scope_kind_tenant() -> None:
    event = _event(TenantScope(tenant_id=TenantId("t")))
    assert event.scope_kind == "tenant"


def test_scope_kind_platform() -> None:
    event = _event(GlobalScope())
    assert event.scope_kind == "platform"


async def test_noop_publisher_does_not_raise() -> None:
    publisher = NoOpBindingChangedPublisher()
    await publisher.publish(_event(GlobalScope()))


async def test_recording_publisher_captures_in_order() -> None:
    publisher = RecordingBindingChangedPublisher()
    granted = _event(GlobalScope())
    revoked = BindingChangedEvent(
        principal_id=PrincipalId("user-1"),
        role_id=RoleId("role:workspace.viewer"),
        scope=GlobalScope(),
        action="revoked",
        binding_id=RoleBindingId("b-1"),
    )
    await publisher.publish(granted)
    await publisher.publish(revoked)
    assert publisher.published == [granted, revoked]


def test_binding_changed_event_action_literal() -> None:
    # Construction with arbitrary strings is rejected by mypy but
    # accepted at runtime — this test is a smoke for the runtime path.
    event = _event(GlobalScope())
    assert event.action in ("granted", "revoked")


def test_recording_publisher_starts_empty() -> None:
    publisher = RecordingBindingChangedPublisher()
    assert publisher.published == []


@pytest.mark.parametrize(
    "scope, expected",
    [
        (WorkspaceScope(workspace_id=WorkspaceId("w")), "workspace"),
        (TenantScope(tenant_id=TenantId("t")), "tenant"),
        (GlobalScope(), "platform"),
    ],
)
def test_scope_kind_parametrised(scope: object, expected: str) -> None:
    assert _event(scope).scope_kind == expected
