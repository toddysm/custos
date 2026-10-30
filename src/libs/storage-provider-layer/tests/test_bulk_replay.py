"""Provider-independent stable selection, result immutability and bulk ownership fencing."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.interfaces.bulk_replay import (
    BulkReplayCommand,
    bulk_request_hash,
    finish_bulk_item,
    require_bulk_entry,
    require_bulk_owner,
    reserve_bulk_replay,
)
from custos_spl.interfaces.dead_letters import DeadLetterData, DeadLetterFilter, new_dead_letter
from custos_spl.pagination import Page
from tests import test_dead_letters as fixtures

data = fixtures.data
NOW = fixtures.NOW


@pytest.fixture
def command(data: DeadLetterData) -> BulkReplayCommand:
    """Reserve one immutable selected entry with a real provider-independent lease."""
    filters = DeadLetterFilter()
    return reserve_bulk_replay(
        None,
        data.workspace_id,
        "command",
        "operator",
        bulk_request_hash(data.workspace_id, filters, None, 1),
        NOW,
        86400,
        Page((new_dead_letter(data, NOW),)),
    )


def test_bulk_owner_expiry_reclaims_same_selection(
    command: BulkReplayCommand,
    data: DeadLetterData,
) -> None:
    """Expiry replaces authority but never applies a new query result to the original request."""
    require_bulk_owner(command, command.token, NOW)
    with pytest.raises(LeaseBusy):
        reserve_bulk_replay(
            command,
            command.workspace_id,
            command.command_id,
            command.actor,
            command.request_hash,
            NOW,
            86400,
        )
    recovered = reserve_bulk_replay(
        command,
        command.workspace_id,
        command.command_id,
        command.actor,
        command.request_hash,
        NOW + timedelta(seconds=300),
        86400,
        Page(),
    )
    assert recovered.items == command.items and recovered.token != command.token
    with pytest.raises(LeaseExpired):
        require_bulk_owner(recovered, command.token, NOW + timedelta(seconds=300))
    require_bulk_entry(recovered, new_dead_letter(data, NOW), "operator")


def test_bulk_progress_first_result_wins(command: BulkReplayCommand) -> None:
    """Late callbacks cannot replace the result already committed by a completed entry attempt."""
    completed = finish_bulk_item(command, "entry", False)
    assert completed.completed and completed.token is None and completed.lease_until is None
    assert finish_bulk_item(completed, "entry", True) == completed
    with pytest.raises(ImmutableViolation):
        finish_bulk_item(command, "foreign", True)


def test_bulk_identity_cannot_rebind_or_select_a_recreated_entry(
    command: BulkReplayCommand,
    data: DeadLetterData,
) -> None:
    """Request identity and selected entry generation remain immutable across all retries."""
    with pytest.raises(ImmutableViolation):
        reserve_bulk_replay(
            command,
            command.workspace_id,
            command.command_id,
            command.actor,
            "f" * 64,
            NOW,
            86400,
        )
    with pytest.raises(ImmutableViolation):
        require_bulk_entry(command, new_dead_letter(data, NOW + timedelta(seconds=1)), "operator")
    with pytest.raises(ImmutableViolation):
        require_bulk_entry(command, new_dead_letter(data, NOW), "another")
    with pytest.raises(ImmutableViolation):
        reserve_bulk_replay(
            replace(command, items=None, next_cursor=None, token=None, lease_until=None),
            command.workspace_id,
            command.command_id,
            command.actor,
            command.request_hash,
            NOW + timedelta(days=2),
            86400,
        )


@pytest.mark.parametrize("limit", [0, 101, True])
def test_bulk_limits_reject_invalid_values(data: DeadLetterData, limit: int) -> None:
    """A provider cannot bypass the public selection bound or treat booleans as page sizes."""
    with pytest.raises(ValueError):
        bulk_request_hash(data.workspace_id, DeadLetterFilter(), None, limit)
