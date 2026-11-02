"""Discovery preserves paused polling controls and uses opaque bounded keysets."""

from __future__ import annotations

import pytest

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.receiver_workspaces import (
    ReceiverWorkspaceProvider,
    receiver_workspace_page,
)
from custos_spl.pagination import Cursor
from custos_spl.pull_memory import MemoryPullControl


def test_memory_workspace_paging_matches_query_contract() -> None:
    """Sort/deduplicate IDs, cap output and reject invalid cursors rather than restarting."""
    first = receiver_workspace_page(["z", "a", "a", "c"], limit=2)
    assert first.items == ("a", "c") and first.next_cursor is not None
    last = receiver_workspace_page(["z", "a", "c"], cursor=first.next_cursor, limit=2)
    assert last.items == ("z",) and last.next_cursor is None
    assert len(receiver_workspace_page((str(i) for i in range(250)), limit=500).items) == 200
    with pytest.raises(ValueError):
        receiver_workspace_page([], limit=False)
    with pytest.raises(ValueError):
        receiver_workspace_page([], cursor=Cursor("invalid"))


async def test_paused_controls_remain_discoverable_without_native_demand() -> None:
    """An empty active poller set must not hide operator recovery work."""
    store = MemoryPullControl()
    assert isinstance(store, ReceiverWorkspaceProvider)
    await store.check_receiver_workspaces_ready()
    await store.initialize_pull_control(
        WorkspaceId("retained"), ConnectorInstanceId("instance"), actor="creator"
    )
    assert (await store.query_receiver_workspaces()).items == ("retained",)
