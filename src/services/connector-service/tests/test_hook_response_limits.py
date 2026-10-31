"""Real parser failure boundaries must preserve fenced upstream work."""

from __future__ import annotations

import sys

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.pull_control import PullTickBlocked

from custos_connector.runtime import HookRunResult, PluginCleanupFailed
from tests._pull import INSTANCE, WS, PullClock, memory_runtime, seed_instance, start_demand


@pytest.mark.parametrize("kind", ["depth", "integer"])
async def test_parser_limits_do_not_release_a_completed_invocation(kind: str) -> None:
    """Exercise actual JSON recursion and integer limits, not a mocked decoder failure."""
    clock = PullClock()
    providers, metadata, plugin, publisher = memory_runtime(clock)
    await seed_instance(providers)
    start_demand(metadata)
    assert providers.pull_demand_reconciler is not None and providers.cursor_service is not None
    await providers.pull_demand_reconciler.reconcile(WS)
    nested = b"[" * 16000 + b"0" + b"]" * 16000 if kind == "depth" else b"1" * 1000
    plugin._result = HookRunResult(
        0,
        b'{"ok":true,"result":{"events":[],"extra":' + nested + b"}}",
        b"",
    )
    previous = sys.get_int_max_str_digits()
    previous_recursion = sys.getrecursionlimit()
    try:
        sys.set_int_max_str_digits(640)
        sys.setrecursionlimit(1000)
        with pytest.raises(PluginCleanupFailed) as failure:
            await providers.cursor_service.tick(WS, INSTANCE)
        assert failure.value.termination_confirmed
    finally:
        sys.set_int_max_str_digits(previous)
        sys.setrecursionlimit(previous_recursion)
    clock.advance(61)
    with pytest.raises(PullTickBlocked, match="invocation_unconfirmed"):
        await metadata.acquire_pull_tick(
            WorkspaceId(WS),
            ConnectorInstanceId(INSTANCE),
            "new-replica",
            ttl_seconds=60,
        )
    assert not publisher.published and len(plugin.calls) == 1
