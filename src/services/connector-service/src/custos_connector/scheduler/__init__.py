"""Pull-loop scheduler for Connector Service (CONN-IMPL-023, #306).

Implements the per-instance pull-loop driver described in
``design/components/connector-service/design.md`` § Pull Cursor Model
and § Single-writer safety:

* Per-instance in-memory scheduling state (``last_tick_at``,
  ``next_earliest_tick_at``, ``consecutive_failures``, ``paused``,
  ``halted``).
* Per scheduler pass: skip paused / halted / in-flight / within-
  interval instances, then fire :meth:`CursorService.tick` for each
  eligible instance. Single-writer safety is provided by the SPL
  cursor lease ``acquire_cursor_lease`` already invoked inside the
  tick — when another replica owns the lease, :class:`LeaseBusy`
  surfaces and the scheduler treats the pass as a no-op skip.
* Exponential backoff on generic failures up to a configurable cap.
* ≥10 s minimum interval, enforced at registration time. Per-instance
  interval overrides MAY only widen the gap.
* The scheduler is a **registry**, not an enumerator — callers
  ``register()`` the instances they want ticked. v1 has no
  cross-workspace enumeration over SPL because the public surface
  (CONN-IMPL-024 enable / pause / resume, CONN-IMPL-025 Listen
  Manager) is the eventual driver of registrations. The scheduler
  therefore loses its rotation on restart and depends on those
  callers to re-register on startup, matching what the design
  documents as ``next tick scheduler pass`` recovery semantics for
  crashed workers (the SPL cursor lease is the durable single-writer
  guarantee; the in-memory rotation is intentionally soft state).
"""

from custos_connector.scheduler.pull_loop import (
    DEFAULT_BACKOFF_CAP_SECONDS,
    PULL_LOOP_HARD_FLOOR_SEC,
    InstanceSchedule,
    PullLoopScheduler,
    SchedulerPassResult,
    TickOutcome,
)

__all__ = [
    "DEFAULT_BACKOFF_CAP_SECONDS",
    "PULL_LOOP_HARD_FLOOR_SEC",
    "InstanceSchedule",
    "PullLoopScheduler",
    "SchedulerPassResult",
    "TickOutcome",
]
