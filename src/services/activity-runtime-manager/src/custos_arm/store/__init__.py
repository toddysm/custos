"""Persistence layer for ARM — execution and artifact records via SPL providers."""

from __future__ import annotations

from custos_arm.store.execution import (
    TERMINAL_STATES,
    ActivityExecution,
    DuplicateExecutionError,
    ExecutionKey,
    ExecutionRepository,
    ExecutionState,
    ExecutionStoreError,
    IllegalTransitionError,
    UnknownExecutionError,
    allowed_transitions,
)

__all__ = [
    "TERMINAL_STATES",
    "ActivityExecution",
    "DuplicateExecutionError",
    "ExecutionKey",
    "ExecutionRepository",
    "ExecutionState",
    "ExecutionStoreError",
    "IllegalTransitionError",
    "UnknownExecutionError",
    "allowed_transitions",
]
