"""Persistence layer for ARM — execution and artifact records via SPL providers."""

from __future__ import annotations

from custos_arm.store.artifact import (
    ArtifactRecord,
    ArtifactStoreClient,
    ArtifactStoreError,
    ArtifactTooLargeError,
)
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
    "ArtifactRecord",
    "ArtifactStoreClient",
    "ArtifactStoreError",
    "ArtifactTooLargeError",
    "DuplicateExecutionError",
    "ExecutionKey",
    "ExecutionRepository",
    "ExecutionState",
    "ExecutionStoreError",
    "IllegalTransitionError",
    "UnknownExecutionError",
    "allowed_transitions",
]
