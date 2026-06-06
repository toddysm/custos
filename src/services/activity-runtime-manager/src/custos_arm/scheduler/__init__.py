"""Activity Scheduler (ARM-IMPL-017).

The Scheduler is the orchestrator-facing entrypoint that drives a single
activity attempt end-to-end, owning the execution state machine, idempotent
replay, and crash reconciliation. It wires every other ARM sub-module —
resolver, limiter, I/O Broker, Secret Injector, runtime driver, Result Mapper,
and Execution Store — behind one :class:`ActivityScheduler.schedule` call.
"""

from __future__ import annotations

from .errors import error_envelope_for, synthesize_failure
from .fsio import (
    FilesystemArtifactReader,
    FilesystemInputArtifactWriter,
    FilesystemSecretSink,
    read_outputs,
    write_ctx,
    write_inputs,
)
from .request import ScheduleRequest
from .scheduler import ActivityScheduler, CancelOutcome, ExecutionKey

__all__ = [
    "ActivityScheduler",
    "CancelOutcome",
    "ExecutionKey",
    "FilesystemArtifactReader",
    "FilesystemInputArtifactWriter",
    "FilesystemSecretSink",
    "ScheduleRequest",
    "error_envelope_for",
    "read_outputs",
    "synthesize_failure",
    "write_ctx",
    "write_inputs",
]
