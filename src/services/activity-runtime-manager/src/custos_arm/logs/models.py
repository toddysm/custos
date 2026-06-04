"""Data types for the Log Streamer (ARM-IMPL-012).

The Log Streamer streams sandbox ``stdout`` / ``stderr`` and forwards
``/custos/out/audit.jsonl`` lines to Observability/Audit. To keep the
streamer decoupled from any concrete transport (Loki, the
Observability/Audit ingestion API, a test double, ...) it emits values
through the :class:`LogSink` Protocol declared here.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class LogStreamName(StrEnum):
    """The two console streams a sandbox process writes to."""

    STDOUT = "stdout"
    STDERR = "stderr"


@dataclass(frozen=True, slots=True)
class LogLine:
    """A single decoded console line observed on a sandbox stream.

    ``seq`` is a per-stream, 1-based monotonically increasing counter that
    lets a sink restore ordering even if delivery is concurrent. The line
    text has its trailing newline (and any ``\\r``) stripped.
    """

    stream: LogStreamName
    message: str
    run_id: str
    step_id: str
    attempt: int
    seq: int


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """A single forwarded ``/custos/out/audit.jsonl`` line.

    Each line in the file is expected to be one JSON object. ``payload``
    holds the parsed object when the line is well-formed; otherwise
    ``malformed`` is ``True``, ``payload`` is ``None``, and ``raw`` carries
    the original text so the audit trail is never silently dropped.
    """

    run_id: str
    step_id: str
    attempt: int
    seq: int
    raw: str
    payload: dict[str, Any] | None
    malformed: bool


@runtime_checkable
class LogSink(Protocol):
    """Transport that receives streamed console lines and audit events.

    Implementations forward to Observability/Audit. Both methods are
    awaited per record, so a slow sink naturally backpressures the
    streamer instead of letting the run buffer in memory.
    """

    async def emit_log(self, line: LogLine) -> None:
        """Forward a single decoded console line."""
        ...

    async def emit_audit(self, event: AuditEvent) -> None:
        """Forward a single audit-log event."""
        ...
