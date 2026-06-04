"""The Log Streamer (ARM-IMPL-012).

:class:`LogStreamer` consumes the sandbox ``stdout`` / ``stderr`` byte
streams and the ``/custos/out/audit.jsonl`` byte stream as async iterators
of raw chunks, splits them into lines incrementally, and forwards each line
to a :class:`~custos_arm.logs.models.LogSink`.

Only the current partial line is ever held in memory, so an arbitrarily
long run is streamed without buffering. A ``max_line_bytes`` ceiling bounds
the partial-line buffer too: a pathological producer that never emits a
newline is flushed in fixed-size segments rather than growing unbounded.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Final

from custos_arm.contract import StepRef

from .models import AuditEvent, LogLine, LogSink, LogStreamName

#: Default ceiling for the partial-line buffer. A line (or unbroken run of
#: bytes) longer than this is emitted in segments of this size so the
#: streamer's memory stays bounded regardless of producer behaviour.
DEFAULT_MAX_LINE_BYTES: Final[int] = 64 * 1024


class LogStreamer:
    """Streams one activity attempt's console + audit output to a sink.

    A streamer instance is scoped to a single ``(runId, stepId, attempt)``
    and is single-use per source: pass the sandbox byte streams to
    :meth:`run` (or the individual ``stream_*`` / ``forward_audit``
    methods) and every decoded line is forwarded to ``sink``.
    """

    def __init__(
        self,
        *,
        sink: LogSink,
        step: StepRef,
        max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
    ) -> None:
        if max_line_bytes <= 0:
            raise ValueError(f"max_line_bytes must be positive; got {max_line_bytes}")
        self._sink = sink
        self._step = step
        self._max_line_bytes = max_line_bytes
        self._stdout_seq = 0
        self._stderr_seq = 0
        self._audit_seq = 0

    async def run(
        self,
        *,
        stdout: AsyncIterator[bytes] | None = None,
        stderr: AsyncIterator[bytes] | None = None,
        audit: AsyncIterator[bytes] | None = None,
    ) -> None:
        """Forward every provided source concurrently until each is drained.

        Any source left as ``None`` is skipped. Per-stream ordering is
        preserved via the line ``seq`` counters; ordering *across* streams
        is intentionally not synchronised.
        """
        tasks = []
        if stdout is not None:
            tasks.append(self.stream_stdout(stdout))
        if stderr is not None:
            tasks.append(self.stream_stderr(stderr))
        if audit is not None:
            tasks.append(self.forward_audit(audit))
        if tasks:
            await asyncio.gather(*tasks)

    async def stream_stdout(self, source: AsyncIterator[bytes]) -> None:
        """Forward decoded ``stdout`` lines from ``source``."""
        await self._stream(source, LogStreamName.STDOUT)

    async def stream_stderr(self, source: AsyncIterator[bytes]) -> None:
        """Forward decoded ``stderr`` lines from ``source``."""
        await self._stream(source, LogStreamName.STDERR)

    async def forward_audit(self, source: AsyncIterator[bytes]) -> None:
        """Forward ``audit.jsonl`` lines from ``source`` as structured events.

        Each well-formed JSON object becomes an :class:`AuditEvent` with a
        populated ``payload``. Lines that are not valid JSON, or are valid
        JSON but not an object, are forwarded as ``malformed`` events so the
        audit trail is never silently dropped.
        """
        async for line in self._iter_lines(source):
            self._audit_seq += 1
            payload: dict[str, object] | None
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                payload, malformed = None, True
            else:
                if isinstance(parsed, dict):
                    payload, malformed = parsed, False
                else:
                    payload, malformed = None, True
            await self._sink.emit_audit(
                AuditEvent(
                    run_id=self._step.run_id,
                    step_id=self._step.step_id,
                    attempt=self._step.attempt,
                    seq=self._audit_seq,
                    raw=line,
                    payload=payload,
                    malformed=malformed,
                )
            )

    async def _stream(self, source: AsyncIterator[bytes], name: LogStreamName) -> None:
        async for line in self._iter_lines(source):
            seq = self._next_log_seq(name)
            await self._sink.emit_log(
                LogLine(
                    stream=name,
                    message=line,
                    run_id=self._step.run_id,
                    step_id=self._step.step_id,
                    attempt=self._step.attempt,
                    seq=seq,
                )
            )

    def _next_log_seq(self, name: LogStreamName) -> int:
        if name is LogStreamName.STDOUT:
            self._stdout_seq += 1
            return self._stdout_seq
        self._stderr_seq += 1
        return self._stderr_seq

    async def _iter_lines(self, source: AsyncIterator[bytes]) -> AsyncIterator[str]:
        """Yield decoded lines from a stream of raw byte chunks.

        Complete lines are yielded directly out of the incoming chunk;
        only the trailing partial line (the bytes after the last newline)
        is carried over to the next chunk. That partial is itself bounded
        to ``max_line_bytes`` by flushing fixed-size segments, so memory
        stays tied to the current line rather than the chunk or run length.
        """
        partial = bytearray()
        async for chunk in source:
            if partial:
                data: bytes = bytes(partial) + chunk
                partial.clear()
            else:
                data = chunk
            start = 0
            while (newline := data.find(b"\n", start)) != -1:
                yield self._decode(data[start:newline])
                start = newline + 1
            remainder = data[start:]
            while len(remainder) >= self._max_line_bytes:
                yield self._decode(remainder[: self._max_line_bytes])
                remainder = remainder[self._max_line_bytes :]
            partial.extend(remainder)
        if partial:
            yield self._decode(bytes(partial))

    @staticmethod
    def _decode(raw: bytes) -> str:
        """Decode a line as UTF-8 (lossy) with a trailing ``\\r`` stripped."""
        return raw.decode("utf-8", errors="replace").rstrip("\r")
