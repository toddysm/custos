"""Tests for the Log Streamer (ARM-IMPL-012)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest

from custos_arm.contract import StepRef
from custos_arm.logs import (
    DEFAULT_MAX_LINE_BYTES,
    AuditEvent,
    LogLine,
    LogSink,
    LogStreamer,
    LogStreamName,
)


class _RecordingSink:
    """In-memory :class:`LogSink` that records everything it receives."""

    def __init__(self) -> None:
        self.logs: list[LogLine] = []
        self.audits: list[AuditEvent] = []

    async def emit_log(self, line: LogLine) -> None:
        self.logs.append(line)

    async def emit_audit(self, event: AuditEvent) -> None:
        self.audits.append(event)


async def _chunks(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


def _step() -> StepRef:
    return StepRef(runId="run-1", stepId="step-1", attempt=2)


def _streamer(sink: LogSink, **kwargs: int) -> LogStreamer:
    return LogStreamer(sink=sink, step=_step(), **kwargs)


# ---------------------------------------------------------------------------
# Protocol / construction
# ---------------------------------------------------------------------------


def test_recording_sink_satisfies_protocol() -> None:
    assert isinstance(_RecordingSink(), LogSink)


def test_non_positive_max_line_bytes_rejected() -> None:
    with pytest.raises(ValueError, match="max_line_bytes must be positive"):
        LogStreamer(sink=_RecordingSink(), step=_step(), max_line_bytes=0)


# ---------------------------------------------------------------------------
# stdout / stderr streaming
# ---------------------------------------------------------------------------


async def test_stdout_lines_decoded_and_sequenced() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks(b"alpha\nbeta\ngamma\n"))

    assert [line.message for line in sink.logs] == ["alpha", "beta", "gamma"]
    assert all(line.stream is LogStreamName.STDOUT for line in sink.logs)
    assert [line.seq for line in sink.logs] == [1, 2, 3]
    first = sink.logs[0]
    assert (first.run_id, first.step_id, first.attempt) == ("run-1", "step-1", 2)


async def test_lines_reassembled_across_chunk_boundaries() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks(b"hel", b"lo\nwor", b"ld\n"))

    assert [line.message for line in sink.logs] == ["hello", "world"]


async def test_trailing_partial_line_without_newline_is_flushed() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks(b"no-newline-at-eof"))

    assert [line.message for line in sink.logs] == ["no-newline-at-eof"]


async def test_crlf_carriage_return_is_stripped() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks(b"windows\r\nline\r\n"))

    assert [line.message for line in sink.logs] == ["windows", "line"]


async def test_blank_lines_are_preserved() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks(b"a\n\nb\n"))

    assert [line.message for line in sink.logs] == ["a", "", "b"]


async def test_invalid_utf8_is_replaced_not_fatal() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks(b"caf\xff\n"))

    assert sink.logs[0].message == "caf\ufffd"


async def test_stderr_uses_independent_sequence() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stderr(_chunks(b"err1\nerr2\n"))

    assert all(line.stream is LogStreamName.STDERR for line in sink.logs)
    assert [line.seq for line in sink.logs] == [1, 2]


async def test_stdout_and_stderr_sequences_are_separate() -> None:
    sink = _RecordingSink()
    streamer = _streamer(sink)
    await streamer.stream_stdout(_chunks(b"out1\nout2\n"))
    await streamer.stream_stderr(_chunks(b"err1\n"))

    stdout_seqs = [log.seq for log in sink.logs if log.stream is LogStreamName.STDOUT]
    stderr_seqs = [log.seq for log in sink.logs if log.stream is LogStreamName.STDERR]
    assert stdout_seqs == [1, 2]
    assert stderr_seqs == [1]


async def test_empty_source_emits_nothing() -> None:
    sink = _RecordingSink()
    await _streamer(sink).stream_stdout(_chunks())

    assert sink.logs == []


# ---------------------------------------------------------------------------
# Bounded buffering
# ---------------------------------------------------------------------------


async def test_long_line_without_newline_is_flushed_in_bounded_segments() -> None:
    sink = _RecordingSink()
    payload = b"x" * 25
    await _streamer(sink, max_line_bytes=10).stream_stdout(_chunks(payload))

    # 25 bytes / cap 10 -> two full 10-byte segments plus a 5-byte remainder.
    assert [line.message for line in sink.logs] == ["x" * 10, "x" * 10, "x" * 5]


async def test_default_max_line_bytes_is_64_kib() -> None:
    assert DEFAULT_MAX_LINE_BYTES == 64 * 1024


# ---------------------------------------------------------------------------
# audit.jsonl forwarding
# ---------------------------------------------------------------------------


async def test_audit_lines_parsed_as_structured_events() -> None:
    sink = _RecordingSink()
    lines = (
        json.dumps({"event": "scan.started", "n": 1}).encode()
        + b"\n"
        + json.dumps({"event": "scan.finished", "n": 2}).encode()
        + b"\n"
    )
    await _streamer(sink).forward_audit(_chunks(lines))

    assert [event.payload for event in sink.audits] == [
        {"event": "scan.started", "n": 1},
        {"event": "scan.finished", "n": 2},
    ]
    assert [event.seq for event in sink.audits] == [1, 2]
    assert all(not event.malformed for event in sink.audits)
    first = sink.audits[0]
    assert (first.run_id, first.step_id, first.attempt) == ("run-1", "step-1", 2)


async def test_malformed_audit_line_forwarded_not_dropped() -> None:
    sink = _RecordingSink()
    await _streamer(sink).forward_audit(_chunks(b"{not json}\n"))

    assert len(sink.audits) == 1
    event = sink.audits[0]
    assert event.malformed is True
    assert event.payload is None
    assert event.raw == "{not json}"


async def test_non_object_json_audit_line_is_malformed() -> None:
    sink = _RecordingSink()
    await _streamer(sink).forward_audit(_chunks(b"[1, 2, 3]\n"))

    event = sink.audits[0]
    assert event.malformed is True
    assert event.payload is None
    assert event.raw == "[1, 2, 3]"


async def test_audit_raw_preserved_for_valid_lines() -> None:
    sink = _RecordingSink()
    await _streamer(sink).forward_audit(_chunks(b'{"k": "v"}\n'))

    assert sink.audits[0].raw == '{"k": "v"}'


# ---------------------------------------------------------------------------
# run() fan-out
# ---------------------------------------------------------------------------


async def test_run_forwards_all_three_sources() -> None:
    sink = _RecordingSink()
    await _streamer(sink).run(
        stdout=_chunks(b"out\n"),
        stderr=_chunks(b"err\n"),
        audit=_chunks(b'{"event": "x"}\n'),
    )

    assert {log.message for log in sink.logs} == {"out", "err"}
    assert sink.audits[0].payload == {"event": "x"}


async def test_run_skips_omitted_sources() -> None:
    sink = _RecordingSink()
    await _streamer(sink).run(stdout=_chunks(b"only-out\n"))

    assert [log.message for log in sink.logs] == ["only-out"]
    assert sink.audits == []


async def test_run_with_no_sources_is_a_noop() -> None:
    sink = _RecordingSink()
    await _streamer(sink).run()

    assert sink.logs == []
    assert sink.audits == []
