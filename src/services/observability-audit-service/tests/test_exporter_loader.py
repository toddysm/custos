"""Tests for the External Exporter Loader runtime (OBS-IMPL-011).

Cover the three acceptance guarantees: an observed customer block triggers
merge → write → reload-signal → ``applied`` event; a bad block emits
``rejected`` and leaves the running (last-written) config untouched; and a good
block emits ``applied``. Also cover the no-op (unchanged) path, the
guarded-run resilience, the best-effort emit, and the start/stop lifecycle.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
import yaml
from custos_spl import AuditEvent

from custos_obs.events import ObsEventName
from custos_obs.exporters.loader import ExporterLoader, ReconcileOutcome

BASE_CONFIG = """
receivers:
  otlp:
    protocols:
      grpc: {}
exporters:
  logging: {}
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [logging]
    metrics:
      receivers: [otlp]
      exporters: [logging]
    traces:
      receivers: [otlp]
      exporters: [logging]
"""

CUSTOMER_BLOCK = """
exporters:
  loki/customer:
    endpoint: https://loki.example/loki/api/v1/push
pipelines:
  logs: [loki/customer]
"""

INVALID_BLOCK = "exporters:\n  bad/name/extra: {}\n"

EXPORTERS_CONFIGMAP = "custos-otel-exporters"


def _parse(config: str) -> dict[str, Any]:
    loaded = yaml.safe_load(config)
    assert isinstance(loaded, dict)
    return loaded


class _RecordingWriter:
    """Captures every effective config written."""

    def __init__(self) -> None:
        self.writes: list[str] = []
        self.fail = False

    async def write(self, effective_config: str) -> None:
        if self.fail:
            raise RuntimeError("write boom")
        self.writes.append(effective_config)


class _RecordingSignaller:
    """Counts reload signals."""

    def __init__(self) -> None:
        self.reloads = 0
        self.fail = False

    async def signal_reload(self) -> None:
        if self.fail:
            raise RuntimeError("reload boom")
        self.reloads += 1


class _ListSource:
    """Yields a fixed list of customer blocks, then completes."""

    def __init__(self, blocks: list[str | None]) -> None:
        self._blocks = blocks

    async def watch(self) -> AsyncIterator[str | None]:
        for block in self._blocks:
            yield block


class _RecordingEmitter:
    """Captures emitted audit events; can be made to fail or cancel."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []
        self.fail = False
        self.cancel = False

    async def __call__(self, event: AuditEvent) -> None:
        if self.cancel:
            raise asyncio.CancelledError
        if self.fail:
            raise RuntimeError("emit boom")
        self.events.append(event)


def _loader(
    *,
    source: _ListSource | None = None,
    writer: _RecordingWriter | None = None,
    signaller: _RecordingSignaller | None = None,
    emitter: _RecordingEmitter | None = None,
) -> ExporterLoader:
    return ExporterLoader(
        base_config=BASE_CONFIG,
        source=source if source is not None else _ListSource([]),
        writer=writer if writer is not None else _RecordingWriter(),
        signaller=signaller if signaller is not None else _RecordingSignaller(),
        emit_event=emitter if emitter is not None else _RecordingEmitter(),
        exporters_configmap=EXPORTERS_CONFIGMAP,
    )


# --------------------------------------------------------------------------- #
# reconcile — good change                                                     #
# --------------------------------------------------------------------------- #


async def test_reconcile_good_change_writes_reloads_and_emits_applied() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    emitter = _RecordingEmitter()
    loader = _loader(writer=writer, signaller=signaller, emitter=emitter)

    outcome = loader.reconcile(CUSTOMER_BLOCK)
    result = await outcome

    assert isinstance(result, ReconcileOutcome)
    assert result.applied is True
    assert result.reloaded is True
    assert result.rejected is False
    assert result.rejection_reason is None
    assert result.exporter_names == ("loki/customer",)

    assert len(writer.writes) == 1
    assert "loki/customer" in writer.writes[0]
    assert signaller.reloads == 1
    assert loader.last_written == writer.writes[0]

    assert len(emitter.events) == 1
    event = emitter.events[0]
    assert event.event_type == ObsEventName.EXPORTER_CONFIG_APPLIED.value
    assert event.payload["configmap"] == EXPORTERS_CONFIGMAP
    assert event.payload["exporter_names"] == ["loki/customer"]


async def test_reconcile_writes_before_signalling_reload() -> None:
    order: list[str] = []

    class _OrderedWriter:
        async def write(self, effective_config: str) -> None:
            order.append("write")

    class _OrderedSignaller:
        async def signal_reload(self) -> None:
            order.append("reload")

    loader = ExporterLoader(
        base_config=BASE_CONFIG,
        source=_ListSource([]),
        writer=_OrderedWriter(),
        signaller=_OrderedSignaller(),
        emit_event=_RecordingEmitter(),
        exporters_configmap=EXPORTERS_CONFIGMAP,
    )
    await loader.reconcile(CUSTOMER_BLOCK)
    assert order == ["write", "reload"]


async def test_reconcile_effective_config_is_valid_merged_yaml() -> None:
    writer = _RecordingWriter()
    loader = _loader(writer=writer)
    await loader.reconcile(CUSTOMER_BLOCK)
    merged = _parse(writer.writes[0])
    assert set(merged["exporters"]) == {"logging", "loki/customer"}
    assert merged["service"]["pipelines"]["logs"]["exporters"] == ["logging", "loki/customer"]


# --------------------------------------------------------------------------- #
# reconcile — unchanged (no-op)                                               #
# --------------------------------------------------------------------------- #


async def test_reconcile_unchanged_config_is_a_noop() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    emitter = _RecordingEmitter()
    loader = _loader(writer=writer, signaller=signaller, emitter=emitter)

    await loader.reconcile(CUSTOMER_BLOCK)
    result = await loader.reconcile(CUSTOMER_BLOCK)

    assert result.applied is True
    assert result.reloaded is False
    assert result.exporter_names == ("loki/customer",)
    # Still only the single first write/reload/emit.
    assert len(writer.writes) == 1
    assert signaller.reloads == 1
    assert len(emitter.events) == 1


async def test_reconcile_none_block_on_fresh_loader_writes_base() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    emitter = _RecordingEmitter()
    loader = _loader(writer=writer, signaller=signaller, emitter=emitter)

    result = await loader.reconcile(None)

    assert result.applied is True
    assert result.reloaded is True
    assert result.exporter_names == ()
    merged = _parse(writer.writes[0])
    assert set(merged["exporters"]) == {"logging"}
    assert emitter.events[0].payload["exporter_names"] == []


# --------------------------------------------------------------------------- #
# reconcile — rejection + rollback                                            #
# --------------------------------------------------------------------------- #


async def test_reconcile_bad_block_emits_rejected_and_does_not_write() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    emitter = _RecordingEmitter()
    loader = _loader(writer=writer, signaller=signaller, emitter=emitter)

    result = await loader.reconcile(INVALID_BLOCK)

    assert result.applied is False
    assert result.rejected is True
    assert result.reloaded is False
    assert result.rejection_reason is not None
    assert result.exporter_names == ()

    assert writer.writes == []
    assert signaller.reloads == 0
    assert loader.last_written is None

    assert len(emitter.events) == 1
    event = emitter.events[0]
    assert event.event_type == ObsEventName.EXPORTER_CONFIG_REJECTED.value
    assert event.payload["configmap"] == EXPORTERS_CONFIGMAP
    assert event.payload["reason"] == result.rejection_reason


async def test_bad_block_after_good_keeps_last_written() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    loader = _loader(writer=writer, signaller=signaller)

    await loader.reconcile(CUSTOMER_BLOCK)
    good = loader.last_written
    assert good is not None

    result = await loader.reconcile(INVALID_BLOCK)

    assert result.rejected is True
    # Running config (last written) is untouched; no second write/reload.
    assert loader.last_written == good
    assert len(writer.writes) == 1
    assert signaller.reloads == 1


# --------------------------------------------------------------------------- #
# run — guarded reconcile over the source                                     #
# --------------------------------------------------------------------------- #


async def test_run_reconciles_each_observed_block() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    emitter = _RecordingEmitter()
    source = _ListSource([CUSTOMER_BLOCK, INVALID_BLOCK])
    loader = _loader(source=source, writer=writer, signaller=signaller, emitter=emitter)

    await loader.run()

    # One applied (good) + one rejected (bad).
    assert len(writer.writes) == 1
    assert signaller.reloads == 1
    kinds = [e.event_type for e in emitter.events]
    assert kinds == [
        ObsEventName.EXPORTER_CONFIG_APPLIED.value,
        ObsEventName.EXPORTER_CONFIG_REJECTED.value,
    ]


async def test_run_survives_write_failure_and_continues() -> None:
    writer = _RecordingWriter()
    writer.fail = True
    signaller = _RecordingSignaller()
    source = _ListSource([CUSTOMER_BLOCK])
    loader = _loader(source=source, writer=writer, signaller=signaller)

    # A write failure inside run() is logged and swallowed, not raised.
    await loader.run()

    assert signaller.reloads == 0
    assert loader.last_written is None


async def test_reconcile_write_failure_propagates_to_caller() -> None:
    writer = _RecordingWriter()
    writer.fail = True
    loader = _loader(writer=writer)
    with pytest.raises(RuntimeError, match="write boom"):
        await loader.reconcile(CUSTOMER_BLOCK)
    # last_written not advanced when the write fails.
    assert loader.last_written is None


async def test_reconcile_reload_failure_leaves_last_written_unset() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    signaller.fail = True
    loader = _loader(writer=writer, signaller=signaller)
    with pytest.raises(RuntimeError, match="reload boom"):
        await loader.reconcile(CUSTOMER_BLOCK)
    # The config was written, but the reload failed before last_written advanced,
    # so the next reconcile re-attempts write + reload.
    assert loader.last_written is None
    assert len(writer.writes) == 1


# --------------------------------------------------------------------------- #
# emit — best effort                                                          #
# --------------------------------------------------------------------------- #


async def test_emit_failure_does_not_abort_apply() -> None:
    writer = _RecordingWriter()
    signaller = _RecordingSignaller()
    emitter = _RecordingEmitter()
    emitter.fail = True
    loader = _loader(writer=writer, signaller=signaller, emitter=emitter)

    # A failing emit is swallowed; the apply still succeeds.
    result = await loader.reconcile(CUSTOMER_BLOCK)
    assert result.applied is True
    assert result.reloaded is True
    assert len(writer.writes) == 1
    assert loader.last_written == writer.writes[0]


async def test_emit_cancellation_propagates() -> None:
    emitter = _RecordingEmitter()
    emitter.cancel = True
    loader = _loader(emitter=emitter)
    with pytest.raises(asyncio.CancelledError):
        await loader.reconcile(CUSTOMER_BLOCK)


async def test_run_propagates_cancellation_through_guard() -> None:
    emitter = _RecordingEmitter()
    emitter.cancel = True
    source = _ListSource([CUSTOMER_BLOCK])
    loader = _loader(source=source, emitter=emitter)
    # A CancelledError raised while reconciling must propagate out of run()
    # (the guard re-raises it) rather than being swallowed as a transient error.
    with pytest.raises(asyncio.CancelledError):
        await loader.run()


# --------------------------------------------------------------------------- #
# lifecycle — start / stop                                                    #
# --------------------------------------------------------------------------- #


async def test_start_stop_drives_and_then_cancels_run() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _BlockingSource:
        async def watch(self) -> AsyncIterator[str | None]:
            yield CUSTOMER_BLOCK
            started.set()
            await release.wait()  # block so the task is still running at stop()
            yield None

    writer = _RecordingWriter()
    loader = ExporterLoader(
        base_config=BASE_CONFIG,
        source=_BlockingSource(),
        writer=writer,
        signaller=_RecordingSignaller(),
        emit_event=_RecordingEmitter(),
        exporters_configmap=EXPORTERS_CONFIGMAP,
    )

    loader.start()
    loader.start()  # idempotent — no second task
    await asyncio.wait_for(started.wait(), timeout=1.0)
    assert len(writer.writes) == 1

    await loader.stop()
    await loader.stop()  # idempotent — already stopped


async def test_stop_without_start_is_a_noop() -> None:
    loader = _loader()
    await loader.stop()
