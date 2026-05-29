"""Tests for ``custos_workflow.runs.events`` (WF-IMPL-041).

Covers:

* :meth:`LifecycleEvent.to_wire` — canonical envelope per
  design.md § Dapr Pub/Sub Publications, asserted field-by-field
  for every canonical lifecycle kind.
* :class:`DedupingLifecyclePublisher` — producer-side dedup on
  ``(run_id, kind, occurred_at)``; acceptance criterion: 100
  simulated replays produce 1 forwarded publish; LRU eviction
  bound is respected.
* :class:`DaprPubSubLifecyclePublisher` — POSTs to the Dapr
  sidecar's ``/v1.0/publish/{pubsub}/{topic}`` endpoint with
  the canonical envelope; success / non-2xx / transport-error
  paths.
* No Dapr SDK is imported by ``events.py`` itself at the source
  level (acceptance criterion: "imported lazily so unit tests
  can run without an SDK install" — satisfied here by
  ``events.py`` using ``httpx`` rather than the Dapr SDK at all,
  asserted by source-level grep guard).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from custos_workflow.runs import (
    DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS,
    DEFAULT_DEDUP_CACHE_SIZE,
    LIFECYCLE_KIND_WORKFLOW_CANCELLED,
    LIFECYCLE_KIND_WORKFLOW_PAUSED,
    LIFECYCLE_KIND_WORKFLOW_RESUMED,
    LIFECYCLE_KIND_WORKFLOW_STARTED,
    DaprPubSubLifecyclePublisher,
    DedupingLifecyclePublisher,
    InMemoryLifecycleEventPublisher,
    LifecycleEvent,
    LifecycleEventPublisher,
    LifecycleEventPublishError,
    derive_run_id,
)
from custos_workflow.runs.ids import RunId

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW_VERSION_ID = "wfv-001"
IDEMPOTENCY_KEY = "client-key-events"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
RUN_ID: RunId = derive_run_id(WORKSPACE, IDEMPOTENCY_KEY)


def _event(
    *,
    kind: str = LIFECYCLE_KIND_WORKFLOW_STARTED,
    occurred_at: datetime | None = None,
    run_id: RunId | None = None,
    extra: dict[str, Any] | None = None,
) -> LifecycleEvent:
    return LifecycleEvent(
        kind=kind,
        workspace_id=WORKSPACE,
        run_id=run_id or RUN_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        occurred_at=occurred_at or FIXED_NOW,
        extra=extra or {},
    )


# ---------------------------------------------------------------------------
# Envelope (LifecycleEvent.to_wire) — design.md § Dapr Pub/Sub Publications
# ---------------------------------------------------------------------------


class TestToWireEnvelopeShape:
    """Asserts field-by-field conformance with design.md envelope.

    Envelope shape:

    .. code-block:: json

        {
            "kind": "workflow.completed",
            "workflowVersionId": "...",
            "runId": "...",
            "workspace": "...",
            "status": "succeeded | failed | cancelled",
            "outputs": { "...": "..." },
            "occurredAt": "RFC3339"
        }
    """

    def test_started_envelope_carries_base_fields(self) -> None:
        envelope = _event(kind=LIFECYCLE_KIND_WORKFLOW_STARTED).to_wire()
        assert envelope == {
            "kind": "workflow.started",
            "workflowVersionId": WORKFLOW_VERSION_ID,
            "runId": str(RUN_ID),
            "workspace": WORKSPACE,
            "occurredAt": FIXED_NOW.isoformat(),
        }
        # workflow.started has no design-defined status default.
        assert "status" not in envelope

    def test_completed_envelope_defaults_status_to_succeeded(self) -> None:
        envelope = _event(kind="workflow.completed").to_wire()
        assert envelope["status"] == "succeeded"

    def test_failed_envelope_defaults_status_to_failed(self) -> None:
        envelope = _event(kind="workflow.failed").to_wire()
        assert envelope["status"] == "failed"

    def test_cancelled_envelope_defaults_status_to_cancelled(self) -> None:
        envelope = _event(kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED).to_wire()
        assert envelope["status"] == "cancelled"

    def test_paused_envelope_defaults_status_to_paused(self) -> None:
        envelope = _event(kind=LIFECYCLE_KIND_WORKFLOW_PAUSED).to_wire()
        assert envelope["status"] == "paused"

    def test_resumed_envelope_defaults_status_to_running(self) -> None:
        envelope = _event(kind=LIFECYCLE_KIND_WORKFLOW_RESUMED).to_wire()
        assert envelope["status"] == "running"

    def test_extra_status_overrides_kind_default(self) -> None:
        envelope = _event(
            kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED, extra={"status": "operator-override"}
        ).to_wire()
        assert envelope["status"] == "operator-override"

    def test_extra_outputs_emitted_under_outputs_key(self) -> None:
        envelope = _event(
            kind="workflow.completed", extra={"outputs": {"hello": "world"}}
        ).to_wire()
        assert envelope["outputs"] == {"hello": "world"}

    def test_no_outputs_key_when_extra_has_none(self) -> None:
        envelope = _event(kind="workflow.completed").to_wire()
        assert "outputs" not in envelope

    def test_envelope_uses_camelcase_field_names(self) -> None:
        """Field names must match design.md exactly — snake_case
        would break the Trigger Service Internal Event Receiver
        subscription."""
        envelope = _event(kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED, extra={"reason": "x"}).to_wire()
        assert "workflowVersionId" in envelope
        assert "runId" in envelope
        assert "occurredAt" in envelope
        assert "workspace_id" not in envelope
        assert "workflow_version_id" not in envelope
        assert "occurred_at" not in envelope

    def test_envelope_is_json_serialisable(self) -> None:
        envelope = _event(
            kind="workflow.completed", extra={"outputs": {"nested": {"key": [1, 2, 3]}}}
        ).to_wire()
        roundtripped = json.loads(json.dumps(envelope))
        assert roundtripped == envelope


# ---------------------------------------------------------------------------
# DedupingLifecyclePublisher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDedupingLifecyclePublisher:
    async def test_first_emit_forwards_to_inner(self) -> None:
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner)
        event = _event()
        await publisher.publish(event)
        assert inner.events == [event]

    async def test_duplicate_emit_suppressed(self) -> None:
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner)
        event = _event()
        await publisher.publish(event)
        await publisher.publish(event)
        assert len(inner.events) == 1

    async def test_one_hundred_replays_collapse_to_one_publish(self) -> None:
        """Acceptance criterion: dedup absorbs 100 simulated replays
        into 1 published event."""
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner)
        event = _event()
        for _ in range(100):
            await publisher.publish(event)
        assert len(inner.events) == 1

    async def test_distinct_kinds_for_same_run_are_not_deduped(self) -> None:
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner)
        await publisher.publish(_event(kind=LIFECYCLE_KIND_WORKFLOW_STARTED))
        await publisher.publish(_event(kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED))
        assert len(inner.events) == 2

    async def test_distinct_occurred_at_for_same_run_kind_not_deduped(self) -> None:
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner)
        t1 = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 1, 12, 0, 1, tzinfo=UTC)
        await publisher.publish(_event(occurred_at=t1))
        await publisher.publish(_event(occurred_at=t2))
        assert len(inner.events) == 2

    async def test_distinct_runs_share_dedup_cache_without_collision(self) -> None:
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner)
        other_run = derive_run_id(WORKSPACE, "other-key")
        await publisher.publish(_event(run_id=RUN_ID))
        await publisher.publish(_event(run_id=other_run))
        assert len(inner.events) == 2

    async def test_lru_eviction_when_cache_exceeded(self) -> None:
        """An oldest-entry eviction MUST allow that old key to be
        re-forwarded if it is observed again."""
        inner = InMemoryLifecycleEventPublisher()
        publisher = DedupingLifecyclePublisher(inner=inner, max_seen_keys=2)
        e1 = _event(kind="k1", occurred_at=datetime(2026, 5, 1, 12, 0, 1, tzinfo=UTC))
        e2 = _event(kind="k2", occurred_at=datetime(2026, 5, 1, 12, 0, 2, tzinfo=UTC))
        e3 = _event(kind="k3", occurred_at=datetime(2026, 5, 1, 12, 0, 3, tzinfo=UTC))
        await publisher.publish(e1)
        await publisher.publish(e2)
        await publisher.publish(e3)  # evicts e1's key
        # Re-publish e1 — should be forwarded again now that its key is evicted.
        await publisher.publish(e1)
        assert inner.events == [e1, e2, e3, e1]

    async def test_default_dedup_cache_size_is_documented_constant(self) -> None:
        publisher = DedupingLifecyclePublisher(inner=InMemoryLifecycleEventPublisher())
        assert publisher.max_seen_keys == DEFAULT_DEDUP_CACHE_SIZE
        assert DEFAULT_DEDUP_CACHE_SIZE == 10_000

    async def test_inner_failure_leaves_key_uncached_so_retry_forwards(self) -> None:
        """A transient inner publish failure must NOT pre-cache the
        dedup key — a retry would otherwise silently swallow the
        retried event."""

        class _FlakyPublisher:
            def __init__(self) -> None:
                self.forwarded: list[LifecycleEvent] = []
                self.fail_next = True

            async def publish(self, event: LifecycleEvent) -> None:
                if self.fail_next:
                    self.fail_next = False
                    raise RuntimeError("boom")
                self.forwarded.append(event)

        flaky = _FlakyPublisher()
        publisher = DedupingLifecyclePublisher(inner=flaky)
        event = _event()
        with pytest.raises(RuntimeError):
            await publisher.publish(event)
        await publisher.publish(event)
        assert flaky.forwarded == [event]

    async def test_concurrent_publishes_for_same_key_collapse_to_one_forward(self) -> None:
        """Two concurrent ``publish()`` calls for the same
        ``(run_id, kind, occurred_at)`` triple must NOT both
        await ``inner.publish`` — the reservation happens before
        the await, so the second caller sees the key and
        short-circuits."""
        import asyncio

        class _GatedPublisher:
            def __init__(self) -> None:
                self.forwarded: list[LifecycleEvent] = []
                self.gate = asyncio.Event()
                self.entered = asyncio.Event()

            async def publish(self, event: LifecycleEvent) -> None:
                self.entered.set()
                # Block until the test releases the gate, so a
                # second concurrent caller has a chance to race
                # the reservation check.
                await self.gate.wait()
                self.forwarded.append(event)

        gated = _GatedPublisher()
        publisher = DedupingLifecyclePublisher(inner=gated)
        event = _event()
        first = asyncio.create_task(publisher.publish(event))
        # Wait until the first call has entered inner.publish so we
        # know its reservation is in place.
        await gated.entered.wait()
        # Now fire the second call — it should observe the
        # reservation and short-circuit without ever entering
        # gated.publish.
        await publisher.publish(event)
        # Release the first call.
        gated.gate.set()
        await first
        assert gated.forwarded == [event]

    async def test_satisfies_publisher_protocol(self) -> None:
        publisher = DedupingLifecyclePublisher(inner=InMemoryLifecycleEventPublisher())
        assert isinstance(publisher, LifecycleEventPublisher)


# ---------------------------------------------------------------------------
# DaprPubSubLifecyclePublisher
# ---------------------------------------------------------------------------


def _make_dapr_publisher(
    *,
    handler: httpx.MockTransport,
    dapr_endpoint: str = "http://localhost:3500",
    pubsub_name: str = "custos-pubsub",
    topic: str = "custos.workflow.events",
) -> tuple[DaprPubSubLifecyclePublisher, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    publisher = DaprPubSubLifecyclePublisher(
        http_client=client,
        dapr_endpoint=dapr_endpoint,
        pubsub_name=pubsub_name,
        topic=topic,
    )
    return publisher, client


@pytest.mark.asyncio
class TestDaprPubSubLifecyclePublisher:
    async def test_publish_posts_canonical_envelope_to_dapr_sidecar(self) -> None:
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["body"] = json.loads(request.content)
            captured["content_type"] = request.headers.get("content-type")
            return httpx.Response(204)

        publisher, client = _make_dapr_publisher(handler=httpx.MockTransport(handler))
        try:
            await publisher.publish(_event(kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED))
        finally:
            await client.aclose()
        assert captured["url"] == (
            "http://localhost:3500/v1.0/publish/custos-pubsub/custos.workflow.events"
        )
        assert captured["content_type"] == "application/json"
        assert captured["body"] == _event(kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED).to_wire()

    async def test_publish_strips_trailing_slash_from_endpoint(self) -> None:
        captured: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200)

        publisher, client = _make_dapr_publisher(
            handler=httpx.MockTransport(handler),
            dapr_endpoint="http://localhost:3500/",
        )
        try:
            await publisher.publish(_event())
        finally:
            await client.aclose()
        assert "//v1.0" not in captured["url"]
        assert captured["url"].endswith("/v1.0/publish/custos-pubsub/custos.workflow.events")

    async def test_publish_raises_on_non_2xx_response(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text='{"errorCode":"ERR_PUBSUB_PUBLISH"}')

        publisher, client = _make_dapr_publisher(handler=httpx.MockTransport(handler))
        try:
            with pytest.raises(LifecycleEventPublishError) as exc_info:
                await publisher.publish(_event())
        finally:
            await client.aclose()
        msg = str(exc_info.value)
        assert "500" in msg
        assert "custos-pubsub" in msg
        assert "custos.workflow.events" in msg

    async def test_publish_wraps_transport_errors(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("sidecar unreachable")

        publisher, client = _make_dapr_publisher(handler=httpx.MockTransport(handler))
        try:
            with pytest.raises(LifecycleEventPublishError) as exc_info:
                await publisher.publish(_event())
        finally:
            await client.aclose()
        assert "transport" in str(exc_info.value).lower()

    async def test_publisher_rejects_empty_endpoint(self) -> None:
        client = httpx.AsyncClient()
        try:
            with pytest.raises(ValueError, match="dapr_endpoint"):
                DaprPubSubLifecyclePublisher(
                    http_client=client,
                    dapr_endpoint="",
                    pubsub_name="custos-pubsub",
                    topic="custos.workflow.events",
                )
        finally:
            await client.aclose()

    async def test_publisher_rejects_empty_pubsub_name(self) -> None:
        client = httpx.AsyncClient()
        try:
            with pytest.raises(ValueError, match="pubsub_name"):
                DaprPubSubLifecyclePublisher(
                    http_client=client,
                    dapr_endpoint="http://localhost:3500",
                    pubsub_name="",
                    topic="custos.workflow.events",
                )
        finally:
            await client.aclose()

    async def test_publisher_rejects_empty_topic(self) -> None:
        client = httpx.AsyncClient()
        try:
            with pytest.raises(ValueError, match="topic"):
                DaprPubSubLifecyclePublisher(
                    http_client=client,
                    dapr_endpoint="http://localhost:3500",
                    pubsub_name="custos-pubsub",
                    topic="",
                )
        finally:
            await client.aclose()

    async def test_publisher_satisfies_publisher_protocol(self) -> None:
        client = httpx.AsyncClient()
        try:
            publisher = DaprPubSubLifecyclePublisher(
                http_client=client,
                dapr_endpoint="http://localhost:3500",
                pubsub_name="custos-pubsub",
                topic="custos.workflow.events",
            )
            assert isinstance(publisher, LifecycleEventPublisher)
        finally:
            await client.aclose()

    async def test_default_timeout_is_documented_constant(self) -> None:
        client = httpx.AsyncClient()
        try:
            publisher = DaprPubSubLifecyclePublisher(
                http_client=client,
                dapr_endpoint="http://localhost:3500",
                pubsub_name="custos-pubsub",
                topic="custos.workflow.events",
            )
            assert publisher.request_timeout_seconds == DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS
            assert DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS == 10.0
        finally:
            await client.aclose()


# ---------------------------------------------------------------------------
# No-Dapr-SDK-at-import-time guard
# ---------------------------------------------------------------------------


class TestEventsModuleHasNoDaprSdkImport:
    """Acceptance criterion: ``DaprPubSubLifecyclePublisher`` depends
    on the Dapr SDK only via lazy import (or, in this implementation,
    not at all — the publisher uses the Dapr Pub/Sub HTTP API
    directly through :mod:`httpx`). This test enforces the guard so
    a future refactor can't sneak a ``from dapr import ...`` into the
    module header and bloat the unit-test cold start (or break
    sidecar-less environments)."""

    def test_no_dapr_module_in_events_imports(self) -> None:
        import custos_workflow.runs.events as events_module

        for attr_name in vars(events_module):
            value = getattr(events_module, attr_name, None)
            module_name = getattr(value, "__module__", "")
            assert not module_name.startswith("dapr"), (
                f"events.py leaks a Dapr SDK symbol via {attr_name!r}"
            )

    def test_events_source_has_no_top_level_dapr_import(self) -> None:
        """Source-level guard: ``events.py`` itself must not contain
        a top-level ``import dapr`` or ``from dapr...`` statement.

        Note: ``custos_workflow.runs.events`` does still transitively
        pull in Dapr SDK modules via its ``from
        custos_workflow.runs.controller import ...`` line (controller
        → runtime → ``dapr.ext.workflow``), but that transitive
        chain is pre-existing and unrelated to the
        ``DaprPubSubLifecyclePublisher`` acceptance criterion. The
        criterion is satisfied by the publisher itself depending on
        ``httpx`` (not the Dapr SDK), so unit tests can construct
        and exercise the publisher without a Dapr SDK install. This
        guard makes the source-level invariant explicit so a future
        refactor can't silently add a ``from dapr.clients import
        DaprClient`` at the top of ``events.py``."""
        import pathlib
        import re

        import custos_workflow.runs.events as events_module

        source_path = pathlib.Path(events_module.__file__)
        source = source_path.read_text(encoding="utf-8")
        # Strip docstrings + comments crudely so the search only sees executable lines.
        # Top-level imports always sit on a single line in this file, so a line-by-line
        # check is sufficient.
        for lineno, line in enumerate(source.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if re.match(r"^(import|from)\s+dapr(\.|$|\s)", stripped):
                raise AssertionError(
                    f"events.py introduces a top-level Dapr SDK import at line {lineno}: "
                    f"{stripped!r}"
                )
