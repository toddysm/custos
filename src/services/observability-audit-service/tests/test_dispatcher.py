"""Tests for the alerting dispatcher (OBS-IMPL-009).

Cover the payload shaping, the two M1 sinks (webhook + SMTP) including their
concrete transports, and the dispatcher's retry / backoff / dead-letter /
outcome-event behaviour.
"""

from __future__ import annotations

import asyncio
import smtplib
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId

from custos_obs.alerting.dispatcher import (
    AlertDispatcher,
    AlertPayload,
    AlertSinkError,
    DeadLetterRecord,
    HttpxWebhookTransport,
    SmtpEmailTransport,
    SmtpSink,
    WebhookSink,
)
from custos_obs.alerting.rules import AlertMatch, AlertRule, MatchableEvent

FIXED_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Helpers / fakes                                                             #
# --------------------------------------------------------------------------- #


def _audit_event(*, event_id: str = "evt-1", event_type: str = "authz.decision") -> AuditEvent:
    return AuditEvent(
        workspace_id=WorkspaceId("ws-1"),
        event_id=event_id,
        event_type=event_type,
        actor="user:alice",
        subject={"component": "authz", "severity": "warning"},
        payload={"decision": "deny"},
        occurred_at=FIXED_NOW,
    )


def _match(*, rule_name: str = "rule-a", sinks: tuple[str, ...] = ("webhook",)) -> AlertMatch:
    rule = AlertRule(name=rule_name, sinks=sinks, event_name="authz.decision")
    event = MatchableEvent.from_audit_event(_audit_event())
    return AlertMatch(rule=rule, event=event, dedup_identity=(rule_name,))


class _RecordingSink:
    """A sink that succeeds after ``fail_times`` failures (or always fails)."""

    def __init__(self, name: str, *, fail_times: int = 0, always_fail: bool = False) -> None:
        self.name = name
        self._fail_times = fail_times
        self._always_fail = always_fail
        self.calls = 0

    async def deliver(self, payload: AlertPayload) -> None:
        self.calls += 1
        if self._always_fail or self.calls <= self._fail_times:
            raise AlertSinkError(f"{self.name} attempt {self.calls} failed")


class _CancellingSink:
    name = "webhook"

    async def deliver(self, payload: AlertPayload) -> None:
        raise asyncio.CancelledError


class _FakeDeadLetter:
    def __init__(self) -> None:
        self.records: list[DeadLetterRecord] = []

    async def record(self, record: DeadLetterRecord) -> None:
        self.records.append(record)


def _emitter() -> tuple[Any, list[AuditEvent]]:
    emitted: list[AuditEvent] = []

    async def emit(event: AuditEvent) -> None:
        emitted.append(event)

    return emit, emitted


def _sleeper() -> tuple[Any, list[float]]:
    slept: list[float] = []

    async def sleep(delay: float) -> None:
        slept.append(delay)

    return sleep, slept


# --------------------------------------------------------------------------- #
# AlertPayload                                                                 #
# --------------------------------------------------------------------------- #


def test_payload_build_and_webhook_body() -> None:
    payload = AlertPayload.build(_audit_event(), rule_name="rule-a", sink="webhook")
    body = payload.to_body()
    assert body["rule"] == "rule-a"
    assert body["event"]["eventId"] == "evt-1"
    assert body["event"]["eventType"] == "authz.decision"
    assert body["event"]["workspaceId"] == "ws-1"
    assert body["event"]["occurredAt"] == FIXED_NOW.isoformat()
    assert body["event"]["payload"] == {"decision": "deny"}


def test_payload_email_rendering() -> None:
    payload = AlertPayload.build(_audit_event(), rule_name="rule-a", sink="smtp")
    assert payload.email_subject() == "[Custos alert] rule-a: authz.decision"
    body = payload.email_body()
    assert "rule-a" in body
    assert "evt-1" in body
    assert "ws-1" in body


# --------------------------------------------------------------------------- #
# Sinks                                                                        #
# --------------------------------------------------------------------------- #


async def test_webhook_sink_posts_every_url() -> None:
    posts: list[tuple[str, dict[str, Any]]] = []

    async def transport(url: str, body: Any) -> None:
        posts.append((url, dict(body)))

    sink = WebhookSink(urls=["https://a.example", "https://b.example"], transport=transport)
    await sink.deliver(AlertPayload.build(_audit_event(), rule_name="r", sink="webhook"))
    assert [u for u, _ in posts] == ["https://a.example", "https://b.example"]


async def test_webhook_sink_without_urls_raises() -> None:
    async def transport(url: str, body: Any) -> None:  # pragma: no cover - never called
        raise AssertionError

    sink = WebhookSink(urls=[], transport=transport)
    with pytest.raises(AlertSinkError, match="no webhook destinations"):
        await sink.deliver(AlertPayload.build(_audit_event(), rule_name="r", sink="webhook"))


async def test_webhook_sink_propagates_transport_error() -> None:
    async def transport(url: str, body: Any) -> None:
        raise AlertSinkError("boom")

    sink = WebhookSink(urls=["https://a.example"], transport=transport)
    with pytest.raises(AlertSinkError, match="boom"):
        await sink.deliver(AlertPayload.build(_audit_event(), rule_name="r", sink="webhook"))


async def test_smtp_sink_delivers_via_transport() -> None:
    sent: list[tuple[str, str]] = []

    async def transport(*, subject: str, body: str) -> None:
        sent.append((subject, body))

    sink = SmtpSink(transport=transport)
    await sink.deliver(AlertPayload.build(_audit_event(), rule_name="r", sink="smtp"))
    assert sent[0][0] == "[Custos alert] r: authz.decision"


# --------------------------------------------------------------------------- #
# Concrete transports                                                          #
# --------------------------------------------------------------------------- #


class _FakeResponse:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPError(f"status {self.status_code}")


class _FakeClient:
    def __init__(self, *, response: _FakeResponse | None = None, error: Exception | None = None):
        self._response = response or _FakeResponse()
        self._error = error
        self.posts: list[tuple[str, Any]] = []

    async def __aenter__(self) -> _FakeClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def post(self, url: str, *, json: Any) -> _FakeResponse:
        self.posts.append((url, json))
        if self._error is not None:
            raise self._error
        return self._response


async def test_httpx_webhook_transport_success_with_factory() -> None:
    client = _FakeClient()
    transport = HttpxWebhookTransport(client_factory=lambda: client)
    await transport("https://a.example", {"rule": "r"})
    assert client.posts == [("https://a.example", {"rule": "r"})]


async def test_httpx_webhook_transport_default_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient()
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: client)
    transport = HttpxWebhookTransport(timeout=2.0)
    await transport("https://a.example", {"rule": "r"})
    assert client.posts


async def test_httpx_webhook_transport_wraps_http_error() -> None:
    client = _FakeClient(error=httpx.ConnectError("refused"))
    transport = HttpxWebhookTransport(client_factory=lambda: client)
    with pytest.raises(AlertSinkError, match=r"webhook POST to https://a\.example failed"):
        await transport("https://a.example", {"rule": "r"})


async def test_httpx_webhook_transport_wraps_status_error() -> None:
    client = _FakeClient(response=_FakeResponse(500))
    transport = HttpxWebhookTransport(client_factory=lambda: client)
    with pytest.raises(AlertSinkError):
        await transport("https://a.example", {"rule": "r"})


class _FakeSMTP:
    last: _FakeSMTP | None = None

    def __init__(self, host: str, port: int, *, fail: bool = False) -> None:
        self.host = host
        self.port = port
        self.started = False
        self.logged_in: tuple[str, str] | None = None
        self.sent: Any = None
        self._fail = fail
        _FakeSMTP.last = self

    def __enter__(self) -> _FakeSMTP:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def starttls(self) -> None:
        self.started = True

    def login(self, username: str, password: str) -> None:
        self.logged_in = (username, password)

    def send_message(self, message: Any) -> None:
        if self._fail:
            raise OSError("relay down")
        self.sent = message


async def test_smtp_transport_sends_with_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    transport = SmtpEmailTransport(
        host="smtp.example",
        sender="alerts@example",
        recipients=["ops@example"],
        username="u",
        password="p",
    )
    await transport(subject="subj", body="body")
    smtp = _FakeSMTP.last
    assert smtp is not None
    assert smtp.started is True
    assert smtp.logged_in == ("u", "p")
    assert smtp.sent["Subject"] == "subj"
    assert smtp.sent["To"] == "ops@example"


async def test_smtp_transport_without_credentials_skips_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    transport = SmtpEmailTransport(
        host="smtp.example",
        sender="alerts@example",
        recipients=["ops@example"],
        use_starttls=False,
    )
    await transport(subject="subj", body="body")
    smtp = _FakeSMTP.last
    assert smtp is not None
    assert smtp.started is False
    assert smtp.logged_in is None


async def test_smtp_transport_wraps_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smtplib, "SMTP", lambda host, port: _FakeSMTP(host, port, fail=True))
    transport = SmtpEmailTransport(
        host="smtp.example", sender="alerts@example", recipients=["ops@example"]
    )
    with pytest.raises(AlertSinkError, match=r"SMTP send via smtp\.example failed"):
        await transport(subject="subj", body="body")


def test_smtp_transport_requires_recipients() -> None:
    with pytest.raises(ValueError, match="at least one recipient"):
        SmtpEmailTransport(host="smtp.example", sender="a@example", recipients=[])


# --------------------------------------------------------------------------- #
# AlertDispatcher                                                              #
# --------------------------------------------------------------------------- #


def _dispatcher(
    sinks: dict[str, Any],
    *,
    dead_letter: _FakeDeadLetter,
    emit: Any,
    sleep: Any,
    max_attempts: int = 5,
    base_backoff_s: float = 1.0,
    max_backoff_s: float = 30.0,
) -> AlertDispatcher:
    return AlertDispatcher(
        sinks=sinks,
        dead_letter=dead_letter,
        emit_event=emit,
        max_attempts=max_attempts,
        base_backoff_s=base_backoff_s,
        max_backoff_s=max_backoff_s,
        sleep=sleep,
        now=lambda: FIXED_NOW,
    )


def test_dispatcher_rejects_zero_attempts() -> None:
    dlq = _FakeDeadLetter()
    emit, _ = _emitter()
    with pytest.raises(ValueError, match="at least 1"):
        AlertDispatcher(sinks={}, dead_letter=dlq, emit_event=emit, max_attempts=0)


def test_dispatcher_defaults_are_usable() -> None:
    dlq = _FakeDeadLetter()
    emit, _ = _emitter()
    dispatcher = AlertDispatcher(sinks={}, dead_letter=dlq, emit_event=emit)
    assert dispatcher._backoff(1) == 1.0
    assert isinstance(dispatcher._now(), datetime)


async def test_dispatch_success_emits_dispatched() -> None:
    sink = _RecordingSink("webhook")
    dlq = _FakeDeadLetter()
    emit, emitted = _emitter()
    sleep, slept = _sleeper()
    dispatcher = _dispatcher({"webhook": sink}, dead_letter=dlq, emit=emit, sleep=sleep)

    await dispatcher.dispatch(_audit_event(), [_match()])

    assert sink.calls == 1
    assert slept == []
    assert dlq.records == []
    assert len(emitted) == 1
    assert emitted[0].event_type == "obs.alert.dispatched"
    assert emitted[0].payload["audit_event_id"] == "evt-1"
    assert emitted[0].payload["sink"] == "webhook"


async def test_dispatch_fans_out_to_all_sinks() -> None:
    webhook = _RecordingSink("webhook")
    smtp = _RecordingSink("smtp")
    dlq = _FakeDeadLetter()
    emit, emitted = _emitter()
    sleep, _ = _sleeper()
    dispatcher = _dispatcher(
        {"webhook": webhook, "smtp": smtp}, dead_letter=dlq, emit=emit, sleep=sleep
    )

    await dispatcher.dispatch(_audit_event(), [_match(sinks=("webhook", "smtp"))])

    assert webhook.calls == 1
    assert smtp.calls == 1
    assert {e.payload["sink"] for e in emitted} == {"webhook", "smtp"}


async def test_dispatch_retries_then_dead_letters() -> None:
    sink = _RecordingSink("webhook", always_fail=True)
    dlq = _FakeDeadLetter()
    emit, emitted = _emitter()
    sleep, slept = _sleeper()
    dispatcher = _dispatcher(
        {"webhook": sink}, dead_letter=dlq, emit=emit, sleep=sleep, max_attempts=3
    )

    await dispatcher.dispatch(_audit_event(), [_match()])

    assert sink.calls == 3
    assert slept == [1.0, 2.0]  # backoff after attempts 1 and 2, none after the last
    assert len(dlq.records) == 1
    record = dlq.records[0]
    assert record.sink == "webhook"
    assert record.event_id == "evt-1"
    assert record.failed_at == FIXED_NOW
    assert "unavailable" in record.reason
    assert len(emitted) == 1
    assert emitted[0].event_type == "obs.alert.failed"
    assert emitted[0].payload["reason"] == record.reason


async def test_dispatch_recovers_after_transient_failures() -> None:
    sink = _RecordingSink("webhook", fail_times=2)
    dlq = _FakeDeadLetter()
    emit, emitted = _emitter()
    sleep, slept = _sleeper()
    dispatcher = _dispatcher({"webhook": sink}, dead_letter=dlq, emit=emit, sleep=sleep)

    await dispatcher.dispatch(_audit_event(), [_match()])

    assert sink.calls == 3
    assert slept == [1.0, 2.0]
    assert dlq.records == []
    assert emitted[0].event_type == "obs.alert.dispatched"


async def test_dispatch_backoff_is_capped() -> None:
    sink = _RecordingSink("webhook", always_fail=True)
    dlq = _FakeDeadLetter()
    emit, _ = _emitter()
    sleep, slept = _sleeper()
    dispatcher = _dispatcher(
        {"webhook": sink},
        dead_letter=dlq,
        emit=emit,
        sleep=sleep,
        max_attempts=4,
        base_backoff_s=10.0,
        max_backoff_s=15.0,
    )

    await dispatcher.dispatch(_audit_event(), [_match()])

    assert slept == [10.0, 15.0, 15.0]  # 10, 20->15, 40->15


async def test_dispatch_unknown_sink_dead_letters_without_retry() -> None:
    dlq = _FakeDeadLetter()
    emit, emitted = _emitter()
    sleep, slept = _sleeper()
    dispatcher = _dispatcher({}, dead_letter=dlq, emit=emit, sleep=sleep)

    await dispatcher.dispatch(_audit_event(), [_match(sinks=("ghost",))])

    assert slept == []
    assert len(dlq.records) == 1
    assert "unknown sink 'ghost'" in dlq.records[0].reason
    assert emitted[0].event_type == "obs.alert.failed"


async def test_dispatch_isolates_sink_failures() -> None:
    good = _RecordingSink("smtp")
    bad = _RecordingSink("webhook", always_fail=True)
    dlq = _FakeDeadLetter()
    emit, emitted = _emitter()
    sleep, _ = _sleeper()
    dispatcher = _dispatcher(
        {"webhook": bad, "smtp": good}, dead_letter=dlq, emit=emit, sleep=sleep, max_attempts=1
    )

    await dispatcher.dispatch(_audit_event(), [_match(sinks=("webhook", "smtp"))])

    assert good.calls == 1
    assert len(dlq.records) == 1
    assert dlq.records[0].sink == "webhook"
    kinds = {e.event_type for e in emitted}
    assert kinds == {"obs.alert.dispatched", "obs.alert.failed"}


async def test_dispatch_propagates_cancellation() -> None:
    dlq = _FakeDeadLetter()
    emit, _ = _emitter()
    sleep, _ = _sleeper()
    dispatcher = _dispatcher(
        {"webhook": _CancellingSink()}, dead_letter=dlq, emit=emit, sleep=sleep
    )

    with pytest.raises(asyncio.CancelledError):
        await dispatcher.dispatch(_audit_event(), [_match()])
    assert dlq.records == []


async def test_dispatch_swallows_emit_failure() -> None:
    sink = _RecordingSink("webhook")
    dlq = _FakeDeadLetter()
    sleep, _ = _sleeper()

    async def emit(event: AuditEvent) -> None:
        raise RuntimeError("emit broke")

    dispatcher = _dispatcher({"webhook": sink}, dead_letter=dlq, emit=emit, sleep=sleep)
    # Should not raise even though emitting the outcome event fails.
    await dispatcher.dispatch(_audit_event(), [_match()])
    assert sink.calls == 1


async def test_dispatch_emit_cancellation_propagates() -> None:
    sink = _RecordingSink("webhook")
    dlq = _FakeDeadLetter()
    sleep, _ = _sleeper()

    async def emit(event: AuditEvent) -> None:
        raise asyncio.CancelledError

    dispatcher = _dispatcher({"webhook": sink}, dead_letter=dlq, emit=emit, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.dispatch(_audit_event(), [_match()])
