"""Alerting Dispatcher — webhook + SMTP sinks + dead-letter (OBS-IMPL-009).

Once the matcher (OBS-IMPL-008) decides an audit event fires a rule, the
dispatcher delivers it to that rule's sinks. Two sinks ship in M1:

* **webhook** — POST a JSON body (rule + audit event) to each configured URL.
* **smtp** — email the alert through the configured relay (``CUSTOS_SMTP_*``).

Delivery is retried with exponential backoff (5 attempts by default). A dispatch
still failing after the last attempt — or addressed to an unknown sink — is
written to an injected dead-letter store and surfaced as
:class:`~custos_obs.errors.AlertSinkUnavailable` via an ``obs.alert.failed``
audit event; a successful delivery emits ``obs.alert.dispatched``. A failure in
one sink never blocks the others.

The dispatcher depends on small injected seams — :class:`AlertSink`,
:class:`DeadLetterStore`, and an ``emit_event`` callable — rather than concrete
transports, mirroring the seam pattern used by the pipeline and retention
worker. Concrete :class:`HttpxWebhookTransport` and :class:`SmtpEmailTransport`
are provided for production wiring; tests inject fakes.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import EmailMessage
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol

from custos_obs.errors import AlertSinkUnavailable
from custos_obs.events import AlertDispatched, AlertFailed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from custos_spl import AuditEvent

    from custos_obs.alerting.rules import AlertMatch

logger = logging.getLogger("custos_obs.alerting.dispatcher")

#: Default number of delivery attempts before a dispatch is dead-lettered.
DEFAULT_MAX_ATTEMPTS = 5

#: Default exponential-backoff base (seconds) between delivery attempts.
DEFAULT_BASE_BACKOFF_S = 1.0

#: Default cap (seconds) on a single backoff sleep.
DEFAULT_MAX_BACKOFF_S = 30.0


class AlertSinkError(RuntimeError):
    """A single delivery attempt to a sink failed. Retryable by the dispatcher."""


@dataclass(frozen=True, slots=True)
class AlertPayload:
    """The immutable, sink-agnostic alert built from a matched audit event."""

    rule_name: str
    sink: str
    event_id: str
    event_type: str
    actor: str
    workspace_id: str
    occurred_at: datetime
    subject: Mapping[str, Any]
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        # Keep the frozen payload genuinely immutable: callers (and sinks) must
        # not be able to mutate the shared subject/payload mappings in place.
        object.__setattr__(self, "subject", MappingProxyType(dict(self.subject)))
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))

    @classmethod
    def build(cls, audit_event: AuditEvent, *, rule_name: str, sink: str) -> AlertPayload:
        """Assemble a payload from an audit event for a given rule + sink."""
        return cls(
            rule_name=rule_name,
            sink=sink,
            event_id=audit_event.event_id,
            event_type=audit_event.event_type,
            actor=audit_event.actor,
            workspace_id=str(audit_event.workspace_id),
            occurred_at=audit_event.occurred_at,
            subject=dict(audit_event.subject),
            payload=dict(audit_event.payload),
        )

    def to_body(self) -> dict[str, Any]:
        """The JSON body POSTed by the webhook sink."""
        return {
            "rule": self.rule_name,
            "event": {
                "eventId": self.event_id,
                "eventType": self.event_type,
                "actor": self.actor,
                "workspaceId": self.workspace_id,
                "occurredAt": self.occurred_at.isoformat(),
                "subject": dict(self.subject),
                "payload": dict(self.payload),
            },
        }

    def email_subject(self) -> str:
        """The subject line used by the SMTP sink."""
        return f"[Custos alert] {self.rule_name}: {self.event_type}"

    def email_body(self) -> str:
        """The plaintext body used by the SMTP sink."""
        return (
            f"Alert rule '{self.rule_name}' fired.\n\n"
            f"Event type: {self.event_type}\n"
            f"Event id:   {self.event_id}\n"
            f"Actor:      {self.actor}\n"
            f"Workspace:  {self.workspace_id}\n"
            f"Occurred:   {self.occurred_at.isoformat()}\n"
        )


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """A dispatch that exhausted its retries (or hit an unknown sink)."""

    rule_name: str
    sink: str
    event_id: str
    reason: str
    failed_at: datetime


class DeadLetterStore(Protocol):
    """Sink for dispatches that could not be delivered."""

    async def record(self, record: DeadLetterRecord) -> None:
        """Persist a dead-lettered dispatch."""
        ...


class AlertSink(Protocol):
    """A named alert delivery target.

    :meth:`deliver` performs a single delivery attempt; it raises
    :class:`AlertSinkError` on a retryable failure and returns on success.
    """

    @property
    def name(self) -> str: ...

    async def deliver(self, payload: AlertPayload) -> None: ...


class WebhookTransport(Protocol):
    """Performs one webhook POST; raises :class:`AlertSinkError` on failure."""

    async def __call__(self, url: str, body: Mapping[str, Any]) -> None: ...


class EmailTransport(Protocol):
    """Sends one email; raises :class:`AlertSinkError` on failure."""

    async def __call__(self, *, subject: str, body: str) -> None: ...


class WebhookSink:
    """Delivers an alert by POSTing it to every configured webhook URL."""

    name = "webhook"

    def __init__(self, *, urls: Sequence[str], transport: WebhookTransport) -> None:
        self._urls = tuple(urls)
        self._transport = transport

    async def deliver(self, payload: AlertPayload) -> None:
        if not self._urls:
            raise AlertSinkError("no webhook destinations configured")
        body = payload.to_body()
        failures: list[str] = []
        for url in self._urls:
            try:
                await self._transport(url, body)
            except AlertSinkError as exc:
                failures.append(str(exc))
        if failures:
            raise AlertSinkError("; ".join(failures))


class SmtpSink:
    """Delivers an alert as an email through an injected transport."""

    name = "smtp"

    def __init__(self, *, transport: EmailTransport) -> None:
        self._transport = transport

    async def deliver(self, payload: AlertPayload) -> None:
        await self._transport(subject=payload.email_subject(), body=payload.email_body())


class HttpxWebhookTransport:
    """Concrete :class:`WebhookTransport` backed by ``httpx.AsyncClient``."""

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._timeout = timeout
        self._client_factory = client_factory

    def _new_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        import httpx

        return httpx.AsyncClient(timeout=self._timeout)

    async def __call__(self, url: str, body: Mapping[str, Any]) -> None:
        import httpx

        try:
            async with self._new_client() as client:
                response = await client.post(url, json=dict(body))
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise AlertSinkError(f"webhook POST to {url} failed: {exc}") from exc


class SmtpEmailTransport:
    """Concrete :class:`EmailTransport` backed by the stdlib ``smtplib`` relay."""

    def __init__(
        self,
        *,
        host: str,
        sender: str,
        recipients: Sequence[str],
        port: int = 587,
        username: str | None = None,
        password: str | None = None,
        use_starttls: bool = True,
    ) -> None:
        if not recipients:
            raise ValueError("SmtpEmailTransport requires at least one recipient")
        self._host = host
        self._sender = sender
        self._recipients = tuple(recipients)
        self._port = port
        self._username = username
        self._password = password
        self._use_starttls = use_starttls

    async def __call__(self, *, subject: str, body: str) -> None:
        import smtplib

        try:
            await asyncio.to_thread(self._send_blocking, subject, body)
        except (OSError, smtplib.SMTPException) as exc:
            # smtplib surfaces transport failures as OSError subclasses and
            # protocol failures as SMTPException; both are retryable.
            raise AlertSinkError(f"SMTP send via {self._host} failed: {exc}") from exc

    def _send_blocking(self, subject: str, body: str) -> None:
        import smtplib

        message = EmailMessage()
        message["From"] = self._sender
        message["To"] = ", ".join(self._recipients)
        message["Subject"] = subject
        message.set_content(body)
        with smtplib.SMTP(self._host, self._port) as smtp:
            if self._use_starttls:
                smtp.starttls()
            if self._username is not None and self._password is not None:
                smtp.login(self._username, self._password)
            smtp.send_message(message)


class AlertDispatcher:
    """Delivers matched alerts to their sinks with retry + dead-lettering.

    For each match, every named sink is attempted independently. A sink that
    keeps raising :class:`AlertSinkError` is retried with exponential backoff up
    to ``max_attempts``; after the last failure (or for an unknown sink) the
    dispatch is dead-lettered and an ``obs.alert.failed`` event is emitted. A
    successful delivery emits ``obs.alert.dispatched``. ``sleep`` and ``now`` are
    injectable so the retry schedule is deterministic in tests.
    """

    def __init__(
        self,
        *,
        sinks: Mapping[str, AlertSink],
        dead_letter: DeadLetterStore,
        emit_event: Callable[[AuditEvent], Awaitable[None]],
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_backoff_s: float = DEFAULT_BASE_BACKOFF_S,
        max_backoff_s: float = DEFAULT_MAX_BACKOFF_S,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._sinks = dict(sinks)
        self._dead_letter = dead_letter
        self._emit_event = emit_event
        self._max_attempts = max_attempts
        self._base_backoff_s = base_backoff_s
        self._max_backoff_s = max_backoff_s
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._now = now if now is not None else lambda: datetime.now(UTC)

    async def dispatch(self, audit_event: AuditEvent, matches: Sequence[AlertMatch]) -> None:
        """Deliver ``audit_event`` to every sink named by every match."""
        for match in matches:
            for sink_name in match.rule.sinks:
                await self._dispatch_to_sink(audit_event, match.rule.name, sink_name)

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff for ``attempt`` (1-based), capped at the max."""
        delay: float = self._base_backoff_s * (2 ** (attempt - 1))
        if delay > self._max_backoff_s:
            return self._max_backoff_s
        return delay

    async def _dispatch_to_sink(
        self, audit_event: AuditEvent, rule_name: str, sink_name: str
    ) -> None:
        payload = AlertPayload.build(audit_event, rule_name=rule_name, sink=sink_name)
        sink = self._sinks.get(sink_name)
        if sink is None:
            await self._fail(payload, reason=f"unknown sink '{sink_name}'")
            return

        last_reason = ""
        for attempt in range(1, self._max_attempts + 1):
            try:
                await sink.deliver(payload)
            except asyncio.CancelledError:
                raise
            except AlertSinkError as exc:
                last_reason = str(exc)
                logger.warning(
                    "alert dispatch to sink %r for rule %r failed (attempt %d/%d): %s",
                    sink_name,
                    rule_name,
                    attempt,
                    self._max_attempts,
                    exc,
                )
                if attempt < self._max_attempts:
                    await self._sleep(self._backoff(attempt))
            else:
                await self._emit(
                    AlertDispatched(
                        rule_name=rule_name, sink=sink_name, audit_event_id=payload.event_id
                    ).to_audit_event()
                )
                return

        await self._fail(payload, reason=last_reason)

    async def _fail(self, payload: AlertPayload, *, reason: str) -> None:
        """Dead-letter a dispatch and emit ``obs.alert.failed``."""
        error = AlertSinkUnavailable(
            f"sink '{payload.sink}' unavailable for rule '{payload.rule_name}': {reason}"
        )
        logger.error("dead-lettering alert: %s", error.detail)
        try:
            await self._dead_letter.record(
                DeadLetterRecord(
                    rule_name=payload.rule_name,
                    sink=payload.sink,
                    event_id=payload.event_id,
                    reason=error.detail,
                    failed_at=self._now(),
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Best-effort: a DLQ outage must not block delivery to other sinks.
            logger.exception("failed to dead-letter alert for rule %r", payload.rule_name)
        await self._emit(
            AlertFailed(
                rule_name=payload.rule_name,
                sink=payload.sink,
                audit_event_id=payload.event_id,
                reason=error.detail,
            ).to_audit_event()
        )

    async def _emit(self, event: AuditEvent) -> None:
        try:
            await self._emit_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to emit %s", event.event_type)


__all__ = [
    "DEFAULT_BASE_BACKOFF_S",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_BACKOFF_S",
    "AlertDispatcher",
    "AlertPayload",
    "AlertSink",
    "AlertSinkError",
    "DeadLetterRecord",
    "DeadLetterStore",
    "EmailTransport",
    "HttpxWebhookTransport",
    "SmtpEmailTransport",
    "SmtpSink",
    "WebhookSink",
    "WebhookTransport",
]
