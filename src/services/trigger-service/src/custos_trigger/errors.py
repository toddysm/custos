"""Locked ``trigger.*`` error taxonomy.

A small, stable set of dot-namespaced reason codes the Trigger Service uses to
label its domain failures. The set is *locked*: every code is pinned on
:class:`TriggerErrorKind` and enumerated in :data:`LOCKED_TRIGGER_KINDS`, and an
enum-grid test guards both against accidental drift. Later TS-IMPL-* tasks raise
:class:`TriggerError` (or a subclass) with one of these kinds; the REST and RPC
layers surface :meth:`TriggerError.to_dict` as the JSON error envelope.

The codes mirror design ``§ Failure Modes`` and the audit-event taxonomy:

* ``trigger.subscription_not_found`` — a subscription id has no row for the
  workspace (manual fire / resume register against an unknown subscription).
* ``trigger.selector_invalid`` — a CEL selector fails to parse / type-check at
  registration time (ADR-011 parity with ``inputMapping``).
* ``trigger.selector_type_error`` — a selector parses but evaluates to a
  non-boolean (or errors) against the ``event`` binding root at match time.
* ``trigger.dispatch_failed`` — dispatch to the Workflow Service exhausted
  ``TRIGGER_DISPATCH_MAX_RETRIES`` and was dead-lettered.
* ``trigger.resume_divergent`` — a resume re-registration carries a payload that
  diverges from the recorded subscription (replay-protection mismatch).
* ``trigger.dedup_duplicate`` — an inbound event hit an existing dedup key; no
  dispatch is performed.
* ``trigger.loop_detected`` — a per-tenant fan-out depth limit was exceeded
  (workflow A starts B starts A).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Final


class TriggerErrorKind(StrEnum):
    """Stable, dot-namespaced reason codes for Trigger Service domain failures."""

    SUBSCRIPTION_NOT_FOUND = "trigger.subscription_not_found"
    SELECTOR_INVALID = "trigger.selector_invalid"
    SELECTOR_TYPE_ERROR = "trigger.selector_type_error"
    DISPATCH_FAILED = "trigger.dispatch_failed"
    RESUME_DIVERGENT = "trigger.resume_divergent"
    DEDUP_DUPLICATE = "trigger.dedup_duplicate"
    LOOP_DETECTED = "trigger.loop_detected"


#: The locked set of ``trigger.*`` kind strings. Adding or removing a member of
#: :class:`TriggerErrorKind` is a deliberate, test-guarded taxonomy change.
LOCKED_TRIGGER_KINDS: Final[frozenset[str]] = frozenset(member.value for member in TriggerErrorKind)


class TriggerError(Exception):
    """A typed Trigger Service domain failure.

    Carries a locked :class:`TriggerErrorKind`, a free-form human-readable
    ``message``, and an optional JSON-safe ``details`` mapping. Callers switch
    on :attr:`kind` for control flow and surface :meth:`to_dict` as the wire
    error envelope.
    """

    def __init__(
        self,
        kind: TriggerErrorKind,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{kind.value}: {message}")
        self.kind = kind
        self.message = message
        self.details: dict[str, Any] = dict(details) if details else {}

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-safe error envelope.

        Always includes ``kind`` and ``message``; ``details`` is included only
        when non-empty so the common case stays compact.
        """
        payload: dict[str, Any] = {"kind": self.kind.value, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return payload


__all__ = [
    "LOCKED_TRIGGER_KINDS",
    "TriggerError",
    "TriggerErrorKind",
]
