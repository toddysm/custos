"""WF-IMPL-075 — Outbound RPC error taxonomy + ``ActivityResultEnvelope`` mapping.

Locks the structured-error taxonomy that the upcoming production
:class:`~custos_workflow.clients.ActivityRuntimeClient` and
:class:`~custos_workflow.clients.ConnectorClient` adapters
(WF-IMPL-076..079) raise from their outbound HTTP RPCs, plus the
deterministic mapping from those errors into the existing
:class:`~custos_workflow.clients.ActivityResultEnvelope` shape so the
retry-decision driver (WF-IMPL-053) sees the **same envelope shape**
regardless of which transport-layer failure mode produced it.

Without this lock, every adapter would invent its own error subclass
and the retry driver would have to special-case each one; with it,
every adapter raises one of the four
:class:`OutboundRpcError` subclasses defined here and the driver
dispatches on :attr:`OutboundRpcError.kind` exhaustively.

Locked kinds
------------

The :data:`LOCKED_OUTBOUND_RPC_KINDS` frozenset is the **complete**
set of values :attr:`OutboundRpcError.kind` can take; the
companion :data:`LOCKED_OUTBOUND_RPC_KIND_TO_STATUS` dict pins the
suggested HTTP status logged with each kind. Both are exhaustively
covered by tests so a new kind cannot land in one without the
other.

The kinds align with the four failure-mode buckets the deferred
adapters can encounter:

* ``workflow.client.transport`` — connect / TLS / DNS / read /
  timeout failures *before* the sidecar produced an HTTP response.
* ``workflow.client.status`` — sidecar produced an HTTP response
  with a non-2xx status code.
* ``workflow.client.decode`` — response body was not valid JSON
  or failed shape validation.
* ``workflow.client.cancelled`` — sidecar reported the request
  was cancelled upstream (typically HTTP 499 in nginx-style
  upstream cancel, or an explicit cancel signal from
  Dapr Workflow).

Envelope mapping
----------------

:func:`map_to_activity_envelope` performs the canonical
``OutboundRpcError → ActivityResultEnvelope`` mapping required by
the ARM design § *Error Envelope* and the Workflow Service design
§ *Retry Policy*:

* :class:`OutboundRpcTransportError` → ``class_="retryable"``.
* :class:`OutboundRpcStatusError` with HTTP 408 / 429 / 5xx →
  ``class_="retryable"``.
* :class:`OutboundRpcStatusError` with HTTP 4xx (excluding 408 /
  429) → ``class_="permanent"``.
* :class:`OutboundRpcDecodeError` → ``class_="permanent"``
  (a malformed response is a contract violation, not a transient).
* :class:`OutboundRpcCancelledError` → ``class_="cancelled"``.

The mapper preserves :attr:`OutboundRpcError.kind` as the
envelope's ``code`` field, the human-readable
:attr:`OutboundRpcError.detail` as ``message``, and walks the
exception ``__cause__`` chain up to **3** levels deep to populate
the nested ``cause`` field per ARM design § *Locked defaults*.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from custos_workflow.clients.activity_runtime import (
    ActivityResultClass,
    ActivityResultEnvelope,
)

__all__ = [
    "LOCKED_OUTBOUND_RPC_KINDS",
    "LOCKED_OUTBOUND_RPC_KIND_TO_STATUS",
    "MAX_CAUSE_DEPTH",
    "OutboundRpcCancelledError",
    "OutboundRpcDecodeError",
    "OutboundRpcError",
    "OutboundRpcStatusError",
    "OutboundRpcTransportError",
    "map_to_activity_envelope",
]


# ---------------------------------------------------------------------------
# Locked taxonomy
# ---------------------------------------------------------------------------


#: Wire-stable ``kind`` for transport-layer failures. Pinned in the
#: locked taxonomy so the retry driver and OTel counter labels stay
#: stable across adapter rewrites.
_KIND_TRANSPORT: Final[str] = "workflow.client.transport"

#: Wire-stable ``kind`` for non-2xx HTTP responses from the sidecar.
_KIND_STATUS: Final[str] = "workflow.client.status"

#: Wire-stable ``kind`` for response-body decode / shape failures.
_KIND_DECODE: Final[str] = "workflow.client.decode"

#: Wire-stable ``kind`` for upstream-cancelled requests.
_KIND_CANCELLED: Final[str] = "workflow.client.cancelled"


#: Complete set of ``kind`` values any :class:`OutboundRpcError`
#: subclass may take. Tests assert exhaustiveness against the
#: subclass tree so a new subclass cannot ship without landing here.
LOCKED_OUTBOUND_RPC_KINDS: Final[frozenset[str]] = frozenset(
    {
        _KIND_TRANSPORT,
        _KIND_STATUS,
        _KIND_DECODE,
        _KIND_CANCELLED,
    }
)


#: Suggested HTTP status code to log alongside each locked kind.
#: ``status`` errors carry their own real status code on the
#: exception; this is the *default* used when the adapter has no
#: response in hand (transport / decode / cancelled).
LOCKED_OUTBOUND_RPC_KIND_TO_STATUS: Final[Mapping[str, int]] = MappingProxyType(
    {
        _KIND_TRANSPORT: 503,  # service unavailable — no response received
        _KIND_STATUS: 0,  # placeholder; real status is on the exception
        _KIND_DECODE: 502,  # bad gateway — sidecar response unintelligible
        _KIND_CANCELLED: 499,  # nginx-style client-closed-request
    }
)


#: Maximum depth the envelope mapper walks the ``__cause__`` chain
#: before truncating. Mirrors the ARM design § *Locked defaults*
#: cap of 3.
MAX_CAUSE_DEPTH: Final[int] = 3


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class OutboundRpcError(ValueError):
    """Base class for every structured outbound-RPC failure.

    Subclasses ``ValueError`` so adapter code that catches the
    generic ``ValueError`` family keeps working, while every
    concrete subclass below also carries the ``kind`` / ``detail``
    machine-readable attributes the envelope mapper consumes.

    :param detail: Human-readable summary of the failure. Surfaced
        on the envelope as the ``message`` field.

    :ivar kind: One of :data:`LOCKED_OUTBOUND_RPC_KINDS`. Set by
        each concrete subclass; assigning a value outside the
        locked set fails at class-definition time.
    :ivar detail: Same as the ``detail`` constructor argument.
    """

    #: Locked taxonomy kind. Concrete subclasses override.
    kind: str = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Reject any subclass that doesn't set a locked kind so the
        # taxonomy stays closed; this guards the WF-IMPL-076..079
        # adapters from accidentally introducing a fifth bucket.
        if cls.kind and cls.kind not in LOCKED_OUTBOUND_RPC_KINDS:
            raise TypeError(
                f"OutboundRpcError subclass {cls.__name__} declares kind "
                f"{cls.kind!r}, which is not in LOCKED_OUTBOUND_RPC_KINDS. "
                f"Allowed: {sorted(LOCKED_OUTBOUND_RPC_KINDS)}."
            )

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class OutboundRpcTransportError(OutboundRpcError):
    """Connect / TLS / DNS / read / write / timeout failure.

    Raised by the adapter when the request never produced an HTTP
    response (no status code observed). Always maps to
    ``class_="retryable"`` because the request never landed on the
    sidecar.
    """

    kind = _KIND_TRANSPORT


class OutboundRpcStatusError(OutboundRpcError):
    """Non-2xx HTTP response from the sidecar.

    :param detail: Human-readable summary.
    :param status_code: HTTP status code observed on the response.
        Used by :func:`map_to_activity_envelope` to choose between
        ``retryable`` (408 / 429 / 5xx) and ``permanent`` (4xx).
    :param code: Optional structured error code echoed from the
        sidecar response body. Preserved on the envelope as a
        ``details.code`` field when present.

    :raises ValueError: If :paramref:`status_code` is not in the
        valid HTTP range (100..599 inclusive).
    """

    kind = _KIND_STATUS

    def __init__(
        self,
        detail: str,
        *,
        status_code: int,
        code: str | None = None,
    ) -> None:
        # Reject obviously-invalid status codes before they reach
        # the envelope mapper's bucket logic so a typo in the
        # adapter doesn't silently fall through the 4xx vs 5xx
        # branch.
        if not 100 <= status_code <= 599:
            raise ValueError(
                f"OutboundRpcStatusError.status_code must be in [100, 599], got {status_code}."
            )
        super().__init__(detail)
        self.status_code = status_code
        self.code = code


class OutboundRpcDecodeError(OutboundRpcError):
    """Response body could not be decoded or failed shape validation.

    Raised when the sidecar returned a 2xx but the body is not the
    JSON envelope the adapter expected. Always maps to
    ``class_="permanent"`` — a malformed response is a contract
    violation, not a transient.
    """

    kind = _KIND_DECODE


class OutboundRpcCancelledError(OutboundRpcError):
    """Request was cancelled upstream of the adapter.

    Typically observed as HTTP 499 from an nginx-style sidecar or
    as an explicit cancel signal propagated from Dapr Workflow.
    Always maps to ``class_="cancelled"`` so the retry driver
    short-circuits the attempt without consuming a retry budget
    slot.
    """

    kind = _KIND_CANCELLED


# ---------------------------------------------------------------------------
# Envelope mapping
# ---------------------------------------------------------------------------


#: HTTP status codes that map to ``retryable`` even though they're
#: in the 4xx range — 408 Request Timeout and 429 Too Many Requests
#: are conventionally retryable per RFC 9110 and RFC 6585.
_RETRYABLE_4XX: Final[frozenset[int]] = frozenset({408, 429})


def _classify_status(status_code: int) -> ActivityResultClass:
    """Bucket an HTTP status code into ``retryable`` or ``permanent``.

    * 408 / 429 → ``retryable`` (per RFC convention).
    * Other 4xx → ``permanent`` (client-side contract violation).
    * 5xx → ``retryable`` (server-side transient assumed).
    * Anything else → ``permanent`` (defensive default; an
      unexpected status is a contract bug, not a transient).
    """
    if status_code in _RETRYABLE_4XX:
        return "retryable"
    if 400 <= status_code < 500:
        return "permanent"
    if 500 <= status_code < 600:
        return "retryable"
    return "permanent"


def _exception_to_cause_dict(exc: BaseException, depth: int) -> Mapping[str, Any] | None:
    """Render an exception (and its ``__cause__`` chain) as a cause dict.

    Walks the chain up to :data:`MAX_CAUSE_DEPTH` levels and stops.
    Returns ``None`` once the depth budget is exhausted so the
    caller can skip emitting the field rather than emitting an
    empty stub.
    """
    if depth <= 0 or exc is None:
        return None
    # Use ``type(exc).__name__`` rather than ``repr(exc)`` so the
    # rendered cause stays stable across CPython versions and
    # doesn't leak the full exception ``args`` (which could carry
    # secrets in adapter contexts).
    cause: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc),
    }
    if exc.__cause__ is not None:
        nested = _exception_to_cause_dict(exc.__cause__, depth - 1)
        if nested is not None:
            cause["cause"] = nested
    return MappingProxyType(cause)


def map_to_activity_envelope(
    exc: OutboundRpcError,
    *,
    attempt: int,
) -> ActivityResultEnvelope:
    """Render an :class:`OutboundRpcError` into an :class:`ActivityResultEnvelope`.

    Performs the canonical mapping pinned in the module docstring;
    the resulting envelope is what the retry-decision driver
    (WF-IMPL-053) and the audit emitter (WF-IMPL-056) see, so the
    mapper is the single point of truth for the
    transport-error-class taxonomy.

    The returned envelope always satisfies
    :meth:`ActivityResultEnvelope.__post_init__`'s invariants:

    * ``class_`` is never ``"success"`` (this function is only
      called on the error path), so :attr:`outputs` is always
      ``None``.
    * :attr:`error` is always populated with at minimum
      ``code`` + ``message``.
    * :attr:`attempt` mirrors the per-step counter the caller
      passed in.

    :param exc: The structured outbound-RPC error to map. Must be
        one of the four concrete :class:`OutboundRpcError`
        subclasses defined in this module — the base class itself
        is rejected because it carries no locked ``kind``.
    :param attempt: Per-step attempt counter to stamp on the
        envelope. Must be a positive integer (the underlying
        :class:`ActivityResultEnvelope` enforces this).

    :returns: A frozen :class:`ActivityResultEnvelope` with
        ``outputs=None`` and a populated ``error`` mapping.

    :raises TypeError: If :paramref:`exc` is not one of the four
        concrete locked subclasses.
    """
    # ``isinstance(exc, OutboundRpcError)`` would accept the
    # abstract base; reject it explicitly so callers can never
    # construct an envelope with a missing ``kind``.
    if type(exc) is OutboundRpcError:
        raise TypeError(
            "map_to_activity_envelope requires a concrete OutboundRpcError "
            "subclass (Transport / Status / Decode / Cancelled), not the "
            "OutboundRpcError base class."
        )

    if isinstance(exc, OutboundRpcCancelledError):
        class_: ActivityResultClass = "cancelled"
    elif isinstance(exc, OutboundRpcTransportError):
        class_ = "retryable"
    elif isinstance(exc, OutboundRpcDecodeError):
        class_ = "permanent"
    elif isinstance(exc, OutboundRpcStatusError):
        class_ = _classify_status(exc.status_code)
    else:
        # Defensive: an exhaustively-covered union should never hit
        # this branch, but a future subclass without an explicit
        # mapping arm should fail loudly rather than silently
        # default to ``permanent``.
        raise TypeError(
            f"map_to_activity_envelope received unsupported OutboundRpcError "
            f"subclass {type(exc).__name__}; add an explicit mapping arm."
        )

    error: dict[str, Any] = {
        "code": exc.kind,
        "message": exc.detail,
    }
    # Surface status-specific context (status code + optional
    # echoed code) so the audit emitter can render a meaningful
    # diagnostic without re-inspecting the exception object.
    if isinstance(exc, OutboundRpcStatusError):
        details: dict[str, Any] = {"status_code": exc.status_code}
        if exc.code is not None:
            details["code"] = exc.code
        error["details"] = MappingProxyType(details)

    cause = _exception_to_cause_dict(exc.__cause__, MAX_CAUSE_DEPTH) if exc.__cause__ else None
    if cause is not None:
        error["cause"] = cause

    return ActivityResultEnvelope(
        class_=class_,
        outputs=None,
        error=MappingProxyType(error),
        attempt=attempt,
    )
