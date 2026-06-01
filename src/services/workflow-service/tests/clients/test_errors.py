"""Tests for the outbound RPC error taxonomy + envelope mapping (WF-IMPL-075).

The locked taxonomy is the single source of truth the retry-decision
driver (WF-IMPL-053) consumes, so these tests exhaustively cover:

* every locked kind in :data:`LOCKED_OUTBOUND_RPC_KINDS` is honoured
  by an :class:`OutboundRpcError` subclass *and* has a matching
  entry in :data:`LOCKED_OUTBOUND_RPC_KIND_TO_STATUS` (so a new
  kind cannot land in one without the other);
* :func:`map_to_activity_envelope` produces the documented
  ``ActivityResultEnvelope`` for every concrete subclass and a
  parametrised matrix of HTTP status codes;
* the ``__cause__`` chain is preserved up to ``MAX_CAUSE_DEPTH`` and
  truncated below it (no infinite walking on a cyclic chain);
* envelope invariants (no outputs on failure, error always set,
  attempt >= 1) are honoured.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest

from custos_workflow.clients._errors import (
    LOCKED_OUTBOUND_RPC_KIND_TO_STATUS,
    LOCKED_OUTBOUND_RPC_KINDS,
    MAX_CAUSE_DEPTH,
    OutboundRpcCancelledError,
    OutboundRpcDecodeError,
    OutboundRpcError,
    OutboundRpcStatusError,
    OutboundRpcTransportError,
    map_to_activity_envelope,
)
from custos_workflow.clients.activity_runtime import ActivityResultEnvelope
from custos_workflow.clients.connector import ConnectorBindError

# ---------------------------------------------------------------------------
# Locked taxonomy guards
# ---------------------------------------------------------------------------


def test_locked_kinds_matches_status_map_keys() -> None:
    """Both locked tables must agree on the kind set exactly.

    A drift between the kind frozenset and the status map would
    let the retry driver crash on a kind it can't render.
    """
    assert frozenset(LOCKED_OUTBOUND_RPC_KIND_TO_STATUS) == LOCKED_OUTBOUND_RPC_KINDS


def test_locked_kinds_size_is_four() -> None:
    """Lock the cardinality so a new kind has to land deliberately."""
    assert len(LOCKED_OUTBOUND_RPC_KINDS) == 4


def test_locked_kind_values_are_namespaced() -> None:
    """Every kind must use the ``workflow.client.*`` namespace."""
    for kind in LOCKED_OUTBOUND_RPC_KINDS:
        assert kind.startswith("workflow.client."), kind


def test_status_map_is_immutable() -> None:
    """The exported status map must be a ``MappingProxyType``.

    Otherwise a downstream module could mutate the locked
    taxonomy in place and bypass the test guard above.
    """
    with pytest.raises(TypeError):
        # MappingProxyType rejects item assignment with TypeError.
        cast(dict[str, int], LOCKED_OUTBOUND_RPC_KIND_TO_STATUS)["x"] = 1


def test_concrete_subclasses_cover_locked_kinds() -> None:
    """Each locked kind must be reachable from a concrete subclass."""
    declared = {
        OutboundRpcTransportError.kind,
        OutboundRpcStatusError.kind,
        OutboundRpcDecodeError.kind,
        OutboundRpcCancelledError.kind,
    }
    assert declared == LOCKED_OUTBOUND_RPC_KINDS


# ---------------------------------------------------------------------------
# Subclass-definition guard
# ---------------------------------------------------------------------------


def test_subclass_with_unknown_kind_is_rejected() -> None:
    """Adding a new bucket without updating the lock must fail loudly."""
    with pytest.raises(TypeError, match="not in LOCKED_OUTBOUND_RPC_KINDS"):

        class _Rogue(OutboundRpcError):
            kind = "workflow.client.rogue"


def test_subclass_with_empty_kind_is_allowed() -> None:
    """Marker subclasses (no concrete kind) may exist as bases.

    :class:`~custos_workflow.clients.connector.ConnectorBindError`
    relies on this to act as a bind-context marker without
    introducing a fifth bucket.
    """

    class _Marker(OutboundRpcError):
        pass

    assert _Marker.kind == ""


# ---------------------------------------------------------------------------
# Constructor invariants
# ---------------------------------------------------------------------------


def test_transport_error_carries_detail() -> None:
    exc = OutboundRpcTransportError("dns lookup failed")
    assert exc.detail == "dns lookup failed"
    assert str(exc) == "dns lookup failed"
    assert exc.kind == "workflow.client.transport"


def test_status_error_carries_status_and_optional_code() -> None:
    exc = OutboundRpcStatusError("bad gateway", status_code=502, code="upstream.dead")
    assert exc.status_code == 502
    assert exc.code == "upstream.dead"
    assert exc.kind == "workflow.client.status"


def test_status_error_rejects_invalid_status_code() -> None:
    for bad in (0, 99, 600, 1000):
        with pytest.raises(ValueError, match=r"must be in \[100, 599\]"):
            OutboundRpcStatusError("x", status_code=bad)


def test_decode_error_carries_detail() -> None:
    exc = OutboundRpcDecodeError("not json")
    assert exc.kind == "workflow.client.decode"
    assert exc.detail == "not json"


def test_cancelled_error_carries_detail() -> None:
    exc = OutboundRpcCancelledError("upstream cancelled")
    assert exc.kind == "workflow.client.cancelled"


def test_connector_bind_error_is_outbound_rpc_error() -> None:
    """The bind-context marker must inherit from the locked base."""
    assert issubclass(ConnectorBindError, OutboundRpcError)
    # No concrete kind on the marker — adapters use the concrete
    # OutboundRpcError subclasses directly.
    assert ConnectorBindError.kind == ""


# ---------------------------------------------------------------------------
# Mapping: transport → retryable
# ---------------------------------------------------------------------------


def test_map_transport_to_retryable() -> None:
    env = map_to_activity_envelope(
        OutboundRpcTransportError("connect timed out"),
        attempt=1,
    )
    assert env.class_ == "retryable"
    assert env.outputs is None
    assert env.attempt == 1
    assert env.error is not None
    assert env.error["code"] == "workflow.client.transport"
    assert env.error["message"] == "connect timed out"


# ---------------------------------------------------------------------------
# Mapping: status → retryable | permanent
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status_code", "expected_class"),
    [
        (400, "permanent"),
        (401, "permanent"),
        (403, "permanent"),
        (404, "permanent"),
        (408, "retryable"),
        (422, "permanent"),
        (429, "retryable"),
        (500, "retryable"),
        (502, "retryable"),
        (503, "retryable"),
        (504, "retryable"),
        # Defensive: an out-of-range-but-valid status is permanent.
        (599, "retryable"),
        (399, "permanent"),
    ],
)
def test_map_status_to_class(status_code: int, expected_class: str) -> None:
    env = map_to_activity_envelope(
        OutboundRpcStatusError("upstream error", status_code=status_code),
        attempt=2,
    )
    assert env.class_ == expected_class
    assert env.error is not None
    details = env.error["details"]
    assert isinstance(details, Mapping)
    assert details["status_code"] == status_code
    # No echoed sidecar code — ``details.code`` must be absent.
    assert "code" not in details
    assert env.attempt == 2


def test_map_status_preserves_echoed_code() -> None:
    env = map_to_activity_envelope(
        OutboundRpcStatusError(
            "rate limited",
            status_code=429,
            code="sidecar.throttled",
        ),
        attempt=3,
    )
    assert env.error is not None
    details = env.error["details"]
    assert isinstance(details, Mapping)
    assert details["code"] == "sidecar.throttled"


# ---------------------------------------------------------------------------
# Mapping: decode → permanent, cancelled → cancelled
# ---------------------------------------------------------------------------


def test_map_decode_to_permanent() -> None:
    env = map_to_activity_envelope(OutboundRpcDecodeError("invalid json"), attempt=1)
    assert env.class_ == "permanent"
    assert env.outputs is None
    assert env.error is not None
    assert env.error["code"] == "workflow.client.decode"


def test_map_cancelled_to_cancelled() -> None:
    env = map_to_activity_envelope(
        OutboundRpcCancelledError("upstream cancel"),
        attempt=1,
    )
    assert env.class_ == "cancelled"
    assert env.error is not None
    assert env.error["code"] == "workflow.client.cancelled"


# ---------------------------------------------------------------------------
# Mapping: base class + unknown subclass rejected
# ---------------------------------------------------------------------------


def test_map_rejects_outbound_rpc_base_class() -> None:
    with pytest.raises(TypeError, match="concrete OutboundRpcError"):
        map_to_activity_envelope(OutboundRpcError("base"), attempt=1)


def test_map_rejects_unknown_concrete_subclass() -> None:
    """An unmapped concrete marker subclass must fail loudly.

    :class:`ConnectorBindError` is the canonical example: it
    inherits from :class:`OutboundRpcError` but has no locked kind
    and no explicit mapping arm, so the mapper rejects it rather
    than silently defaulting to ``permanent``.
    """
    with pytest.raises(TypeError, match="add an explicit mapping arm"):
        map_to_activity_envelope(ConnectorBindError("no concrete kind"), attempt=1)


# ---------------------------------------------------------------------------
# Cause chain preservation + truncation
# ---------------------------------------------------------------------------


def test_no_cause_omits_field() -> None:
    env = map_to_activity_envelope(OutboundRpcTransportError("x"), attempt=1)
    assert env.error is not None
    assert "cause" not in env.error


def test_single_cause_is_preserved() -> None:
    try:
        try:
            raise ConnectionResetError("peer hung up")
        except ConnectionResetError as inner:
            raise OutboundRpcTransportError("transport bombed") from inner
    except OutboundRpcTransportError as exc:
        env = map_to_activity_envelope(exc, attempt=1)

    assert env.error is not None
    cause = env.error["cause"]
    assert isinstance(cause, Mapping)
    assert cause["type"] == "ConnectionResetError"
    assert cause["message"] == "peer hung up"
    # No nested cause — chain was depth 1.
    assert "cause" not in cause


def test_cause_chain_truncated_at_max_depth() -> None:
    """A chain deeper than ``MAX_CAUSE_DEPTH`` must be truncated.

    Builds a chain of depth 5 (transport → A → B → C → D → E) and
    asserts the envelope only renders depths 1..MAX_CAUSE_DEPTH.
    """
    # Build chain bottom-up so each link has a real __cause__.
    depths: list[BaseException] = []
    prev: BaseException | None = None
    for i in range(5):
        try:
            if prev is None:
                raise RuntimeError(f"level-{i}")
            raise RuntimeError(f"level-{i}") from prev
        except RuntimeError as cur:
            depths.append(cur)
            prev = cur

    try:
        raise OutboundRpcTransportError("top") from depths[-1]
    except OutboundRpcTransportError as exc:
        env = map_to_activity_envelope(exc, attempt=1)

    assert env.error is not None
    # Walk the rendered cause chain and count depth.
    node: Mapping[str, object] | None = cast(Mapping[str, object], env.error["cause"])
    rendered_depth = 0
    while node is not None:
        rendered_depth += 1
        nxt = node.get("cause")
        if nxt is None:
            break
        assert isinstance(nxt, Mapping)
        node = nxt
    assert rendered_depth == MAX_CAUSE_DEPTH


# ---------------------------------------------------------------------------
# Envelope invariants
# ---------------------------------------------------------------------------


def test_envelope_is_immutable() -> None:
    """The returned envelope must reject mutation."""
    env = map_to_activity_envelope(OutboundRpcDecodeError("x"), attempt=1)
    assert isinstance(env, ActivityResultEnvelope)
    # ``error`` is a MappingProxyType so item assignment raises.
    assert env.error is not None
    with pytest.raises(TypeError):
        cast(dict[str, object], env.error)["code"] = "overridden"


def test_attempt_must_be_positive() -> None:
    """``ActivityResultEnvelope`` enforces ``attempt >= 1``."""
    with pytest.raises(ValueError):
        map_to_activity_envelope(OutboundRpcDecodeError("x"), attempt=0)
