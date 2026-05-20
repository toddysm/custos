"""Tests for the transaction-handle binding helpers.

These tests do not exercise any backend — they pin the contract of
`bind_handle` / `check_handle` so adapters and callers can rely on
cross-provider misuse being caught uniformly.

See `design/components/storage-provider-layer/design.md` § Transaction
Model.
"""

from __future__ import annotations

import gc

import pytest

from custos_spl import (
    InvalidTransactionHandle,
    TransactionHandle,
    bind_handle,
    check_handle,
)


class _PgTx(TransactionHandle):
    """A typical adapter-side handle: wraps a fake backend transaction."""

    __slots__ = ("conn",)

    def __init__(self, conn: object) -> None:
        self.conn = conn


class _FakeProvider:
    """Stand-in for an adapter instance."""


# ----- bind_handle -----


def test_bind_handle_records_owner() -> None:
    provider = _FakeProvider()
    handle = _PgTx(conn=object())
    bind_handle(handle, provider)
    # Round-trip: the same provider validates fine.
    check_handle(handle, provider)


def test_bind_handle_rejects_non_handle() -> None:
    provider = _FakeProvider()
    with pytest.raises(TypeError, match="TransactionHandle"):
        bind_handle("not-a-handle", provider)  # type: ignore[arg-type]


def test_bind_handle_accepts_bare_transaction_handle() -> None:
    """The base class is valid on its own — adapters may use it directly."""
    provider = _FakeProvider()
    handle = TransactionHandle()
    bind_handle(handle, provider)
    check_handle(handle, provider)


# ----- check_handle: success paths -----


def test_check_handle_none_is_allowed_by_default() -> None:
    """`tx=None` is the implicit-transaction shape — no error."""
    check_handle(None, _FakeProvider())


def test_check_handle_distinct_providers_do_not_collide() -> None:
    """Two providers issue independent handles; each validates only on its own."""
    p1 = _FakeProvider()
    p2 = _FakeProvider()
    h1 = _PgTx(conn=object())
    h2 = _PgTx(conn=object())
    bind_handle(h1, p1)
    bind_handle(h2, p2)
    check_handle(h1, p1)
    check_handle(h2, p2)


# ----- check_handle: failure paths -----


def test_check_handle_rejects_cross_provider_use() -> None:
    p1 = _FakeProvider()
    p2 = _FakeProvider()
    handle = _PgTx(conn=object())
    bind_handle(handle, p1)
    with pytest.raises(
        InvalidTransactionHandle, match="different provider"
    ):
        check_handle(handle, p2)


def test_check_handle_rejects_unregistered_handle() -> None:
    """A bare-constructed handle that was never bound is not valid."""
    provider = _FakeProvider()
    handle = _PgTx(conn=object())
    with pytest.raises(InvalidTransactionHandle, match="never registered"):
        check_handle(handle, provider)


def test_check_handle_required_rejects_none() -> None:
    with pytest.raises(InvalidTransactionHandle, match="required"):
        check_handle(None, _FakeProvider(), required=True)


def test_check_handle_rejects_non_handle_object() -> None:
    with pytest.raises(InvalidTransactionHandle, match="TransactionHandle"):
        check_handle("nope", _FakeProvider())  # type: ignore[arg-type]


# ----- lifetime / GC behaviour -----


def test_handle_registry_uses_weak_keys() -> None:
    """Once the handle is dropped, the registry entry must auto-evict.

    This is what keeps the module-level registry from growing unbounded
    across the process lifetime.
    """
    from custos_spl.middleware.transactions import _owner

    provider = _FakeProvider()
    handle = _PgTx(conn=object())
    bind_handle(handle, provider)
    assert handle in _owner

    del handle
    gc.collect()
    # After GC, the registry must not retain the entry.
    assert len(_owner) == 0 or all(
        not isinstance(k, _PgTx) for k in _owner
    )


def test_handle_supports_weakref() -> None:
    """Base class must include __weakref__ for the registry to work."""
    import weakref

    handle = TransactionHandle()
    ref = weakref.ref(handle)
    assert ref() is handle


# ----- error classification -----


def test_invalid_transaction_handle_is_spl_error() -> None:
    """Callers catch InvalidTransactionHandle as a programming error,
    NOT as a transient SPLError — but it must remain in the hierarchy."""
    from custos_spl import SPLError

    assert issubclass(InvalidTransactionHandle, SPLError)
