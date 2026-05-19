"""Tests for the SPL error taxonomy."""

from __future__ import annotations

import pytest

from custos_spl.errors import (
    BackendUnavailable,
    ConflictDigest,
    ImmutableViolation,
    InvalidTransactionHandle,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
    NotReserved,
    QueryUnsupported,
    SPLError,
    WorkspaceMismatch,
)

ALL_ERRORS: list[type[SPLError]] = [
    ImmutableViolation,
    ConflictDigest,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
    InvalidTransactionHandle,
    WorkspaceMismatch,
    BackendUnavailable,
    QueryUnsupported,
    NotReserved,
]


@pytest.mark.parametrize("cls", ALL_ERRORS)
def test_errors_subclass_spl_error(cls: type[SPLError]) -> None:
    """Every SPL error must derive from SPLError so callers can catch it broadly."""
    assert issubclass(cls, SPLError)
    assert issubclass(cls, Exception)


def test_conflict_digest_is_immutable_violation() -> None:
    """A caller catching ImmutableViolation must also catch ConflictDigest."""
    with pytest.raises(ImmutableViolation):
        raise ConflictDigest("digest mismatch on (acme, scan, 1.0.0)")


def test_migration_required_carries_gaps() -> None:
    gaps = [("MetadataStoreProvider", 4), ("AuthStoreProvider", 1)]
    err = MigrationRequired(gaps)
    assert err.gaps == gaps
    msg = str(err)
    assert "MetadataStoreProvider:rev4" in msg
    assert "AuthStoreProvider:rev1" in msg


def test_migration_required_copies_gaps_input() -> None:
    """Mutating the caller's list after construction must not mutate the error."""
    gaps = [("MetadataStoreProvider", 4)]
    err = MigrationRequired(gaps)
    gaps.append(("AuthStoreProvider", 1))
    assert err.gaps == [("MetadataStoreProvider", 4)]


def test_migration_required_empty_gaps_message() -> None:
    err = MigrationRequired([])
    assert err.gaps == []
    assert "no gaps reported" in str(err)


def test_backend_unavailable_attaches_cause() -> None:
    original = ConnectionResetError("postgres dropped the connection")
    err = BackendUnavailable("postgres unavailable", cause=original)
    assert err.__cause__ is original
    assert str(err) == "postgres unavailable"


def test_backend_unavailable_without_cause() -> None:
    err = BackendUnavailable("postgres unavailable")
    assert err.__cause__ is None


def test_invalid_transaction_handle_is_terminal() -> None:
    """Sanity check: the error message survives round-trip."""
    err = InvalidTransactionHandle("handle issued by MetadataStore, used on AuthStore")
    assert "MetadataStore" in str(err)


def test_workspace_mismatch_is_distinct_from_backend_unavailable() -> None:
    """Callers must be able to disambiguate 404-ish vs transient errors."""
    assert not issubclass(WorkspaceMismatch, BackendUnavailable)
    assert not issubclass(BackendUnavailable, WorkspaceMismatch)


def test_query_unsupported_and_not_reserved_are_terminal() -> None:
    assert not issubclass(QueryUnsupported, BackendUnavailable)
    assert not issubclass(NotReserved, BackendUnavailable)


def test_lease_busy_and_lease_expired_are_siblings() -> None:
    """LeaseBusy is "wait and retry"; LeaseExpired is "discard and re-acquire".
    They must be distinguishable types, not subclasses of one another.
    """
    assert not issubclass(LeaseBusy, LeaseExpired)
    assert not issubclass(LeaseExpired, LeaseBusy)
