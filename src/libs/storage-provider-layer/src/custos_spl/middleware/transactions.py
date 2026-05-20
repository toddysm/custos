"""Transaction handle binding — single chokepoint for cross-provider misuse.

The Storage Provider Layer offers **intra-provider** transactions only:
a caller can wrap multiple calls on one provider in `with_transaction`
and share atomicity, but handles never cross provider boundaries (see
`design/components/storage-provider-layer/design.md` § Transaction
Model). Passing a handle from `MetadataStoreProvider` to a method on
`AuthStoreProvider` is a programming error and raises
`InvalidTransactionHandle`.

This module is the canonical implementation of that check. Adapters use
it from two places:

  - Inside `with_transaction`, after constructing the concrete handle,
    they call `bind_handle(handle, self)` to record the owning provider
    before invoking the caller's body.
  - At the top of every tx-aware method (e.g. `append_step`,
    `append_audit`), they call `check_handle(tx, self)` to validate
    that the incoming handle was issued by *this* provider instance.

The binding registry is a module-level `weakref.WeakKeyDictionary`
keyed by the handle, and the value is a `weakref.ref` to the
provider. Two reasons for the value-side weakref: entries auto-evict
once the handle goes out of scope at the end of `with_transaction`,
and — crucially — the comparison uses object identity (`is`) on the
live provider rather than `id()`, so a provider that has been
garbage-collected cannot have its address recycled into a false
positive match against a new object.

The functions are intentionally narrow: they do not import any adapter,
nor do they know about backends. Their only job is the contract check.
"""

from __future__ import annotations

import weakref
from typing import Final

from custos_spl.errors import InvalidTransactionHandle
from custos_spl.interfaces.metadata_store import TransactionHandle

_owner: Final[
    weakref.WeakKeyDictionary[TransactionHandle, weakref.ref[object]]
] = weakref.WeakKeyDictionary()
"""handle → weakref(provider). Weak keys so handles GC normally; weak
values so a dead provider cannot have its identity recycled into a
false match (id() values can be reused after GC)."""


def bind_handle(handle: TransactionHandle, provider: object) -> None:
    """Record that `handle` was issued by `provider`.

    Adapters call this once inside `with_transaction`, immediately
    after constructing the concrete `TransactionHandle` subclass and
    before invoking the caller's body.

    Raises:
        TypeError: if `handle` is not a `TransactionHandle` instance,
            or if `provider` does not support weak references.
    """
    if not isinstance(handle, TransactionHandle):
        raise TypeError(
            f"bind_handle expected a TransactionHandle, "
            f"got {type(handle).__name__}"
        )
    try:
        _owner[handle] = weakref.ref(provider)
    except TypeError as exc:  # pragma: no cover - depends on host class
        raise TypeError(
            f"provider {type(provider).__name__} does not support weak "
            f"references; adapters must allow weakrefs so handle "
            f"ownership cannot be spoofed by id() reuse"
        ) from exc


def check_handle(
    handle: TransactionHandle | None,
    provider: object,
    *,
    required: bool = False,
) -> None:
    """Validate that `handle` was issued by `provider`.

    Called at the top of every tx-aware adapter method. The check is
    cheap (one dict lookup, one `is` compare) and never touches the
    backend.

    Args:
        handle: The transaction handle the caller passed in, or `None`
            if they want a fresh implicit transaction.
        provider: The adapter instance running the call — almost
            always `self`.
        required: If `True`, raise when `handle is None`. The default
            (`False`) matches the audit outbox pattern, where the
            handle is optional and the adapter falls back to an
            implicit transaction.

    Raises:
        InvalidTransactionHandle: if `handle` is `None` and `required`
            is `True`, if `handle` is not a `TransactionHandle`
            instance, if the handle was never registered with
            `bind_handle` (or its `with_transaction` scope has already
            ended), or if it was issued by a different provider.
    """
    if handle is None:
        if required:
            raise InvalidTransactionHandle(
                "transaction handle is required for this call but was None"
            )
        return
    if not isinstance(handle, TransactionHandle):
        raise InvalidTransactionHandle(
            f"expected a TransactionHandle, got {type(handle).__name__}"
        )
    owner_ref = _owner.get(handle)
    if owner_ref is None:
        raise InvalidTransactionHandle(
            "transaction handle was never registered (or its "
            "with_transaction scope has already ended)"
        )
    owner = owner_ref()
    if owner is None:
        # Provider was GC'd while the handle was still around. The
        # binding is stale; treat the handle as unusable rather than
        # risk an id() collision against any future object.
        raise InvalidTransactionHandle(
            "transaction handle's issuing provider has been garbage "
            "collected — the handle is no longer usable"
        )
    if owner is not provider:
        raise InvalidTransactionHandle(
            "transaction handle was issued by a different provider "
            "than the one it is being used on — cross-provider "
            "transactions are not supported"
        )


__all__ = [
    "bind_handle",
    "check_handle",
]
