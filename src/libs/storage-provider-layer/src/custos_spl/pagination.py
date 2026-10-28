"""Pagination types shared across provider list operations.

`Cursor` is opaque to callers — the adapter encodes its backend-specific
pagination token inside `Cursor.token`. Callers MUST treat it as a black
box and pass it back unchanged on the next page request.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Cursor:
    """Opaque pagination token.

    The adapter chooses the encoding (offset, keyset, base64-encoded
    backend cursor, etc.). Callers MUST NOT inspect or modify `token`.
    """

    token: str


@dataclass(frozen=True)
class Page(Generic[T]):
    """A single page of results plus the cursor to fetch the next page.

    `next_cursor` is `None` iff this page is the last one. An empty
    `items` tuple with a non-`None` `next_cursor` is legal — it means
    "no matches in this window, keep paging".

    `items` is stored as a `tuple` so the Page is deeply immutable: a
    caller cannot mutate the page contents after the fact, and a list
    handed in to the constructor is snapshotted (not aliased).
    """

    items: tuple[T, ...] = field(default=())
    next_cursor: Cursor | None = None

    def __init__(
        self,
        items: Iterable[T] = (),
        next_cursor: Cursor | None = None,
    ) -> None:
        object.__setattr__(self, "items", tuple(items))
        object.__setattr__(self, "next_cursor", next_cursor)


def query_page_limit(limit: int | None) -> int:
    """Resolve trigger-query page sizes, rejecting invalid or non-positive values."""
    if limit is None:
        return 50
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("query limit must be a positive integer")
    return min(limit, 200)


def encode_query_cursor(scope: tuple[str | None, ...], key: str) -> Cursor:
    """Encode an identifier and query scope as an opaque, versioned page cursor."""
    raw = json.dumps([1, scope, key], ensure_ascii=True).encode("ascii")
    return Cursor(base64.urlsafe_b64encode(raw).decode("ascii"))


def decode_query_cursor(cursor: Cursor | None, scope: tuple[str | None, ...]) -> str | None:
    """Decode a query cursor, rejecting malformed tokens and scope/filter changes."""
    if cursor is None:
        return None
    try:
        raw = base64.b64decode(cursor.token, altchars=b"-_", validate=True)
        value = json.loads(raw)
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise ValueError("invalid query cursor") from exc
    if (
        not isinstance(value, list)
        or len(value) != 3
        or type(value[0]) is not int
        or value[0] != 1
        or value[1] != list(scope)
        or not isinstance(value[2], str)
    ):
        raise ValueError("query cursor does not match this query")
    return value[2]


__all__ = [
    "Cursor",
    "Page",
    "decode_query_cursor",
    "encode_query_cursor",
    "query_page_limit",
]
