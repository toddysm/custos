"""Pagination types shared across provider list operations.

`Cursor` is opaque to callers — the adapter encodes its backend-specific
pagination token inside `Cursor.token`. Callers MUST treat it as a black
box and pass it back unchanged on the next page request.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    `items` list with a non-`None` `next_cursor` is legal — it means
    "no matches in this window, keep paging".
    """

    items: list[T]
    next_cursor: Cursor | None


__all__ = ["Cursor", "Page"]
