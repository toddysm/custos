"""Tests for SPL pagination types."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from custos_spl.pagination import Cursor, Page


def test_cursor_holds_opaque_token() -> None:
    c = Cursor(token="some-backend-encoded-token")
    assert c.token == "some-backend-encoded-token"


def test_cursor_is_frozen() -> None:
    c = Cursor(token="abc")
    with pytest.raises(FrozenInstanceError):
        c.token = "xyz"  # type: ignore[misc]


def test_cursor_equality_by_token() -> None:
    assert Cursor(token="abc") == Cursor(token="abc")
    assert Cursor(token="abc") != Cursor(token="def")


def test_page_with_no_next_cursor_is_terminal() -> None:
    p: Page[str] = Page(items=["a", "b"], next_cursor=None)
    assert p.items == ("a", "b")
    assert p.next_cursor is None


def test_page_with_empty_items_and_cursor_is_legal() -> None:
    """Empty page + non-None cursor means "no matches in this window, keep paging"."""
    p: Page[int] = Page(items=[], next_cursor=Cursor(token="next"))
    assert p.items == ()
    assert p.next_cursor == Cursor(token="next")


def test_page_is_generic_over_item_type() -> None:
    p_str: Page[str] = Page(items=["x"], next_cursor=None)
    p_int: Page[int] = Page(items=[1, 2, 3], next_cursor=Cursor(token="more"))
    assert p_str.items[0] == "x"
    assert sum(p_int.items) == 6


def test_page_items_are_immutable() -> None:
    """Page is a boundary DTO — callers must not be able to mutate items."""
    p: Page[str] = Page(items=["a", "b"], next_cursor=None)
    with pytest.raises(AttributeError):
        p.items.append("c")  # type: ignore[attr-defined]


def test_page_snapshots_items_input() -> None:
    """Mutating the source list after constructing a Page must not affect the Page."""
    src = ["a", "b"]
    p: Page[str] = Page(items=src, next_cursor=None)
    src.append("c")
    assert p.items == ("a", "b")
