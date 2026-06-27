"""Scaffold smoke test: the package imports and reports its version."""

from __future__ import annotations

import copy_image


def test_version() -> None:
    assert copy_image.__version__ == "0.1.0"
