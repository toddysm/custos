"""Scaffold smoke tests: the package imports and the entry point fails closed."""

from __future__ import annotations

import copy_image
from copy_image.__main__ import main


def test_version() -> None:
    assert copy_image.__version__ == "0.1.0"


def test_scaffold_entrypoint_fails_closed() -> None:
    # Until the contract + copy engine land (COPY-IMPL-002/004), the entry
    # point must not report success.
    assert main([]) == 2
