"""Reference Custos slack-notifier sink connector plugin.

A minimal sink connector: it advertises a single notify capability,
no events block (the connector cannot emit events into the
platform), and a workload-identity-backed credential shape. The
plugin exists primarily to validate the optional-``events`` code path
in CONN-IMPL-005 and the fallback-tag publish path in CONN-IMPL-007.
"""

from __future__ import annotations

from .plugin import handle

__all__ = ["handle"]
__version__ = "1.0.0"
