"""Out-of-the-box Custos GHCR connector plugin.

Implements the connector-service plugin runtime hook ABI (``bind`` and
``health``) over stdin/stdout JSON. See
``docs/developers/connector-plugin-author.md`` for the wire contract and
``README.md`` for the two-layer GHCR token model.
"""

from __future__ import annotations

from .plugin import PluginError, handle

__all__ = ["PluginError", "handle"]
