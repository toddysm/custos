"""Out-of-the-box Custos copy-image activity.

Copies an OCI image from a bound ``source`` connector to a bound ``dest``
connector (canonical binding: Docker Hub -> GHCR). Implements the
file-based ARM activity contract (``/custos/in`` -> ``/custos/out``); see
``docs/developers/activity-author.md`` and ``README.md``.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
