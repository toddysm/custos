"""Package version string for the Activity Runtime Manager.

Kept in a tiny standalone module so ``custos_arm.app`` can pull it in
without re-entering the package ``__init__`` (which itself re-exports
``create_app``).
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
