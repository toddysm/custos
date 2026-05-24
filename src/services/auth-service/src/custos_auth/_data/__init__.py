"""Package-data root for auth-service runtime assets.

Currently hosts the bundled platform-M1 permissions registry YAML
consumed by :mod:`custos_auth.permission_registry`. Importable via
``importlib.resources.files("custos_auth._data").joinpath(...)``.
"""

from __future__ import annotations

__all__: list[str] = []
