"""Custos Storage Provider Layer.

See `design/components/storage-provider-layer/design.md` for the full contract.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("custos_spl")
except PackageNotFoundError:
    __version__ = "0+unknown"
