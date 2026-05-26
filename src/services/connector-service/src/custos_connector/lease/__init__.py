"""Lease Manager package (CONN-IMPL-017).

Re-exports the public surface so callers can ``from
custos_connector.lease import LeaseManager, LeaseError`` without
needing to know which module each symbol lives in.
"""

from __future__ import annotations

from custos_connector.lease.errors import LeaseError, LeaseErrorCode, http_status_for
from custos_connector.lease.service import LeaseManager, TtlInputs

__all__ = [
    "LeaseError",
    "LeaseErrorCode",
    "LeaseManager",
    "TtlInputs",
    "http_status_for",
]
