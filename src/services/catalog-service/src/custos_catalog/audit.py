"""Audit hook stub.

CS-IMPL-019 (#220) will replace this with the real observability + audit
pipeline that writes to the AuditStoreProvider outbox and emits an OTel
span. Until then we log structured events at INFO so smoke tests can
assert emission without standing up a backend.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger("custos_catalog.audit")


def emit_event(name: str, payload: Mapping[str, Any]) -> None:
    """Emit a structured audit event.

    The stub serializes the payload as JSON for log search-ability; the
    final implementation in CS-IMPL-019 will dual-write to the audit
    outbox and emit an OTel span via the existing ``opentelemetry-api``
    dependency. Callers MUST pass JSON-serializable payloads.
    """
    try:
        body = json.dumps(dict(payload), default=str, sort_keys=True)
    except (TypeError, ValueError):
        body = repr(dict(payload))
    logger.info("audit_event name=%s payload=%s", name, body)


__all__ = ["emit_event"]
