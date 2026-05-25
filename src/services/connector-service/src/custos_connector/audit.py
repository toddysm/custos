"""Audit emission stub for connector-service (Phase B placeholder).

The real audit pipeline (CONN-IMPL-029 in Phase K) will write through the
SPL ``MetadataStoreProvider.append_audit`` outbox the same way
catalog-service does post-CS-IMPL-019. Until then, the dev-shim
call-context middleware fires :func:`emit_event` for every
``auth.callctx.shim_used`` event, and the FastAPI dependency layer fires
it for every ``authz.decision`` (CONN-IMPL-004 acceptance criterion).

The stub here logs at INFO so operators can see the dev shim is active and
test fixtures can assert via ``caplog``; a follow-up issue will replace
this hook with typed ``audit_*`` helpers that talk to
:class:`custos_spl.MetadataStoreProvider`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

#: Audit logger name; tests capture this via :class:`pytest.LogCaptureFixture`.
_AUDIT_LOGGER = logging.getLogger("custos_connector.audit")
logger = _AUDIT_LOGGER


def emit_event(name: str, payload: Mapping[str, Any]) -> None:
    """Emit a structured audit-style log line.

    Retained for the call-context dev-shim hook + the authorization-decision
    hook fired from :func:`custos_connector.middleware.require_permission`.
    Both fire before the FastAPI DI machinery has yielded a configured
    :class:`~custos_spl.MetadataStoreProvider`, so the legacy log-only hook
    is the right tool until the real audit-pipeline ticket lands.

    Args:
        name: Canonical event name (e.g. ``auth.callctx.shim_used``,
            ``authz.decision``).
        payload: Per-event attributes. Must be JSON-serialisable; values
            that aren't are coerced via ``repr`` so the audit log line is
            never lost.
    """
    try:
        body = json.dumps(dict(payload), default=str, sort_keys=True)
    except (TypeError, ValueError):
        body = repr(dict(payload))
    _AUDIT_LOGGER.info("audit_event name=%s payload=%s", name, body)


__all__ = ["emit_event", "logger"]
