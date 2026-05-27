"""Pull-cursor lifecycle for Connector Service (CONN-IMPL-022).

Implements the cursor-side of the Pull Cursor Model documented in
``design/components/connector-service/design.md`` § Pull Cursor Model:

* One cursor per ``ConnectorInstance`` (the platform is the single
  writer; the plugin is the single producer of the opaque ``value``).
* At-least-once delivery semantics: publish-ack every event in the
  batch BEFORE the cursor commit, so a crash between publish and
  commit re-emits the batch on next tick — duplicates are absorbed
  by the Trigger Service's per-subscription ``DedupKey``.
* Single-writer safety via the SPL ``acquire_cursor_lease`` /
  ``commit_cursor`` / ``release_cursor_lease`` primitive (60 s
  window by default per tick).
* Encoding migration: a connector-type ``cursorEncoding`` bump
  triggers ``CursorEncodingMismatch`` from the plugin; ticks halt
  pending operator rewind.
* Cursor expiry: an upstream-rejected position triggers
  ``CursorExpired`` from the plugin; ticks halt pending operator
  action.

The Pull-loop scheduler (CONN-IMPL-023, #306) and the admin REST
surface (CONN-IMPL-024, #307) layer on top of this module — neither
ships here.
"""

from custos_connector.cursor.service import (
    DEFAULT_CURSOR_LEASE_TTL_SECONDS,
    HALT_STATUS_ENCODING_MISMATCH,
    HALT_STATUS_EXPIRED,
    CursorEncodingMismatchHalt,
    CursorEnvelopeRecord,
    CursorExpiredHalt,
    CursorHalted,
    CursorInstanceUnavailable,
    CursorService,
    EventPublisher,
    TickResult,
)

__all__ = [
    "DEFAULT_CURSOR_LEASE_TTL_SECONDS",
    "HALT_STATUS_ENCODING_MISMATCH",
    "HALT_STATUS_EXPIRED",
    "CursorEncodingMismatchHalt",
    "CursorEnvelopeRecord",
    "CursorExpiredHalt",
    "CursorHalted",
    "CursorInstanceUnavailable",
    "CursorService",
    "EventPublisher",
    "TickResult",
]
