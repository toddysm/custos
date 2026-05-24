"""Service-token expiry sweeper (AS-IMPL-016, GH-#251).

The sweeper is a small in-process background loop launched from
:func:`custos_auth.create_app`'s lifespan. Every
``CUSTOS_AUTH_TOKEN_SWEEPER_INTERVAL_SECONDS`` (default 300 s) it
runs one ``sweep_once`` cycle:

1. Snapshot the set of tokens whose ``expires_at < now`` via
   :meth:`AuthStoreProvider.list_expired_service_tokens`.
2. For each row in the snapshot:
    * Look up the owning :class:`ServiceAccount` to discover the
      workspace bucket (defensive — if the SA has been hard-deleted
      between the snapshot and the lookup we fall back to the
      platform sentinel rather than dropping the audit row).
    * Emit ``token.expired`` (best-effort, post-snapshot) via
      :func:`audit_token_expired`.
    * Publish ``custos.auth.token-revoked`` so per-replica authn
      caches evict in O(1) via the ``token_id`` reverse index.
3. Issue a single
   :meth:`AuthStoreProvider.delete_expired_service_tokens` with the
   same ``before`` value so the deleted row set matches the audit
   row set exactly.

The sweeper is **at-least-once** and **idempotent**: re-running the
same cycle is a no-op because the second snapshot is empty (the
rows were physically deleted in step 3). A crash between steps 2
and 3 leaves the rows in place; the next cycle re-emits the audit
row (acceptable — operators see "this token expired" once or
twice, never zero times) and re-deletes.

Jitter
------

Every replica adds ±25 % of the configured interval to its sleep so
N replicas don't queue an exact-second-boundary sweep at the same
time and stampede the SPL connection pool. The jitter is local-
random (no shared seed) so the per-replica delay distribution is
unsynchronised.

Cancellation
------------

The lifespan handler stores the :class:`asyncio.Task` on
``app.state.token_sweeper_task`` and cancels + awaits it on
shutdown. The loop catches :class:`asyncio.CancelledError` and
exits cleanly without running a partial cycle.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Final

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.interfaces.auth_store import ServiceAccount, ServiceToken

from custos_auth.audit import PLATFORM_WORKSPACE_ID, audit_token_expired
from custos_auth.token_revoked_events import (
    TokenRevokedEvent,
    TokenRevokedPublisher,
)

logger = logging.getLogger(__name__)

#: Width of the per-cycle jitter window expressed as a fraction of
#: the configured interval. ±25 % means a replica with a 300 s
#: interval sleeps between 225 s and 375 s.
_JITTER_FRACTION: Final[float] = 0.25


async def sweep_once(
    *,
    auth_store: AuthStoreProvider,
    metadata_store: MetadataStoreProvider,
    publisher: TokenRevokedPublisher,
    now: datetime | None = None,
) -> int:
    """Run a single sweep cycle. Returns the number of rows deleted.

    Exposed as a public coroutine so tests (and ad-hoc admin
    tooling) can drive a deterministic cycle without spinning up
    the background loop.
    """
    moment = now if now is not None else datetime.now(UTC)
    expired = await auth_store.list_expired_service_tokens(moment)
    for token in expired:
        await _emit_for(
            token,
            auth_store=auth_store,
            metadata_store=metadata_store,
            publisher=publisher,
        )
    deleted = await auth_store.delete_expired_service_tokens(moment)
    return deleted


async def _emit_for(
    token: ServiceToken,
    *,
    auth_store: AuthStoreProvider,
    metadata_store: MetadataStoreProvider,
    publisher: TokenRevokedPublisher,
) -> None:
    """Audit + publish a single expired-token row.

    Audit and publish are independent best-effort calls; a drop on
    one does not skip the other.
    """
    workspace_id = await _resolve_workspace(auth_store, str(token.service_account_id))
    try:
        await audit_token_expired(
            metadata_store,
            workspace_id=workspace_id,
            token_id=str(token.token_id),
            service_account_id=str(token.service_account_id),
            expires_at=token.expires_at,
        )
    except Exception:
        logger.warning(
            "token.expired audit emission failed for token_id=%s",
            token.token_id,
            exc_info=True,
        )
    try:
        await publisher.publish(
            TokenRevokedEvent(
                token_id=str(token.token_id),
                token_hash=token.hash,
                service_account_id=str(token.service_account_id),
            ),
        )
    except Exception:
        logger.warning(
            "token-revoked publish failed for token_id=%s",
            token.token_id,
            exc_info=True,
        )


async def _resolve_workspace(auth_store: AuthStoreProvider, service_account_id: str) -> str:
    """Return the SA's workspace_id, or the platform sentinel.

    SAs are never hard-deleted under the design contract, but the
    sweeper is platform housekeeping — surfacing a TOCTOU race as a
    drop would leak rows from the audit feed. Falling back to the
    platform bucket keeps the row in the pipeline (operators can
    re-bucket later) without leaking it under an arbitrary
    workspace.
    """
    from custos_spl.ids import PrincipalId

    sa = await auth_store.get_principal(PrincipalId(service_account_id))
    if isinstance(sa, ServiceAccount):
        return str(sa.workspace_id)
    return PLATFORM_WORKSPACE_ID


def _jittered_interval(interval_seconds: int) -> float:
    """Return a sleep duration with ±:data:`_JITTER_FRACTION` jitter.

    Local-random so the per-replica delay distribution is
    unsynchronised across N replicas.
    """
    if interval_seconds <= 0:
        return 0.0
    spread = interval_seconds * _JITTER_FRACTION
    return max(0.0, interval_seconds + random.uniform(-spread, spread))


async def run_sweeper_loop(
    *,
    auth_store: AuthStoreProvider,
    metadata_store: MetadataStoreProvider,
    publisher: TokenRevokedPublisher,
    interval_seconds: int,
) -> None:
    """Run :func:`sweep_once` forever with jittered sleeps.

    Exits cleanly on :class:`asyncio.CancelledError` (the lifespan
    handler cancels and awaits the task on shutdown). Any other
    exception is logged and the loop continues — a misbehaving SPL
    must not silently disable platform housekeeping.
    """
    if interval_seconds <= 0:
        logger.info("token sweeper disabled (interval_seconds=%d)", interval_seconds)
        return
    logger.info(
        "token sweeper starting (interval=%ds, jitter=±%d%%)",
        interval_seconds,
        int(_JITTER_FRACTION * 100),
    )
    while True:
        try:
            await asyncio.sleep(_jittered_interval(interval_seconds))
            deleted = await sweep_once(
                auth_store=auth_store,
                metadata_store=metadata_store,
                publisher=publisher,
            )
            if deleted:
                logger.info("token sweeper deleted %d expired token(s)", deleted)
        except asyncio.CancelledError:
            logger.info("token sweeper stopping")
            raise
        except Exception:
            logger.exception("token sweeper cycle failed; continuing")


__all__ = [
    "run_sweeper_loop",
    "sweep_once",
]
