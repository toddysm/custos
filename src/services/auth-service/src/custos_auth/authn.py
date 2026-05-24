"""Service-token verification (AS-IMPL-014).

The verify path takes a raw bearer string and returns:

* a SPL :class:`~custos_spl.interfaces.auth_store.Principal` (the
  :class:`ServiceAccount` row the token authenticates) on success, or
* ``None`` on any negative outcome (unknown, malformed, revoked,
  expired, owning SA disabled).

The caller — the verify HTTP endpoint, or
:func:`custos_auth.callctx_dev_shim` once Phase G wires it — maps a
``None`` to a 401 with the call-context error envelope and a
:class:`Principal` to the dev-shim claims set.

Flow
----

1. **Shape gate**. Reject bearers that do not match
   :func:`~custos_auth.tokens.looks_like_custos_token` without
   touching the SPL or the cache. This keeps an attacker's
   garbage-input probe off the hot path and limits the per-replica
   cache key space.
2. **Authn-cache read**. If the input's SHA-256 hash is cached and
   still live, return the cached :class:`Principal` immediately and
   emit an ``authn.success`` row with ``cache_hit=True``. No SPL
   lookup, no ``token.used`` row — that event is reserved for the
   first verify after a rotation, signalled by a cache miss.
3. **SPL fetch**. On a cache miss the verifier calls
   :meth:`AuthStoreProvider.get_service_token_by_hash` with the
   computed hash. A missing row → ``unknown-token`` failure path.
4. **Liveness checks**. The row must not be revoked, must not be
   expired, and its owning SA must exist and not be disabled. Each
   negative outcome maps to a distinct ``authn.failure`` reason so
   operators can disambiguate from the audit pipeline.
5. **Cache write + ``token.used``**. A successful liveness check
   stores the principal in the authn cache (so subsequent verifies
   within the TTL window hit step 2) and emits ``token.used`` +
   ``authn.success`` (``cache_hit=False``). The dual-event design
   matches the AS-IMPL-014 acceptance criterion that ``token.used``
   fires on first use after rotation while ``authn.success`` fires
   at the gateway entry path on every successful verify.

Constant-time
-------------

The lookup is by deterministic hash, so the SPL adapter's
indexed lookup does the heavy lifting. After the SPL returns a
candidate row, the verifier re-compares the computed
``token_hash`` against the row's stored ``hash`` with
:func:`hmac.compare_digest` so a hash-table collision (impossible
at the SHA-256 level, but the SPL adapter is free to use any
storage scheme) cannot let a different token through. The
mismatch path is indistinguishable from a genuine miss on the
wire — both surface the ``unknown-token`` audit reason.

Best-effort audit
-----------------

The verify path runs *before* the call-context middleware has
established a calling identity, so failure to emit an audit row
cannot roll back anything — there is no transaction to roll back.
Audit calls are wrapped in :func:`_safe_audit` so a failing
metadata-store does not turn a successful verify into a 5xx;
:func:`audit_authn_*` already swallows storage exceptions and bumps
the failure counter, but the wrapping here adds belt-and-braces
against an audit-emitter bug.
"""

from __future__ import annotations

import hmac
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Final

from custos_spl.interfaces.auth_store import (
    AuthStoreProvider,
    Principal,
    ServiceAccount,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_auth.audit import (
    audit_authn_failure,
    audit_authn_success,
    audit_token_used,
)
from custos_auth.authn_cache import AuthnCache
from custos_auth.tokens import hash_token, looks_like_custos_token

_LOGGER = logging.getLogger("custos_auth.authn")

#: ``reason`` values used in :func:`audit_authn_failure` rows. The
#: set is closed by design — adding a new reason requires a
#: corresponding test so operators do not silently lose a
#: disambiguation knob.
REASON_MALFORMED: Final[str] = "malformed-token"
REASON_UNKNOWN: Final[str] = "unknown-token"
REASON_REVOKED: Final[str] = "revoked"
REASON_EXPIRED: Final[str] = "expired"
REASON_SA_MISSING: Final[str] = "sa-missing"
REASON_SA_DISABLED: Final[str] = "sa-disabled"


async def verify_token(
    raw_token: str,
    *,
    auth_store: AuthStoreProvider,
    metadata_store: MetadataStoreProvider,
    authn_cache: AuthnCache,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Principal | None:
    """Verify ``raw_token`` and return the authenticated :class:`Principal`.

    Returns ``None`` on any negative outcome; the caller surfaces a
    401. Emits exactly one audit row per call:
    ``authn.success`` on the happy path (with ``cache_hit`` tagging
    the source) or ``authn.failure`` (with ``reason`` carrying the
    distinguishing tag).

    ``now`` is injectable so tests can exercise the expiry branch
    without sleeping. Defaults to UTC wall clock.
    """
    if not looks_like_custos_token(raw_token):
        await _safe_audit(audit_authn_failure(metadata_store, reason=REASON_MALFORMED))
        return None

    token_hash = hash_token(raw_token)

    cached = authn_cache.get(token_hash)
    if cached is not None:
        await _safe_audit(
            audit_authn_success(
                metadata_store,
                workspace_id=_workspace_of(cached.principal),
                token_id=cached.token_id,
                service_account_id=str(cached.principal.principal_id),
                cache_hit=True,
            )
        )
        return cached.principal

    row = await auth_store.get_service_token_by_hash(token_hash)
    if row is None:
        # The plaintext did not match any stored hash. We do not
        # carry the hash on the failure row — the audit pipeline
        # treats ``unknown-token`` as the catch-all for both
        # genuine misses and chosen-plaintext probes.
        await _safe_audit(audit_authn_failure(metadata_store, reason=REASON_UNKNOWN))
        return None

    # Defence in depth: the SPL adapter indexes on ``hash`` so the
    # lookup is already O(1) on the digest, but the contract does
    # not forbid a future adapter from using a hash-table scheme
    # that could in principle return a near-miss row. Re-compare
    # the lookup hash against the row's stored hash in constant
    # time (``hmac.compare_digest``) so a hash-table collision
    # cannot let a different token through. A mismatch maps to the
    # ``unknown-token`` reason — the indistinguishability from a
    # genuine miss is intentional anti-probing behaviour.
    if not hmac.compare_digest(token_hash, row.hash):
        await _safe_audit(audit_authn_failure(metadata_store, reason=REASON_UNKNOWN))
        return None

    token_id = str(row.token_id)
    service_account_id = str(row.service_account_id)

    if row.revoked_at is not None:
        # A revoked row could legitimately still be in the SPL
        # table during the eviction-event-in-flight window for any
        # subscriber that has not yet processed the event; the
        # SPL row is the authoritative source so we honour it
        # regardless of what the cache might still be holding.
        await _safe_audit(
            audit_authn_failure(
                metadata_store,
                reason=REASON_REVOKED,
                token_id=token_id,
                service_account_id=service_account_id,
            )
        )
        return None

    if row.expires_at <= now():
        await _safe_audit(
            audit_authn_failure(
                metadata_store,
                reason=REASON_EXPIRED,
                token_id=token_id,
                service_account_id=service_account_id,
            )
        )
        return None

    sa = await auth_store.get_principal(row.service_account_id)
    if sa is None or not isinstance(sa, ServiceAccount):
        # Either the SA was hard-deleted (design forbids this but
        # we are defensive) or some adapter quirk returned a non-
        # SA row. Either way, the token cannot authenticate a
        # principal that does not exist as an SA.
        await _safe_audit(
            audit_authn_failure(
                metadata_store,
                reason=REASON_SA_MISSING,
                token_id=token_id,
                service_account_id=service_account_id,
            )
        )
        return None

    if sa.disabled_at is not None:
        # Disabled SAs cannot authenticate. The token row may still
        # be unrevoked — disabling the SA is the "kill switch"
        # path and is observable at verify time without requiring
        # an operator to revoke every outstanding token.
        await _safe_audit(
            audit_authn_failure(
                metadata_store,
                reason=REASON_SA_DISABLED,
                workspace_id=str(sa.workspace_id),
                token_id=token_id,
                service_account_id=service_account_id,
            )
        )
        return None

    workspace_id = str(sa.workspace_id)

    # Cache miss + happy path → emit ``token.used`` (first use
    # signal) and stash the principal so subsequent verifies inside
    # the TTL window short-circuit through the cache.
    authn_cache.put(token_hash, principal=sa, token_id=token_id)
    await _safe_audit(
        audit_token_used(
            metadata_store,
            workspace_id=workspace_id,
            token_id=token_id,
            service_account_id=service_account_id,
        )
    )
    await _safe_audit(
        audit_authn_success(
            metadata_store,
            workspace_id=workspace_id,
            token_id=token_id,
            service_account_id=service_account_id,
            cache_hit=False,
        )
    )
    return sa


def _workspace_of(principal: Principal) -> str:
    """Return the audit-bucket workspace id for ``principal``.

    Service-account principals carry a concrete workspace id; the
    audit rows for token operations key under that id. Users have a
    tenant id rather than a workspace id, but the verify path only
    issues principals that are service accounts (the SPL contract
    on :meth:`AuthStoreProvider.get_service_token_by_hash` returns
    rows keyed to a SA's ``principal_id``), so this helper safely
    casts to ``ServiceAccount``.
    """
    assert isinstance(principal, ServiceAccount), (
        "verify_token only ever caches ServiceAccount principals"
    )
    return str(principal.workspace_id)


async def _safe_audit(coro: Awaitable[None]) -> None:
    """Run an audit coroutine and swallow any exception.

    The audit emitters already swallow store-side exceptions and
    bump :data:`custos_auth.audit.EMIT_FAILURES_TOTAL`; this wrapper
    catches the still-broader programming-error class so a buggy
    audit call does not turn a successful verify into a 5xx. Logged
    at WARNING.
    """
    try:
        await coro
    except Exception:  # guard the verify hot path
        _LOGGER.warning("authn audit emission raised; continuing", exc_info=True)


__all__ = [
    "REASON_EXPIRED",
    "REASON_MALFORMED",
    "REASON_REVOKED",
    "REASON_SA_DISABLED",
    "REASON_SA_MISSING",
    "REASON_UNKNOWN",
    "verify_token",
]
