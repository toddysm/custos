"""Typed call-context dataclass returned by :func:`CallContextVerifier.verify`."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class CallContext:
    """The decoded, verified call-context for a single internal request.

    The fields mirror the JWT claims minted by
    :class:`custos_auth.callctx_signer.CallContextSigner` (AS-IMPL-017):

    * ``acting_principal_id``  <- ``actingPrincipalId`` claim
    * ``workspace_id``         <- ``workspaceId`` claim (``None`` for
                                  platform-global RPCs)
    * ``caller_component``     <- ``callerComponent`` claim
    * ``jti``                  <- ``jti`` claim
    * ``issued_at``            <- ``iat`` claim
    * ``expires_at``           <- ``exp`` claim
    * ``issuer``               <- ``iss`` claim
    * ``audience``             <- ``aud`` claim
    * ``permissions``          <- ``permissions`` claim (empty when the
                                  token was minted without RBAC grants
                                  embedded; consumer-owned permission
                                  strings, e.g. ``catalog:workflows:read``)
    * ``kid``                  <- JWT header ``kid``

    Frozen so the context can be shared between coroutines / handlers
    without copy-on-write defensive duplication.
    """

    acting_principal_id: str
    workspace_id: str | None
    caller_component: str
    jti: str
    issued_at: int
    expires_at: int
    issuer: str
    audience: str
    kid: str
    permissions: frozenset[str] = field(default_factory=frozenset)

    def has_permission(self, name: str) -> bool:
        """Return ``True`` when ``name`` is present in :attr:`permissions`.

        Matching is exact. Wildcard expansion (``"catalog:*"``) is **not**
        applied here; callers wanting glob semantics should resolve the
        membership in their own RBAC layer before checking.
        """
        return name in self.permissions


__all__ = ["CallContext"]
