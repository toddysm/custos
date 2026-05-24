"""Shared call-context verifier for Custos components (AS-IMPL-019).

This package ships the helper every Custos component (catalog, workflow,
trigger, …) uses to verify the EdDSA-signed call-context JWT carried in the
``X-Call-Context`` header. The verifier fetches and caches the Auth
Service's JWKS, validates the signature plus ``iss``/``aud``/``exp``/``iat``,
and returns a typed :class:`CallContext` object the caller can use for
authorization.

Design reference:
``design/components/auth-service/design.md`` § Internal vs External Auth —
Trust Model: *"Receiving components verify the signature locally (no Auth
Service round-trip) using the published JWKS."*

Audit hook
----------
The verifier never directly emits a ``call-context.invalid`` audit event —
that emission is the calling component's responsibility because each
component owns its own audit outbox. The library raises
:class:`InvalidCallContextError` with a stable ``reason`` field; middleware
in the consuming service decides whether to record the event.
"""

from __future__ import annotations

from custos_callctx._context import CallContext
from custos_callctx._errors import InvalidCallContextError, InvalidReason
from custos_callctx._jwks import JwksCache
from custos_callctx._verifier import (
    CALLCTX_HEADER,
    DEFAULT_AUDIENCE,
    DEFAULT_ISSUER,
    CallContextVerifier,
)

__all__ = [
    "CALLCTX_HEADER",
    "DEFAULT_AUDIENCE",
    "DEFAULT_ISSUER",
    "CallContext",
    "CallContextVerifier",
    "InvalidCallContextError",
    "InvalidReason",
    "JwksCache",
]

__version__ = "0.1.0"
