"""Public REST + RPC API surface for the Trigger Service (TS-IMPL-015..018).

The routers here drive the matching/dispatch pipeline behind the call-context
middleware and surface failures through the RFC 7807 ``application/problem+json``
envelope in :mod:`custos_trigger.api.errors`.
"""

from __future__ import annotations

from custos_trigger.api.errors import (
    PROBLEM_MEDIA_TYPE,
    ProblemDetail,
    problem_response,
    register_exception_handlers,
)

__all__ = [
    "PROBLEM_MEDIA_TYPE",
    "ProblemDetail",
    "problem_response",
    "register_exception_handlers",
]
