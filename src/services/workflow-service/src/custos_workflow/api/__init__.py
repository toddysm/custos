"""Public REST + Internal RPC surface for the Workflow Service.

This package implements the WF-IMPL-061..072 API Adapter + Validator
sub-module. WF-IMPL-061 ships only :mod:`custos_workflow.api.errors`
— the RFC 7807 :class:`ProblemDetail` envelope, the locked kind →
status mapping, and the FastAPI exception handlers that translate
:mod:`custos_workflow.runs.errors` /
:mod:`custos_workflow.validator.errors` into wire envelopes.

The wire Pydantic models (WF-IMPL-062), dependency factories
(WF-IMPL-064), REST routers (WF-IMPL-065 / -066), Internal RPC
routers (WF-IMPL-067 / -068), and the ``create_app`` wiring
(WF-IMPL-069 / -070) land in their own PRs and extend this
package incrementally.
"""

from __future__ import annotations

from custos_workflow.api.errors import (
    LOCKED_API_KIND_TO_STATUS,
    LOCKED_API_KINDS,
    PROBLEM_TYPE_PREFIX,
    ProblemDetail,
    register_exception_handlers,
)
from custos_workflow.api.models import (
    CancelRunRequest,
    RaiseExternalEventRequest,
    RunListQuery,
    RunListResponse,
    RunRefResponse,
    RunResponse,
    StartRunRequest,
    StartRunResponse,
    StepAttemptSummary,
    StepResponse,
)

__all__ = [
    "LOCKED_API_KINDS",
    "LOCKED_API_KIND_TO_STATUS",
    "PROBLEM_TYPE_PREFIX",
    "CancelRunRequest",
    "ProblemDetail",
    "RaiseExternalEventRequest",
    "RunListQuery",
    "RunListResponse",
    "RunRefResponse",
    "RunResponse",
    "StartRunRequest",
    "StartRunResponse",
    "StepAttemptSummary",
    "StepResponse",
    "register_exception_handlers",
]
