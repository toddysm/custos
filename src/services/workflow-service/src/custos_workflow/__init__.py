"""Custos Workflow Service (COMP-003).

This package hosts the Workflow Service runtime: the Definition Compiler
that turns a published ``WorkflowVersion`` into an ``ExecutionGraph``, the
Run / Step / StepAttempt orchestration state machine over Dapr Workflow,
sub-orchestration management for dynamic loops and approval gates, resume
subscription lifecycle, and publication of workflow lifecycle events to
``custos.workflow.events``.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/workflow-service/design.md

The scaffold ships only the package skeleton and a placeholder
:func:`create_app` factory. Real wiring lands incrementally across
WF-IMPL-014 through WF-IMPL-028 (see issue #363, the WF-IMPL-000-COMPILER
sub-module tracker).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import-time only, never executed
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"


def create_app() -> FastAPI:
    """Build and return the Workflow Service FastAPI application.

    This is the canonical entry point used by ``custos_workflow.__main__``
    and by ASGI servers. The real implementation lands in WF-IMPL-015
    (FastAPI app skeleton + healthz/readyz + call-context middleware
    shim). Until then, the factory raises :class:`NotImplementedError`
    so any accidental wiring fails loudly rather than silently serving
    a half-built surface.
    """
    raise NotImplementedError(
        "custos_workflow.create_app() is a scaffold placeholder. "
        "The FastAPI application is wired in WF-IMPL-015 "
        "(see https://github.com/toddysm/custos/issues/349)."
    )
