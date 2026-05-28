"""Custos Workflow Service (COMP-003).

This package hosts the Workflow Service runtime: the Definition Compiler
that turns a published ``WorkflowVersion`` into an ``ExecutionGraph``, the
Run / Step / StepAttempt orchestration state machine over Dapr Workflow,
sub-orchestration management for dynamic loops and approval gates, resume
subscription lifecycle, and publication of workflow lifecycle events to
``custos.workflow.events``.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/workflow-service/design.md

WF-IMPL-015 wires the FastAPI application factory, the call-context
middleware shim, and the ``/healthz`` / ``/readyz`` probes. The rest of
the runtime (compiler internals, Run/Step coordination, resume
subscriptions) lands incrementally across WF-IMPL-016 through
WF-IMPL-028 (see issue #363, the WF-IMPL-000-COMPILER sub-module
tracker).
"""

from __future__ import annotations

from custos_workflow._version import __version__
from custos_workflow.app import create_app

__all__ = ["__version__", "create_app"]
