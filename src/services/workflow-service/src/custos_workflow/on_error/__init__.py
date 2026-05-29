"""On-error route compiler (WF-IMPL-023).

Takes a step's declared ``on_error:`` block (if any) plus the
step's fully-resolved :class:`~custos_workflow.graph.ResolvedRetryPolicy`
and produces an ordered tuple of
:class:`~custos_workflow.graph.OnErrorRoute` triples that the
Step Coordinator walks in declaration order.

See ``design/components/workflow-service/design.md`` § Retry
Policy → § Implicit ``on_error`` policy, § Where ``retry:`` may
appear, and § Runtime behavior for the rules implemented here.
"""

from __future__ import annotations

from custos_workflow.on_error.compile import compile_on_error

__all__ = ["compile_on_error"]
