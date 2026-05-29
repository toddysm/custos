"""Shared pytest fixtures for the Workflow Service unit-test suite.

The :func:`fake_run_components` fixture returns a fully-wired
:class:`~custos_workflow.providers.RunComponents` bundle backed by a
:class:`~custos_workflow.runtime.FakeWorkflowRuntime` plus
:class:`~custos_workflow.runs.controller.InMemoryLifecycleEventPublisher`,
so :func:`~custos_workflow.create_app` can run its lifespan without
touching the Dapr sidecar (WF-IMPL-043 acceptance criterion).
"""

from __future__ import annotations

import pytest

from custos_workflow.providers import RunComponents, load_run_components
from custos_workflow.runtime import FakeWorkflowRuntime


@pytest.fixture
def fake_run_components() -> RunComponents:
    """A sidecar-free :class:`RunComponents` for ``create_app(run_components=...)``.

    The bundle uses a fresh :class:`FakeWorkflowRuntime` each call so
    tests that run multiple lifespans observe independent worker
    state. The default in-memory adapters (publisher, store,
    reconciler) are sufficient for every Phase-A app-shape test;
    Phase-E integration tests will substitute their own.
    """
    runtime = FakeWorkflowRuntime()
    return load_run_components(env={}, workflow_runtime=runtime)
