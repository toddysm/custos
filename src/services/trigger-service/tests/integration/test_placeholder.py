"""Integration-suite placeholder for the Trigger Service (TS-IMPL-001).

The real Postgres-backed integration flows (manual-fire → ``StartRun`` and
resume-register → internal event → ``RaiseExternalEvent``) land in
TS-IMPL-020. This placeholder keeps the ``tests/integration`` path present
so the ``trigger-service-integration`` CI job collects at least one
``integration``-marked test and stays green during scaffolding.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.integration
def test_integration_suite_placeholder() -> None:
    # Sanity-check the package is importable inside the integration
    # environment; replaced by real end-to-end flows in TS-IMPL-020.
    module = importlib.import_module("custos_trigger")
    assert module.__version__ == "0.1.0"
