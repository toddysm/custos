"""End-to-end one-shot (DEVCLI-IMPL-009).

``custosctl e2e`` chains the building blocks into a single smoke run:
bring the platform up → onboard the OOTB catalog → verify it → apply a workflow
→ start a run → wait for it → assert success, optionally tearing down after.
It works against either target (``up``/``down`` already dispatch on target).

Every step delegates to a patchable module function so this orchestration is
unit-tested without a cluster or gateway.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from custosctl import activities, connectors, fixtures, lifecycle, seed, workflows
from custosctl.config import Settings
from custosctl.shell import CommandError

Echo = Callable[[str], None]

#: OOTB entities the post-seed catalog check expects.
_VERIFY_ACTIVITY = ("custos.builtin", "copy-image")
_VERIFY_CONNECTOR = "custos-dockerhub"


def run_e2e(
    settings: Settings,
    *,
    echo: Echo,
    workflow: str | None = None,
    workspace: str | None = None,
    inputs: dict[str, Any] | None = None,
    skip_up: bool = False,
    teardown: bool = False,
    allow_existing: bool = True,
    timeout: float = 1200.0,
    interval: float = 5.0,
) -> bool:
    """Run the end-to-end flow. Returns ``True`` iff the workflow run succeeded.

    Infrastructure failures (bring-up, seed, apply, run) propagate as
    exceptions; a completed-but-unsuccessful run returns ``False``.
    """
    workflow_path = workflow or str(fixtures.sample_workflow_path())
    try:
        if skip_up:
            echo("==> skipping platform bring-up (--skip-up)")
        else:
            echo("==> bringing the platform up")
            lifecycle.up(settings, echo=echo)

        echo("==> onboarding the OOTB catalog (seed-ootb)")
        seed.seed_ootb(settings, allow_existing=allow_existing)

        echo("==> verifying the OOTB catalog")
        _verify_catalog(settings, echo=echo)

        echo(f"==> applying workflow {workflow_path}")
        ref = workflows.apply(settings, path=workflow_path, workspace=workspace)
        version_id = f"{ref['workspaceId']}/{ref['workflowName']}@{ref['version']}"
        echo(f"    published {ref['workflowName']}@{ref['version']} -> {version_id}")

        echo("==> starting a run")
        started = workflows.run(
            settings,
            workflow_version_id=version_id,
            workspace=workspace,
            inputs=inputs,
        )
        run_id = started["runId"]
        echo(f"    run {run_id} ({started.get('status')})")

        echo("==> waiting for the run to reach a terminal status")
        record = workflows.wait_for(
            settings,
            run_id=run_id,
            workspace=workspace,
            timeout=timeout,
            interval=interval,
        )
        succeeded = workflows.is_success(record)
        if succeeded:
            echo(f"PASS: run {run_id} succeeded")
        else:
            echo(f"FAIL: run {run_id} ended {record.get('status')}")
            reason = record.get("reason")
            if reason:
                echo(f"    reason: {reason}")
        return succeeded
    finally:
        if teardown:
            echo("==> tearing down")
            try:
                lifecycle.down(settings, echo=echo)
            except (CommandError, RuntimeError) as exc:
                echo(f"warning: teardown failed: {exc}")


def _verify_catalog(settings: Settings, *, echo: Echo) -> None:
    namespace, activity_type = _VERIFY_ACTIVITY
    acts = activities.list_versions(settings, namespace=namespace, activity_type=activity_type)
    if not acts:
        raise RuntimeError(
            f"OOTB verify failed: {namespace}/{activity_type} activity-type is not registered"
        )
    echo(f"    activity {namespace}/{activity_type}: {len(acts)} version(s)")

    conns = connectors.list_versions(settings, connector_type=_VERIFY_CONNECTOR)
    if not conns:
        raise RuntimeError(
            f"OOTB verify failed: {_VERIFY_CONNECTOR} connector-type is not registered"
        )
    echo(f"    connector {_VERIFY_CONNECTOR}: {len(conns)} version(s)")


__all__ = ["run_e2e"]
