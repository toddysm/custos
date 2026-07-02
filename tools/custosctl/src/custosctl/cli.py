"""custosctl command-line entrypoint (Click).

DEVCLI-IMPL-001 scaffold: the root group with the global ``--target`` /
``--verbose`` / ``--yes`` options, a ``CUSTOS_*``-backed :class:`Settings`
carried on the Click context, and the ``doctor`` preflight. Lifecycle and
API command groups are registered by later DEVCLI tasks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import click

from custosctl import __version__, activities, connectors, lifecycle, seed, workflows
from custosctl.api import ApiError
from custosctl.config import Settings, Target
from custosctl.shell import CommandError, ToolStatus, kube_context_reachable, probe_tool

#: External CLIs each target relies on. Presence gates the preflight.
_LOCAL_TOOLS: tuple[str, ...] = ("docker", "kind", "kubectl", "helm")
_REMOTE_TOOLS: tuple[str, ...] = ("kubectl", "helm")

#: Per-tool version invocation (not every CLI accepts ``--version``).
_TOOL_VERSION_ARGS: dict[str, tuple[str, ...]] = {
    "docker": ("--version",),
    "kind": ("--version",),
    "kubectl": ("version", "--client"),
    "helm": ("version", "--short"),
}


@dataclass(slots=True)
class Context:
    """Per-invocation state carried on the Click context object."""

    settings: Settings
    verbose: bool = False
    assume_yes: bool = False


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="custosctl")
@click.option(
    "--target",
    type=click.Choice([t.value for t in Target]),
    default=None,
    help="Deploy target: local (kind) or remote (existing kube-context). Overrides CUSTOS_TARGET.",
)
@click.option("--verbose", is_flag=True, help="Verbose output.")
@click.option(
    "--yes",
    "assume_yes",
    is_flag=True,
    help="Assume 'yes' for destructive-op confirmations.",
)
@click.pass_context
def cli(ctx: click.Context, target: str | None, verbose: bool, assume_yes: bool) -> None:
    """custosctl — deploy Custos locally or remotely and drive extensions."""
    settings = Settings()
    if target is not None:
        settings = settings.model_copy(update={"target": Target(target)})
    ctx.obj = Context(settings=settings, verbose=verbose, assume_yes=assume_yes)


def _render_tool_row(status: ToolStatus) -> str:
    mark = "ok  " if status.ok else "MISS"
    version = status.version or ("" if status.ok else "not found on PATH")
    return f"  [{mark}] {status.name:<8} {version}"


@cli.command()
@click.pass_obj
def doctor(obj: Context) -> None:
    """Preflight the environment for the selected target.

    For ``local`` this checks docker/kind/kubectl/helm are installed. For
    ``remote`` it checks kubectl/helm and that the selected kube-context is
    reachable. Exits non-zero if any required check fails.
    """
    settings = obj.settings
    target = settings.target
    click.echo(f"custosctl doctor — target: {target.value}")

    tools = _LOCAL_TOOLS if target is Target.LOCAL else _REMOTE_TOOLS
    statuses = [probe_tool(name, _TOOL_VERSION_ARGS.get(name, ("--version",))) for name in tools]
    for status in statuses:
        click.echo(_render_tool_row(status))

    ok = all(s.ok for s in statuses)

    if target is Target.REMOTE:
        context = settings.effective_kube_context()
        label = context or "(current kubectl context)"
        reachable = kube_context_reachable(context)
        mark = "ok  " if reachable else "MISS"
        click.echo(f"  [{mark}] context  {label}")
        ok = ok and reachable

    if not ok:
        raise click.ClickException("preflight failed; resolve the items marked MISS above")
    click.echo("preflight OK")


@cli.command()
@click.pass_obj
def up(obj: Context) -> None:
    """Bring the platform up (local kind cluster or remote kube-context)."""
    try:
        lifecycle.up(obj.settings, echo=click.echo)
    except (CommandError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command()
@click.option(
    "--force",
    is_flag=True,
    help="Remote only: also delete the namespace and its PVCs (destructive).",
)
@click.pass_obj
def down(obj: Context, force: bool) -> None:
    """Tear the platform down.

    Local deletes the kind cluster; remote only uninstalls the release (and,
    with --force, deletes the namespace/PVCs) — it never deletes the cluster.
    """
    settings = obj.settings
    if force and settings.target is Target.LOCAL:
        raise click.ClickException(
            "--force applies only to --target remote (local 'down' deletes the "
            "whole kind cluster anyway)"
        )
    if settings.target is Target.LOCAL:
        prompt = (
            f"Delete kind cluster '{settings.cluster}' and uninstall release '{settings.release}'?"
        )
    else:
        context = settings.effective_kube_context() or "(current kubectl context)"
        extra = f" and DELETE namespace '{settings.namespace}' (PVCs)" if force else ""
        prompt = f"Uninstall release '{settings.release}' from kube-context '{context}'{extra}?"
    if not obj.assume_yes:
        click.confirm(prompt, abort=True)
    try:
        lifecycle.down(settings, echo=click.echo, force=force)
    except (CommandError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command()
@click.pass_obj
def status(obj: Context) -> None:
    """Report cluster/context, release, and pod status."""
    try:
        healthy = lifecycle.status(obj.settings, echo=click.echo)
    except (CommandError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    if not healthy:
        raise SystemExit(1)


@cli.group()
def connector() -> None:
    """Register and list connector-types in the catalog."""


@connector.command("register")
@click.argument("path")
@click.option(
    "--image-ref",
    default=None,
    help="Digest-pinned image ref (…@sha256:…). If omitted, derived from "
    "CUSTOS_IMAGE_PREFIX and resolved via docker buildx / skopeo / crane.",
)
@click.pass_obj
def connector_register(obj: Context, path: str, image_ref: str | None) -> None:
    """Register a connector-type from an extension folder or manifest PATH."""
    try:
        ref = connectors.register(obj.settings, path=path, image_ref=image_ref)
    except (CommandError, RuntimeError, ApiError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"registered {ref['type']}@{ref['version']} ({ref['digest']})")


@connector.command("list")
@click.argument("connector_type")
@click.pass_obj
def connector_list(obj: Context, connector_type: str) -> None:
    """List registered versions of CONNECTOR_TYPE."""
    try:
        items = connectors.list_versions(obj.settings, connector_type=connector_type)
    except (CommandError, RuntimeError, ApiError) as exc:
        raise click.ClickException(str(exc)) from exc
    if not items:
        click.echo(f"no versions registered for connector-type '{connector_type}'")
        return
    for item in items:
        click.echo(f"  {item['version']:<12} {item['digest']}")


@cli.group()
def activity() -> None:
    """Register and list activity-types in the catalog."""


@activity.command("register")
@click.argument("path")
@click.option(
    "--image-ref",
    default=None,
    help="Digest-pinned image ref (…@sha256:…). If omitted, derived from "
    "CUSTOS_IMAGE_PREFIX and resolved via docker buildx / skopeo / crane.",
)
@click.option(
    "--workspace",
    default=None,
    help="Workspace to register under (defaults to the manifest's metadata.namespace).",
)
@click.pass_obj
def activity_register(
    obj: Context, path: str, image_ref: str | None, workspace: str | None
) -> None:
    """Register an activity-type from an extension folder or manifest PATH."""
    try:
        ref = activities.register(obj.settings, path=path, image_ref=image_ref, workspace=workspace)
    except (CommandError, RuntimeError, ApiError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"registered {ref['namespace']}/{ref['type']}@{ref['version']} ({ref['digest']})")


@activity.command("list")
@click.argument("namespace")
@click.argument("activity_type")
@click.option(
    "--workspace",
    default=None,
    help="Workspace to query (defaults to NAMESPACE).",
)
@click.pass_obj
def activity_list(obj: Context, namespace: str, activity_type: str, workspace: str | None) -> None:
    """List registered versions of NAMESPACE/ACTIVITY_TYPE."""
    try:
        items = activities.list_versions(
            obj.settings,
            namespace=namespace,
            activity_type=activity_type,
            workspace=workspace,
        )
    except (CommandError, RuntimeError, ApiError) as exc:
        raise click.ClickException(str(exc)) from exc
    if not items:
        click.echo(f"no versions registered for activity-type '{namespace}/{activity_type}'")
        return
    for item in items:
        click.echo(f"  {item['version']:<12} {item['digest']}")


def _parse_inputs(inputs_file: str | None, input_pairs: tuple[str, ...]) -> dict[str, object]:
    """Build the run inputs from an optional JSON/YAML file plus k=v overrides."""
    result: dict[str, object] = {}
    if inputs_file is not None:
        import yaml

        try:
            loaded = yaml.safe_load(Path(inputs_file).read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise click.ClickException(
                f"could not read --inputs-file {inputs_file}: {exc}"
            ) from exc
        if not isinstance(loaded, dict):
            raise click.ClickException("--inputs-file must contain a JSON/YAML object")
        for raw_key, value in loaded.items():
            key = str(raw_key).strip()
            if not key:
                raise click.ClickException("--inputs-file contains an empty key")
            result[key] = value
    for pair in input_pairs:
        key, sep, raw = pair.partition("=")
        key = key.strip()
        if not sep or not key:
            raise click.ClickException(
                f"--input must be KEY=VALUE with a non-empty key; got {pair!r}"
            )
        try:
            result[key] = json.loads(raw)
        except json.JSONDecodeError:
            result[key] = raw
    return result


@cli.group()
def workflow() -> None:
    """Publish workflow definitions and drive runs."""


@workflow.command("apply")
@click.argument("file")
@click.option("--workspace", default=None, help="Workspace (defaults to CUSTOS_WORKSPACE).")
@click.pass_obj
def workflow_apply(obj: Context, file: str, workspace: str | None) -> None:
    """Publish the workflow definition in FILE."""
    try:
        ref = workflows.apply(obj.settings, path=file, workspace=workspace)
    except (CommandError, RuntimeError, ApiError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"published {ref['workflowName']}@{ref['version']} (workspace {ref['workspaceId']})")


@workflow.command("run")
@click.argument("workflow_version_id")
@click.option("--workspace", default=None, help="Workspace (defaults to CUSTOS_WORKSPACE).")
@click.option("--input", "inputs", multiple=True, help="Run input as KEY=VALUE (repeatable).")
@click.option("--inputs-file", default=None, help="JSON/YAML file of run inputs.")
@click.option("--idempotency-key", default=None, help="Idempotency-Key for the start request.")
@click.pass_obj
def workflow_run(
    obj: Context,
    workflow_version_id: str,
    workspace: str | None,
    inputs: tuple[str, ...],
    inputs_file: str | None,
    idempotency_key: str | None,
) -> None:
    """Start a run of WORKFLOW_VERSION_ID."""
    parsed_inputs = _parse_inputs(inputs_file, inputs)
    try:
        ref = workflows.run(
            obj.settings,
            workflow_version_id=workflow_version_id,
            workspace=workspace,
            inputs=parsed_inputs,
            idempotency_key=idempotency_key,
        )
    except (CommandError, RuntimeError, ApiError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"started {ref['runId']} (status {ref['status']})")


@workflow.command("status")
@click.argument("run_id")
@click.option("--workspace", default=None, help="Workspace (defaults to CUSTOS_WORKSPACE).")
@click.option("--watch", is_flag=True, help="Poll until the run reaches a terminal status.")
@click.option(
    "--timeout",
    type=click.FloatRange(min=0),
    default=600.0,
    help="Watch timeout in seconds.",
)
@click.option(
    "--interval",
    type=click.FloatRange(min=0),
    default=3.0,
    help="Watch poll interval in seconds.",
)
@click.pass_obj
def workflow_status(
    obj: Context,
    run_id: str,
    workspace: str | None,
    watch: bool,
    timeout: float,
    interval: float,
) -> None:
    """Show the status of RUN_ID (optionally --watch to a terminal state)."""
    try:
        if watch:
            record = workflows.wait_for(
                obj.settings,
                run_id=run_id,
                workspace=workspace,
                timeout=timeout,
                interval=interval,
            )
        else:
            record = workflows.get_status(obj.settings, run_id=run_id, workspace=workspace)
    except (CommandError, RuntimeError, ApiError, TimeoutError) as exc:
        raise click.ClickException(str(exc)) from exc
    status = record.get("status", "unknown")
    click.echo(f"{run_id}: {status}")
    reason = record.get("reason")
    if reason:
        click.echo(f"  reason: {reason}")
    if watch and not workflows.is_success(record):
        raise SystemExit(1)


@cli.command("seed-ootb")
@click.option(
    "--allow-existing",
    is_flag=True,
    help="Treat an already-registered type (409 digest conflict) as non-fatal.",
)
@click.pass_obj
def seed_ootb(obj: Context, allow_existing: bool) -> None:
    """Onboard the OOTB connectors and activities (wraps scripts/seed-ootb.sh)."""
    try:
        seed.seed_ootb(obj.settings, allow_existing=allow_existing)
    except (CommandError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo("OOTB catalog onboarded")


def main() -> None:
    """Console-script entrypoint."""
    cli()


if __name__ == "__main__":
    main()
