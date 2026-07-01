"""custosctl command-line entrypoint (Click).

DEVCLI-IMPL-001 scaffold: the root group with the global ``--target`` /
``--verbose`` / ``--yes`` options, a ``CUSTOS_*``-backed :class:`Settings`
carried on the Click context, and the ``doctor`` preflight. Lifecycle and
API command groups are registered by later DEVCLI tasks.
"""

from __future__ import annotations

from dataclasses import dataclass

import click

from custosctl import __version__
from custosctl.config import Settings, Target
from custosctl.shell import ToolStatus, kube_context_reachable, probe_tool

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


def main() -> None:
    """Console-script entrypoint."""
    cli()


if __name__ == "__main__":
    main()
