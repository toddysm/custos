"""Platform lifecycle: ``up`` / ``down`` / ``status`` for local and remote.

For ``target=local`` these orchestrate the evaluation guide
[`docs/users/evaluation/local-cluster.md`] as a single scripted flow (create
the kind cluster, install prerequisites, pre-provision Postgres, ``helm
install --wait``). For ``target=remote`` the same platform install runs against
an existing kube-context — custosctl never creates or deletes the cluster, and
``down`` only removes the namespace/PVCs when explicitly ``--force``d. Every
external action is a patchable :mod:`custosctl.shell` helper so this module is
unit-tested without a real cluster.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

import httpx

from custosctl import shell
from custosctl.config import Settings, Target, resolve_repo_root

#: Progress sink — the CLI passes ``click.echo``; tests pass a list append.
Echo = Callable[[str], None]

_CNPG_REPO = "https://cloudnative-pg.github.io/charts"
_CNPG_VERSION = "0.22.1"


def _chart_dir(root: Path) -> Path:
    return root / "deploy" / "helm" / "custos"


def _values_file(root: Path, profile: str) -> Path:
    return _chart_dir(root) / f"values-{profile}.yaml"


def up(settings: Settings, *, echo: Echo) -> None:
    """Bring the platform up on the configured target (local kind or remote)."""
    if settings.target is Target.REMOTE:
        _up_remote(settings, echo=echo)
    else:
        _up_local(settings, echo=echo)


def _up_local(settings: Settings, *, echo: Echo) -> None:
    root = resolve_repo_root(settings.repo_root)
    context = settings.effective_kube_context()
    _require_values(root, settings.profile)

    # 1. kind cluster
    if shell.kind_cluster_exists(settings.cluster):
        echo(f"==> kind cluster '{settings.cluster}' already exists")
    else:
        echo(f"==> creating kind cluster '{settings.cluster}'")
        shell.kind_create(settings.cluster, settings.kind_node_image)

    # 2. prerequisites (default install for local)
    if settings.effective_prereqs() == "skip":
        echo("==> skipping prerequisites (CUSTOS_PREREQS=skip)")
    else:
        _install_prereqs(root, context=context, echo=echo)

    # 3. platform
    _install_platform(settings, root=root, context=context, echo=echo)
    _report_health(settings, echo=echo)
    echo("platform up")


def _up_remote(settings: Settings, *, echo: Echo) -> None:
    root = resolve_repo_root(settings.repo_root)
    context = settings.effective_kube_context()
    label = context or "(current kubectl context)"
    _require_values(root, settings.profile)

    # Never create a cluster on remote — verify the context is reachable first.
    if not shell.kube_context_reachable(context):
        raise RuntimeError(
            f"kube-context {label} is not reachable; select it with "
            "CUSTOS_KUBE_CONTEXT or 'kubectl config use-context'"
        )
    echo(f"==> using kube-context: {label}")

    # Prerequisites default to skip for remote (the cluster likely provides them).
    if settings.effective_prereqs() == "install":
        _guard_prereqs_context(context)
        _install_prereqs(root, context=context, echo=echo)
    else:
        echo(
            "==> skipping prerequisites (CUSTOS_PREREQS=skip); assuming the cluster "
            "already provides Dapr/Gateway/CNPG/observability"
        )

    _install_platform(settings, root=root, context=context, echo=echo)
    _report_health(settings, echo=echo)
    echo("platform up")


def _require_values(root: Path, profile: str) -> Path:
    values = _values_file(root, profile)
    if not values.is_file():
        raise RuntimeError(f"values file not found: {values} (unknown profile {profile!r})")
    return values


def _guard_prereqs_context(context: str | None) -> None:
    """Refuse remote ``prereqs=install`` when the pinned context isn't current.

    ``scripts/install-prereqs.sh`` operates on kubectl's *current* context and
    cannot be pointed at an explicit ``--context``. If the user pinned a
    different context (``CUSTOS_KUBE_CONTEXT``), installing prerequisites would
    silently target the wrong cluster, so fail with actionable guidance.
    """
    if context is None:
        return
    current = shell.kubectl_current_context()
    if current is not None and current != context:
        raise RuntimeError(
            f"CUSTOS_PREREQS=install runs install-prereqs.sh against kubectl's "
            f"current context ({current!r}), not {context!r}. Switch first with "
            f"'kubectl config use-context {context}', or unset CUSTOS_KUBE_CONTEXT "
            "to use the current context."
        )


def _install_prereqs(root: Path, *, context: str | None, echo: Echo) -> None:
    echo("==> installing out-of-band prerequisites (install-prereqs.sh)")
    shell.run_script(root / "scripts" / "install-prereqs.sh", cwd=root)
    echo("==> installing CloudNativePG operator")
    shell.helm_repo_add("cnpg", _CNPG_REPO)
    shell.helm_repo_update()
    shell.helm_install(
        "cnpg",
        "cnpg/cloudnative-pg",
        namespace="cnpg-system",
        version=_CNPG_VERSION,
        create_namespace=True,
        wait=True,
        timeout="5m",
        context=context,
    )


def _install_platform(settings: Settings, *, root: Path, context: str | None, echo: Echo) -> None:
    chart = _chart_dir(root)
    values = _values_file(root, settings.profile)

    echo("==> resolving chart dependencies (make deps)")
    shell.make_target("deps", cwd=root)

    echo(f"==> ensuring namespace '{settings.namespace}'")
    shell.kubectl_ensure_namespace(settings.namespace, context=context)

    echo("==> pre-provisioning Postgres (CNPG Cluster)")
    manifest = shell.helm_template(
        settings.release,
        chart,
        namespace=settings.namespace,
        values=values,
        sets=["cnpg.storageClass=standard"],
        show_only="charts/cnpg/templates/cluster.yaml",
    )
    shell.kubectl_apply_stdin(manifest, namespace=settings.namespace, context=context)
    shell.kubectl_wait(
        f"cluster/{settings.release}",
        namespace=settings.namespace,
        condition="Ready",
        timeout="5m",
        context=context,
    )

    echo("==> installing Custos (helm upgrade --install --wait)")
    shell.helm_install(
        settings.release,
        chart,
        namespace=settings.namespace,
        values=values,
        sets=["postgres.embedded=false"],
        wait=True,
        timeout=settings.helm_timeout,
        context=context,
    )


def down(settings: Settings, *, echo: Echo, force: bool = False) -> None:
    """Uninstall the release.

    ``local`` also deletes the kind cluster. ``remote`` never touches the
    cluster and only deletes the namespace (and its PVCs) when ``force``.
    """
    context = settings.effective_kube_context()
    echo(f"==> uninstalling release '{settings.release}'")
    shell.helm_uninstall(settings.release, namespace=settings.namespace, context=context)

    if settings.target is Target.LOCAL:
        echo(f"==> deleting kind cluster '{settings.cluster}'")
        shell.kind_delete(settings.cluster)
    elif force:
        echo(f"==> deleting namespace '{settings.namespace}' and its PVCs (--force)")
        shell.kubectl_delete_namespace(settings.namespace, context=context)
    else:
        echo(
            f"note: left namespace '{settings.namespace}' and its PVCs in place "
            "(pass --force to delete them)"
        )
    echo("platform down")


def status(settings: Settings, *, echo: Echo) -> bool:
    """Report cluster/context, release, and pod state. ``True`` when fully up."""
    context = settings.effective_kube_context()

    if settings.target is Target.LOCAL:
        up_ok = shell.kind_cluster_exists(settings.cluster)
        echo(f"kind cluster '{settings.cluster}': {'present' if up_ok else 'absent'}")
    else:
        label = context or "(current kubectl context)"
        up_ok = shell.kube_context_reachable(context)
        echo(f"kube-context '{label}': {'reachable' if up_ok else 'unreachable'}")
    if not up_ok:
        return False

    installed = shell.helm_release_exists(settings.release, settings.namespace, context=context)
    echo(f"helm release '{settings.release}': {'installed' if installed else 'not installed'}")

    pods = shell.kubectl_pod_phases(settings.namespace, context=context)
    if not pods:
        echo(f"no pods in namespace '{settings.namespace}'")
    else:
        running = sum(1 for _, phase in pods if phase == "Running")
        echo(f"pods in '{settings.namespace}': {running}/{len(pods)} Running")
        for name, phase in pods:
            echo(f"  {phase:<10} {name}")
    all_running = bool(pods) and all(phase == "Running" for _, phase in pods)

    if settings.gateway:
        healthy = _poll_health(settings.gateway, insecure=settings.insecure, attempts=1, delay=0.0)
        echo(f"gateway {settings.gateway}: {'healthy' if healthy else 'unreachable'}")

    return up_ok and installed and all_running


def _report_health(settings: Settings, *, echo: Echo) -> None:
    if not settings.gateway:
        echo(
            "note: set CUSTOS_GATEWAY (e.g. via a kubectl port-forward) to poll gateway "
            "health; helm --wait already blocked until every workload was Ready"
        )
        return
    if _poll_health(settings.gateway, insecure=settings.insecure):
        echo("gateway healthy (/healthz, /readyz)")
    else:
        raise RuntimeError(f"gateway {settings.gateway} did not become healthy")


def _poll_health(
    gateway: str,
    *,
    insecure: bool,
    attempts: int = 30,
    delay: float = 2.0,
) -> bool:
    base = gateway.rstrip("/")
    verify = not insecure
    for attempt in range(attempts):
        try:
            with httpx.Client(verify=verify, timeout=5.0) as client:
                healthz = client.get(f"{base}/healthz")
                readyz = client.get(f"{base}/readyz")
            if healthz.status_code == 200 and readyz.status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        if attempt < attempts - 1 and delay > 0:
            time.sleep(delay)
    return False


__all__ = ["down", "status", "up"]
