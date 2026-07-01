"""Local (kind) platform lifecycle: ``up`` / ``down`` / ``status``.

These functions orchestrate the same steps as the evaluation guide
[`docs/users/evaluation/local-cluster.md`] as a single, scripted flow:
create the kind cluster, install the out-of-band prerequisites, pre-provision
Postgres (the install-ordering caveat), then ``helm install --wait`` the
umbrella chart. Every external action is a patchable :mod:`custosctl.shell`
helper so this module can be unit-tested without a real cluster.

Remote-target lifecycle lands in DEVCLI-IMPL-003 (#954); the CLI routes there.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

import httpx

from custosctl import shell
from custosctl.config import Settings, resolve_repo_root

#: Progress sink — the CLI passes ``click.echo``; tests pass a list append.
Echo = Callable[[str], None]

_CNPG_REPO = "https://cloudnative-pg.github.io/charts"
_CNPG_VERSION = "0.22.1"


def _chart_dir(root: Path) -> Path:
    return root / "deploy" / "helm" / "custos"


def _values_file(root: Path, profile: str) -> Path:
    return _chart_dir(root) / f"values-{profile}.yaml"


def up(settings: Settings, *, echo: Echo) -> None:
    """Bring the platform up on a local kind cluster."""
    root = resolve_repo_root(settings.repo_root)
    context = settings.effective_kube_context()
    chart = _chart_dir(root)
    values = _values_file(root, settings.profile)
    if not values.is_file():
        raise RuntimeError(
            f"values file not found: {values} (unknown profile {settings.profile!r})"
        )

    # 1. kind cluster
    if shell.kind_cluster_exists(settings.cluster):
        echo(f"==> kind cluster '{settings.cluster}' already exists")
    else:
        echo(f"==> creating kind cluster '{settings.cluster}'")
        shell.kind_create(settings.cluster, settings.kind_node_image)

    # 2. out-of-band prerequisites (Dapr, Envoy Gateway, cert-manager, ...)
    if settings.prereqs == "skip":
        echo("==> skipping prerequisites (CUSTOS_PREREQS=skip)")
    else:
        echo("==> installing out-of-band prerequisites (install-prereqs.sh)")
        shell.run_script(root / "scripts" / "install-prereqs.sh", cwd=root)

    # 3. CloudNativePG operator (required; not covered by install-prereqs.sh)
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

    # 4. chart dependencies
    echo("==> resolving chart dependencies (make deps)")
    shell.make_target("deps", cwd=root)

    # 5. namespace + Postgres pre-provision (install-ordering caveat; guide step 5)
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

    # 6. install Custos
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

    # 7. gateway health
    _report_health(settings, echo=echo)
    echo("platform up")


def down(settings: Settings, *, echo: Echo) -> None:
    """Uninstall the release and delete the local kind cluster."""
    context = settings.effective_kube_context()
    echo(f"==> uninstalling release '{settings.release}'")
    shell.helm_uninstall(settings.release, namespace=settings.namespace, context=context)
    echo(f"==> deleting kind cluster '{settings.cluster}'")
    shell.kind_delete(settings.cluster)
    echo("platform down")


def status(settings: Settings, *, echo: Echo) -> bool:
    """Report cluster/release/pod state. Returns ``True`` when fully up."""
    context = settings.effective_kube_context()
    exists = shell.kind_cluster_exists(settings.cluster)
    echo(f"kind cluster '{settings.cluster}': {'present' if exists else 'absent'}")
    if not exists:
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

    return exists and installed and all_running


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
