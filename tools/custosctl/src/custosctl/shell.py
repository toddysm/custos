"""Thin helpers around external CLIs (``docker``/``kind``/``kubectl``/``helm``).

``doctor`` needs only presence + version probing; the lifecycle commands
(``up``/``down``/``status``) add a small ``run`` subprocess primitive plus
named ``kind``/``helm``/``kubectl`` actions. Keeping each action as its own
patchable function lets the lifecycle tests assert the orchestration without
spawning real processes.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ToolStatus:
    """Result of probing a single external CLI."""

    name: str
    found: bool
    version: str | None = None

    @property
    def ok(self) -> bool:
        return self.found


def which(tool: str) -> str | None:
    """Return the resolved path to ``tool`` on PATH, or ``None``."""
    return shutil.which(tool)


def probe_tool(name: str, version_args: tuple[str, ...] = ("--version",)) -> ToolStatus:
    """Probe a CLI: check it is on PATH and capture a one-line version.

    A missing tool yields ``found=False``. A tool that is present but whose
    version invocation fails still reports ``found=True`` with ``version=None``
    — presence is what gates the preflight; the version is advisory.
    """
    path = which(name)
    if path is None:
        return ToolStatus(name=name, found=False)
    version: str | None = None
    try:
        completed = subprocess.run(
            [path, *version_args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if completed.returncode == 0:
            output = completed.stdout.strip() or completed.stderr.strip()
            version = output.splitlines()[0].strip() if output else None
    except (OSError, subprocess.SubprocessError):
        version = None
    return ToolStatus(name=name, found=True, version=version)


def kube_context_reachable(context: str | None, *, timeout: int = 15) -> bool:
    """Return whether ``kubectl cluster-info`` succeeds for ``context``.

    ``context=None`` probes the current context. Any non-zero exit, missing
    ``kubectl``, or timeout is reported as unreachable rather than raising.
    """
    kubectl = which("kubectl")
    if kubectl is None:
        return False
    args = [kubectl, "cluster-info"]
    if context:
        args += ["--context", context]
    try:
        completed = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


class CommandError(RuntimeError):
    """A subprocess invoked by the lifecycle commands exited non-zero."""

    def __init__(self, argv: Sequence[str], returncode: int, stderr: str = "") -> None:
        self.argv = list(argv)
        self.returncode = returncode
        self.stderr = stderr
        printable = " ".join(self.argv)
        detail = f": {stderr.strip()}" if stderr.strip() else ""
        super().__init__(f"command failed ({returncode}): {printable}{detail}")


def run(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    input_text: str | None = None,
    env: Mapping[str, str] | None = None,
    timeout: int | None = None,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run ``argv`` (never through a shell) and optionally capture its output.

    ``capture=False`` inherits the parent's stdio so long-running commands
    (``helm install``, ``kind create``) stream live progress. ``capture=True``
    returns stdout/stderr for parsing. ``env``, when given, is the full child
    environment (callers merge onto ``os.environ`` themselves). Raises
    :class:`CommandError` when ``check`` and the process exits non-zero.
    """
    completed = subprocess.run(
        list(argv),
        cwd=str(cwd) if cwd is not None else None,
        input=input_text,
        env=dict(env) if env is not None else None,
        capture_output=capture,
        text=True,
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode != 0:
        raise CommandError(
            argv,
            completed.returncode,
            completed.stderr or "" if capture else "",
        )
    return completed


# --- kind -----------------------------------------------------------------


def kind_cluster_exists(name: str) -> bool:
    """Return whether a ``kind`` cluster named ``name`` already exists."""
    completed = run(["kind", "get", "clusters"], capture=True, check=False)
    if completed.returncode != 0:
        return False
    return name in completed.stdout.split()


def kind_create(name: str, node_image: str) -> None:
    run(["kind", "create", "cluster", "--name", name, "--image", node_image])


def kind_delete(name: str) -> None:
    run(["kind", "delete", "cluster", "--name", name])


# --- helm / make / scripts ------------------------------------------------


def run_script(path: Path, *, cwd: Path, args: Sequence[str] = ()) -> None:
    run([str(path), *args], cwd=cwd)


def make_target(target: str, *, cwd: Path) -> None:
    run(["make", "-C", str(cwd), target])


def resolve_image_digest(image_ref: str) -> str:
    """Resolve ``image_ref`` to its ``sha256:<hex>`` manifest digest.

    Tries ``docker buildx imagetools``, then ``skopeo``, then ``crane`` —
    whichever is installed and can reach the registry. Raises
    :class:`CommandError` when none can resolve a digest.
    """
    probes: list[tuple[str, list[str]]] = [
        (
            "docker",
            [
                "docker",
                "buildx",
                "imagetools",
                "inspect",
                image_ref,
                "--format",
                "{{.Manifest.Digest}}",
            ],
        ),
        ("skopeo", ["skopeo", "inspect", "--format", "{{.Digest}}", f"docker://{image_ref}"]),
        ("crane", ["crane", "digest", image_ref]),
    ]
    for tool, argv in probes:
        if which(tool) is None:
            continue
        completed = run(argv, capture=True, check=False)
        digest = completed.stdout.strip()
        if completed.returncode == 0 and digest.startswith("sha256:"):
            return digest
    raise CommandError(
        ["<digest-resolver>"],
        1,
        f"could not resolve a digest for {image_ref}; install docker buildx, "
        "skopeo, or crane with pull access, or pass --image-ref",
    )


def helm_release_exists(release: str, namespace: str, *, context: str | None = None) -> bool:
    argv = ["helm", "status", release, "-n", namespace]
    if context:
        argv += ["--kube-context", context]
    return run(argv, capture=True, check=False).returncode == 0


def helm_repo_add(name: str, url: str) -> None:
    run(["helm", "repo", "add", name, url, "--force-update"])


def helm_repo_update() -> None:
    run(["helm", "repo", "update"])


def helm_install(
    release: str,
    chart: str | Path,
    *,
    namespace: str,
    values: Path | None = None,
    sets: Sequence[str] = (),
    version: str | None = None,
    create_namespace: bool = False,
    wait: bool = True,
    timeout: str = "15m",
    context: str | None = None,
) -> None:
    argv = ["helm", "upgrade", "--install", release, str(chart), "-n", namespace]
    if version is not None:
        argv += ["--version", version]
    if create_namespace:
        argv += ["--create-namespace"]
    if values is not None:
        argv += ["-f", str(values)]
    for item in sets:
        argv += ["--set", item]
    if wait:
        argv += ["--wait", "--timeout", timeout]
    if context:
        argv += ["--kube-context", context]
    run(argv)


def helm_template(
    release: str,
    chart: str | Path,
    *,
    namespace: str,
    values: Path | None = None,
    sets: Sequence[str] = (),
    show_only: str | None = None,
) -> str:
    """Render chart manifests to a string (``helm template``)."""
    argv = ["helm", "template", release, str(chart), "-n", namespace]
    if values is not None:
        argv += ["-f", str(values)]
    for item in sets:
        argv += ["--set", item]
    if show_only is not None:
        argv += ["--show-only", show_only]
    return run(argv, capture=True).stdout


def helm_uninstall(
    release: str, *, namespace: str, context: str | None = None, ignore_not_found: bool = True
) -> None:
    argv = ["helm", "uninstall", release, "-n", namespace]
    if ignore_not_found:
        argv += ["--ignore-not-found"]
    if context:
        argv += ["--kube-context", context]
    run(argv)


# --- kubectl --------------------------------------------------------------


def _kubectl(argv: Sequence[str], *, context: str | None) -> list[str]:
    out = ["kubectl"]
    if context:
        out += ["--context", context]
    return [*out, *argv]


def kubectl_current_context() -> str | None:
    """Return kubectl's current context, or ``None`` if unset/unavailable."""
    completed = run(["kubectl", "config", "current-context"], capture=True, check=False)
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def kubectl_ensure_namespace(namespace: str, *, context: str | None = None) -> None:
    """Create ``namespace`` if absent (idempotent via dry-run + apply)."""
    manifest = run(
        _kubectl(
            ["create", "namespace", namespace, "--dry-run=client", "-o", "yaml"],
            context=context,
        ),
        capture=True,
    ).stdout
    run(_kubectl(["apply", "-f", "-"], context=context), input_text=manifest)


def kubectl_apply_stdin(manifest: str, *, namespace: str, context: str | None = None) -> None:
    run(
        _kubectl(["apply", "-n", namespace, "-f", "-"], context=context),
        input_text=manifest,
    )


def kubectl_delete_namespace(namespace: str, *, context: str | None = None) -> None:
    """Delete ``namespace`` (and everything in it, incl. PVCs). Idempotent."""
    run(_kubectl(["delete", "namespace", namespace, "--ignore-not-found"], context=context))


def kubectl_delete_secret(name: str, *, namespace: str, context: str | None = None) -> None:
    run(
        _kubectl(
            ["delete", "secret", name, "-n", namespace, "--ignore-not-found"],
            context=context,
        )
    )


def kubectl_wait(
    resource: str,
    *,
    namespace: str,
    condition: str,
    timeout: str = "5m",
    context: str | None = None,
) -> None:
    run(
        _kubectl(
            [
                "wait",
                f"--for=condition={condition}",
                resource,
                "-n",
                namespace,
                f"--timeout={timeout}",
            ],
            context=context,
        )
    )


def kubectl_pod_phases(namespace: str, *, context: str | None = None) -> list[tuple[str, str]]:
    """Return ``(pod_name, phase)`` for every pod in ``namespace``.

    An empty list is returned when the namespace is absent or unreadable —
    ``status`` treats that as "nothing deployed" rather than an error.
    """
    completed = run(
        _kubectl(["get", "pods", "-n", namespace, "-o", "json"], context=context),
        capture=True,
        check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return []
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return []
    pods: list[tuple[str, str]] = []
    for item in payload.get("items", []):
        name = item.get("metadata", {}).get("name", "?")
        phase = item.get("status", {}).get("phase", "Unknown")
        pods.append((name, phase))
    return pods


__all__ = [
    "CommandError",
    "ToolStatus",
    "helm_install",
    "helm_release_exists",
    "helm_repo_add",
    "helm_repo_update",
    "helm_template",
    "helm_uninstall",
    "kind_cluster_exists",
    "kind_create",
    "kind_delete",
    "kube_context_reachable",
    "kubectl_apply_stdin",
    "kubectl_current_context",
    "kubectl_delete_namespace",
    "kubectl_ensure_namespace",
    "kubectl_pod_phases",
    "kubectl_wait",
    "make_target",
    "probe_tool",
    "resolve_image_digest",
    "run",
    "run_script",
    "which",
]
