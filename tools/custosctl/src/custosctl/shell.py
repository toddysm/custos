"""Thin helpers around external CLIs (``docker``/``kind``/``kubectl``/``helm``).

The scaffold only needs presence + version probing for ``doctor``; richer
subprocess orchestration (cluster create, helm install) arrives with the
lifecycle tasks. Keeping these as small, individually patchable functions
lets the ``doctor`` tests stub tool availability without spawning real
processes.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass


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


__all__ = ["ToolStatus", "kube_context_reachable", "probe_tool", "which"]
