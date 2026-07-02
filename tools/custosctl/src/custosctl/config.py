"""Configuration model for custosctl.

Values are read from the environment and an optional ``.env`` file using
the ``CUSTOS_`` prefix (e.g. ``CUSTOS_TARGET``, ``CUSTOS_GATEWAY``). CLI
flags, when present, override these — the CLI layer is responsible for
applying overrides on top of a loaded :class:`Settings`.

Only the fields needed by the DEVCLI-IMPL-001 scaffold (target selection
and the ``doctor`` preflight) are consumed today; the remaining fields are
declared here so later lifecycle/API commands read a single, stable config
surface. See ``design/components/custosctl/design.md`` § Configuration.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Markers that identify the Custos repository root — the lifecycle commands
#: shell out to these paths (the umbrella chart, the prereq/onboarding scripts,
#: the Makefile), so ``custosctl`` must locate the checkout it runs against.
_REPO_MARKERS: tuple[str, ...] = (
    "deploy/helm/custos/Chart.yaml",
    "scripts/install-prereqs.sh",
    "Makefile",
)


class Target(StrEnum):
    """Where custosctl operates.

    ``LOCAL`` manages a ``kind`` cluster custosctl creates/deletes;
    ``REMOTE`` operates against an existing kube-context and never
    creates or deletes the cluster.
    """

    LOCAL = "local"
    REMOTE = "remote"


class Settings(BaseSettings):
    """custosctl configuration, sourced from ``CUSTOS_*`` env / ``.env``."""

    model_config = SettingsConfigDict(
        env_prefix="CUSTOS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- target + cluster ---
    target: Target = Target.LOCAL
    kube_context: str | None = None
    cluster: str = "custos-local"
    kind_node_image: str = "kindest/node:v1.31.2"

    # --- release ---
    namespace: str = "custos-system"
    release: str = "custos"
    profile: str = "connected-eval"

    # --- images (map to the umbrella chart's global.imageRegistry / global.imageTag) ---
    image_prefix: str = "ghcr.io/toddysm/custos"
    image_tag: str | None = None

    # --- API surface (required by the API-driven commands, not by doctor/up) ---
    gateway: str | None = None
    token: SecretStr | None = None
    insecure: bool = False
    workspace: str | None = None

    # --- lifecycle knobs ---
    prereqs: str | None = None
    repo_root: Path | None = None
    helm_timeout: str = "15m"

    def effective_kube_context(self) -> str | None:
        """Resolve the kube-context to operate against.

        For ``LOCAL`` the context defaults to ``kind-<cluster>`` (the name
        ``kind create cluster --name <cluster>`` produces) unless an
        explicit ``kube_context`` is set. For ``REMOTE`` the explicit
        ``kube_context`` (``CUSTOS_KUBE_CONTEXT``) is used when set;
        otherwise ``None`` is returned, meaning custosctl operates against
        kubectl's *current* context (as selected by
        ``kubectl config use-context``).
        """
        if self.kube_context:
            return self.kube_context
        if self.target is Target.LOCAL:
            return f"kind-{self.cluster}"
        return None

    def effective_prereqs(self) -> str:
        """Whether ``up`` installs the out-of-band prerequisites.

        Returns the explicit ``prereqs`` value when set, otherwise the
        per-target default: ``install`` for ``LOCAL`` (a fresh kind cluster
        has nothing), ``skip`` for ``REMOTE`` (an existing cluster is assumed
        to already provide the operators).
        """
        if self.prereqs:
            return self.prereqs
        return "install" if self.target is Target.LOCAL else "skip"


def _is_repo_root(path: Path) -> bool:
    return all((path / marker).is_file() for marker in _REPO_MARKERS)


def resolve_repo_root(configured: Path | None, start: Path | None = None) -> Path:
    """Locate the Custos checkout the lifecycle commands operate on.

    Uses ``configured`` (``CUSTOS_REPO_ROOT``) when set, otherwise walks up
    from ``start`` (default: the current working directory) looking for the
    repository markers. Raises :class:`RuntimeError` with actionable guidance
    when no checkout can be found.
    """
    if configured is not None:
        root = configured.expanduser().resolve()
        if not _is_repo_root(root):
            raise RuntimeError(
                f"CUSTOS_REPO_ROOT={root} does not look like a Custos checkout "
                f"(missing one of: {', '.join(_REPO_MARKERS)})"
            )
        return root
    here = (start or Path.cwd()).resolve()
    for candidate in (here, *here.parents):
        if _is_repo_root(candidate):
            return candidate
    raise RuntimeError(
        "could not locate the Custos repository root from "
        f"{here}; run custosctl from inside the checkout or set CUSTOS_REPO_ROOT"
    )


__all__ = ["Settings", "Target", "resolve_repo_root"]
