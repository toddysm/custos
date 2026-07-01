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

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # --- lifecycle knobs ---
    prereqs: str | None = None

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


__all__ = ["Settings", "Target"]
