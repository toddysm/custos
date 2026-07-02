"""OOTB onboarding wrapper (DEVCLI-IMPL-008).

``custosctl seed-ootb`` runs the repository's ``scripts/seed-ootb.sh`` against
the configured gateway, passing the gateway/token/image-prefix/insecure values
through the environment the script expects. The script itself resolves the OOTB
image digests and registers the connector- and activity-types.
"""

from __future__ import annotations

import os

from custosctl import shell
from custosctl.config import Settings, resolve_repo_root


def seed_ootb(settings: Settings, *, allow_existing: bool = False) -> None:
    """Run ``scripts/seed-ootb.sh`` with the gateway environment from settings."""
    if not settings.gateway or not settings.gateway.strip():
        raise RuntimeError("CUSTOS_GATEWAY is required for seed-ootb (the gateway base URL)")
    if settings.token is None or not settings.token.get_secret_value().strip():
        raise RuntimeError("CUSTOS_TOKEN is required for seed-ootb (a platform admin token)")

    root = resolve_repo_root(settings.repo_root)
    script = root / "scripts" / "seed-ootb.sh"
    if not script.is_file():
        raise RuntimeError(f"seed-ootb script not found: {script}")

    env = dict(os.environ)
    env["GATEWAY"] = settings.gateway.strip()
    env["TOKEN"] = settings.token.get_secret_value().strip()
    env["IMAGE_PREFIX"] = settings.image_prefix
    if settings.insecure:
        env["INSECURE"] = "1"
    else:
        env.pop("INSECURE", None)

    argv = [str(script)]
    if allow_existing:
        argv.append("--allow-existing")
    try:
        shell.run(argv, cwd=root, env=env)
    except OSError as exc:
        raise RuntimeError(
            f"could not run {script}: {exc} — ensure it is executable "
            "(chmod +x) and bash is on PATH"
        ) from exc


__all__ = ["seed_ootb"]
