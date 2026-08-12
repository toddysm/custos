"""Operator ceremony for creating the first platform-admin credential."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from enum import StrEnum

import yaml

from custosctl import shell
from custosctl.api import ApiClient
from custosctl.config import Settings, resolve_repo_root

Echo = Callable[[str], None]

TOKEN_PREFIX = "custos_"
DEFAULT_SECRET_KEY = "token"
DEFAULT_PRINCIPAL_ID = "custos-bootstrap-admin"


class BootstrapMode(StrEnum):
    INIT = "init"
    RECOVER = "recover"


def mint_token() -> str:
    """Generate a canonical 256-bit Custos bearer token locally."""
    return f"{TOKEN_PREFIX}{secrets.token_urlsafe(32)}"


def _secret_manifest(name: str, namespace: str, key: str, token: str) -> str:
    return yaml.safe_dump(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "namespace": namespace},
            "type": "Opaque",
            "stringData": {key: token},
        },
        sort_keys=False,
    )


def _helm_sets(mode: BootstrapMode, secret_name: str, secret_key: str) -> list[str]:
    return [
        "postgres.embedded=false",
        f"bootstrap.adminToken.mode={mode.value}",
        f"bootstrap.adminToken.secretName={secret_name}",
        f"bootstrap.adminToken.secretKey={secret_key}",
    ]


def run_ceremony(
    settings: Settings,
    *,
    mode: BootstrapMode,
    show_token: bool,
    keep_secret: bool,
    echo: Echo,
) -> str:
    """Create, apply, verify, and optionally clean up a bootstrap credential."""
    if not show_token and not keep_secret:
        raise RuntimeError("pass --show-token or --keep-secret so the credential is not lost")
    if not settings.gateway or not settings.gateway.strip():
        raise RuntimeError("CUSTOS_GATEWAY is required to verify the bootstrap credential")

    root = resolve_repo_root(settings.repo_root)
    chart = root / "deploy" / "helm" / "custos"
    values = chart / f"values-{settings.profile}.yaml"
    if not values.is_file():
        raise RuntimeError(f"unknown deployment profile: {settings.profile}")

    context = settings.effective_kube_context()
    secret_name = f"custos-bootstrap-admin-{secrets.token_hex(4)}"
    token = mint_token()
    manifest = _secret_manifest(
        secret_name,
        settings.namespace,
        DEFAULT_SECRET_KEY,
        token,
    )

    echo(f"==> creating temporary Secret '{secret_name}'")
    shell.kubectl_apply_stdin(manifest, namespace=settings.namespace, context=context)
    try:
        echo(f"==> running bootstrap-admin {mode.value}")
        shell.helm_install(
            settings.release,
            chart,
            namespace=settings.namespace,
            values=values,
            sets=_helm_sets(mode, secret_name, DEFAULT_SECRET_KEY),
            timeout=settings.helm_timeout,
            context=context,
        )
        echo("==> verifying credential through the API Gateway")
        with ApiClient(
            base_url=settings.gateway,
            token=token,
            verify=not settings.insecure,
        ) as client:
            client.get(f"/v1/service-accounts/{DEFAULT_PRINCIPAL_ID}/tokens")
        echo("==> restoring bootstrap mode to disabled")
        shell.helm_install(
            settings.release,
            chart,
            namespace=settings.namespace,
            values=values,
            sets=[
                "postgres.embedded=false",
                "bootstrap.adminToken.mode=disabled",
                "bootstrap.adminToken.secretName=",
            ],
            timeout=settings.helm_timeout,
            context=context,
        )
    except Exception as exc:
        raise RuntimeError(
            f"bootstrap-admin {mode.value} did not complete; temporary Secret "
            f"'{secret_name}' was retained for recovery"
        ) from exc

    if not keep_secret:
        shell.kubectl_delete_secret(secret_name, namespace=settings.namespace, context=context)
        echo("==> temporary Secret deleted")
    else:
        echo(f"==> temporary Secret retained as '{secret_name}'")
    return token


__all__ = ["BootstrapMode", "run_ceremony"]
