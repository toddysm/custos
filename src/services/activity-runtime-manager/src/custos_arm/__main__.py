"""Run the activity application with optional TLS and a separate metrics-only listener."""

from __future__ import annotations

import os


def main() -> None:
    """Keep legacy HTTP defaults; managed mode adds TLS, metrics and bounded HTTP drain."""
    # Imported lazily so ``python -m custos_arm --help``-style probes do
    # not require uvicorn to be installed for non-runtime tooling.
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    from custos_arm.config import ENV_RUNTIME_ENABLED, _opt_bool
    from custos_arm.observe.runtime import metrics_listener

    certificate = os.environ.get("ARM_TLS_CERT_FILE") or None
    key = os.environ.get("ARM_TLS_KEY_FILE") or None
    if bool(certificate) != bool(key):
        raise ValueError("ARM_TLS_CERT_FILE and ARM_TLS_KEY_FILE must be configured together")
    metrics_port = int(os.environ.get("ARM_METRICS_PORT", "8090"))
    if not 1 <= metrics_port <= 65535:
        raise ValueError("ARM_METRICS_PORT must be in 1..65535")
    enabled = _opt_bool(ENV_RUNTIME_ENABLED, dict(os.environ), False)
    if enabled and int(os.environ.get("WEB_CONCURRENCY", "1")) != 1:
        raise ValueError("managed runtime requires one process per Pod; scale replicas instead")
    with metrics_listener(
        enabled=enabled,
        port=metrics_port,
        host=host,
    ):
        uvicorn.run(
            "custos_arm:create_app",
            host=host,
            port=port,
            factory=True,
            ssl_certfile=certificate,
            ssl_keyfile=key,
            workers=1 if enabled else None,
            timeout_graceful_shutdown=(
                int(os.environ["CUSTOS_HTTP_DRAIN_SECONDS"])
                if "CUSTOS_HTTP_DRAIN_SECONDS" in os.environ
                else None
            ),
        )


if __name__ == "__main__":  # pragma: no cover - exercised via ``python -m``
    main()
