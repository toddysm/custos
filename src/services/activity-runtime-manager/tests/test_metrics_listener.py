"""Verify real metrics scraping without weakening the private HTTPS application listener."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from custos_arm.__main__ import main


def test_real_metrics_listener_drops_unbounded_activity_error_labels() -> None:
    """A fresh process exports existing counters over HTTP without exposing private error text."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("OTEL_")}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import urllib.request\n"
            "from custos_arm.observe.runtime import metrics_listener\n"
            "with metrics_listener(enabled=True, port=0) as port:\n"
            "    from custos_arm.observe.telemetry import ATTEMPTS_TOTAL\n"
            "    ATTEMPTS_TOTAL.add(1, {'class': 'retryable', 'code': 'private-unit-code'})\n"
            "    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))\n"
            "    body=opener.open(f'http://127.0.0.1:{port}/metrics', timeout=3).read().decode()\n"
            "    assert 'custos_arm_attempts_total' in body\n"
            "    assert 'retryable' in body and 'private-unit-code' not in body\n"
            "print('metrics-verified')\n",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "metrics-verified\n"


def test_managed_entrypoint_owns_tls_and_http_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the actual launch configuration without starting a production listener."""
    from collections.abc import Iterator
    from contextlib import contextmanager
    from typing import Any

    calls: list[dict[str, Any]] = []
    events: list[str] = []

    @contextmanager
    def listener(**kwargs: object) -> Iterator[int]:
        """Record ownership around the server and require the dedicated metrics port."""
        assert kwargs["enabled"] and kwargs["port"] == 8090
        events.append("metrics-open")
        try:
            yield 8090
        finally:
            events.append("metrics-close")

    def run(app: str, **kwargs: Any) -> None:
        """Capture exactly the server options used by the chart's real entry point."""
        assert app == "custos_arm:create_app"
        events.append("server")
        calls.append(kwargs)

    monkeypatch.setenv("ARM_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ARM_TLS_CERT_FILE", "/custos/tls/tls.crt")
    monkeypatch.setenv("ARM_TLS_KEY_FILE", "/custos/tls/tls.key")
    monkeypatch.setenv("CUSTOS_HTTP_DRAIN_SECONDS", "15")
    monkeypatch.setattr("custos_arm.observe.runtime.metrics_listener", listener)
    monkeypatch.setattr("uvicorn.run", run)
    main()
    assert calls[0]["ssl_certfile"] == "/custos/tls/tls.crt"
    assert calls[0]["ssl_keyfile"] == "/custos/tls/tls.key"
    assert calls[0]["timeout_graceful_shutdown"] == 15
    assert calls[0]["workers"] == 1
    assert events == ["metrics-open", "server", "metrics-close"]
