"""The Connector HTTP drain leaves time for its owned polling and sandbox cleanup."""

from typing import Any

import pytest

from custos_connector.__main__ import main


@pytest.mark.parametrize("configured", [None, "15"])
def test_optional_http_drain_preserves_legacy_default(
    monkeypatch: pytest.MonkeyPatch,
    configured: str | None,
) -> None:
    """Pass the managed timeout without changing an unconfigured process's shutdown behavior."""
    monkeypatch.delenv("CUSTOS_HTTP_DRAIN_SECONDS", raising=False)
    if configured is not None:
        monkeypatch.setenv("CUSTOS_HTTP_DRAIN_SECONDS", configured)
    calls: list[dict[str, Any]] = []

    def run(app: str, **kwargs: Any) -> None:
        """Capture the actual entry point's server options without starting a listener."""
        assert app == "custos_connector:create_app"
        calls.append(kwargs)

    monkeypatch.setattr("uvicorn.run", run)
    main()
    assert calls[0]["timeout_graceful_shutdown"] == (None if configured is None else 15)
