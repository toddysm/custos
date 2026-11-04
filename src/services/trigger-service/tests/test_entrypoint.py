"""HTTP shutdown cannot consume the owned receiver's entire termination grace."""

from typing import Any

import pytest

from custos_trigger.__main__ import main


@pytest.mark.parametrize("configured", [None, "15"])
def test_http_drain_precedes_lifespan_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    configured: str | None,
) -> None:
    """Keep legacy defaults off-mode and pass the bounded managed wait to the real entry point."""
    monkeypatch.delenv("CUSTOS_HTTP_DRAIN_SECONDS", raising=False)
    monkeypatch.setenv("TRIGGER_SOURCE_RECEIVERS_ENABLED", "false")
    if configured is not None:
        monkeypatch.setenv("CUSTOS_HTTP_DRAIN_SECONDS", configured)
    calls: list[dict[str, Any]] = []

    def run(app: str, **kwargs: Any) -> None:
        """Capture only server configuration; tests must not start a long-lived listener."""
        assert app == "custos_trigger:create_app"
        calls.append(kwargs)

    monkeypatch.setattr("uvicorn.run", run)
    main()
    assert calls[0]["timeout_graceful_shutdown"] == (None if configured is None else 15)
