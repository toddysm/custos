"""Verify the managed HTTP wait leaves time for owned Workflow and Dapr shutdown."""

from typing import Any

import pytest

from custos_workflow.__main__ import main


@pytest.mark.parametrize("configured", [None, "15"])
def test_optional_http_drain_preserves_legacy_default(
    monkeypatch: pytest.MonkeyPatch,
    configured: str | None,
) -> None:
    """Exercise the actual entry point without opening a service listener."""
    monkeypatch.delenv("CUSTOS_HTTP_DRAIN_SECONDS", raising=False)
    if configured is not None:
        monkeypatch.setenv("CUSTOS_HTTP_DRAIN_SECONDS", configured)
    calls: list[dict[str, Any]] = []

    def run(app: str, **kwargs: Any) -> None:
        """Capture the server options passed by the package console entry point."""
        assert app == "custos_workflow:create_app"
        calls.append(kwargs)

    monkeypatch.setattr("uvicorn.run", run)
    main()
    assert calls[0]["timeout_graceful_shutdown"] == (None if configured is None else 15)
