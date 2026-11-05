"""HTTP shutdown cannot consume the owned receiver's entire termination grace."""

from collections.abc import Iterator
from contextlib import contextmanager
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


@pytest.mark.parametrize(("active", "workers"), [(True, "1"), (True, "2"), (False, "2")])
def test_source_mode_owns_exporters_in_the_serving_process(
    monkeypatch: pytest.MonkeyPatch,
    active: bool,
    workers: str,
) -> None:
    """Reject worker spawning only when process-owned source exporters are active."""
    monkeypatch.setenv("TRIGGER_SOURCE_RECEIVERS_ENABLED", str(active).lower())
    monkeypatch.setenv("WEB_CONCURRENCY", workers)
    calls: list[dict[str, Any]] = []
    entered: list[bool] = []

    @contextmanager
    def telemetry(*, enabled: bool) -> Iterator[None]:
        """Capture ownership without starting a collector or exporting test data."""
        entered.append(enabled)
        yield

    def run(app: str, **kwargs: Any) -> None:
        """Capture actual entry-point arguments without starting Uvicorn."""
        assert app == "custos_trigger:create_app"
        calls.append(kwargs)

    monkeypatch.setattr("custos_trigger._runtime.receiver_telemetry", telemetry)
    monkeypatch.setattr("uvicorn.run", run)
    if active and workers != "1":
        with pytest.raises(ValueError, match="one process"):
            main()
        assert not entered and not calls
    else:
        main()
        assert entered == [active]
        assert calls[0].get("workers") == (1 if active else None)
