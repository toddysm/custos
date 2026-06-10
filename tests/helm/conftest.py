"""Test fixtures for the Helm render tests.

The tests in this directory shell out to ``helm template`` against the
umbrella chart in ``deploy/helm/custos``. The fixtures here render each
profile once per session and cache the parsed manifest stream so the
individual tests stay cheap.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
UMBRELLA = REPO_ROOT / "deploy" / "helm" / "custos"
PROFILES = (
    "connected-eval",
    "connected-ha",
    "airgapped-eval",
    "airgapped-ha",
)


@pytest.fixture(scope="session", autouse=True)
def chart_dependencies() -> None:
    """Vendor the umbrella chart's subcharts once per test session.

    `helm dependency build` populates ./charts/ from Chart.lock. All
    dependencies are local `file://` subcharts (the upstream operators/CRD
    bundles are installed out-of-band — see #851/#852), so `build` resolves
    entirely offline. Running it once per session (rather than per render)
    avoids redundant vendoring across the many render tests in this directory.
    """
    subprocess.run(
        ["helm", "dependency", "build", str(UMBRELLA)],
        check=True,
        capture_output=True,
    )


def _render(profile: str) -> list[dict[str, Any]]:
    """Run ``helm template`` for one profile, returning the parsed docs.

    Subchart dependencies are vendored once per session by the autouse
    ``chart_dependencies`` fixture, so this only invokes ``helm template``.
    """
    result = subprocess.run(
        [
            "helm",
            "template",
            "custos",
            str(UMBRELLA),
            "-f",
            str(UMBRELLA / f"values-{profile}.yaml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc is not None]
    return docs


@pytest.fixture(scope="session")
def rendered() -> dict[str, list[dict[str, Any]]]:
    """Render every umbrella profile once per session."""
    out: dict[str, list[dict[str, Any]]] = {}
    for profile in PROFILES:
        out[profile] = _render(profile)
    return out


@pytest.fixture(scope="session")
def all_profiles() -> Iterator[str]:
    """Convenience iterator over the four umbrella profiles."""
    return iter(PROFILES)
