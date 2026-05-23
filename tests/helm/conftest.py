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


def _render(profile: str) -> list[dict[str, Any]]:
    """Run ``helm template`` for one profile, returning the parsed docs."""
    # `helm dependency update` populates ./charts/; safe to re-run.
    subprocess.run(
        ["helm", "dependency", "update", str(UMBRELLA)],
        check=True,
        capture_output=True,
    )
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
