"""Keep deployed Dapr pins and transport authentication at the approved secure baseline."""

from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_secure_dapr_pin_is_consistent_across_installation_paths() -> None:
    """Connected, offline and hosted installs select the same secure patch and require mTLS."""
    script = (ROOT / "scripts/install-prereqs.sh").read_text()
    match = re.search(r'^DAPR_VERSION="(\d+\.\d+\.\d+)"$', script, re.MULTILINE)
    assert match is not None
    version = match.group(1)
    assert tuple(map(int, version.split("."))) >= (1, 18, 6)
    values = yaml.safe_load((ROOT / "deploy/helm/custos/values.yaml").read_text())
    assert values["dapr"]["global"]["tag"] == version
    assert values["dapr"]["global"]["mtls"]["enabled"] is True
    smoke = (ROOT / ".github/workflows/deploy-smoke.yml").read_text()
    assert f"--version {version} \\" in smoke
    for installer in (script, smoke):
        assert "--set global.mtls.enabled=true" in installer
    images = [
        line
        for line in (ROOT / "deploy/offline/images.txt").read_text().splitlines()
        if line.startswith("ghcr.io/dapr/")
    ]
    assert len(images) == 6
    assert all(image.endswith(":" + version) for image in images)
