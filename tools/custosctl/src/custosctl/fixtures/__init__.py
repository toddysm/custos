"""Packaged fixtures shipped with custosctl.

The sample workflow (:func:`sample_workflow_path`) exercises the OOTB catalog —
the ``copy-image`` activity bound to two ``oci-registry`` connector instances —
and is the definition the ``e2e`` command (#960) applies and runs.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

_SAMPLE_WORKFLOW = "sample-workflow.yaml"


def sample_workflow_path() -> Path:
    """Return the on-disk path to the packaged sample workflow document."""
    return Path(str(resources.files(__package__).joinpath(_SAMPLE_WORKFLOW)))


__all__ = ["sample_workflow_path"]
