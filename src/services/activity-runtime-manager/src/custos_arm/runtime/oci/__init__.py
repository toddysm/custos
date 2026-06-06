"""OCI Container Driver (ARM-IMPL-015+).

The OCI driver realizes a :class:`~custos_arm.runtime.models.SandboxPlan` as a
Kubernetes ``Job``. This subpackage is split per the design § OCI driver split:

* :mod:`custos_arm.runtime.oci.job` (ARM-IMPL-015) — the pure *Job builder*:
  translate a ``SandboxPlan`` into a Kubernetes ``Job`` manifest (no cluster
  access, no I/O).
* the lifecycle monitor (ARM-IMPL-016) — ``start`` / ``await_terminal`` /
  ``cancel`` / ``collect`` / ``cleanup`` against a real cluster.
"""

from __future__ import annotations

from custos_arm.runtime.oci.errors import (
    ACTIVITY_CANCELLED,
    ACTIVITY_IMAGE_PULL_FAILED,
    ACTIVITY_OOM_KILLED,
    ACTIVITY_SANDBOX_FAILURE,
    ACTIVITY_TIMEOUT,
    SYSTEM_SANDBOX_FAILURE,
    ImagePullError,
    OciDriverError,
    SandboxFailureError,
)
from custos_arm.runtime.oci.job import (
    ACTIVITY_CONTAINER_NAME,
    CONNECTOR_ENDPOINT_ENV,
    INPUT_BRIDGE_CONTAINER_NAME,
    INPUT_READY_SENTINEL,
    JOB_NAME_PREFIX,
    MANAGED_BY,
    OUTPUT_BRIDGE_CONTAINER_NAME,
    SIDECAR_CONTAINER_NAME,
    DuplicateMountError,
    MissingBridgeMountError,
    UnpinnedImageError,
    build_activity_job,
    job_name,
)
from custos_arm.runtime.oci.lifecycle import (
    OciContainerDriver,
    classify_signal,
    is_image_pull_waiting_reason,
    signal_error_code,
)

__all__ = [
    "ACTIVITY_CANCELLED",
    "ACTIVITY_CONTAINER_NAME",
    "ACTIVITY_IMAGE_PULL_FAILED",
    "ACTIVITY_OOM_KILLED",
    "ACTIVITY_SANDBOX_FAILURE",
    "ACTIVITY_TIMEOUT",
    "CONNECTOR_ENDPOINT_ENV",
    "INPUT_BRIDGE_CONTAINER_NAME",
    "INPUT_READY_SENTINEL",
    "JOB_NAME_PREFIX",
    "MANAGED_BY",
    "OUTPUT_BRIDGE_CONTAINER_NAME",
    "SIDECAR_CONTAINER_NAME",
    "SYSTEM_SANDBOX_FAILURE",
    "DuplicateMountError",
    "ImagePullError",
    "MissingBridgeMountError",
    "OciContainerDriver",
    "OciDriverError",
    "SandboxFailureError",
    "UnpinnedImageError",
    "build_activity_job",
    "classify_signal",
    "is_image_pull_waiting_reason",
    "job_name",
    "signal_error_code",
]
