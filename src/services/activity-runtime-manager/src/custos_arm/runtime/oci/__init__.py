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

from custos_arm.runtime.oci.job import (
    ACTIVITY_CONTAINER_NAME,
    CONNECTOR_ENDPOINT_ENV,
    JOB_NAME_PREFIX,
    MANAGED_BY,
    SIDECAR_CONTAINER_NAME,
    build_activity_job,
    job_name,
)

__all__ = [
    "ACTIVITY_CONTAINER_NAME",
    "CONNECTOR_ENDPOINT_ENV",
    "JOB_NAME_PREFIX",
    "MANAGED_BY",
    "SIDECAR_CONTAINER_NAME",
    "build_activity_job",
    "job_name",
]
