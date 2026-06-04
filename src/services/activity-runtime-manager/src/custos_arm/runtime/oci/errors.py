"""OCI Container Driver lifecycle errors and error codes (ARM-IMPL-016).

The driver never *classifies* a finished attempt — that is the Result Mapper's
job, which turns a :class:`~custos_arm.runtime.models.SandboxOutcome` into an
:class:`~custos_arm.result.ResultClass`. But some failure modes have no exit
code to classify (the activity container never runs): an image that cannot be
pulled, or a Kubernetes-level failure standing the sandbox up. For those the
driver raises a typed :class:`OciDriverError` carrying the design-mandated
``activity.*`` / ``system.*`` code and :class:`~custos_arm.contract.ErrorClass`
so the Scheduler can synthesize the failure envelope (design § Failure Modes).
"""

from __future__ import annotations

from typing import Final

from custos_arm.contract import ErrorClass

__all__ = [
    "ACTIVITY_CANCELLED",
    "ACTIVITY_IMAGE_PULL_FAILED",
    "ACTIVITY_OOM_KILLED",
    "ACTIVITY_SANDBOX_FAILURE",
    "ACTIVITY_TIMEOUT",
    "SYSTEM_SANDBOX_FAILURE",
    "ImagePullError",
    "OciDriverError",
    "SandboxFailureError",
]

#: Activity image could not be pulled (registry 5xx, missing tag, bad creds).
ACTIVITY_IMAGE_PULL_FAILED: Final[str] = "activity.image_pull_failed"
#: Activity container was OOM-killed by the kernel.
ACTIVITY_OOM_KILLED: Final[str] = "activity.oom_killed"
#: Activity exceeded its deadline and was terminated.
ACTIVITY_TIMEOUT: Final[str] = "activity.timeout"
#: Activity was cancelled by the Workflow Service.
ACTIVITY_CANCELLED: Final[str] = "activity.cancelled"
#: Activity terminated abnormally (SIGKILL / uncategorized crash).
ACTIVITY_SANDBOX_FAILURE: Final[str] = "activity.sandbox_failure"
#: A Kubernetes-level failure standing up or reaping the sandbox.
SYSTEM_SANDBOX_FAILURE: Final[str] = "system.sandbox_failure"


class OciDriverError(RuntimeError):
    """Base class for OCI-driver lifecycle failures with no activity exit code.

    Carries the design-mandated ``code`` and ``error_class`` so the Scheduler
    can build the failure envelope without re-deriving the classification.
    """

    def __init__(self, code: str, error_class: ErrorClass, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.error_class = error_class


class ImagePullError(OciDriverError):
    """The activity image could not be pulled, so the container never ran.

    Retryable: a registry hiccup or a not-yet-replicated tag may resolve on the
    next attempt (design § Failure Modes → ``activity.image_pull_failed``).
    """

    def __init__(self, message: str) -> None:
        super().__init__(ACTIVITY_IMAGE_PULL_FAILED, ErrorClass.RETRYABLE, message)


class SandboxFailureError(OciDriverError):
    """A Kubernetes-level failure standing up or reaping the sandbox.

    Retryable: the orchestrator may relaunch the attempt (design § Failure
    Modes → ``system.sandbox_failure``).
    """

    def __init__(self, message: str) -> None:
        super().__init__(SYSTEM_SANDBOX_FAILURE, ErrorClass.RETRYABLE, message)
