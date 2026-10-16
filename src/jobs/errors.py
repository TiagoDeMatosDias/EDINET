"""Exceptions raised by the job service.

``JobPending``, ``JobFailed`` and ``JobCancelled`` reach API clients through the
handlers installed in ``src.web_app.api``. The rest are worker-protocol errors
that ``worker_api`` maps onto error codes of the app's envelope.
"""

from __future__ import annotations


class JobError(Exception):
    """Base class of every job-service error."""


class JobPending(JobError):
    """The job has not finished yet; the client is told to look at the job tray."""

    def __init__(self, job_id: str, waiting_for_worker: bool = False) -> None:
        super().__init__(job_id)
        self.job_id = job_id
        self.waiting_for_worker = waiting_for_worker


class JobFailed(JobError):
    """The job ended in ``failed`` or ``interrupted``; ``message`` is public."""

    def __init__(self, message: str, job_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.job_id = job_id


class JobCancelled(JobError):
    def __init__(self, job_id: str | None = None) -> None:
        super().__init__("The job was cancelled")
        self.job_id = job_id


class InvalidJobState(JobError):
    """The requested transition is not allowed from the job's current state."""


class WorkerProtocolError(JobError):
    """Base of errors reported to workers with a fixed code and status."""

    code = "worker_error"
    status_code = 400

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.default_message)

    default_message = "The request could not be processed"


class LeaseLost(WorkerProtocolError):
    code = "lease_lost"
    status_code = 409
    default_message = "The job is no longer leased to this worker"


class UnknownSession(WorkerProtocolError):
    code = "unknown_session"
    status_code = 409
    default_message = "The worker session is not registered; register again"


class WorkerDisabled(WorkerProtocolError):
    code = "worker_disabled"
    status_code = 403
    default_message = "This worker is disabled"


class VersionMismatch(WorkerProtocolError):
    code = "version_mismatch"
    status_code = 409
    default_message = "The worker and the server must run the same version"


class NoDataAccess(WorkerProtocolError):
    code = "no_data_access"
    status_code = 409
    default_message = "The worker cannot open the server's databases"


class ArtifactTooLarge(WorkerProtocolError):
    code = "artifact_too_large"
    status_code = 413
    default_message = "The artifact exceeds the size limit"


class InvalidArtifactName(WorkerProtocolError):
    code = "invalid_artifact_name"
    status_code = 422
    default_message = "The artifact name is not valid"


class FinalizeError(JobError):
    """Raised by a finalizer; ``message`` becomes the job's public error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


__all__ = [
    "ArtifactTooLarge",
    "FinalizeError",
    "InvalidArtifactName",
    "InvalidJobState",
    "JobCancelled",
    "JobError",
    "JobFailed",
    "JobPending",
    "LeaseLost",
    "NoDataAccess",
    "UnknownSession",
    "VersionMismatch",
    "WorkerDisabled",
    "WorkerProtocolError",
]
