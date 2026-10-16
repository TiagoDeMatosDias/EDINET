"""Wire contract between the server and job workers (``/api/worker/*``).

Pinned by ``docs/Feature Development/Job Workers Plan.md``: the server routes
and the worker client both import these models, so they cannot drift apart.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 15
CLAIM_WAIT_SECONDS = 25
MAX_ATTEMPTS = 3
INTERACTIVE_WAIT_SECONDS = 60
SWEEP_INTERVAL_SECONDS = 5
MAX_RESULT_BYTES = 32 * 1024 * 1024
MAX_PROGRESS_BYTES = 16 * 1024
ENROLLMENT_PREFIX = "SRW1."
WORKER_TOKEN_PREFIX = "srw_"
LOCAL_WORKER_ID = "local"

Lane = Literal["interactive", "batch"]
JobStatus = Literal[
    "open",
    "in_progress",
    "cancelling",
    "completed",
    "failed",
    "cancelled",
    "interrupted",
]
CancelMode = Literal["none", "soft", "hard"]
StepStatus = Literal["running", "completed", "failed", "cancelled"]

ACTIVE_STATUSES: frozenset[str] = frozenset({"open", "in_progress", "cancelling"})
RUNNING_STATUSES: frozenset[str] = frozenset({"in_progress", "cancelling"})
TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed", "cancelled", "interrupted"})

# The pipeline UI (``/api/jobs``) keeps its original status vocabulary.
PUBLIC_PIPELINE_STATUS: dict[str, str] = {
    "open": "pending",
    "in_progress": "running",
    "cancelling": "cancelling",
    "completed": "completed",
    "failed": "failed",
    "cancelled": "cancelled",
    "interrupted": "interrupted",
}


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- enrollment / registration ---------------------------------------------


class EnrollRequest(_Model):
    code: str = Field(min_length=8, max_length=200)
    name: str = Field(default="", max_length=120)
    hostname: str = Field(default="", max_length=255)
    version: str = Field(max_length=64)


class EnrollResponse(_Model):
    worker_id: str
    token: str
    server_version: str


class RegisterRequest(_Model):
    session_id: str = Field(min_length=8, max_length=64)
    version: str = Field(max_length=64)
    hostname: str = Field(default="", max_length=255)
    pid: int = 0
    platform: str = Field(default="", max_length=120)
    kinds: list[str] = Field(default_factory=list, max_length=100)


class WorkerPaths(_Model):
    market_db: str
    filings_db: str
    data_dir: str
    jobs_dir: str


class RegisterResponse(_Model):
    worker_id: str
    name: str
    session_id: str
    server_version: str
    lease_seconds: int = LEASE_SECONDS
    heartbeat_seconds: int = HEARTBEAT_SECONDS
    claim_wait_seconds: int = CLAIM_WAIT_SECONDS
    interactive_slots: int
    batch_slots: int
    config_version: int
    paths: WorkerPaths
    settings: dict[str, Any] = Field(default_factory=dict)


class UnregisterRequest(_Model):
    session_id: str


# --- claiming ----------------------------------------------------------------


class ClaimRequest(_Model):
    session_id: str
    lanes: list[Lane] = Field(min_length=1, max_length=2)
    kinds: list[str] = Field(default_factory=list, max_length=100)


class JobAssignment(_Model):
    job_id: str
    kind: str
    lane: Lane
    title: str = ""
    payload: dict[str, Any]
    attempt: int
    lease_token: str
    lease_expires_at: str
    owner_user_id: str | None = None
    workspace: str


class ClaimResponse(_Model):
    job: JobAssignment


# --- heartbeats and progress ------------------------------------------------


class JobLease(_Model):
    job_id: str
    lease_token: str


class HeartbeatRequest(_Model):
    session_id: str
    data_access: bool = False
    data_access_detail: str | None = Field(default=None, max_length=500)
    interactive_busy: int = 0
    batch_busy: int = 0
    jobs: list[JobLease] = Field(default_factory=list, max_length=64)


class JobLeaseState(_Model):
    job_id: str
    lease_ok: bool
    cancel: CancelMode = "none"
    lease_expires_at: str | None = None


class HeartbeatResponse(_Model):
    jobs: list[JobLeaseState] = Field(default_factory=list)
    config_version: int
    shutdown: bool = False


class StepEvent(_Model):
    ordinal: int = Field(ge=0)
    status: StepStatus
    progress_percent: float | None = Field(default=None, ge=0, le=100)
    status_message: str | None = Field(default=None, max_length=500)
    result: Any = None
    error_message: str | None = Field(default=None, max_length=1000)
    duration_ms: int | None = Field(default=None, ge=0)


class ProgressRequest(_Model):
    lease_token: str
    progress_percent: float | None = Field(default=None, ge=0, le=100)
    status_message: str | None = Field(default=None, max_length=500)
    progress: dict[str, Any] | None = None
    step: StepEvent | None = None


class ProgressResponse(_Model):
    cancel: CancelMode = "none"
    lease_expires_at: str


class ArtifactResponse(_Model):
    name: str
    size_bytes: int
    sha256: str


# --- finishing ---------------------------------------------------------------


class CompleteRequest(_Model):
    lease_token: str
    result: dict[str, Any] = Field(default_factory=dict)


class FailRequest(_Model):
    lease_token: str
    error: str = Field(default="The job failed", max_length=1000)
    retryable: bool = False
    cancelled: bool = False


class ReleaseRequest(_Model):
    lease_token: str
    reason: str = Field(default="The worker was stopped", max_length=500)


class TerminalResponse(_Model):
    status: JobStatus


__all__ = [
    "ACTIVE_STATUSES",
    "CLAIM_WAIT_SECONDS",
    "ENROLLMENT_PREFIX",
    "HEARTBEAT_SECONDS",
    "INTERACTIVE_WAIT_SECONDS",
    "LEASE_SECONDS",
    "LOCAL_WORKER_ID",
    "MAX_ATTEMPTS",
    "MAX_PROGRESS_BYTES",
    "MAX_RESULT_BYTES",
    "PUBLIC_PIPELINE_STATUS",
    "RUNNING_STATUSES",
    "SWEEP_INTERVAL_SECONDS",
    "TERMINAL_STATUSES",
    "WORKER_TOKEN_PREFIX",
    "ArtifactResponse",
    "CancelMode",
    "ClaimRequest",
    "ClaimResponse",
    "CompleteRequest",
    "EnrollRequest",
    "EnrollResponse",
    "FailRequest",
    "HeartbeatRequest",
    "HeartbeatResponse",
    "JobAssignment",
    "JobLease",
    "JobLeaseState",
    "JobStatus",
    "Lane",
    "ProgressRequest",
    "ProgressResponse",
    "RegisterRequest",
    "RegisterResponse",
    "ReleaseRequest",
    "StepEvent",
    "StepStatus",
    "TerminalResponse",
    "UnregisterRequest",
    "WorkerPaths",
]
