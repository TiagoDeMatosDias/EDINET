"""Validated HTTP contracts for the pipeline API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JobStatusValue = Literal[
    "pending",
    "running",
    "cancelling",
    "cancelled",
    "completed",
    "failed",
    "interrupted",
]


class PipelineConfig(BaseModel):
    """Pipeline configuration accepted by the queue endpoint."""

    model_config = ConfigDict(extra="forbid")

    steps: list[dict[str, Any]] | None = Field(
        default=None,
        description="Ordered step configurations to execute",
    )
    config: dict[str, Any] = Field(
        description="Configuration parameters for the pipeline run",
    )


class JobStepStatus(BaseModel):
    """Persisted state for one ordered pipeline step."""

    ordinal: int
    step_name: str
    overwrite: bool = False
    status: JobStatusValue
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    progress_percent: int | float = 0.0
    status_message: str | None = None


class JobStatus(BaseModel):
    """Current persisted status of a pipeline job."""

    job_id: str
    status: JobStatusValue
    current_step: str | None = None
    progress_percent: int | float = 0.0
    step_count: int = 0
    completed_step_count: int = 0
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    status_message: str | None = None
    steps: list[JobStepStatus] = Field(default_factory=list)


class AvailableStepsResponse(BaseModel):
    """Available pipeline steps and their metadata."""

    steps: list[dict[str, Any]]


class StepInfo(BaseModel):
    """Metadata for one pipeline step."""

    name: str
    canonical_name: str
    description: str | None = None
    parameters: dict[str, Any] | None = None
    category: str | None = None
    aliases: list[str] = Field(default_factory=list)


class JobCreateResponse(BaseModel):
    """Identity and initial state of a newly queued job."""

    job_id: str
    status: JobStatusValue
    created_at: datetime


class CancelJobRequest(BaseModel):
    """Cancellation options for a pipeline job."""

    model_config = ConfigDict(extra="forbid")

    force: bool = Field(
        default=False,
        description="Request force cancellation (unsupported during a step)",
    )


class ServerConfigResponse(BaseModel):
    """Non-sensitive capabilities needed by API clients."""

    workspace_id: str = "default"
    version: str
    max_upload_bytes: int
    max_export_bytes: int
    max_backtest_artifact_bytes: int


class JobOutputResponse(BaseModel):
    """Bounded terminal output for a pipeline job."""

    job_id: str
    status: JobStatusValue
    output: dict[str, Any]


class JobHealthResponse(BaseModel):
    """Aggregate job state that does not expose job content."""

    queue_depth: int
    active: int
    counts_by_status: dict[str, int]


class HealthResponse(BaseModel):
    """Bounded process and queue health information."""

    status: Literal["healthy"]
    version: str
    timestamp: datetime
    jobs: JobHealthResponse
