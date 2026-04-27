"""
FastAPI router for EDINET orchestrator API.

This module wraps the existing orchestrator functionality with HTTP endpoints.
The underlying pipeline logic is unchanged and remains fully backward compatible.
"""

import logging
from typing import Any, Optional
from uuid import UUID, uuid4
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, Body
from pydantic import BaseModel, Field

# Import existing orchestrator functionality (no modifications needed)
# Note: These imports will work when the server is run from the project root
from src.orchestrator import run, list_available_steps, validate_input
from config import Config

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models for Request/Response Validation
# =============================================================================

class PipelineConfig(BaseModel):
    """Pipeline configuration from API request."""
    steps: list[dict[str, Any]] | None = Field(
        default=None,
        description="List of step configurations to execute",
    )
    config: dict[str, Any] = Field(
        description="Configuration parameters for the pipeline run",
    )


class StepResult(BaseModel):
    """Result from executing a single step."""
    step_name: str
    success: bool
    result: Optional[Any] = None
    error_message: Optional[str] = None
    duration_ms: int | None = None


class PipelineRunResponse(BaseModel):
    """Response after completing a pipeline run."""
    job_id: str
    status: str  # completed, failed
    steps_executed: list[StepResult]
    total_duration_ms: int | None = None
    error_message: Optional[str] = None


class JobStatus(BaseModel):
    """Current status of a running pipeline job."""
    job_id: str
    status: str  # pending, running, completed, failed, cancelled
    current_step: Optional[str] = None
    progress_percent: int | float = 0.0
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class AvailableStepsResponse(BaseModel):
    """List of available pipeline steps with metadata."""
    steps: list[dict[str, Any]]


class StepMetadata(BaseModel):
    """Metadata for a single step."""
    name: str
    canonical_name: str
    description: Optional[str] = None
    parameters: dict[str, Any] | None = None
    category: Optional[str] = None


class StepInfo(BaseModel):
    """Extended information about a single step."""
    name: str
    canonical_name: str
    description: Optional[str] = None
    parameters: dict[str, Any] | None = None
    category: Optional[str] = None
    aliases: list[str] | None = None


class JobCreateResponse(BaseModel):
    """Response when creating a new pipeline job."""
    job_id: str
    status: str
    created_at: datetime


class CancelJobRequest(BaseModel):
    """Request to cancel a running job."""
    force: bool = Field(
        default=False,
        description="Force cancellation even if step is in progress",
    )


# =============================================================================
# Job State Tracking (In-Memory for simplicity)
# =============================================================================

class PipelineJob:
    """Tracks the state of a running pipeline job."""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.status: str = "pending"
        self.config: dict[str, Any] | None = None
        self.steps_executed: list[StepResult] = []
        self.current_step: Optional[str] = None
        self.progress_percent: float = 0.0
        self.created_at: datetime = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error_message: Optional[str] = None
        self.cancelled: bool = False
    
    def to_dict(self) -> dict:
        """Convert job state to dictionary for JSON serialization."""
        return {
            "job_id": self.job_id,
            "status": self.status,
            "current_step": self.current_step,
            "progress_percent": self.progress_percent,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
        }


# In-memory job registry (replace with Redis/SQLite for production)
_job_registry: dict[str, PipelineJob] = {}


def get_or_create_job(job_id: str) -> PipelineJob:
    """Get existing job or create new one."""
    if job_id not in _job_registry:
        _job_registry[job_id] = PipelineJob(job_id)
    return _job_registry[job_id]


def cleanup_completed_jobs(max_age_hours: int = 24) -> None:
    """Remove completed jobs older than max_age_hours."""
    cutoff = datetime.now() - __import__('datetime').timedelta(hours=max_age_hours)
    for job_id, job in list(_job_registry.items()):
        if (job.completed_at and job.completed_at < cutoff) or \
           (job.status == "completed" and not job.started_at):
            del _job_registry[job_id]


# =============================================================================
# FastAPI Application Setup
# =============================================================================

app = FastAPI(
    title="EDINET Orchestrator API",
    description="HTTP API for running EDINET research pipelines",
    version="1.0.0"
)


@app.on_event("startup")
def on_startup():
    """Clean up old completed jobs on startup."""
    cleanup_completed_jobs()


# =============================================================================
# API Endpoints
# =============================================================================

@app.get(
    "/api/steps",
    response_model=AvailableStepsResponse,
    summary="List available pipeline steps",
    description="Returns metadata for all discovered pipeline steps"
)
def list_steps() -> AvailableStepsResponse:
    """Return list of available pipeline steps."""
    try:
        steps = list_available_steps()
        return AvailableStepsResponse(steps=steps)
    except Exception as e:
        logger.error("Failed to list steps: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/steps/{step_name}",
    response_model=StepInfo,
    summary="Get step metadata",
    description="Returns detailed information about a specific pipeline step"
)
def get_step(step_name: str) -> StepInfo:
    """Return metadata for a specific step."""
    try:
        steps = list_available_steps()
        # Find the step by name (case-insensitive)
        step_info = None
        for s in steps:
            if s.get("name", "").lower() == step_name.lower():
                step_info = StepInfo(
                    name=s["name"],
                    canonical_name=s.get("canonical_name", s["name"]),
                    description=s.get("description"),
                    parameters=s.get("parameters"),
                    category=s.get("category"),
                    aliases=s.get("aliases") or [],
                )
                break
        
        if step_info is None:
            raise HTTPException(
                status_code=404,
                detail=f"Step '{step_name}' not found"
            )
        return step_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get step metadata for %s: %s", step_name, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/api/pipeline/run",
    response_model=PipelineRunResponse,
    summary="Execute a pipeline",
    description="Runs the specified pipeline configuration and returns results"
)
def run_pipeline(config: PipelineConfig = Body(...)) -> PipelineRunResponse:
    """Execute a pipeline with the given configuration."""
    job_id = str(uuid4())
    job = get_or_create_job(job_id)
    
    # Store config for potential debugging
    if config.config:
        job.config = config.config
    
    try:
        job.status = "running"
        job.started_at = datetime.now()
        
        # Normalize and validate input using existing orchestrator
        normalized_steps = validate_input(
            config=config.config,
            steps=config.steps
        )
        
        logger.info("Starting pipeline run for job %s with steps: %s",
                   job_id, [s["name"] for s in normalized_steps])
        
        results: list[StepResult] = []
        total_start_time = datetime.now()
        
        for step_config in normalized_steps:
            step_name = step_config["name"]
            overwrite = step_config.get("overwrite", False)
            
            job.current_step = step_name
            job.progress_percent = (
                len(results) / max(len(normalized_steps), 1) * 100
            )
            
            try:
                # Execute the step using existing orchestrator
                result = run(
                    config=config.config,
                    steps=[step_config],
                    on_step_start=lambda s: None,  # No-op placeholder
                    on_step_done=lambda s: None,   # No-op placeholder
                    cancel_event=None,             # No cancellation support yet
                )
                
                step_result = StepResult(
                    step_name=step_name,
                    success=True,
                    result=result,
                    duration_ms=int((datetime.now() - total_start_time).total_seconds() * 1000)
                )
                results.append(step_result)
                logger.info("Step completed: %s", step_name)
                
            except Exception as e:
                logger.error("Step failed: %s - %s", step_name, str(e))
                step_result = StepResult(
                    step_name=step_name,
                    success=False,
                    error_message=str(e),
                    duration_ms=int((datetime.now() - total_start_time).total_seconds() * 1000)
                )
                results.append(step_result)
                job.error_message = f"Step '{step_name}' failed: {str(e)}"
        
        # Mark as completed
        job.status = "completed"
        job.completed_at = datetime.now()
        total_duration_ms = int((datetime.now() - job.started_at).total_seconds() * 1000) if job.started_at else None
        
        logger.info("Pipeline run completed for job %s",
                   job_id)
        
        return PipelineRunResponse(
            job_id=job_id,
            status="completed",
            steps_executed=results,
            total_duration_ms=total_duration_ms,
            error_message=None
        )
        
    except Exception as e:
        logger.error("Pipeline run failed for job %s: %s", job_id, str(e))
        job.status = "failed"
        job.error_message = str(e)
        job.completed_at = datetime.now()
        
        return PipelineRunResponse(
            job_id=job_id,
            status="failed",
            steps_executed=[],
            error_message=str(e)
        )


@app.get(
    "/api/jobs/{job_id}",
    response_model=JobStatus,
    summary="Get job status",
    description="Returns the current status of a pipeline execution"
)
def get_job_status(job_id: str) -> JobStatus:
    """Return the current status of a pipeline job."""
    if job_id not in _job_registry:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    
    job = _job_registry[job_id]
    return JobStatus(
        job_id=job.job_id,
        status=job.status,
        current_step=job.current_step,
        progress_percent=job.progress_percent,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error_message=job.error_message
    )


@app.post(
    "/api/jobs/{job_id}/cancel",
    response_model=JobStatus,
    summary="Cancel a running job",
    description="Cancels an in-progress pipeline execution"
)
def cancel_job(job_id: str, request: CancelJobRequest) -> JobStatus:
    """Cancel a running pipeline job."""
    if job_id not in _job_registry:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    
    job = _job_registry[job_id]
    
    # Only allow cancellation of running jobs
    if job.status not in ("pending", "running"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status '{job.status}'"
        )
    
    try:
        # For now, we just mark as cancelled
        # In a real implementation, you'd need to interrupt running threads/processes
        job.status = "cancelled"
        job.completed_at = datetime.now()
        job.error_message = f"Cancelled by user (force={request.force})"
        
        logger.info("Job %s cancelled",
                   job_id)
        
    except Exception as e:
        logger.error("Failed to cancel job %s: %s", job_id, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    
    return get_job_status(job_id)


@app.get(
    "/api/jobs",
    response_model=list[JobStatus],
    summary="List recent jobs",
    description="Returns the most recently created pipeline jobs"
)
def list_jobs(limit: int = Query(default=10, ge=1, le=100)) -> list[JobStatus]:
    """Return a list of recent job statuses."""
    # Sort by creation time and take top N
    sorted_jobs = sorted(
        _job_registry.values(),
        key=lambda j: j.created_at,
        reverse=True
    )[:limit]
    
    return [JobStatus(**j.to_dict()) for j in sorted_jobs]


@app.get(
    "/api/jobs/{job_id}/output",
    summary="Get job output",
    description="Returns the output/results from a completed pipeline run"
)
def get_job_output(job_id: str) -> dict:
    """Return the output from a completed pipeline job."""
    if job_id not in _job_registry:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    
    job = _job_registry[job_id]
    
    # Only allow output retrieval for completed jobs
    if job.status != "completed":
        raise HTTPException(
            status_code=400,
            detail="Can only retrieve output from completed jobs"
        )
    
    return {
        "job_id": job.job_id,
        "status": job.status,
        "output": {step.step_name: step.result for step in job.steps_executed if step.success}
    }


# =============================================================================
# Health Check Endpoint
# =============================================================================

@app.get("/health")
def health_check() -> dict:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "jobs_active": len([j for j in _job_registry.values() if j.status == "running"])
    }


# =============================================================================
# Main Entry Point (for standalone server)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Start the FastAPI server
    uvicorn.run(
        "src.orchestrator.api.router:app",
        host="0.0.0.0",
        port=8000,
        reload=True  # Enable auto-reload during development
    )
