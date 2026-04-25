"""
Standalone FastAPI server for the EDINET orchestrator API.

This script can be run independently to expose the pipeline as HTTP endpoints.
The existing CLI and GUI functionality remains unchanged.

Usage:
    python -m src.api.server

Or with custom host/port:
    python -m src.api.server --host 0.0.0.0 --port 8001
"""

import argparse
import logging
from pathlib import Path

# Add parent directory to path for imports when run as script
__file_path = Path(__file__).resolve().parent.parent
if str(__file_path) not in str(Path.cwd()):
    # Only add if not already there (avoid duplicates)
    pass  # Let Python handle import resolution normally

from src.api.router import app, cleanup_completed_jobs
import uvicorn


def setup_logging(level: int = logging.INFO):
    """Configure logging for the API server."""
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    logging.basicConfig(
        format=log_format,
        level=level,
        handlers=[
            logging.StreamHandler(),  # Console output
            logging.FileHandler("api_server.log")  # File log
        ]
    )
    return logging.getLogger(__name__)


def main():
    """Main entry point for the API server."""
    parser = argparse.ArgumentParser(
        description="EDINET Orchestrator HTTP API Server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0",
        help="Host to bind the server to"
    )
    parser.add_argument(
        "--port", type=int, default=8000,
        help="Port to listen on"
    )
    parser.add_argument(
        "--reload", action="store_true",
        help="Enable auto-reload for development"
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(
        level=getattr(logging, args.log_level.upper(), logging.INFO)
    )
    
    logger.info("=" * 60)
    logger.info("EDINET Orchestrator API Server")
    logger.info("=" * 60)
    logger.info(f"Host: {args.host}")
    logger.info(f"Port: {args.port}")
    logger.info(f"Reload mode: {args.reload}")
    logger.info("")
    logger.info("Available endpoints:")
    logger.info("  GET  /health              - Health check")
    logger.info("  GET  /api/steps           - List available pipeline steps")
    logger.info("  GET  /api/steps/{name}    - Get step metadata")
    logger.info("  POST /api/pipeline/run    - Execute a pipeline")
    logger.info("  GET  /api/jobs            - List recent jobs")
    logger.info("  GET  /api/jobs/{id}       - Get job status")
    logger.info("  POST /api/jobs/{id}/cancel - Cancel running job")
    logger.info("  GET  /api/jobs/{id}/output - Get completed job output")
    logger.info("")
    
    # Start the server
    uvicorn.run(
        "src.api.router:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="warning"  # Let our custom handler control logging
    )


if __name__ == "__main__":
    main()
