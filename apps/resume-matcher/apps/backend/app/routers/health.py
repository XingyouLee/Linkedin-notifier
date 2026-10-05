"""Health check and status endpoints."""

from fastapi import APIRouter

from app.database import db
from app.llm import check_llm_health, get_llm_config, get_runtime_llm_config, is_llm_configured
from app.schemas import HealthResponse, StatusResponse
from app.workspace_auth import get_current_workspace_id

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Basic health check endpoint."""
    llm_status = await check_llm_health()

    return HealthResponse(
        status="healthy" if llm_status["healthy"] else "degraded",
        llm=llm_status,
    )


@router.get("/status", response_model=StatusResponse)
async def get_status() -> StatusResponse:
    """Get comprehensive application status.

    Returns:
        - LLM configuration status
        - Master resume existence
        - Database statistics
    """
    db_stats = db.get_stats()
    workspace_id = get_current_workspace_id()

    if workspace_id:
        runtime_config = get_runtime_llm_config()
        llm_status = await check_llm_health(runtime_config)
        generation_available = is_llm_configured(runtime_config) and llm_status["healthy"]
        return StatusResponse(
            status="ready" if generation_available and db_stats["has_master_resume"] else "setup_required",
            llm_configured=is_llm_configured(runtime_config),
            llm_healthy=llm_status["healthy"],
            generation_available=generation_available,
            has_master_resume=db_stats["has_master_resume"],
            database_stats=db_stats,
            workspace_mode=True,
            workspace_id=workspace_id,
            settings_available=False,
        )

    config = get_llm_config()
    llm_status = await check_llm_health(config)
    generation_available = is_llm_configured(config) and llm_status["healthy"]

    return StatusResponse(
        status="ready" if llm_status["healthy"] and db_stats["has_master_resume"] else "setup_required",
        llm_configured=is_llm_configured(config),
        llm_healthy=llm_status["healthy"],
        generation_available=generation_available,
        has_master_resume=db_stats["has_master_resume"],
        database_stats=db_stats,
        workspace_mode=False,
        workspace_id=None,
        settings_available=True,
    )
