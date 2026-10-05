"""Integration tests for health and status endpoints."""

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.database import db
from app.main import app
from app.workspace_auth import workspace_scope


@pytest.fixture
def client():
    """Async HTTP client for testing FastAPI endpoints."""
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


class TestHealthEndpoint:
    """GET /api/v1/health"""

    @patch("app.routers.health.check_llm_health", new_callable=AsyncMock)
    async def test_health_returns_healthy(self, mock_health, client):
        mock_health.return_value = {
            "healthy": True,
            "provider": "openai",
            "model": "gpt-4",
        }
        async with client:
            resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"

    @patch("app.routers.health.check_llm_health", new_callable=AsyncMock)
    async def test_health_returns_degraded(self, mock_health, client):
        mock_health.return_value = {
            "healthy": False,
            "provider": "openai",
            "model": "gpt-4",
            "error_code": "api_key_missing",
        }
        async with client:
            resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"


class TestStatusEndpoint:
    """GET /api/v1/status"""

    @patch("app.routers.health.db")
    @patch("app.routers.health.check_llm_health", new_callable=AsyncMock)
    @patch("app.routers.health.get_llm_config")
    async def test_status_ready(self, mock_config, mock_health, mock_db, client):
        mock_config.return_value = type("C", (), {"api_key": "sk-test", "provider": "openai"})()
        mock_health.return_value = {"healthy": True}
        mock_db.get_stats.return_value = {
            "total_resumes": 1,
            "total_jobs": 0,
            "total_improvements": 0,
            "has_master_resume": True,
        }
        async with client:
            resp = await client.get("/api/v1/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"
        assert data["llm_healthy"] is True
        assert data["generation_available"] is True
        assert data["has_master_resume"] is True

    @patch("app.routers.health.db")
    @patch("app.routers.health.check_llm_health", new_callable=AsyncMock)
    @patch("app.routers.health.get_llm_config")
    async def test_status_setup_required(self, mock_config, mock_health, mock_db, client):
        mock_config.return_value = type("C", (), {"api_key": "", "provider": "openai"})()
        mock_health.return_value = {"healthy": False}
        mock_db.get_stats.return_value = {
            "total_resumes": 0,
            "total_jobs": 0,
            "total_improvements": 0,
            "has_master_resume": False,
        }
        async with client:
            resp = await client.get("/api/v1/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "setup_required"
        assert data["generation_available"] is False

    @patch("app.routers.health.get_runtime_llm_config")
    @patch("app.routers.health.check_llm_health", new_callable=AsyncMock)
    @patch("app.routers.health.get_llm_config")
    async def test_status_workspace_session_skips_shared_config(
        self,
        mock_config,
        mock_health,
        mock_runtime_config,
        client,
        isolated_database,
        make_workspace_session_cookie,
    ):
        mock_runtime_config.return_value = type(
            "RuntimeConfig",
            (),
            {"api_key": "runtime-key", "provider": "openai", "model": "gpt-test", "api_base": None},
        )()
        mock_health.return_value = {"healthy": True}
        with workspace_scope("workspace-a"):
            master_resume = db.create_resume(
                content="# Resume A",
                filename="resume-a.md",
                is_master=True,
                processing_status="ready",
            )
            job = db.create_job("Job A", resume_id=master_resume["resume_id"])
            db.create_improvement(
                original_resume_id=master_resume["resume_id"],
                tailored_resume_id=master_resume["resume_id"],
                job_id=job["job_id"],
                improvements=[],
            )

        async with client:
            cookie_name, cookie_value = make_workspace_session_cookie("workspace-a")
            client.cookies.set(cookie_name, cookie_value)
            resp = await client.get("/api/v1/status")

        assert resp.status_code == 200
        data = resp.json()
        assert data == {
            "status": "ready",
            "llm_configured": True,
            "llm_healthy": True,
            "generation_available": True,
            "has_master_resume": True,
            "database_stats": {
                "total_resumes": 1,
                "total_jobs": 1,
                "total_improvements": 1,
                "has_master_resume": True,
            },
            "workspace_mode": True,
            "workspace_id": "workspace-a",
            "settings_available": False,
        }
        mock_config.assert_not_called()
        mock_runtime_config.assert_called_once()
        mock_health.assert_awaited_once()

    @patch("app.routers.health.get_runtime_llm_config")
    @patch("app.routers.health.check_llm_health", new_callable=AsyncMock)
    async def test_status_workspace_session_reports_generation_unavailable_when_runtime_llm_unhealthy(
        self,
        mock_health,
        mock_runtime_config,
        client,
        isolated_database,
        make_workspace_session_cookie,
    ):
        mock_runtime_config.return_value = type(
            "RuntimeConfig",
            (),
            {"api_key": "runtime-key", "provider": "openai", "model": "gpt-test", "api_base": None},
        )()
        mock_health.return_value = {"healthy": False}

        with workspace_scope("workspace-a"):
            db.create_resume(
                content="# Resume A",
                filename="resume-a.md",
                is_master=True,
                processing_status="ready",
            )

        async with client:
            cookie_name, cookie_value = make_workspace_session_cookie("workspace-a")
            client.cookies.set(cookie_name, cookie_value)
            resp = await client.get("/api/v1/status")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "setup_required"
        assert data["llm_configured"] is True
        assert data["llm_healthy"] is False
        assert data["generation_available"] is False
