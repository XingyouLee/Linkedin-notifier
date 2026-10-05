"""Integration tests for the LinkedIn notifier launch flow."""
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from app.database import db
from app.main import app
import app.routers.linkedin_notifier as linkedin_notifier_router
from app.schemas.models import (
    GenerateContentResponse,
    ImproveResumeData,
    ImproveResumeResponse,
    LaunchResumeContext,
    ResumeData,
)
from app.workspace_auth import workspace_scope


@pytest.fixture
def client(make_workspace_session_cookie):
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


async def test_canonical_profile_resume_prefers_db_text_and_uses_path_only_for_filename():
    content, filename = await linkedin_notifier_router._canonical_profile_resume_to_markdown(
        resume_text="# From cached text",
        resume_path="resume/xingyouli.md",
        fallback_filename="fallback.md",
    )

    assert content == "# From cached text"
    assert filename == "xingyouli.md"


async def test_canonical_profile_resume_requires_db_backfill_before_launch():
    with pytest.raises(HTTPException) as exc_info:
        await linkedin_notifier_router._canonical_profile_resume_to_markdown(
            resume_text=None,
            resume_path="resume/xingyouli.md",
            fallback_filename="fallback.md",
        )

    assert exc_info.value.status_code == 422
    assert "sync_profiles_from_source(force=True)" in exc_info.value.detail


class TestLinkedInNotifierLaunch:
    @patch("app.routers.linkedin_notifier._create_hidden_resume", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._canonical_profile_resume_to_markdown", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._fetch_launch_record")
    @patch("app.routers.linkedin_notifier._verify_launch_token")
    @patch("app.routers.linkedin_notifier.db")
    async def test_launch_initializes_workspace_context(
        self,
        mock_db,
        mock_verify,
        mock_fetch_record,
        mock_canonical_profile_resume_to_markdown,
        mock_create_hidden_resume,
        client,
    ):
        mock_verify.return_value = {"profile_id": 7, "job_id": "job-123"}
        mock_fetch_record.return_value = {
            "profile_id": 7,
            "job_id": "job-123",
            "display_name": "Xingyou Li",
            "resume_path": "resume/xingyouli.md",
            "resume_text": None,
            "title": "Data Engineer",
            "company": "Example Co",
            "job_url": "https://example.com/job-123",
            "description": "Build data pipelines with Python and SQL.",
        }
        mock_canonical_profile_resume_to_markdown.return_value = ("# Xingyou Li", "xingyouli.md")
        mock_create_hidden_resume.return_value = (
            {
                "resume_id": "res-launch-1",
                "filename": "xingyouli.md",
                "content": "# Xingyou Li",
                "processing_status": "pending",
            },
            LaunchResumeContext(
                resume_id="res-launch-1",
                filename="xingyouli.md",
                source_label="Profile resume",
                is_default=True,
                processing_status="pending",
                excerpt="# Xingyou Li",
            ),
        )
        mock_db.create_job.return_value = {"job_id": "job-workspace-1"}
        mock_db.update_job.return_value = {"job_id": "job-workspace-1"}

        async with client:
            response = await client.get(
                "/api/v1/integrations/linkedin-notifier/launch",
                params={"token": "launch-token"},
            )

        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["profile"]["profile_id"] == 7
        assert payload["job"]["job_id"] == "job-workspace-1"
        assert payload["job"]["canonical_job_id"] == "job-123"
        assert payload["resume"]["resume_id"] == "res-launch-1"
        assert payload["resume"]["is_default"] is True
        assert "resume_matcher_session=" in response.headers["set-cookie"]

    @patch("app.routers.linkedin_notifier._canonical_profile_resume_to_markdown", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._fetch_launch_record")
    @patch("app.routers.linkedin_notifier._verify_launch_token")
    async def test_launch_reuses_existing_workspace_context_for_same_token(
        self,
        mock_verify,
        mock_fetch_record,
        mock_canonical_profile_resume_to_markdown,
        client,
        isolated_database,
        monkeypatch,
    ):
        monkeypatch.setenv("RESUME_MATCHER_SESSION_SECRET", "test-session-secret")
        mock_verify.return_value = {"profile_id": 7, "job_id": "job-123"}
        mock_fetch_record.return_value = {
            "profile_id": 7,
            "job_id": "job-123",
            "display_name": "Xingyou Li",
            "resume_path": "resume/xingyouli.md",
            "resume_text": None,
            "title": "Data Engineer",
            "company": "Example Co",
            "job_url": "https://example.com/job-123",
            "description": "Build data pipelines with Python and SQL.",
        }
        mock_canonical_profile_resume_to_markdown.return_value = ("# Xingyou Li", "xingyouli.md")

        async with client:
            first = await client.get(
                "/api/v1/integrations/linkedin-notifier/launch",
                params={"token": "launch-token"},
            )
            second = await client.get(
                "/api/v1/integrations/linkedin-notifier/launch",
                params={"token": "launch-token"},
            )

        assert first.status_code == 200
        assert second.status_code == 200

        first_payload = first.json()["data"]
        second_payload = second.json()["data"]
        assert second_payload["resume"]["resume_id"] == first_payload["resume"]["resume_id"]
        assert second_payload["job"]["job_id"] == first_payload["job"]["job_id"]

        all_resumes = db.resumes.all()
        all_jobs = db.jobs.all()
        assert len(all_resumes) == 1
        assert len(all_jobs) == 1
        assert all_resumes[0]["filename"] == "xingyouli.md"
        assert all_jobs[0]["canonical_job_id"] == "job-123"

    @patch("app.routers.linkedin_notifier._canonical_profile_resume_to_markdown", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._fetch_launch_record")
    @patch("app.routers.linkedin_notifier._verify_launch_token")
    async def test_launch_refreshes_existing_profile_resume_when_source_changes(
        self,
        mock_verify,
        mock_fetch_record,
        mock_canonical_profile_resume_to_markdown,
        client,
        isolated_database,
        monkeypatch,
    ):
        monkeypatch.setenv("RESUME_MATCHER_SESSION_SECRET", "test-session-secret")
        mock_verify.return_value = {"profile_id": 7, "job_id": "job-123"}
        mock_fetch_record.return_value = {
            "profile_id": 7,
            "job_id": "job-123",
            "display_name": "Xingyou Li",
            "resume_path": "resume/xingyouli.md",
            "resume_text": "stale cached text",
            "title": "Data Engineer",
            "company": "Example Co",
            "job_url": "https://example.com/job-123",
            "description": "Build data pipelines with Python and SQL.",
        }
        mock_canonical_profile_resume_to_markdown.side_effect = [
            ("# Old Xingyou Resume", "xingyouli.md"),
            ("# New Xingyou Resume", "xingyouli.md"),
        ]

        async with client:
            first = await client.get(
                "/api/v1/integrations/linkedin-notifier/launch",
                params={"token": "launch-token"},
            )
            second = await client.get(
                "/api/v1/integrations/linkedin-notifier/launch",
                params={"token": "launch-token"},
            )

        assert first.status_code == 200
        assert second.status_code == 200

        first_payload = first.json()["data"]
        second_payload = second.json()["data"]
        assert second_payload["resume"]["resume_id"] == first_payload["resume"]["resume_id"]

        stored_resume = db.get_resume(first_payload["resume"]["resume_id"])
        assert stored_resume is not None
        assert stored_resume["content"] == "# New Xingyou Resume"
        assert stored_resume["original_markdown"] == "# New Xingyou Resume"
        assert stored_resume["processing_status"] == "pending"

        all_resumes = db.resumes.all()
        assert len(all_resumes) == 1

    @patch("app.routers.linkedin_notifier._create_hidden_resume", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._upload_file_to_markdown", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._verify_launch_token")
    async def test_upload_override_stores_hidden_resume(
        self,
        mock_verify,
        mock_upload_file_to_markdown,
        mock_create_hidden_resume,
        client,
    ):
        mock_verify.return_value = {"profile_id": 7, "job_id": "job-123"}
        mock_upload_file_to_markdown.return_value = ("# Uploaded Resume", "override.md")
        mock_create_hidden_resume.return_value = (
            {
                "resume_id": "uploaded-1",
                "filename": "override.md",
                "processing_status": "pending",
            },
            LaunchResumeContext(
                resume_id="uploaded-1",
                filename="override.md",
                source_label="Uploaded resume",
                is_default=False,
                processing_status="pending",
                excerpt="# Uploaded Resume",
            ),
        )

        async with client:
            response = await client.post(
                "/api/v1/integrations/linkedin-notifier/resumes/upload?token=launch-token",
                files={"file": ("override.md", b"# Uploaded Resume", "text/markdown")},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["resume_id"] == "uploaded-1"
        assert payload["processing_status"] == "pending"
        assert payload["is_master"] is False


class TestLinkedInNotifierGenerate:
    @patch("app.routers.linkedin_notifier.resumes_router.generate_cover_letter_endpoint", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier.resumes_router.improve_resume_confirm_endpoint", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier.resumes_router.improve_resume_preview_endpoint", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._ensure_resume_ready", new_callable=AsyncMock)
    @patch("app.routers.linkedin_notifier._verify_launch_token")
    @patch("app.routers.linkedin_notifier.db")
    async def test_generate_runs_preview_confirm_and_cover_letter_fallback(
        self,
        mock_db,
        mock_verify,
        mock_ensure_resume_ready,
        mock_preview,
        mock_confirm,
        mock_generate_cover_letter,
        client,
        sample_resume,
    ):
        selected_resume = {
            "resume_id": "resume-1",
            "content": "# Resume",
            "content_type": "md",
            "processing_status": "pending",
            "processed_data": None,
        }
        ready_resume = {
            **selected_resume,
            "processing_status": "ready",
            "processed_data": sample_resume,
        }
        tailored_resume = {
            "resume_id": "tailored-1",
            "cover_letter": None,
        }
        job_record = {"job_id": "job-workspace-1", "content": "Original JD"}

        def get_resume_side_effect(resume_id: str):
            if resume_id == "resume-1":
                return selected_resume
            if resume_id == "tailored-1":
                return tailored_resume
            return None

        mock_db.get_resume.side_effect = get_resume_side_effect
        mock_db.get_job.return_value = job_record
        mock_db.get_master_resume.return_value = {"resume_id": "master-old"}
        mock_db.set_master_resume.return_value = True
        mock_db.update_job.return_value = {"job_id": "job-workspace-1", "content": "Updated JD"}
        mock_verify.return_value = {"profile_id": 7, "job_id": "job-123"}
        mock_ensure_resume_ready.return_value = ready_resume

        validated_resume = ResumeData.model_validate(sample_resume)
        mock_preview.return_value = ImproveResumeResponse(
            request_id="preview-1",
            data=ImproveResumeData(
                request_id="preview-1",
                resume_id=None,
                job_id="job-workspace-1",
                resume_preview=validated_resume,
                improvements=[],
                warnings=[],
                refinement_attempted=False,
                refinement_successful=False,
            ),
        )
        mock_confirm.return_value = ImproveResumeResponse(
            request_id="confirm-1",
            data=ImproveResumeData(
                request_id="confirm-1",
                resume_id="tailored-1",
                job_id="job-workspace-1",
                resume_preview=validated_resume,
                improvements=[],
                cover_letter=None,
                warnings=[],
                refinement_attempted=False,
                refinement_successful=False,
            ),
        )
        mock_generate_cover_letter.return_value = GenerateContentResponse(
            content="Motivated cover letter",
            message="generated",
        )

        async with client:
            response = await client.post(
                "/api/v1/integrations/linkedin-notifier/generate",
                json={
                    "token": "launch-token",
                    "resume_id": "resume-1",
                    "job_id": "job-workspace-1",
                    "job_description": "Updated JD",
                },
            )

        assert response.status_code == 200
        payload = response.json()["data"]
        assert payload["resume_id"] == "tailored-1"
        assert payload["cover_letter_generated"] is True
        assert payload["builder_path"] == "/builder?id=tailored-1"
        mock_preview.assert_awaited_once()
        mock_confirm.assert_awaited_once()
        mock_generate_cover_letter.assert_awaited_once_with("tailored-1")
        assert mock_db.set_master_resume.call_args_list[0].args == ("resume-1",)
        assert mock_db.set_master_resume.call_args_list[-1].args == ("master-old",)


@patch("app.routers.linkedin_notifier.parse_resume_to_json", new_callable=AsyncMock)
async def test_ensure_resume_ready_surfaces_upstream_provider_details(
    mock_parse_resume_to_json,
    isolated_database,
):
    resume = db.create_resume(
        content="# Resume",
        content_type="md",
        filename="resume.md",
        is_master=False,
        processed_data=None,
        processing_status="pending",
        original_markdown="# Resume",
    )
    mock_parse_resume_to_json.side_effect = RuntimeError(
        "FATAL_API::endpoint=xcode model_name=gpt-5.4 status=403 error=insufficient_user_quota"
    )

    with pytest.raises(HTTPException) as exc_info:
        await linkedin_notifier_router._ensure_resume_ready(resume)

    assert exc_info.value.status_code == 502
    assert "endpoint=xcode" in exc_info.value.detail
    assert "insufficient_user_quota" in exc_info.value.detail
    stored = db.get_resume(resume["resume_id"])
    assert stored["processing_status"] == "failed"


@patch("app.routers.linkedin_notifier._canonical_profile_resume_to_markdown", new_callable=AsyncMock)
@patch("app.routers.linkedin_notifier._fetch_launch_record")
async def test_refresh_selected_profile_resume_if_needed_updates_default_launch_resume(
    mock_fetch_launch_record,
    mock_canonical_profile_resume_to_markdown,
    isolated_database,
):
    mock_fetch_launch_record.return_value = {
        "profile_id": 7,
        "job_id": "job-123",
        "resume_path": "resume/xingyouli.md",
        "resume_text": "stale cached text",
    }
    mock_canonical_profile_resume_to_markdown.return_value = ("# New Xingyou Resume", "xingyouli.md")

    with workspace_scope("workspace-1"):
        resume = db.create_resume(
            content="# Old Xingyou Resume",
            content_type="md",
            filename="xingyouli.md",
            is_master=False,
            processed_data={"parsed": True},
            processing_status="ready",
            original_markdown="# Old Xingyou Resume",
        )
        resume = linkedin_notifier_router._mark_hidden_resume(
            resume["resume_id"],
            source_profile_id=7,
            source_job_id="job-123",
        )

        refreshed = await linkedin_notifier_router._refresh_selected_profile_resume_if_needed(
            resume=resume,
            profile_id=7,
            job_id="job-123",
        )

        assert refreshed["resume_id"] == resume["resume_id"]
        assert refreshed["content"] == "# New Xingyou Resume"
        assert refreshed["original_markdown"] == "# New Xingyou Resume"
        assert refreshed["processing_status"] == "pending"
        assert refreshed["processed_data"] is None
