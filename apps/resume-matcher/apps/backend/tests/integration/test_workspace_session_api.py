"""Integration tests for workspace session scoping."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.database import db
from app.main import app
from app.workspace_auth import WORKSPACE_SESSION_COOKIE_NAME, workspace_scope


@contextmanager
def temporary_database(path: Path):
    """Temporarily point the global TinyDB instance at a test-local file."""
    original_path = db.db_path
    db.close()
    db.db_path = path
    try:
        yield
    finally:
        db.close()
        db.db_path = original_path
        db.close()


@pytest.fixture
def isolated_database(tmp_path):
    with temporary_database(tmp_path / "database.json"):
        yield


async def test_resume_routes_require_workspace_session(isolated_database):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/resumes/list")

    assert response.status_code == 401
    assert response.json()["detail"] == "Workspace session is required."


async def test_resume_list_and_fetch_are_workspace_scoped(
    isolated_database,
    make_workspace_session_cookie,
):
    with workspace_scope("workspace-a"):
        resume_a = db.create_resume(
            content="# Resume A",
            filename="resume-a.md",
            is_master=True,
            processing_status="ready",
        )

    with workspace_scope("workspace-b"):
        resume_b = db.create_resume(
            content="# Resume B",
            filename="resume-b.md",
            is_master=True,
            processing_status="ready",
        )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client_a:
        cookie_name, cookie_value = make_workspace_session_cookie("workspace-a")
        client_a.cookies.set(cookie_name, cookie_value)

        list_response = await client_a.get("/api/v1/resumes/list", params={"include_master": True})
        assert list_response.status_code == 200
        list_payload = list_response.json()["data"]
        assert [item["resume_id"] for item in list_payload] == [resume_a["resume_id"]]

        fetch_response = await client_a.get(
            "/api/v1/resumes",
            params={"resume_id": resume_a["resume_id"]},
        )
        assert fetch_response.status_code == 200
        assert fetch_response.json()["data"]["resume_id"] == resume_a["resume_id"]

        cross_workspace_fetch = await client_a.get(
            "/api/v1/resumes",
            params={"resume_id": resume_b["resume_id"]},
        )
        assert cross_workspace_fetch.status_code == 404

    async with AsyncClient(transport=transport, base_url="http://test") as client_b:
        cookie_name, cookie_value = make_workspace_session_cookie("workspace-b")
        client_b.cookies.set(WORKSPACE_SESSION_COOKIE_NAME, cookie_value)

        list_response = await client_b.get("/api/v1/resumes/list", params={"include_master": True})
        assert list_response.status_code == 200
        list_payload = list_response.json()["data"]
        assert [item["resume_id"] for item in list_payload] == [resume_b["resume_id"]]
