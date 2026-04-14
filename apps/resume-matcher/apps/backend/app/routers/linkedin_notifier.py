"""LinkedIn notifier integration endpoints for Resume Matcher launch flow."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, File, HTTPException, Query, Request, Response, UploadFile
from psycopg import connect
from psycopg.rows import dict_row

from app.database import db
from app.runtime_env import get_shared_setting, resolve_repo_roots
from app.routers import resumes as resumes_router
from app.schemas import (
    ImproveResumeConfirmRequest,
    ImproveResumeRequest,
    IntegrationGenerateRequest,
    IntegrationGenerateResponse,
    IntegrationGenerateData,
    IntegrationLaunchData,
    IntegrationLaunchResponse,
    LaunchJobContext,
    LaunchProfileContext,
    LaunchResumeContext,
    ResumeUploadResponse,
)
from app.services.parser import parse_document, parse_resume_to_json
from app.workspace_auth import (
    derive_workspace_id_from_launch_token,
    set_workspace_session_cookie,
    workspace_scope,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/integrations/linkedin-notifier",
    tags=["LinkedIn Notifier Integration"],
)

REPO_ROOT, PRIMARY_REPO_ROOT = resolve_repo_roots(Path(__file__))
USER_INFO_DIR = REPO_ROOT / "include" / "user_info"
MAX_FILE_SIZE = 4 * 1024 * 1024
DOCUMENT_SUFFIXES = {".pdf", ".doc", ".docx"}
TEXT_SUFFIXES = {".md", ".txt"}
DOCUMENT_CONTENT_TYPES = {
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}
TEXT_CONTENT_TYPES = {
    "text/plain",
    "text/markdown",
    "text/x-markdown",
    "application/octet-stream",
}
DEFAULT_SESSION_FALLBACK_TTL_SECONDS = 60 * 60 * 24 * 7


def _get_notifier_setting(name: str) -> str:
    return get_shared_setting(name)


def _require_notifier_setting(name: str) -> str:
    value = _get_notifier_setting(name).strip()
    if not value:
        raise HTTPException(
            status_code=500,
            detail=f"{name} is not configured for Resume Matcher integration.",
        )
    return value


def _urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def _verify_launch_token(token: str) -> dict[str, Any]:
    secret = _require_notifier_setting("MATERIALS_LINK_SECRET")

    try:
        encoded_payload, encoded_signature = str(token).split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid launch token format.") from exc

    payload_bytes = _urlsafe_b64decode(encoded_payload)
    expected_signature = hmac.new(
        secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    actual_signature = _urlsafe_b64decode(encoded_signature)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise HTTPException(status_code=400, detail="Invalid launch token signature.")

    payload = json.loads(payload_bytes.decode("utf-8"))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=400, detail="Launch token expired.")
    return payload


def _resolve_session_expiry(payload: dict[str, Any]) -> int:
    """Resolve the session expiry timestamp from the launch payload."""
    exp = payload.get("exp")
    if exp is None:
        return int(time.time()) + DEFAULT_SESSION_FALLBACK_TTL_SECONDS
    return int(exp)


def _fetch_launch_record(profile_id: int, job_id: str) -> dict[str, Any]:
    jobs_db_url = _require_notifier_setting("JOBS_DB_URL")
    query = """
        SELECT
            pj.profile_id,
            pj.job_id,
            p.display_name,
            p.resume_path,
            p.resume_text,
            j.title,
            j.company,
            j.job_url,
            j.description
        FROM profile_jobs pj
        JOIN profiles p ON p.id = pj.profile_id
        JOIN jobs j ON j.id = pj.job_id
        WHERE pj.profile_id = %s AND pj.job_id = %s
        LIMIT 1
    """
    with connect(jobs_db_url, row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (int(profile_id), str(job_id)))
            record = cursor.fetchone()

    if not record:
        raise HTTPException(status_code=404, detail="Profile/job launch context not found.")
    if not str(record.get("description") or "").strip():
        raise HTTPException(status_code=422, detail="Job description is not available yet.")
    return dict(record)


def _user_info_roots() -> list[Path]:
    roots = [USER_INFO_DIR]
    primary_user_info_dir = PRIMARY_REPO_ROOT / "include" / "user_info"
    if primary_user_info_dir != USER_INFO_DIR:
        roots.append(primary_user_info_dir)
    return roots


def _extract_user_info_relative_tail(path: Path) -> Path | None:
    parts = list(path.parts)
    for index in range(len(parts) - 1):
        if parts[index] == "include" and parts[index + 1] == "user_info":
            tail_parts = parts[index + 2 :]
            return Path(*tail_parts) if tail_parts else None
    return None


def _resolve_resume_path(resume_path: str) -> Path:
    raw_path = Path(os.path.expanduser(str(resume_path)))
    relative_tail = _extract_user_info_relative_tail(raw_path)
    if raw_path.is_absolute():
        if relative_tail is None:
            raise HTTPException(
                status_code=404,
                detail=f"Could not resolve profile resume path: {resume_path}",
            )
    else:
        relative_tail = relative_tail or raw_path

    candidates: list[Path] = []
    for root in _user_info_roots():
        root_resolved = root.resolve(strict=False)
        candidate = (root / relative_tail).resolve(strict=False)
        if candidate.is_relative_to(root_resolved):
            candidates.append(candidate)

    deduped_candidates: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped_candidates.append(candidate)

    for candidate in deduped_candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    raise HTTPException(
        status_code=404,
        detail=f"Could not resolve profile resume path: {resume_path}",
    )


async def _resume_source_to_markdown(
    *,
    resume_text: str | None,
    resume_path: str | None,
    fallback_filename: str,
) -> tuple[str, str]:
    if resume_text and str(resume_text).strip():
        filename = Path(resume_path).name if resume_path else fallback_filename
        return str(resume_text), filename

    if not resume_path:
        raise HTTPException(status_code=422, detail="Profile resume is not configured.")

    resolved_path = _resolve_resume_path(resume_path)
    suffix = resolved_path.suffix.lower()
    filename = resolved_path.name

    if suffix in TEXT_SUFFIXES:
        try:
            return resolved_path.read_text(encoding="utf-8"), filename
        except UnicodeDecodeError:
            return resolved_path.read_text(encoding="utf-8-sig"), filename

    if suffix in DOCUMENT_SUFFIXES:
        content = resolved_path.read_bytes()
        markdown_content = await parse_document(content, filename)
        return markdown_content, filename

    try:
        return resolved_path.read_text(encoding="utf-8"), filename
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                "Unsupported resume file type. Supported profile resumes are markdown, "
                "txt, PDF, DOC, and DOCX."
            ),
        ) from exc


async def _upload_file_to_markdown(file: UploadFile) -> tuple[str, str]:
    filename = file.filename or "resume-upload"
    suffix = Path(filename).suffix.lower()
    content_type = (file.content_type or "").lower()

    if suffix not in TEXT_SUFFIXES | DOCUMENT_SUFFIXES and content_type not in (
        DOCUMENT_CONTENT_TYPES | TEXT_CONTENT_TYPES
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid file type. Allowed: markdown, txt, PDF, DOC, DOCX."
            ),
        )

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file.")
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size: {MAX_FILE_SIZE // (1024 * 1024)}MB",
        )

    if suffix in DOCUMENT_SUFFIXES or content_type in DOCUMENT_CONTENT_TYPES:
        try:
            markdown_content = await parse_document(content, filename)
        except Exception as exc:
            logger.error("Document parsing failed for integration upload %s: %s", filename, exc)
            raise HTTPException(
                status_code=422,
                detail="Failed to parse uploaded document. Please use a valid PDF or Word file.",
            ) from exc
        return markdown_content, filename

    for encoding in ("utf-8", "utf-8-sig"):
        try:
            return content.decode(encoding), filename
        except UnicodeDecodeError:
            continue
    raise HTTPException(status_code=422, detail="Failed to decode uploaded markdown/text file.")


def _summarize_resume(
    *,
    resume_id: str,
    filename: str | None,
    content: str,
    source_label: str,
    processing_status: str,
    is_default: bool,
) -> LaunchResumeContext:
    preview = " ".join((content or "").split())
    excerpt = preview[:280] + ("..." if len(preview) > 280 else "")
    return LaunchResumeContext(
        resume_id=resume_id,
        filename=filename,
        source_label=source_label,
        is_default=is_default,
        processing_status=processing_status,
        excerpt=excerpt or None,
    )


def _mark_hidden_resume(resume_id: str, **metadata: Any) -> dict[str, Any]:
    return db.update_resume(
        resume_id,
        {
            "hidden_from_lists": True,
            "integration_source": "linkedin_notifier",
            **metadata,
        },
    )


async def _create_hidden_resume(
    *,
    content: str,
    filename: str | None,
    source_label: str,
    is_default: bool,
    metadata: dict[str, Any],
) -> tuple[dict[str, Any], LaunchResumeContext]:
    stored = db.create_resume(
        content=content,
        content_type="md",
        filename=filename,
        is_master=False,
        processed_data=None,
        processing_status="pending",
        original_markdown=content,
    )
    stored = _mark_hidden_resume(stored["resume_id"], **metadata)
    summary = _summarize_resume(
        resume_id=stored["resume_id"],
        filename=stored.get("filename"),
        content=stored.get("content", ""),
        source_label=source_label,
        processing_status=stored.get("processing_status", "pending"),
        is_default=is_default,
    )
    return stored, summary


def _find_existing_default_launch_resume(*, profile_id: int, job_id: str) -> dict[str, Any] | None:
    matches = [
        resume
        for resume in db.list_resumes()
        if resume.get("integration_source") == "linkedin_notifier"
        and int(resume.get("source_profile_id") or -1) == int(profile_id)
        and str(resume.get("source_job_id") or "") == str(job_id)
        and not resume.get("uploaded_via_launch", False)
    ]
    if not matches:
        return None
    return min(matches, key=lambda resume: str(resume.get("created_at") or ""))


def _find_existing_launch_job(*, profile_id: int, job_id: str) -> dict[str, Any] | None:
    matches = [
        job
        for job in db.list_jobs()
        if job.get("integration_source") == "linkedin_notifier"
        and int(job.get("profile_id") or -1) == int(profile_id)
        and str(job.get("canonical_job_id") or "") == str(job_id)
    ]
    if not matches:
        return None
    return min(matches, key=lambda job: str(job.get("created_at") or ""))


def _restore_master_resume(previous_master_id: str | None, selected_resume_id: str) -> None:
    if previous_master_id and previous_master_id != selected_resume_id:
        restored = db.set_master_resume(previous_master_id)
        if restored:
            return

    selected_resume = db.get_resume(selected_resume_id)
    if selected_resume and selected_resume.get("is_master"):
        db.update_resume(selected_resume_id, {"is_master": False})


async def _ensure_resume_ready(resume: dict[str, Any]) -> dict[str, Any]:
    if resume.get("processed_data"):
        updates: dict[str, Any] = {}
        if resume.get("processing_status") != "ready":
            updates["processing_status"] = "ready"
        if not resume.get("original_markdown") and resume.get("content_type") == "md":
            updates["original_markdown"] = resume.get("content", "")
        if updates:
            resume = db.update_resume(resume["resume_id"], updates)
        return resume

    db.update_resume(resume["resume_id"], {"processing_status": "processing"})
    try:
        processed_data = await parse_resume_to_json(resume.get("content", ""))
    except Exception as exc:
        logger.warning(
            "Structured parsing failed for integration resume %s: %s",
            resume["resume_id"],
            exc,
        )
        db.update_resume(resume["resume_id"], {"processing_status": "failed"})
        raise HTTPException(
            status_code=422,
            detail="Failed to parse the selected resume into structured data.",
        ) from exc

    return db.update_resume(
        resume["resume_id"],
        {
            "processed_data": processed_data,
            "processing_status": "ready",
            "original_markdown": resume.get("original_markdown") or resume.get("content", ""),
        },
    )


@router.get("/launch", response_model=IntegrationLaunchResponse)
async def initialize_launch(
    request: Request,
    response: Response,
    token: str = Query(...),
) -> IntegrationLaunchResponse:
    """Create a launch workspace from a Discord deep-link token.

    This imports the canonical JD and resume into Resume Matcher without using the
    LLM. Tailoring only happens later when the user clicks Generate.
    """
    payload = _verify_launch_token(token)
    workspace_id = derive_workspace_id_from_launch_token(token)
    record = _fetch_launch_record(int(payload["profile_id"]), str(payload["job_id"]))

    with workspace_scope(workspace_id):
        existing_resume = _find_existing_default_launch_resume(
            profile_id=int(record["profile_id"]),
            job_id=str(record["job_id"]),
        )
        if existing_resume:
            resume_summary = _summarize_resume(
                resume_id=existing_resume["resume_id"],
                filename=existing_resume.get("filename"),
                content=existing_resume.get("content", ""),
                source_label="Profile resume",
                processing_status=existing_resume.get("processing_status", "pending"),
                is_default=True,
            )
        else:
            markdown_content, filename = await _resume_source_to_markdown(
                resume_text=record.get("resume_text"),
                resume_path=record.get("resume_path"),
                fallback_filename=f"profile-{record['profile_id']}.md",
            )
            if not str(markdown_content).strip():
                raise HTTPException(status_code=422, detail="Resolved profile resume is empty.")

            _, resume_summary = await _create_hidden_resume(
                content=markdown_content,
                filename=filename,
                source_label="Profile resume",
                is_default=True,
                metadata={
                    "source_profile_id": int(record["profile_id"]),
                    "source_job_id": str(record["job_id"]),
                    "canonical_resume_path": record.get("resume_path"),
                },
            )

        existing_job = _find_existing_launch_job(
            profile_id=int(record["profile_id"]),
            job_id=str(record["job_id"]),
        )
        if existing_job:
            workspace_job = existing_job
        else:
            workspace_job = db.create_job(
                str(record["description"]).strip(),
                resume_id=resume_summary.resume_id,
            )
            workspace_job = db.update_job(
                workspace_job["job_id"],
                {
                    "hidden_from_lists": True,
                    "integration_source": "linkedin_notifier",
                    "canonical_job_id": str(record["job_id"]),
                    "profile_id": int(record["profile_id"]),
                    "title": record.get("title"),
                    "company": record.get("company"),
                    "job_url": record.get("job_url"),
                },
            ) or workspace_job

    set_workspace_session_cookie(
        response,
        workspace_id=workspace_id,
        expires_at=_resolve_session_expiry(payload),
        request=request,
    )

    return IntegrationLaunchResponse(
        data=IntegrationLaunchData(
            profile=LaunchProfileContext(
                profile_id=int(record["profile_id"]),
                display_name=str(record.get("display_name") or f"Profile {record['profile_id']}"),
                canonical_resume_path=record.get("resume_path"),
            ),
            job=LaunchJobContext(
                canonical_job_id=str(record["job_id"]),
                job_id=workspace_job["job_id"],
                title=record.get("title"),
                company=record.get("company"),
                job_url=record.get("job_url"),
                description=str(record["description"]),
            ),
            resume=resume_summary,
        )
    )


@router.post("/resumes/upload", response_model=ResumeUploadResponse)
async def upload_resume_override(
    response: Response,
    request: Request,
    token: str = Query(...),
    file: UploadFile = File(...),
) -> ResumeUploadResponse:
    """Upload a launch-only resume override without triggering LLM parsing."""
    payload = _verify_launch_token(token)
    workspace_id = derive_workspace_id_from_launch_token(token)
    with workspace_scope(workspace_id):
        markdown_content, filename = await _upload_file_to_markdown(file)
        stored, _ = await _create_hidden_resume(
            content=markdown_content,
            filename=filename,
            source_label="Uploaded resume",
            is_default=False,
            metadata={
                "uploaded_via_launch": True,
            },
        )

    set_workspace_session_cookie(
        response,
        workspace_id=workspace_id,
        expires_at=_resolve_session_expiry(payload),
        request=request,
    )

    return ResumeUploadResponse(
        message=f"File {filename} uploaded successfully.",
        request_id=stored["resume_id"],
        resume_id=stored["resume_id"],
        processing_status=stored.get("processing_status", "pending"),
        is_master=False,
    )


@router.post("/generate", response_model=IntegrationGenerateResponse)
async def generate_materials(
    response: Response,
    http_request: Request,
    payload_request: IntegrationGenerateRequest,
) -> IntegrationGenerateResponse:
    """Generate tailored materials only after an explicit user action."""
    payload = _verify_launch_token(payload_request.token)
    workspace_id = derive_workspace_id_from_launch_token(payload_request.token)

    with workspace_scope(workspace_id):
        resume = db.get_resume(payload_request.resume_id)
        if not resume:
            raise HTTPException(status_code=404, detail="Selected resume not found.")

        job = db.get_job(payload_request.job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Launch job not found.")

        job_description = str(payload_request.job_description or "").strip()
        if job_description:
            job = db.update_job(payload_request.job_id, {"content": job_description}) or job

        if not str(job.get("content") or "").strip():
            raise HTTPException(status_code=422, detail="Job description cannot be empty.")

        previous_master = db.get_master_resume()
        previous_master_id = previous_master.get("resume_id") if previous_master else None

        try:
            if not db.set_master_resume(payload_request.resume_id):
                raise HTTPException(status_code=404, detail="Selected resume not found.")

            resume = await _ensure_resume_ready(resume)

            preview = await resumes_router.improve_resume_preview_endpoint(
                ImproveResumeRequest(
                    resume_id=payload_request.resume_id,
                    job_id=payload_request.job_id,
                    prompt_id=payload_request.prompt_id,
                )
            )
            confirm = await resumes_router.improve_resume_confirm_endpoint(
                ImproveResumeConfirmRequest(
                    resume_id=payload_request.resume_id,
                    job_id=payload_request.job_id,
                    improved_data=preview.data.resume_preview,
                    improvements=preview.data.improvements,
                )
            )

            tailored_resume_id = confirm.data.resume_id
            if not tailored_resume_id:
                raise HTTPException(status_code=500, detail="Tailored resume was not created.")

            tailored_resume = db.get_resume(tailored_resume_id)
            cover_letter_generated = bool((tailored_resume or {}).get("cover_letter"))
            if not cover_letter_generated:
                generated = await resumes_router.generate_cover_letter_endpoint(tailored_resume_id)
                cover_letter_generated = bool(generated.content.strip())

            set_workspace_session_cookie(
                response,
                workspace_id=workspace_id,
                expires_at=_resolve_session_expiry(payload),
                request=http_request,
            )

            return IntegrationGenerateResponse(
                data=IntegrationGenerateData(
                    resume_id=tailored_resume_id,
                    cover_letter_generated=cover_letter_generated,
                    cover_letter_pdf_url=(
                        f"/api/v1/resumes/{quote(tailored_resume_id)}/cover-letter/pdf?pageSize=A4"
                    ),
                    resume_pdf_url=(
                        f"/api/v1/resumes/{quote(tailored_resume_id)}/pdf?template=swiss-single&pageSize=A4"
                    ),
                    builder_path=f"/builder?id={quote(tailored_resume_id)}",
                )
            )
        finally:
            _restore_master_resume(previous_master_id, payload_request.resume_id)
