from __future__ import annotations

import base64
import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from dags import database
from dags import materials_generation
from dags import materials_links
from dags import materials_rendering

app = FastAPI(title="LinkedIn Notifier Materials")
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
ARTIFACT_LABELS = {
    "resume_json": "Resume JSON",
    "cover_letter_json": "Cover Letter JSON",
    "resume_md": "Resume Markdown",
    "cover_letter_md": "Cover Letter Markdown",
    "resume_html": "Resume HTML",
    "cover_letter_html": "Cover Letter HTML",
    "resume_pdf": "Resume PDF",
    "cover_letter_pdf": "Cover Letter PDF",
}
STAGE_LABELS = {
    "extraction": "Extracting resume details",
    "alignment": "Matching your background to the job",
    "resume": "Drafting the resume",
    "cover_letter": "Drafting the cover letter",
    "rendering": "Rendering final files",
    "completed": "Completed",
    "failed": "Failed",
}
ERROR_MESSAGES = {
    "provider_unavailable": "The generation provider is temporarily unavailable. Please try again shortly.",
    "provider_auth_error": "Generation is temporarily unavailable because the provider rejected authentication.",
    "provider_request_failed": "The generation provider could not finish this request.",
    "missing_job_description": "This job posting does not have a saved description yet, so materials cannot be generated.",
    "missing_resume_source": "No resume source is configured for this profile, so materials cannot be generated.",
    "invalid_output": "The model returned output we could not use. Please try again.",
    "pdf_render_failed": "We generated the text versions, but PDF export failed.",
    "generation_failed": "Material generation failed unexpectedly.",
}


def _get_valid_token_record(token: str) -> dict:
    payload = materials_links.verify_materials_token(token)
    token_record = database.get_material_access_token(token)
    if not token_record:
        raise HTTPException(status_code=404, detail="Materials link not found")
    if token_record.get("revoked_at"):
        raise HTTPException(status_code=410, detail="Materials link has been revoked")
    database.touch_material_access_token(int(token_record["id"]))
    return {
        "payload": payload,
        "record": token_record,
    }


def _render_materials_page(
    request: Request,
    *,
    page_state: str,
    error_title: str | None = None,
    error_message: str | None = None,
    context: dict | None = None,
    generation: dict | None = None,
    token: str | None = None,
    artifact_links: list[dict] | None = None,
    generation_summary: dict | None = None,
    resume_preview: str = "",
    cover_letter_preview: str = "",
):
    return TEMPLATES.TemplateResponse(
        request,
        "materials.html",
        {
            "page_state": page_state,
            "error_title": error_title,
            "error_message": error_message,
            "context": context,
            "generation": generation,
            "token": token,
            "artifact_links": artifact_links or [],
            "generation_summary": generation_summary or {},
            "resume_preview": resume_preview,
            "cover_letter_preview": cover_letter_preview,
        },
    )


def _build_artifact_links(token: str, artifact_map: dict[str, dict]) -> list[dict]:
    artifact_links = []
    for artifact_type, label in ARTIFACT_LABELS.items():
        artifact = artifact_map.get(artifact_type)
        available = bool(artifact and artifact.get("content_text"))
        artifact_links.append(
            {
                "artifact_type": artifact_type,
                "label": label,
                "available": available,
                "url": f"/materials/download/{artifact_type}?token={token}" if available else None,
            }
        )
    return artifact_links


def _build_generation_summary(generation: dict | None, artifact_map: dict[str, dict]) -> dict:
    if not generation:
        return {
            "status_label": "Preparing generation",
            "stage_label": "Starting up",
            "message": "We are starting to generate tailored materials for this job.",
            "message_tone": "info",
            "has_partial_artifacts": False,
        }

    status = generation.get("status") or "pending"
    stage = generation.get("stage") or "pending"
    available_count = sum(1 for artifact in artifact_map.values() if artifact.get("content_text"))
    has_partial_artifacts = 0 < available_count < len(ARTIFACT_LABELS)
    error_code = generation.get("error_code")
    error_message = generation.get("error_message_user") or ERROR_MESSAGES.get(error_code)

    if status == "completed":
        if has_partial_artifacts:
            message = "Some files are ready, but a few outputs are still unavailable."
            if "resume_pdf" not in artifact_map or "cover_letter_pdf" not in artifact_map:
                message = ERROR_MESSAGES["pdf_render_failed"]
            tone = "warning"
        else:
            message = "Your tailored application materials are ready to review and download."
            tone = "success"
    elif status == "failed":
        message = error_message or ERROR_MESSAGES["generation_failed"]
        tone = "error"
    else:
        message = "Generation is in progress. Reload this page in a moment to check for completed files."
        tone = "info"

    return {
        "status_label": status.replace("_", " ").title(),
        "stage_label": STAGE_LABELS.get(stage, stage.replace("_", " ").title()),
        "message": message,
        "message_tone": tone,
        "has_partial_artifacts": has_partial_artifacts,
    }


def _build_download_response(artifact: dict) -> Response:
    artifact_type = str(artifact.get("artifact_type") or "artifact")
    mime_type = str(artifact.get("mime_type") or "text/plain")
    content_text = artifact.get("content_text") or ""

    if artifact_type.endswith("_pdf"):
        prefix, _, encoded = str(content_text).partition(",")
        if not prefix.startswith("data:application/pdf;base64") or not encoded:
            raise HTTPException(status_code=500, detail="Stored PDF artifact is invalid")
        try:
            content = base64.b64decode(encoded)
        except Exception as error:
            raise HTTPException(status_code=500, detail="Stored PDF artifact could not be decoded") from error
        return Response(
            content=content,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{artifact_type}.pdf"'},
        )

    extension = ".txt"
    if artifact_type.endswith("_md"):
        extension = ".md"
    elif artifact_type.endswith("_html"):
        extension = ".html"
    elif artifact_type.endswith("_json"):
        extension = ".json"

    return Response(
        content=str(content_text),
        media_type=mime_type,
        headers={"Content-Disposition": f'attachment; filename="{artifact_type}{extension}"'},
    )


@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"


@app.get("/materials", response_class=HTMLResponse)
def materials_page(request: Request, token: str = Query(...)):
    try:
        token_data = _get_valid_token_record(token)
    except ValueError as error:
        return _render_materials_page(
            request,
            page_state="invalid_token",
            error_title="Invalid link",
            error_message=str(error),
        )
    except HTTPException as error:
        detail = str(error.detail)
        if error.status_code == 410:
            return _render_materials_page(
                request,
                page_state="expired_token",
                error_title="Link expired",
                error_message=detail,
            )
        if error.status_code == 404:
            return _render_materials_page(
                request,
                page_state="invalid_token",
                error_title="Link unavailable",
                error_message=detail,
            )
        raise

    payload = token_data["payload"]
    context = database.get_material_generation_context(
        profile_id=int(payload["profile_id"]),
        job_id=str(payload["job_id"]),
    )
    if not context:
        return _render_materials_page(
            request,
            page_state="missing_context",
            error_title="Job details unavailable",
            error_message="We could not find the saved profile and job data for this link.",
        )

    generation = database.get_latest_material_generation(
        profile_id=int(payload["profile_id"]),
        job_id=str(payload["job_id"]),
    )
    if generation is None or generation.get("status") == "failed":
        try:
            materials_generation.generate_materials_for_profile_job(
                profile_id=int(payload["profile_id"]),
                job_id=str(payload["job_id"]),
                access_token_id=int(token_data["record"]["id"]),
            )
        except Exception:
            generation = database.get_latest_material_generation(
                profile_id=int(payload["profile_id"]),
                job_id=str(payload["job_id"]),
            )
        else:
            generation = database.get_latest_material_generation(
                profile_id=int(payload["profile_id"]),
                job_id=str(payload["job_id"]),
            )

    artifacts = []
    if generation:
        artifacts = database.get_material_artifacts(int(generation["id"]))

    artifact_map = {artifact["artifact_type"]: artifact for artifact in artifacts}
    generation_summary = _build_generation_summary(generation, artifact_map)
    page_state = generation.get("status") if generation else "generating"
    if generation_summary.get("has_partial_artifacts"):
        page_state = "partial_success"
    if generation and generation.get("error_code") in {"missing_job_description", "missing_resume_source"}:
        page_state = "missing_data"

    return _render_materials_page(
        request,
        page_state=page_state,
        context=context,
        generation=generation,
        token=token,
        artifact_links=_build_artifact_links(token, artifact_map),
        generation_summary=generation_summary,
        resume_preview=(
            artifact_map.get("resume_html", {}).get("content_text")
            or materials_rendering.render_error_html(
                "Resume preview unavailable",
                "No resume preview available yet.",
            )
        ),
        cover_letter_preview=(
            artifact_map.get("cover_letter_html", {}).get("content_text")
            or materials_rendering.render_error_html(
                "Cover letter preview unavailable",
                "No cover letter preview available yet.",
            )
        ),
    )


@app.get("/materials/download/{artifact_type}")
def download_artifact(artifact_type: str, token: str = Query(...)):
    try:
        token_data = _get_valid_token_record(token)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=f"Invalid materials link: {error}") from error

    payload = token_data["payload"]
    generation = database.get_latest_material_generation(
        profile_id=int(payload["profile_id"]),
        job_id=str(payload["job_id"]),
    )
    if not generation:
        raise HTTPException(status_code=404, detail="No generated materials are available for this link yet")

    artifacts = database.get_material_artifacts(int(generation["id"]))
    artifact = next((item for item in artifacts if item.get("artifact_type") == artifact_type), None)
    if not artifact or not artifact.get("content_text"):
        detail = f"{ARTIFACT_LABELS.get(artifact_type, 'Requested artifact')} is not available for this generation"
        if artifact_type.endswith("_pdf"):
            detail += ". PDF export may have failed even if the text versions were generated successfully"
        raise HTTPException(status_code=404, detail=detail)
    return _build_download_response(artifact)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
