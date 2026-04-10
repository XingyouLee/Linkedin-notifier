from __future__ import annotations

import json
import os
import time
from typing import Any

from dags import database
from dags import llm_runtime
from dags import materials_prompts
from dags import materials_rendering
from dags.resume_utils import load_resume_text


_REQUIRED_TOP_LEVEL_KEYS = {
    "extraction": [
        "candidate_profile",
        "experiences",
        "projects",
        "education",
        "constraints",
    ],
    "alignment": [
        "target_role",
        "must_cover",
        "gaps",
        "selected_evidence_ids",
        "banned_claims",
        "tone",
        "keywords",
    ],
    "resume": ["headline", "summary_lines", "sections", "skills"],
    "cover_letter": ["subject", "greeting", "paragraphs", "closing"],
}


def _parse_candidate_summary_config(raw_value) -> dict:
    if isinstance(raw_value, dict):
        return raw_value
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _map_generation_error(error: Exception) -> tuple[str, str]:
    message = str(error)
    if message.startswith("invalid_"):
        return "invalid_output", "The model returned output we could not use. Please try again."
    if "TRANSIENT_API::" in message:
        return (
            "provider_unavailable",
            "The generation provider is temporarily unavailable. Please retry shortly.",
        )
    if "FATAL_API::" in message and "401" in message:
        return (
            "provider_auth_error",
            "Generation is unavailable because the LLM provider authentication failed.",
        )
    if "FATAL_API::" in message:
        return (
            "provider_request_failed",
            "The generation request failed at the LLM provider.",
        )
    return "generation_failed", "Material generation failed unexpectedly."


def _validate_generated_document(*, stage_name: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError(f"invalid_{stage_name}_payload")

    for key in _REQUIRED_TOP_LEVEL_KEYS[stage_name]:
        if key not in payload:
            raise ValueError(f"invalid_{stage_name}_payload_missing_{key}")

    if stage_name == "resume":
        if not isinstance(payload.get("summary_lines"), list):
            raise ValueError("invalid_resume_payload_summary_lines")
        if not isinstance(payload.get("skills"), list):
            raise ValueError("invalid_resume_payload_skills")
        warnings = payload.get("warnings")
        if warnings is not None and not isinstance(warnings, list):
            raise ValueError("invalid_resume_payload_warnings")
        sections = payload.get("sections")
        if not isinstance(sections, list):
            raise ValueError("invalid_resume_payload_sections")
        for section in sections:
            if not isinstance(section, dict):
                raise ValueError("invalid_resume_payload_section")
            if not isinstance(section.get("entries") or [], list):
                raise ValueError("invalid_resume_payload_entries")

    if stage_name == "cover_letter":
        warnings = payload.get("warnings")
        if warnings is not None and not isinstance(warnings, list):
            raise ValueError("invalid_cover_letter_payload_warnings")
        paragraphs = payload.get("paragraphs")
        if not isinstance(paragraphs, list):
            raise ValueError("invalid_cover_letter_payload_paragraphs")
        for paragraph in paragraphs:
            if isinstance(paragraph, str):
                continue
            if not isinstance(paragraph, dict):
                raise ValueError("invalid_cover_letter_payload_paragraphs")
            if not isinstance(paragraph.get("text") or "", str):
                raise ValueError("invalid_cover_letter_payload_text")

    return payload


def _run_json_stage(
    *,
    endpoints: list[dict[str, str]],
    model_name: str,
    prompt: str,
    start_index: int,
) -> tuple[dict[str, Any], str]:
    last_error = None
    for attempt in range(3):
        attempt_prompt = prompt
        if attempt > 0:
            attempt_prompt += " Previous output was invalid JSON. Return strictly valid JSON only."
        try:
            return llm_runtime.request_llm_json_with_fallback(
                endpoints=endpoints,
                model_name=model_name,
                prompt=attempt_prompt,
                start_index=(start_index + attempt) % len(endpoints),
                return_metadata=True,
            )
        except RuntimeError:
            raise
        except Exception as error:
            last_error = error
            time.sleep(min(2**attempt, 4))
    raise last_error or RuntimeError("generation_stage_failed")


def generate_materials_for_profile_job(
    *,
    profile_id: int,
    job_id: str,
    access_token_id: int | None = None,
) -> dict[str, Any]:
    context = database.get_material_generation_context(profile_id=profile_id, job_id=job_id)
    if not context:
        raise ValueError("missing_profile_job_context")
    if not (context.get("description") or "").strip():
        raise ValueError("missing_job_description")

    candidate_summary = _parse_candidate_summary_config(context.get("candidate_summary_config"))
    resume_text, resume_error = load_resume_text(
        resume_path=context.get("resume_path"),
        resume_text=context.get("resume_text"),
    )
    if resume_error or not (resume_text or "").strip():
        raise ValueError("missing_resume_source")

    endpoints = llm_runtime.parse_llm_endpoints_from_env()
    if not endpoints:
        raise RuntimeError("FATAL_API::no_llm_endpoints_available")

    model_name = context.get("model_name") or os.getenv("FITTING_MODEL_NAME", "gpt-5.4")
    generation_id = database.create_material_generation(
        profile_id=profile_id,
        job_id=job_id,
        access_token_id=access_token_id,
        status="generating",
        stage="extraction",
        prompt_version=materials_prompts.PROMPT_VERSION,
    )

    try:
        extraction, used_model_name = _run_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_extraction_prompt(
                resume_text=resume_text,
                candidate_summary=candidate_summary,
            ),
            start_index=0,
        )
        extraction = _validate_generated_document(
            stage_name="extraction",
            payload=extraction,
        )
        database.update_material_generation_status(
            generation_id,
            status="generating",
            stage="alignment",
            model_name_used=used_model_name,
        )

        alignment, used_model_name = _run_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_alignment_prompt(
                job_title=context.get("title") or "",
                job_description=context.get("description") or "",
                extracted_inventory=extraction,
                candidate_summary=candidate_summary,
            ),
            start_index=1,
        )
        alignment = _validate_generated_document(
            stage_name="alignment",
            payload=alignment,
        )
        database.update_material_generation_status(
            generation_id,
            status="generating",
            stage="resume",
            model_name_used=used_model_name,
        )

        resume, used_model_name = _run_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_resume_prompt(
                job_title=context.get("title") or "",
                company=context.get("company") or "",
                extracted_inventory=extraction,
                alignment_plan=alignment,
            ),
            start_index=2,
        )
        resume = _validate_generated_document(stage_name="resume", payload=resume)
        database.update_material_generation_status(
            generation_id,
            status="generating",
            stage="cover_letter",
            model_name_used=used_model_name,
        )

        cover_letter, used_model_name = _run_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_cover_letter_prompt(
                job_title=context.get("title") or "",
                company=context.get("company") or "",
                job_description=context.get("description") or "",
                extracted_inventory=extraction,
                alignment_plan=alignment,
                resume_text=resume_text,
                generated_resume=resume,
            ),
            start_index=3,
        )
        cover_letter = _validate_generated_document(
            stage_name="cover_letter",
            payload=cover_letter,
        )

        resume_json = json.dumps(resume, ensure_ascii=False, indent=2)
        cover_letter_json = json.dumps(cover_letter, ensure_ascii=False, indent=2)
        resume_md = materials_rendering.render_resume_markdown(
            resume,
            profile_name=context.get("display_name") or context.get("profile_key") or "Candidate",
        )
        cover_letter_md = materials_rendering.render_cover_letter_markdown(
            cover_letter,
            profile_name=context.get("display_name") or context.get("profile_key") or "Candidate",
            company=context.get("company") or "Hiring Team",
        )
        resume_html = materials_rendering.render_resume_document_html(
            resume,
            profile_name=context.get("display_name") or context.get("profile_key") or "Candidate",
        )
        cover_letter_html = materials_rendering.render_cover_letter_document_html(
            cover_letter,
            profile_name=context.get("display_name") or context.get("profile_key") or "Candidate",
            company=context.get("company") or "Hiring Team",
        )
        resume_pdf = materials_rendering.render_pdf_data_url_from_html(resume_html)
        cover_letter_pdf = materials_rendering.render_pdf_data_url_from_html(cover_letter_html)
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="resume_json",
            mime_type="application/json",
            content_text=resume_json,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="cover_letter_json",
            mime_type="application/json",
            content_text=cover_letter_json,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="resume_md",
            mime_type="text/markdown",
            content_text=resume_md,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="cover_letter_md",
            mime_type="text/markdown",
            content_text=cover_letter_md,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="resume_html",
            mime_type="text/html",
            content_text=resume_html,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="cover_letter_html",
            mime_type="text/html",
            content_text=cover_letter_html,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="resume_pdf",
            mime_type="application/pdf",
            content_text=resume_pdf,
        )
        database.save_material_artifact(
            generation_id=generation_id,
            artifact_type="cover_letter_pdf",
            mime_type="application/pdf",
            content_text=cover_letter_pdf,
        )
        database.update_material_generation_status(
            generation_id,
            status="completed",
            stage="completed",
            model_name_used=used_model_name,
        )
        return {
            "generation_id": generation_id,
            "resume": resume,
            "cover_letter": cover_letter,
            "model_name": used_model_name,
        }
    except Exception as error:
        error_code, user_message = _map_generation_error(error)
        database.update_material_generation_status(
            generation_id,
            status="failed",
            stage="failed",
            error_code=error_code,
            error_message_user=user_message,
            error_message_internal=str(error),
        )
        raise
