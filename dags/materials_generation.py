from __future__ import annotations

import json
import os
import re
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

_GENERIC_RESUME_HEADLINE_TERMS = {"candidate", "applicant", "seeking"}
_GENERIC_RESUME_PHRASES = (
    "results-driven",
    "passionate",
    "dynamic",
    "proven track record",
    "learning-oriented approach",
)
_GENERIC_COVER_LETTER_PHRASES = (
    "i am passionate about",
    "results-driven",
    "dream opportunity",
    "perfect fit",
)
_EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_PATTERN = re.compile(r"\+?\d[\d\s().-]{7,}\d")
_LINKEDIN_PATTERN = re.compile(r"(https?://)?(www\.)?linkedin\.com/[^\s)]+", re.IGNORECASE)


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


def _extract_resume_contact_profile(*, resume_text: str, profile_name: str) -> dict[str, Any]:
    extracted = {
        "name": "",
        "location": "",
        "contact": {
            "email": "",
            "phone": "",
            "linkedin": "",
        },
    }

    for raw_line in resume_text.splitlines()[:30]:
        line = str(raw_line or "").strip()
        if not line:
            continue
        normalized_line = line.lstrip("- ").strip()
        lower_line = normalized_line.casefold()

        if line.startswith("# ") and not extracted["name"]:
            extracted["name"] = line[2:].strip()
            continue
        if lower_line.startswith("location:") and not extracted["location"]:
            extracted["location"] = normalized_line.split(":", 1)[1].strip()
        if lower_line.startswith("email:") and not extracted["contact"]["email"]:
            extracted["contact"]["email"] = normalized_line.split(":", 1)[1].strip()
        if lower_line.startswith("phone:") and not extracted["contact"]["phone"]:
            extracted["contact"]["phone"] = normalized_line.split(":", 1)[1].strip()
        if lower_line.startswith("linkedin:") and not extracted["contact"]["linkedin"]:
            extracted["contact"]["linkedin"] = normalized_line.split(":", 1)[1].strip()

        if not extracted["contact"]["email"]:
            email_match = _EMAIL_PATTERN.search(line)
            if email_match:
                extracted["contact"]["email"] = email_match.group(0)
        if not extracted["contact"]["phone"]:
            phone_match = _PHONE_PATTERN.search(line)
            if phone_match:
                extracted["contact"]["phone"] = phone_match.group(0).strip()
        if not extracted["contact"]["linkedin"]:
            linkedin_match = _LINKEDIN_PATTERN.search(line)
            if linkedin_match:
                extracted["contact"]["linkedin"] = linkedin_match.group(0).strip()

    extracted["name"] = extracted["name"] or str(profile_name or "").strip()
    return extracted


def _merge_candidate_profile(
    *,
    extracted_candidate_profile: dict[str, Any] | None,
    resume_text: str,
    profile_name: str,
) -> dict[str, Any]:
    merged = dict(extracted_candidate_profile) if isinstance(extracted_candidate_profile, dict) else {}
    merged_contact = dict(merged.get("contact")) if isinstance(merged.get("contact"), dict) else {}
    parsed_profile = _extract_resume_contact_profile(
        resume_text=resume_text,
        profile_name=profile_name,
    )
    parsed_contact = parsed_profile.get("contact") if isinstance(parsed_profile.get("contact"), dict) else {}

    merged["name"] = str(merged.get("name") or parsed_profile.get("name") or profile_name).strip()
    merged["location"] = str(merged.get("location") or parsed_profile.get("location") or "").strip()
    merged["contact"] = {
        "email": str(merged_contact.get("email") or parsed_contact.get("email") or "").strip(),
        "phone": str(merged_contact.get("phone") or parsed_contact.get("phone") or "").strip(),
        "linkedin": str(merged_contact.get("linkedin") or parsed_contact.get("linkedin") or "").strip(),
    }
    return merged


def _normalize_text_key(value: Any) -> str:
    return "".join(char.lower() for char in str(value or "") if char.isalnum())


def _normalized_words(value: Any) -> list[str]:
    normalized = "".join(
        char.lower() if char.isalnum() else " "
        for char in str(value or "")
    )
    return [part for part in normalized.split() if part]


def _source_date_candidates(source_item: dict[str, Any]) -> set[str]:
    candidates: set[str] = set()
    start_date = str(source_item.get("start_date") or source_item.get("start") or "").strip()
    end_date = str(source_item.get("end_date") or source_item.get("end") or "").strip()
    date_range = str(source_item.get("date_range") or source_item.get("date") or "").strip()
    if date_range:
        candidates.add(_normalize_text_key(date_range))
    if start_date or end_date:
        candidates.add(_normalize_text_key(f"{start_date} - {end_date}".strip(" -")))
    single_date = str(source_item.get("date") or "").strip()
    if single_date:
        candidates.add(_normalize_text_key(single_date))
    return {candidate for candidate in candidates if candidate}


def _match_entry_to_source(
    *,
    entry: dict[str, Any],
    source_items: list[dict[str, Any]],
    role_key: str,
    company_key: str,
) -> dict[str, Any] | None:
    header_fields = entry.get("header_fields") if isinstance(entry.get("header_fields"), dict) else {}
    entry_role = _normalize_text_key(header_fields.get("role"))
    entry_company = _normalize_text_key(header_fields.get("company"))
    entry_dates = _normalize_text_key(header_fields.get("dates"))

    matches = []
    for source_item in source_items:
        source_role = _normalize_text_key(source_item.get(role_key))
        source_company = _normalize_text_key(source_item.get(company_key))
        source_dates = _source_date_candidates(source_item)
        if source_role and source_role != entry_role:
            continue
        if source_company and source_company != entry_company:
            continue
        if source_dates and entry_dates and entry_dates not in source_dates:
            continue
        matches.append(source_item)

    if len(matches) == 1:
        return matches[0]
    return None


def _entry_header_integrity_errors(
    *,
    entry: dict[str, Any],
    source_item: dict[str, Any],
    role_key: str,
    company_key: str,
    error_prefix: str,
) -> list[str]:
    errors: list[str] = []
    header_fields = entry.get("header_fields") if isinstance(entry.get("header_fields"), dict) else {}
    entry_role = _normalize_text_key(header_fields.get("role"))
    entry_company = _normalize_text_key(header_fields.get("company"))
    entry_dates = _normalize_text_key(header_fields.get("dates"))

    source_role = _normalize_text_key(source_item.get(role_key))
    source_company = _normalize_text_key(source_item.get(company_key))
    source_dates = _source_date_candidates(source_item)

    if source_role and entry_role != source_role:
        errors.append(f"{error_prefix}_header_role_mismatch")
    if source_company and entry_company != source_company:
        errors.append(f"{error_prefix}_header_company_mismatch")
    if source_dates and entry_dates not in source_dates:
        errors.append(f"{error_prefix}_header_dates_mismatch")
    return errors


def _collect_extracted_evidence_ids(extracted_inventory: dict[str, Any]) -> set[str]:
    collected: set[str] = set()
    for section_name in ("experiences", "projects"):
        for item in extracted_inventory.get(section_name) or []:
            item_id = str(item.get("evidence_id") or "").strip()
            if item_id:
                collected.add(item_id)
            for highlight in (item.get("highlights") or item.get("bullets") or []):
                evidence_id = str((highlight or {}).get("evidence_id") or "").strip()
                if evidence_id:
                    collected.add(evidence_id)
    for item in extracted_inventory.get("education") or []:
        item_id = str(item.get("evidence_id") or "").strip()
        if item_id:
            collected.add(item_id)
    return collected


def _alignment_semantic_errors(
    *,
    alignment: dict[str, Any],
    extracted_inventory: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    known_evidence_ids = _collect_extracted_evidence_ids(extracted_inventory)
    experience_ids = {
        str(item.get("evidence_id") or "").strip()
        for item in extracted_inventory.get("experiences") or []
        if str(item.get("evidence_id") or "").strip()
    }
    project_ids = {
        str(item.get("evidence_id") or "").strip()
        for item in extracted_inventory.get("projects") or []
        if str(item.get("evidence_id") or "").strip()
    }

    for evidence_id in alignment.get("selected_evidence_ids") or []:
        if str(evidence_id).strip() not in known_evidence_ids:
            errors.append(f"selected_evidence_id_not_found:{evidence_id}")
    for experience_id in alignment.get("prioritized_experience_ids") or []:
        if str(experience_id).strip() not in experience_ids:
            errors.append(f"prioritized_experience_id_not_found:{experience_id}")
    prioritized_project_ids = [
        str(project_id).strip()
        for project_id in alignment.get("prioritized_project_ids") or []
        if str(project_id).strip()
    ]
    for project_id in prioritized_project_ids:
        if str(project_id).strip() not in project_ids:
            errors.append(f"prioritized_project_id_not_found:{project_id}")
    return errors


def _resume_semantic_errors(
    *,
    resume: dict[str, Any],
    extracted_inventory: dict[str, Any],
    alignment_plan: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    headline = str(resume.get("headline") or "").strip()
    headline_words = set(_normalized_words(headline))
    if headline_words & _GENERIC_RESUME_HEADLINE_TERMS:
        errors.append("headline_uses_applicant_wording")

    summary_lines = [
        str(line).strip()
        for line in resume.get("summary_lines") or []
        if str(line).strip()
    ]
    joined_summary = " ".join(summary_lines).lower()
    for phrase in _GENERIC_RESUME_PHRASES:
        if phrase in joined_summary:
            errors.append(f"summary_uses_generic_phrase:{phrase}")
    concrete_summary_signal = any(
        token in joined_summary
        for token in ("pipeline", "pipelines", "model", "modeling", "etl", "spark", "databricks", "airflow", "sql")
    ) or any(any(char.isdigit() for char in line) for line in summary_lines)
    if summary_lines and not concrete_summary_signal:
        errors.append("summary_lacks_concrete_supported_signal")

    selected_evidence_ids = {
        str(evidence_id).strip()
        for evidence_id in alignment_plan.get("selected_evidence_ids") or []
        if str(evidence_id).strip()
    }
    extracted_experiences = extracted_inventory.get("experiences") or []
    extracted_projects = extracted_inventory.get("projects") or []
    experience_by_id = {
        str(item.get("evidence_id") or "").strip(): item
        for item in extracted_experiences
        if str(item.get("evidence_id") or "").strip()
    }
    project_by_id = {
        str(item.get("evidence_id") or "").strip(): item
        for item in extracted_projects
        if str(item.get("evidence_id") or "").strip()
    }
    project_name_keys = {
        _normalize_text_key(item.get("name"))
        for item in extracted_projects
        if _normalize_text_key(item.get("name"))
    }
    used_experience_ids: set[str] = set()
    used_project_ids: set[str] = set()

    sections = resume.get("sections") or []
    for section in sections:
        if not isinstance(section, dict):
            continue
        title = str(section.get("title") or "").strip()
        entries = section.get("entries") or []
        if title == "Experience":
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                header_fields = entry.get("header_fields") if isinstance(entry.get("header_fields"), dict) else {}
                entry_role_key = _normalize_text_key(header_fields.get("role"))
                if entry_role_key in project_name_keys:
                    errors.append("project_entry_misplaced_in_experience_section")
                source_meta = entry.get("source_meta") if isinstance(entry.get("source_meta"), dict) else {}
                if source_meta.get("source_type") != "experience":
                    errors.append("experience_entry_missing_source_meta")
                    continue
                source_id = str(source_meta.get("source_id") or "").strip()
                matched_source = experience_by_id.get(source_id)
                if not matched_source:
                    matched_source = _match_entry_to_source(
                        entry=entry,
                        source_items=extracted_experiences,
                        role_key="title",
                        company_key="employer",
                    )
                    if matched_source:
                        source_id = str(matched_source.get("evidence_id") or "").strip()
                if not matched_source or not source_id:
                    errors.append("experience_entry_not_mapped_to_source")
                    continue
                errors.extend(
                    _entry_header_integrity_errors(
                        entry=entry,
                        source_item=matched_source,
                        role_key="title",
                        company_key="employer",
                        error_prefix="experience_entry",
                    )
                )
                if source_id in used_experience_ids:
                    errors.append(f"experience_source_split_across_multiple_entries:{source_id}")
                used_experience_ids.add(source_id)
                supported_ids = {
                    str((highlight or {}).get("evidence_id") or "").strip()
                    for highlight in (matched_source.get("highlights") or matched_source.get("bullets") or [])
                    if str((highlight or {}).get("evidence_id") or "").strip()
                }
                supported_ids.add(source_id)
                source_evidence_ids = [
                    str(evidence_id).strip()
                    for evidence_id in source_meta.get("source_evidence_ids") or []
                    if str(evidence_id).strip()
                ]
                if not source_evidence_ids:
                    errors.append(f"experience_entry_missing_source_evidence_ids:{source_id}")
                for evidence_id in source_evidence_ids:
                    if evidence_id not in supported_ids:
                        errors.append(f"experience_entry_uses_unsupported_evidence:{source_id}:{evidence_id}")
                    if (
                        selected_evidence_ids
                        and evidence_id not in selected_evidence_ids
                        and source_id not in selected_evidence_ids
                    ):
                        errors.append(f"experience_entry_uses_unapproved_evidence:{source_id}:{evidence_id}")
        if title == "Projects":
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                source_meta = entry.get("source_meta") if isinstance(entry.get("source_meta"), dict) else {}
                if source_meta.get("source_type") != "project":
                    errors.append("project_entry_missing_source_meta")
                    continue
                source_id = str(source_meta.get("source_id") or "").strip()
                matched_source = project_by_id.get(source_id)
                if not matched_source:
                    matched_source = _match_entry_to_source(
                        entry=entry,
                        source_items=extracted_projects,
                        role_key="name",
                        company_key="company",
                    )
                    if matched_source:
                        source_id = str(matched_source.get("evidence_id") or "").strip()
                if not matched_source or not source_id:
                    errors.append("project_entry_not_mapped_to_source")
                    continue
                errors.extend(
                    _entry_header_integrity_errors(
                        entry=entry,
                        source_item=matched_source,
                        role_key="name",
                        company_key="company",
                        error_prefix="project_entry",
                    )
                )
                if source_id in used_project_ids:
                    errors.append(f"project_source_split_across_multiple_entries:{source_id}")
                used_project_ids.add(source_id)
                supported_ids = {
                    str((highlight or {}).get("evidence_id") or "").strip()
                    for highlight in (matched_source.get("highlights") or matched_source.get("bullets") or [])
                    if str((highlight or {}).get("evidence_id") or "").strip()
                }
                supported_ids.add(source_id)
                source_evidence_ids = [
                    str(evidence_id).strip()
                    for evidence_id in source_meta.get("source_evidence_ids") or []
                    if str(evidence_id).strip()
                ]
                for evidence_id in source_evidence_ids:
                    if evidence_id not in supported_ids:
                        errors.append(f"project_entry_uses_unsupported_evidence:{source_id}:{evidence_id}")

    required_experience_ids = {
        str(source_id).strip()
        for source_id in alignment_plan.get("prioritized_experience_ids") or []
        if str(source_id).strip()
    }
    required_project_ids = {
        str(source_id).strip()
        for source_id in alignment_plan.get("prioritized_project_ids") or []
        if str(source_id).strip()
    }
    if required_experience_ids and not required_experience_ids.issubset(used_experience_ids):
        errors.append("missing_prioritized_experience_entry")
    if required_project_ids:
        required_project_count = 1
        if len(used_experience_ids) <= 1:
            required_project_count = min(3, len(required_project_ids))
        if len(used_project_ids & required_project_ids) < required_project_count:
            errors.append("missing_prioritized_project_entry")
    return errors


def _cover_letter_semantic_errors(
    *,
    cover_letter: dict[str, Any],
    alignment_plan: dict[str, Any],
    company: str,
) -> list[str]:
    errors: list[str] = []
    paragraphs = []
    for paragraph in cover_letter.get("paragraphs") or []:
        if isinstance(paragraph, dict):
            text = str((paragraph or {}).get("text") or "").strip()
        else:
            text = str(paragraph or "").strip()
        if text:
            paragraphs.append(text)

    if len(paragraphs) != 3:
        errors.append("cover_letter_requires_three_short_paragraphs")

    joined = "\n".join(paragraphs).lower()
    for phrase in _GENERIC_COVER_LETTER_PHRASES:
        if phrase in joined:
            errors.append(f"cover_letter_uses_generic_phrase:{phrase}")

    normalized_company = _normalize_text_key(company)
    paragraph_three = paragraphs[2] if len(paragraphs) >= 3 else ""
    if normalized_company and normalized_company not in _normalize_text_key(paragraph_three):
        errors.append("cover_letter_motivation_missing_company_reference")

    motivation_terms = set()
    for theme in alignment_plan.get("motivation_themes") or []:
        for word in _normalized_words(theme):
            if len(word) >= 5 and word not in {"opportunity", "learning", "developing", "environment", "impact"}:
                motivation_terms.add(word)
    paragraph_three_words = set(_normalized_words(paragraph_three))
    if motivation_terms and not (paragraph_three_words & motivation_terms):
        errors.append("cover_letter_motivation_lacks_jd_specific_hook")
    return errors


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
            for entry in section.get("entries") or []:
                if not isinstance(entry, dict):
                    raise ValueError("invalid_resume_payload_entry")
                if not isinstance(entry.get("header_fields"), dict):
                    raise ValueError("invalid_resume_payload_header_fields")
                if not isinstance(entry.get("bullets") or [], list):
                    raise ValueError("invalid_resume_payload_bullets")
                source_meta = entry.get("source_meta")
                if source_meta is not None and not isinstance(source_meta, dict):
                    raise ValueError("invalid_resume_payload_source_meta")

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


def _humanize_validation_error(error: str) -> str:
    if error == "headline_uses_applicant_wording":
        return "Rewrite the headline as a professional profile headline. Do not use words like candidate, applicant, seeking, or looking for."
    if error == "summary_lacks_concrete_supported_signal":
        return "Make the summary concrete. At least one summary line must mention specific supported evidence such as pipeline scale, SQL improvement, data modeling work, or ETL ownership."
    if error == "experience_entry_missing_source_meta":
        return "Each Experience entry must include source_meta with source_type='experience', the real source_id from extracted_inventory.experiences, and supporting source_evidence_ids."
    if error == "project_entry_missing_source_meta":
        return "Each Projects entry must include source_meta with source_type='project', the real source_id from extracted_inventory.projects, and supporting source_evidence_ids."
    if error == "experience_entry_not_mapped_to_source":
        return "Every Experience entry must map cleanly back to one extracted source experience. Keep the original role, company, and dates aligned to the source."
    if error == "project_entry_not_mapped_to_source":
        return "Every Projects entry must map cleanly back to one extracted project. Keep the original project name and dates aligned to the source."
    if error == "experience_entry_header_role_mismatch":
        return "Do not rewrite the role title for an Experience entry. Keep the original source role title."
    if error == "experience_entry_header_company_mismatch":
        return "Do not rewrite the company name for an Experience entry. Keep the original source company."
    if error == "experience_entry_header_dates_mismatch":
        return "Do not rewrite the date range for an Experience entry. Keep the original source dates."
    if error == "project_entry_header_role_mismatch":
        return "Do not rewrite the project title for a Projects entry. Keep the original source project name."
    if error == "project_entry_header_company_mismatch":
        return "Do not rewrite the company or organization for a Projects entry unless the source includes it."
    if error == "project_entry_header_dates_mismatch":
        return "Do not rewrite the date range for a Projects entry. Keep the original source dates."
    if error == "project_entry_misplaced_in_experience_section":
        return "Do not place projects in the Experience section."
    if error == "missing_prioritized_experience_entry":
        return "Include the prioritized source experience entry required by the alignment plan."
    if error == "missing_prioritized_project_entry":
        return "Include the strongest prioritized project entries from the alignment plan. For early-career profiles with limited work history, keep 1-2 relevant projects instead of dropping the Projects section."
    if error == "cover_letter_requires_three_short_paragraphs":
        return "Return exactly 3 short cover-letter paragraphs."
    if error == "cover_letter_motivation_missing_company_reference":
        return "Paragraph 3 must explicitly mention the exact company name and explain why this company is a fit."
    if error == "cover_letter_motivation_lacks_jd_specific_hook":
        return "Paragraph 3 must include a concrete JD- or company-specific hook such as the domain, team, technology, product, mission, or work context."
    if error.startswith("summary_uses_generic_phrase:"):
        return f"Avoid generic summary filler phrase: {error.split(':', 1)[1]}."
    if error.startswith("cover_letter_uses_generic_phrase:"):
        return f"Avoid generic cover-letter phrase: {error.split(':', 1)[1]}."
    if error.startswith("experience_source_split_across_multiple_entries:"):
        return "Do not split one source experience into multiple Experience entries."
    if error.startswith("project_source_split_across_multiple_entries:"):
        return "Do not split one source project into multiple Projects entries."
    if error.startswith("experience_entry_missing_source_evidence_ids:"):
        return "Each Experience entry must list the supporting source_evidence_ids from the extracted source experience."
    if error.startswith("experience_entry_uses_unsupported_evidence:"):
        return "Experience entry source_evidence_ids must come from the matched source experience."
    if error.startswith("experience_entry_uses_unapproved_evidence:"):
        return "Experience entry source_evidence_ids must stay within the evidence approved by the alignment plan."
    if error.startswith("project_entry_uses_unsupported_evidence:"):
        return "Projects entry source_evidence_ids must come from the matched source project."
    if error.startswith("selected_evidence_id_not_found:"):
        return "Alignment selected_evidence_ids must reference evidence ids that exist in the extracted inventory."
    if error.startswith("prioritized_experience_id_not_found:"):
        return "Alignment prioritized_experience_ids must reference the exact extracted experience evidence_id values verbatim from extracted_inventory. Do not invent shorthand ids."
    if error.startswith("prioritized_project_id_not_found:"):
        return "Alignment prioritized_project_ids must reference the exact extracted project evidence_id values verbatim from extracted_inventory. Do not invent shorthand ids."
    if error.startswith("invalid_") and "_payload_missing_" in error:
        missing_key = error.rsplit("_", 1)[-1]
        return f"Return valid JSON with the required top-level key '{missing_key}'."
    if error.startswith("invalid_"):
        return "Return valid JSON that matches the required schema exactly."
    return error.replace("_", " ")


def _run_validated_json_stage(
    *,
    endpoints: list[dict[str, str]],
    model_name: str,
    prompt: str,
    stage_name: str,
    start_index: int,
    semantic_validator,
) -> tuple[dict[str, Any], str]:
    feedback_errors: list[str] = []
    last_error = None
    for semantic_attempt in range(3):
        attempt_prompt = prompt
        if feedback_errors:
            feedback_text = "\n".join(
                f"- {_humanize_validation_error(error)}"
                for error in feedback_errors
            )
            attempt_prompt += (
                "\n\nPrevious output failed semantic validation. Regenerate from scratch and fix all issues below.\n"
                f"{feedback_text}\n"
                "Return valid JSON only."
            )
        payload, used_model_name = _run_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=attempt_prompt,
            start_index=start_index + semantic_attempt,
        )
        try:
            payload = _validate_generated_document(stage_name=stage_name, payload=payload)
        except ValueError as error:
            feedback_errors = [str(error)]
            last_error = error
            continue
        feedback_errors = semantic_validator(payload)
        if not feedback_errors:
            return payload, used_model_name
        last_error = ValueError(
            f"invalid_{stage_name}_content: " + " | ".join(feedback_errors)
        )
    raise last_error or RuntimeError(f"{stage_name}_validation_failed")


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
    generation_id: int | None = None,
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

    job_title = context.get("title") or ""
    company_name = context.get("company") or ""
    job_description = context.get("description") or ""
    profile_name = context.get("display_name") or context.get("profile_key") or "Candidate"
    model_name = context.get("model_name") or os.getenv("FITTING_MODEL_NAME", "gpt-5.4")
    if generation_id is None:
        generation_id = database.create_material_generation(
            profile_id=profile_id,
            job_id=job_id,
            access_token_id=access_token_id,
            status="generating",
            stage="extraction",
            prompt_version=materials_prompts.PROMPT_VERSION,
        )
    else:
        database.update_material_generation_status(
            generation_id,
            status="generating",
            stage="extraction",
            model_name_used=model_name,
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

        alignment, used_model_name = _run_validated_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_alignment_prompt(
                job_title=job_title,
                job_description=job_description,
                extracted_inventory=extraction,
                candidate_summary=candidate_summary,
            ),
            stage_name="alignment",
            start_index=1,
            semantic_validator=lambda payload: _alignment_semantic_errors(
                alignment=payload,
                extracted_inventory=extraction,
            ),
        )
        database.update_material_generation_status(
            generation_id,
            status="generating",
            stage="resume",
            model_name_used=used_model_name,
        )

        resume, used_model_name = _run_validated_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_resume_prompt(
                job_title=job_title,
                company=company_name,
                job_description=job_description,
                extracted_inventory=extraction,
                alignment_plan=alignment,
            ),
            stage_name="resume",
            start_index=2,
            semantic_validator=lambda payload: _resume_semantic_errors(
                resume=payload,
                extracted_inventory=extraction,
                alignment_plan=alignment,
            ),
        )
        resume = materials_rendering.stabilize_resume_payload(resume)
        database.update_material_generation_status(
            generation_id,
            status="generating",
            stage="cover_letter",
            model_name_used=used_model_name,
        )

        cover_letter, used_model_name = _run_validated_json_stage(
            endpoints=endpoints,
            model_name=model_name,
            prompt=materials_prompts.build_cover_letter_prompt(
                job_title=job_title,
                company=company_name,
                job_description=job_description,
                extracted_inventory=extraction,
                alignment_plan=alignment,
                resume_text=resume_text,
                generated_resume=resume,
            ),
            stage_name="cover_letter",
            start_index=3,
            semantic_validator=lambda payload: _cover_letter_semantic_errors(
                cover_letter=payload,
                alignment_plan=alignment,
                company=context.get("company") or "",
            ),
        )

        merged_candidate_profile = _merge_candidate_profile(
            extracted_candidate_profile=extraction.get("candidate_profile"),
            resume_text=resume_text,
            profile_name=profile_name,
        )
        resume_json = json.dumps(resume, ensure_ascii=False, indent=2)
        cover_letter_json = json.dumps(cover_letter, ensure_ascii=False, indent=2)
        resume_md = materials_rendering.render_resume_markdown(
            resume,
            profile_name=profile_name,
            candidate_profile=merged_candidate_profile,
        )
        cover_letter_md = materials_rendering.render_cover_letter_markdown(
            cover_letter,
            profile_name=profile_name,
            company=company_name or "Hiring Team",
        )
        resume_html = materials_rendering.render_resume_document_html(
            resume,
            profile_name=profile_name,
            candidate_profile=merged_candidate_profile,
        )
        cover_letter_html = materials_rendering.render_cover_letter_document_html(
            cover_letter,
            profile_name=profile_name,
            company=company_name or "Hiring Team",
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
