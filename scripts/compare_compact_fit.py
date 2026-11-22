#!/usr/bin/env python3
"""Re-score the latest 10 stored JDs one at a time with a compact output prompt.

Reads the remote jobs database with SELECT only. Does not scan LinkedIn,
start Airflow, write database rows, or send notifications.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dags.runtime_utils import load_env
from scripts.compare_batch_fit import (
    MODEL_NAME,
    PROFILE_KEY,
    _eval_view,
    _serialize_value,
    _stored_match,
    grok_endpoint,
    load_feedback_examples,
    load_scored_jobs,
    request_batch,
)
from scripts.compare_batch_fit import fitting_notifier

COUNT = 10
OUTPUT_DIR = "artifacts/compact-fit-comparison"
COMPACT_PROMPT = """Return valid JSON only. No markdown, comments, trailing commas, or text outside JSON.

You are a strict job-fit evaluator for hiring in the Netherlands. Prioritize reducing false positives. Be conservative and evidence-driven. Uncertainty, vague descriptions, and superficial keyword overlap must reduce confidence and score.


CANDIDATE BASELINE


- Treat the supplied Candidate Summary as the canonical baseline for experience, seniority, role direction, skills, gaps, and languages.
- Use the resume as supporting evidence only. Never inflate experience or seniority beyond that baseline.
- Internships, thesis work, coursework, student jobs, academic work, side projects, and volunteering do not count as additional full-time professional experience.
- Do not reproduce the Candidate Summary in the output.


READING THE JOB


Read the entire title and job description, including snippets, metadata, and requirement bullets. Distinguish mandatory requirements from preferences. Never invent favorable requirements or responsibilities when evidence is missing.


EXPERIENCE AND SENIORITY


- Set required_years only when the job explicitly states a numeric experience requirement, including written-out numbers.
- Recognize English and Dutch expressions such as "3+ years", "3-5 years", "minimum 3 years", "3 jaar", "minimaal drie jaar", and "3 tot 5 jaar".
- Use the minimum stated number for ranges.
- Otherwise return null. Phrases such as "ruime ervaring", "enkele jaren", "proven experience", and "extensive experience" do not specify a number.
- Ignore numbers describing company age, founding dates, contracts, project timelines, or working hours.
- Extract seniority independently from explicit title and responsibility signals. Senior, Lead, Staff, Principal, Architect, Manager, and Director are meaningful seniority signals, but must never be converted into required_years.
- Compare requirements with the Candidate Summary baseline. Tool overlap does not erase an experience or seniority mismatch.
- For a junior candidate with approximately one year of experience: 2+ years warrants caution; 3+ years with independent ownership rarely supports Moderate Fit; 4+ years usually limits the result to Weak Fit; 5+ years usually means Not Recommended.
- Senior scope is a substantial mismatch for a junior candidate even without stated years. Leadership, architecture, mentoring, and platform ownership increase the mismatch.
- Set experience_blocker=true for a material experience, seniority, or ownership mismatch that prevents a credible interview case. A small, manageable gap alone is insufficient.
- exp_requirement must briefly describe the actual experience or seniority evidence. When years are unstated, say so. Use one plain-English line, at most 15 words.


ROLE AND SKILL FIT


For this data-engineering profile:
- Give credit for Python, SQL, Spark/PySpark, Databricks, Azure, Airflow, ETL/ELT, data modeling, ingestion, orchestration, reliability, Docker, and CI/CD.
- Favor junior data engineering, analytics engineering, ETL/ELT, SQL/Python transformation roles, and relevant IT/data traineeships.
- Python backend roles are credible only when data workflows are central and scope matches the candidate.
- BI/analyst roles qualify for Moderate Fit or higher only when SQL/Python transformations, warehouse modeling, semantic layers, or ETL/ELT are substantial responsibilities.
- Dashboarding, stakeholder reporting, marketing/eCommerce insights, generic software development, research-heavy ML, and senior platform or consulting ownership are weak matches by default.
- Generic titles alone do not establish role fit.
- Missing mandatory skills must materially reduce the score. Do not treat preferences as mandatory.
- List only missing critical skills, at most three short skill names. Return [] when none are identified.


LANGUAGE


- Set dutch_required=true only when Dutch proficiency is explicitly mandatory.
- Set language_blocker=true when that mandatory proficiency exceeds the candidate's documented level. Cap the score at 40.
- Dutch being preferred is a risk, not an automatic blocker.
- A Dutch-language JD alone does not imply mandatory Dutch proficiency.


SCORING


- Strong Fit, 70–100: direct role alignment, clearly junior or limited scope, credible experience alignment, and no major blockers.
- Moderate Fit, 50–69: meaningful alignment with manageable gaps and a credible recruiter screening case.
- Weak Fit, 25–49: substantial gaps, weak role evidence, vague requirements, or missing must-haves.
- Not Recommended, 0–24: severe mismatch, strong blockers, or fundamentally unsuitable role direction.


Before assigning Strong Fit or Moderate Fit, recheck experience, seniority, ownership, language, role evidence, and mandatory skills. Downgrade if material risks remain. Strong Fit should be rare for this profile.


OUTPUT CONTRACT


Return exactly these fields and nested keys:


{
  "fit_score": 0,
  "decision": "Not Recommended",
  "exp_requirement": "...",
  "language_check": {
    "dutch_required": false,
    "language_blocker": false
  },
  "experience_check": {
    "required_years": null,
    "seniority_required": "unknown",
    "experience_blocker": false
  },
  "skills_match": {
    "missing_critical_skills": []
  },
  "summary": "..."
}


The values above illustrate the structure only. Replace them with the actual assessment.


- fit_score: integer from 0 to 100.
- decision: exactly Strong Fit, Moderate Fit, Weak Fit, or Not Recommended, consistent with the score.
- required_years: number or null, following the extraction rules.
- seniority_required: entry, junior, mid, senior, lead, staff, principal, architect, manager, director, or unknown.
- summary: one plain-English sentence, at most 35 words, explaining the decisive match evidence and main risks. Explicitly mention insufficient JD evidence when it affects the decision.
- Do not output candidate_summary, candidate_years, candidate_seniority, gap_years, severity, reason, impact, strong_matches, partial_matches, risk_factors, or additional keys.
- Keep output compact. Explain each issue once; do not repeat exp_requirement in summary.


Candidate Summary: <<<{{candidate_summary}}>>>
Job Title: <<<{{job_title}}>>>
Job Description: <<<{{job_description}}>>>
Candidate Resume: <<<{{candidate_resume}}>>>
{{user_feedback_examples}}
"""


def build_prompt(
    job: dict[str, Any],
    profile: dict[str, Any],
    candidate_summary: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
) -> str:
    feedback = fitting_notifier._render_feedback_examples(feedback_examples).strip()
    return (
        COMPACT_PROMPT.replace(
            "{{candidate_summary}}", json.dumps(candidate_summary, ensure_ascii=False)
        )
        .replace("{{job_title}}", str(job.get("title") or "").strip() or "not provided")
        .replace("{{job_description}}", str(job.get("description") or "").strip())
        .replace("{{candidate_resume}}", str(profile["resume_text"]).strip())
        .replace("{{user_feedback_examples}}", feedback)
    )


def _years(match: dict[str, Any]) -> Any:
    experience = match.get("experience_check")
    if not isinstance(experience, dict):
        return None
    return experience.get("required_years")


def _flag(match: dict[str, Any], section: str, key: str) -> Any:
    payload = match.get(section)
    if not isinstance(payload, dict):
        return None
    return payload.get(key)


def _skills(match: dict[str, Any]) -> list[str]:
    skills = match.get("skills_match")
    if not isinstance(skills, dict):
        return []
    missing = skills.get("missing_critical_skills")
    if not isinstance(missing, list):
        return []
    return [str(item) for item in missing]


def _word_count(text: str) -> int:
    return len([part for part in str(text or "").split() if part])


def _json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    think = re.search(r"</think>\s*(.*)\Z", stripped, flags=re.DOTALL)
    if think and think.group(1).strip():
        stripped = think.group(1).strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("response_missing_json_object")
    parsed = json.loads(stripped[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("response_json_not_object")
    return parsed


def score_job(
    job: dict[str, Any],
    prompt: str,
    endpoint: dict[str, str],
    candidate_summary: dict[str, Any],
) -> dict[str, Any]:
    stored = _stored_match(job)
    stored_chars = len(json.dumps(stored, ensure_ascii=False))
    base = {
        "job_id": str(job["id"]),
        "title": job.get("title") or "",
        "company": job.get("company") or "",
        "stored_fit_score": job.get("fit_score"),
        "stored_decision": job.get("fit_decision") or "",
        "stored_chars": stored_chars,
        "stored_summary_words": _word_count(_eval_view(stored).get("summary") or ""),
        "prompt_chars": len(prompt),
    }
    last_error = None
    for attempt in range(2):
        try:
            response = request_batch(prompt, endpoint)
            parsed = _json_object(response["output_text"])
            fitting_notifier._validate_llm_match_response(parsed)
            raw_score = fitting_notifier._coerce_number(parsed.get("fit_score"))
            raw_decision = fitting_notifier._normalize_match_decision(parsed.get("decision"))
            capped = fitting_notifier._apply_fit_caps(
                parsed,
                job_title=job.get("title") or "",
                jd_text=job.get("description") or "",
                candidate_summary=candidate_summary,
            )
            compact_score = capped.get("fit_score")
            compact_decision = capped.get("decision") or ""
            stored_score = job.get("fit_score")
            return {
                **base,
                "status": "ok",
                "attempts": attempt + 1,
                "latency_seconds": response["latency_seconds"],
                "response_model": response.get("response_model"),
                "usage": response.get("usage"),
                "compact_chars": len(response["output_text"]),
                "raw_fit_score": None if raw_score is None else int(round(raw_score)),
                "raw_decision": raw_decision,
                "compact_fit_score": compact_score,
                "compact_decision": compact_decision,
                "score_delta": None
                if stored_score is None or compact_score is None
                else int(compact_score) - int(stored_score),
                "decision_flip": (job.get("fit_decision") or "") != compact_decision,
                "stored_required_years": _years(stored),
                "compact_required_years": _years(parsed),
                "stored_seniority": _flag(stored, "experience_check", "seniority_required"),
                "compact_seniority": _flag(parsed, "experience_check", "seniority_required"),
                "stored_experience_blocker": _flag(stored, "experience_check", "experience_blocker"),
                "compact_experience_blocker": _flag(parsed, "experience_check", "experience_blocker"),
                "stored_dutch_required": _flag(stored, "language_check", "dutch_required"),
                "compact_dutch_required": _flag(parsed, "language_check", "dutch_required"),
                "stored_language_blocker": _flag(stored, "language_check", "language_blocker"),
                "compact_language_blocker": _flag(parsed, "language_check", "language_blocker"),
                "stored_missing": _skills(stored),
                "compact_missing": _skills(parsed),
                "stored_summary": _eval_view(stored).get("summary") or "",
                "compact_summary": parsed.get("summary") or "",
                "compact_summary_words": _word_count(parsed.get("summary") or ""),
                "compact_exp_words": _word_count(parsed.get("exp_requirement") or ""),
                "error": None,
            }
        except Exception as error:
            last_error = f"{type(error).__name__}: {error}"
            print(f"{job['id']} attempt {attempt + 1} failed: {last_error}", flush=True)
    return {**base, "status": "error", "error": last_error}


def _cell(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def write_artifacts(payload: dict[str, Any]) -> dict[str, str]:
    directory = Path(OUTPUT_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = directory / f"compact_fit_{stamp}"
    json_path = base.with_suffix(".json")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "job_id",
        "title",
        "status",
        "stored_fit_score",
        "stored_decision",
        "raw_fit_score",
        "raw_decision",
        "compact_fit_score",
        "compact_decision",
        "score_delta",
        "decision_flip",
        "stored_chars",
        "compact_chars",
        "latency_seconds",
        "compact_summary_words",
        "stored_required_years",
        "compact_required_years",
        "stored_language_blocker",
        "compact_language_blocker",
        "error",
    ]
    csv_path = base.with_suffix(".csv")
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in payload["jobs"]:
            writer.writerow({key: row.get(key) for key in fieldnames})

    ok_rows = [row for row in payload["jobs"] if row.get("status") == "ok"]
    deltas = [abs(row["score_delta"]) for row in ok_rows if row.get("score_delta") is not None]
    mae = round(sum(deltas) / len(deltas), 2) if deltas else None
    flips = sum(1 for row in ok_rows if row.get("decision_flip"))
    stored_chars = sum(row.get("stored_chars") or 0 for row in ok_rows)
    compact_chars = sum(row.get("compact_chars") or 0 for row in ok_rows)
    markdown_path = base.with_suffix(".md")
    with markdown_path.open("w", encoding="utf-8") as handle:
        handle.write("# Compact prompt vs stored scores\n\n")
        handle.write(
            "Same 10 most recently scored jobs, one JD per call, compact output prompt. "
            "Scores below are after production fit caps. Deltas mix shorter output with changed scoring rules.\n\n"
        )
        handle.write(f"- Profile: `{payload['profile']}`\n")
        handle.write(f"- Model: `{payload['model']}`\n")
        handle.write(f"- OK: {len(ok_rows)}/{payload['job_count']}\n")
        handle.write(f"- MAE: {mae}\n")
        handle.write(f"- Decision flips: {flips}\n")
        handle.write(f"- Stored output chars: {stored_chars}\n")
        handle.write(f"- Compact output chars: {compact_chars}\n\n")
        handle.write("| job | stored | compact | delta | flip | chars | words |\n")
        handle.write("| --- | --- | --- | --- | --- | --- | --- |\n")
        for row in payload["jobs"]:
            handle.write(
                f"| {_cell(row.get('title'))} | {row.get('stored_fit_score')} {row.get('stored_decision')} | "
                f"{row.get('compact_fit_score')} {row.get('compact_decision')} | {row.get('score_delta')} | "
                f"{row.get('decision_flip')} | {row.get('stored_chars')} -> {row.get('compact_chars')} | "
                f"{row.get('compact_summary_words')} |\n"
            )
        handle.write("\n")
        for row in payload["jobs"]:
            handle.write(f"## {_cell(row.get('title'))} `{row['job_id']}`\n\n")
            if row.get("status") != "ok":
                handle.write(f"Error: `{row.get('error')}`\n\n")
                continue
            handle.write(f"- stored summary: {_cell(row.get('stored_summary'))}\n")
            handle.write(f"- compact summary: {_cell(row.get('compact_summary'))}\n")
            handle.write(
                f"- years: stored `{row.get('stored_required_years')}` / compact `{row.get('compact_required_years')}`\n"
            )
            handle.write(
                f"- seniority: stored `{row.get('stored_seniority')}` / compact `{row.get('compact_seniority')}`\n"
            )
            handle.write(
                f"- experience blocker: stored `{row.get('stored_experience_blocker')}` / compact `{row.get('compact_experience_blocker')}`\n"
            )
            handle.write(
                f"- dutch required: stored `{row.get('stored_dutch_required')}` / compact `{row.get('compact_dutch_required')}`\n"
            )
            handle.write(
                f"- language blocker: stored `{row.get('stored_language_blocker')}` / compact `{row.get('compact_language_blocker')}`\n"
            )
            handle.write(
                "- stored missing: "
                + _cell("; ".join(row.get("stored_missing") or []))
                + "\n"
            )
            handle.write(
                "- compact missing: "
                + _cell("; ".join(row.get("compact_missing") or []))
                + "\n\n"
            )
    return {"json": str(json_path), "csv": str(csv_path), "markdown": str(markdown_path)}


def main() -> int:
    load_env(override_if_missing=True)
    database_url = os.getenv("JOBS_DB_URL", "").strip()
    if not database_url:
        raise SystemExit("JOBS_DB_URL is required")
    profile, jobs = load_scored_jobs(database_url, PROFILE_KEY, COUNT)
    feedback_examples = load_feedback_examples(database_url, profile["id"])
    candidate_summary = fitting_notifier._load_candidate_summary_config(
        profile["candidate_summary_config"]
    )
    endpoint = grok_endpoint()
    print(f"Loaded {len(jobs)} scored JDs for {PROFILE_KEY}", flush=True)
    rows = []
    for index, job in enumerate(jobs, start=1):
        print(f"Scoring {index}/{len(jobs)} {job['id']}", flush=True)
        prompt = build_prompt(job, profile, candidate_summary, feedback_examples)
        row = score_job(job, prompt, endpoint, candidate_summary)
        rows.append(row)
        print(
            f"{job['id']} status={row['status']} "
            f"stored={row.get('stored_fit_score')} compact={row.get('compact_fit_score')} "
            f"chars={row.get('stored_chars')}->{row.get('compact_chars')}",
            flush=True,
        )
    for job in jobs:
        job["batch_timestamp"] = _serialize_value(job.get("batch_timestamp"))
        job["last_seen_at"] = _serialize_value(job.get("last_seen_at"))
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": PROFILE_KEY,
        "model": MODEL_NAME,
        "job_count": len(jobs),
        "jobs": rows,
    }
    paths = write_artifacts(payload)
    for kind, path in paths.items():
        print(f"{kind}: {path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
