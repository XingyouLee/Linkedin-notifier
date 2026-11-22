#!/usr/bin/env python3
"""Score stored JDs in batches of 10, 20, and 30 and compare to saved fits.

Reads the remote jobs database with SELECT only. Does not scan LinkedIn,
start Airflow, write database rows, or send notifications.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from psycopg.rows import dict_row


def _install_airflow_stub() -> None:
    airflow = types.ModuleType("airflow")
    sdk = types.ModuleType("airflow.sdk")

    def _dag(*_args, **_kwargs):
        def decorator(fn):
            def wrapped(*_a, **_k):
                return None

            return wrapped

        return decorator

    def _task(*_args, **_kwargs):
        def decorator(fn):
            return fn

        return decorator

    _task.branch = _task
    sdk.dag = _dag
    sdk.task = _task
    airflow.sdk = sdk
    sys.modules.setdefault("airflow", airflow)
    sys.modules.setdefault("airflow.sdk", sdk)


_install_airflow_stub()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dags import fitting_notifier  # noqa: E402
from dags.runtime_utils import load_env  # noqa: E402
from scripts.compare_llm_models import (  # noqa: E402
    _connect_read_only,
    _normalize_chat_endpoint,
    _serialize_value,
    load_feedback_examples,
)

BATCH_SIZES = (10, 20, 30)
MODEL_NAME = "grok-4.7"
PROFILE_KEY = "Xingyou Li"
REQUEST_TIMEOUT_SECONDS = 300
OUTPUT_DIR = "artifacts/batch-fit-comparison"


def load_scored_jobs(database_url: str, profile_key: str, count: int):
    with _connect_read_only(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, profile_key, display_name, resume_text,
                    candidate_summary_config, fit_prompt_config, model_name
                FROM profiles
                WHERE profile_key = %s AND is_active = TRUE
                LIMIT 1
                """,
                (profile_key,),
            )
            profile = cur.fetchone()
            if not profile:
                raise ValueError(f"Profile not found or inactive: {profile_key}")
            if not profile.get("resume_text"):
                raise ValueError(f"Profile {profile_key} has no resume_text")
            if not profile.get("candidate_summary_config"):
                raise ValueError(f"Profile {profile_key} has no candidate_summary_config")

            cur.execute(
                """
                SELECT
                    j.id,
                    j.title,
                    j.company,
                    j.description,
                    j.batch_id,
                    b.timestamp AS batch_timestamp,
                    pj.last_seen_at,
                    pj.fit_score,
                    pj.fit_decision,
                    pj.llm_match
                FROM profile_jobs pj
                JOIN jobs j ON j.id = pj.job_id
                LEFT JOIN batches b ON b.id = j.batch_id
                WHERE pj.profile_id = %s
                  AND j.description IS NOT NULL
                  AND NULLIF(BTRIM(j.description), '') IS NOT NULL
                  AND pj.fit_score IS NOT NULL
                  AND pj.llm_match IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(pj.llm_match_error, '')), '') IS NULL
                ORDER BY pj.fit_updated_at DESC NULLS LAST, j.id DESC
                LIMIT %s
                """,
                (profile["id"], count),
            )
            jobs = [dict(row) for row in cur.fetchall()]

    if len(jobs) != count:
        raise ValueError(
            f"Only {len(jobs)} scored job descriptions available for profile "
            f"{profile_key}; required exactly {count}"
        )
    return dict(profile), jobs


def grok_endpoint() -> dict[str, str]:
    endpoint = os.getenv("Grok_API_ENDPOINT", "").strip()
    api_key = os.getenv("Grok_API_KEY", "").strip()
    if not endpoint or not api_key:
        raise ValueError("Grok requires Grok_API_ENDPOINT and Grok_API_KEY")
    return {
        "request_url": _normalize_chat_endpoint(endpoint),
        "api_key": api_key,
    }


def _stored_match(job: dict[str, Any]) -> dict[str, Any]:
    raw = job.get("llm_match")
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _eval_view(match: dict[str, Any] | None) -> dict[str, Any]:
    match = match or {}
    experience = match.get("experience_check")
    if not isinstance(experience, dict):
        experience = {}
    language = match.get("language_check")
    if not isinstance(language, dict):
        language = {}
    skills = match.get("skills_match")
    if not isinstance(skills, dict):
        skills = {}
    missing = skills.get("missing_critical_skills")
    if not isinstance(missing, list):
        missing = []
    return {
        "summary": match.get("summary") or "",
        "exp_requirement": match.get("exp_requirement") or "",
        "experience_reason": experience.get("reason") or "",
        "language_impact": language.get("impact") or "",
        "missing_critical_skills": missing,
    }


def build_batch_prompt(
    jobs: list[dict[str, Any]],
    profile: dict[str, Any],
    candidate_summary: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
) -> str:
    template = fitting_notifier.database._normalize_fit_prompt_text(
        profile.get("fit_prompt_config")
    )
    cut_at = template.find("JSON structure:")
    if cut_at == -1:
        cut_at = template.find("{{user_feedback_examples}}")
    rules = template[:cut_at].strip() if cut_at != -1 else template
    rules = (
        rules.replace("{{user_feedback_examples}}", "")
        .replace("{{candidate_summary}}", "")
        .replace("{{job_title}}", "")
        .replace("{{job_description}}", "")
        .replace("{{candidate_resume}}", "")
    )
    feedback = fitting_notifier._render_feedback_examples(feedback_examples)
    job_blocks = []
    for index, job in enumerate(jobs, start=1):
        job_blocks.append(
            "\n".join(
                [
                    f"JOB {index}",
                    f"job_id: {job['id']}",
                    f"title: {job.get('title') or 'not provided'}",
                    "description:",
                    str(job.get("description") or "").strip(),
                ]
            )
        )
    contract = (
        "Evaluate every JOB below independently against the same candidate. "
        "Do not let one job's requirements change the score of another. "
        "Return ONLY valid JSON. No markdown. No comments. "
        "JSON structure: "
        '{"results": ['
        '{"job_id": "the job_id from the JOB block", '
        '"fit_score": 0-100, '
        '"decision": "Strong Fit | Moderate Fit | Weak Fit | Not Recommended", '
        '"exp_requirement": "one-line JD experience requirement", '
        '"language_check": {"dutch_required": true/false, "language_blocker": true/false, "impact": "short explanation"}, '
        '"experience_check": {"required_years": number|null, "candidate_years": number|null, "gap_years": number|null, '
        '"seniority_required": "entry | junior | mid | senior | lead | staff | principal | architect | manager | director | unknown", '
        '"candidate_seniority": "entry | junior | mid | senior | lead | staff | principal | architect | manager | director | unknown", '
        '"experience_blocker": true/false, "reason": "short explanation", "severity": "none | minor | moderate | severe"}, '
        '"skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []}, '
        '"risk_factors": [], '
        '"summary": "3-5 concise lines"}'
        "]}"
    )
    return "\n\n".join(
        [
            rules,
            feedback.strip(),
            contract,
            "Candidate Summary: " + json.dumps(candidate_summary, ensure_ascii=False),
            "Candidate Resume:\n" + str(profile["resume_text"]).strip(),
            "JOBS:\n\n" + "\n\n".join(job_blocks),
        ]
    )


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    return stripped


def _parse_batch_payload(output_text: str) -> list[dict[str, Any]]:
    parsed = json.loads(_strip_fence(output_text))
    if isinstance(parsed, list):
        results = parsed
    elif isinstance(parsed, dict) and isinstance(parsed.get("results"), list):
        results = parsed["results"]
    else:
        raise ValueError("response_missing_results_array")
    if not all(isinstance(item, dict) for item in results):
        raise ValueError("response_result_not_object")
    return results


def _stream_chat_text(response: requests.Response) -> tuple[str, str | None, dict | None, list[str]]:
    parts = []
    reasoning_parts = []
    model_name = None
    usage = None
    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        chunk = json.loads(data)
        model_name = chunk.get("model") or model_name
        usage = chunk.get("usage") or usage
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}
            content = delta.get("content")
            if content:
                parts.append(content)
            reasoning = delta.get("reasoning_content") or delta.get("reasoning")
            if reasoning:
                reasoning_parts.append(reasoning)
    return "".join(parts), model_name, usage, reasoning_parts


def request_batch(prompt: str, endpoint: dict[str, str]) -> dict[str, Any]:
    payload = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    started = time.monotonic()
    response = requests.post(
        endpoint["request_url"],
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {endpoint['api_key']}",
        },
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
        stream=True,
    )
    try:
        response.raise_for_status()
        output_text, response_model, usage, reasoning_parts = _stream_chat_text(response)
    finally:
        response.close()
    return {
        "latency_seconds": round(time.monotonic() - started, 3),
        "response_model": response_model,
        "output_text": output_text,
        "usage": usage,
        "reasoning_chars": sum(len(part) for part in reasoning_parts),
    }


def score_item(
    job: dict[str, Any],
    item: dict[str, Any],
    candidate_summary: dict[str, Any],
) -> dict[str, Any]:
    fitting_notifier._validate_llm_match_response(item)
    raw_score = fitting_notifier._coerce_number(item.get("fit_score"))
    raw_decision = fitting_notifier._normalize_match_decision(item.get("decision"))
    capped = fitting_notifier._apply_fit_caps(
        item,
        job_title=job.get("title") or "",
        jd_text=job.get("description") or "",
        candidate_summary=candidate_summary,
    )
    stored = _stored_match(job)
    stored_score = job.get("fit_score")
    batch_score = capped.get("fit_score")
    stored_decision = job.get("fit_decision") or ""
    batch_decision = capped.get("decision") or ""
    return {
        "status": "ok",
        "raw_fit_score": None if raw_score is None else int(round(raw_score)),
        "raw_decision": raw_decision,
        "batch_fit_score": batch_score,
        "batch_decision": batch_decision,
        "stored_fit_score": stored_score,
        "stored_decision": stored_decision,
        "score_delta": None
        if stored_score is None or batch_score is None
        else int(batch_score) - int(stored_score),
        "decision_flip": stored_decision != batch_decision,
        "stored_eval": _eval_view(stored),
        "batch_eval": _eval_view(capped),
        "error": None,
    }


def align_batch(
    jobs: list[dict[str, Any]],
    results: list[dict[str, Any]],
    candidate_summary: dict[str, Any],
) -> dict[str, Any]:
    expected = [str(job["id"]) for job in jobs]
    by_id: dict[str, dict[str, Any]] = {}
    duplicate_ids = []
    extra_ids = []
    for item in results:
        job_id = str(item.get("job_id") or "").strip()
        if not job_id:
            extra_ids.append("")
            continue
        if job_id not in expected:
            extra_ids.append(job_id)
            continue
        if job_id in by_id:
            duplicate_ids.append(job_id)
            continue
        by_id[job_id] = item

    rows = []
    for job in jobs:
        job_id = str(job["id"])
        item = by_id.get(job_id)
        base = {
            "job_id": job_id,
            "title": job.get("title") or "",
            "company": job.get("company") or "",
        }
        if item is None:
            rows.append(
                {
                    **base,
                    "status": "missing",
                    "stored_fit_score": job.get("fit_score"),
                    "stored_decision": job.get("fit_decision") or "",
                    "stored_eval": _eval_view(_stored_match(job)),
                    "error": "missing_from_batch_response",
                }
            )
            continue
        try:
            rows.append({**base, **score_item(job, item, candidate_summary)})
        except Exception as error:
            rows.append(
                {
                    **base,
                    "status": "invalid",
                    "stored_fit_score": job.get("fit_score"),
                    "stored_decision": job.get("fit_decision") or "",
                    "stored_eval": _eval_view(_stored_match(job)),
                    "error": f"{type(error).__name__}: {error}",
                }
            )
    return {
        "rows": rows,
        "missing_ids": [row["job_id"] for row in rows if row["status"] == "missing"],
        "invalid_ids": [row["job_id"] for row in rows if row["status"] == "invalid"],
        "duplicate_ids": duplicate_ids,
        "extra_ids": extra_ids,
    }


def run_batch(
    jobs: list[dict[str, Any]],
    profile: dict[str, Any],
    candidate_summary: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
    endpoint: dict[str, str],
) -> dict[str, Any]:
    prompt = build_batch_prompt(jobs, profile, candidate_summary, feedback_examples)
    attempts = []
    last_error = None
    for attempt in range(2):
        try:
            response = request_batch(prompt, endpoint)
            results = _parse_batch_payload(response["output_text"])
            aligned = align_batch(jobs, results, candidate_summary)
            attempts.append(
                {
                    "attempt": attempt + 1,
                    "status": "parsed",
                    "latency_seconds": response["latency_seconds"],
                    "response_model": response.get("response_model"),
                    "usage": response.get("usage"),
                    "output_chars": len(response["output_text"]),
                }
            )
            ok_rows = [row for row in aligned["rows"] if row["status"] == "ok"]
            deltas = [abs(row["score_delta"]) for row in ok_rows if row["score_delta"] is not None]
            flips = sum(1 for row in ok_rows if row["decision_flip"])
            return {
                "size": len(jobs),
                "status": "success",
                "prompt_chars": len(prompt),
                "attempts": attempts,
                "latency_seconds": response["latency_seconds"],
                "response_model": response.get("response_model"),
                "usage": response.get("usage"),
                "output_chars": len(response["output_text"]),
                "returned_count": len(results),
                "ok_count": len(ok_rows),
                "mae": round(sum(deltas) / len(deltas), 2) if deltas else None,
                "decision_flips": flips,
                **aligned,
            }
        except Exception as error:
            last_error = f"{type(error).__name__}: {error}"
            attempts.append({"attempt": attempt + 1, "status": "error", "error": last_error})
            print(f"batch {len(jobs)} attempt {attempt + 1} failed: {last_error}")
    return {
        "size": len(jobs),
        "status": "error",
        "prompt_chars": len(prompt),
        "attempts": attempts,
        "error": last_error,
        "rows": [],
        "missing_ids": [str(job["id"]) for job in jobs],
        "invalid_ids": [],
        "duplicate_ids": [],
        "extra_ids": [],
        "ok_count": 0,
        "mae": None,
        "decision_flips": 0,
    }


def _cell(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def write_artifacts(payload: dict[str, Any]) -> dict[str, str]:
    directory = Path(OUTPUT_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = directory / f"batch_fit_{stamp}"
    json_path = base.with_suffix(".json")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = base.with_suffix(".csv")
    fieldnames = [
        "batch_size",
        "job_id",
        "title",
        "company",
        "status",
        "stored_fit_score",
        "stored_decision",
        "raw_fit_score",
        "raw_decision",
        "batch_fit_score",
        "batch_decision",
        "score_delta",
        "decision_flip",
        "error",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for batch in payload["batches"]:
            for row in batch.get("rows") or []:
                writer.writerow(
                    {
                        "batch_size": batch["size"],
                        **{key: row.get(key) for key in fieldnames[1:]},
                    }
                )

    markdown_path = base.with_suffix(".md")
    with markdown_path.open("w", encoding="utf-8") as handle:
        handle.write("# Batch fit vs stored scores\n\n")
        handle.write(
            "Jobs are the 60 most recently scored rows (`fit_updated_at` desc). "
            "The table does not store the scoring model; this run treats those rows as grok-4.7. "
            "Batch scores below are after the production fit caps. Raw model scores are in the CSV.\n\n"
        )
        handle.write(f"- Profile: `{payload['profile']}`\n")
        handle.write(f"- Model: `{payload['model']}`\n")
        handle.write(f"- Jobs: {payload['job_count']}\n\n")
        handle.write("| batch | status | ok | missing | invalid | extra | MAE | decision flips | latency_s | prompt_chars | output_chars |\n")
        handle.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for batch in payload["batches"]:
            handle.write(
                "| {size} | {status} | {ok} | {missing} | {invalid} | {extra} | {mae} | {flips} | {latency} | {prompt} | {output} |\n".format(
                    size=batch["size"],
                    status=batch["status"],
                    ok=batch.get("ok_count"),
                    missing=len(batch.get("missing_ids") or []),
                    invalid=len(batch.get("invalid_ids") or []),
                    extra=len(batch.get("extra_ids") or []),
                    mae=batch.get("mae"),
                    flips=batch.get("decision_flips"),
                    latency=batch.get("latency_seconds"),
                    prompt=batch.get("prompt_chars"),
                    output=batch.get("output_chars"),
                )
            )
        handle.write("\n")
        for batch in payload["batches"]:
            handle.write(f"## Batch {batch['size']}\n\n")
            if batch["status"] != "success":
                handle.write(f"Error: `{batch.get('error')}`\n\n")
                continue
            handle.write("| job | stored | batch | delta | flip |\n")
            handle.write("| --- | --- | --- | --- | --- |\n")
            for row in batch["rows"]:
                title = _cell(row.get("title") or row["job_id"])
                handle.write(
                    f"| {title} | {row.get('stored_fit_score')} {row.get('stored_decision')} | "
                    f"{row.get('batch_fit_score')} {row.get('batch_decision')} | "
                    f"{row.get('score_delta')} | {row.get('decision_flip')} |\n"
                )
            handle.write("\n")
            for row in batch["rows"]:
                if row.get("status") != "ok":
                    handle.write(f"### {row['job_id']} {row.get('status')}\n\n{row.get('error')}\n\n")
                    continue
                handle.write(f"### {_cell(row.get('title'))} `{row['job_id']}`\n\n")
                stored_eval = row["stored_eval"]
                batch_eval = row["batch_eval"]
                handle.write(f"- stored summary: {_cell(stored_eval.get('summary'))}\n")
                handle.write(f"- batch summary: {_cell(batch_eval.get('summary'))}\n")
                handle.write(f"- stored exp: {_cell(stored_eval.get('exp_requirement'))}\n")
                handle.write(f"- batch exp: {_cell(batch_eval.get('exp_requirement'))}\n")
                handle.write(f"- stored language: {_cell(stored_eval.get('language_impact'))}\n")
                handle.write(f"- batch language: {_cell(batch_eval.get('language_impact'))}\n")
                handle.write(
                    "- stored missing: "
                    + _cell("; ".join(str(item) for item in stored_eval.get("missing_critical_skills") or []))
                    + "\n"
                )
                handle.write(
                    "- batch missing: "
                    + _cell("; ".join(str(item) for item in batch_eval.get("missing_critical_skills") or []))
                    + "\n\n"
                )
    return {"json": str(json_path), "csv": str(csv_path), "markdown": str(markdown_path)}


def main(argv: list[str] | None = None) -> int:
    load_env(override_if_missing=True)
    database_url = os.getenv("JOBS_DB_URL", "").strip()
    if not database_url:
        raise SystemExit("JOBS_DB_URL is required")
    count = sum(BATCH_SIZES)
    profile, jobs = load_scored_jobs(database_url, PROFILE_KEY, count)
    feedback_examples = load_feedback_examples(database_url, profile["id"])
    candidate_summary = fitting_notifier._load_candidate_summary_config(
        profile["candidate_summary_config"]
    )
    endpoint = grok_endpoint()
    print(f"Loaded {len(jobs)} scored JDs for {PROFILE_KEY}")
    selected = set(argv if argv is not None else sys.argv[1:])
    batches = []
    offset = 0
    for size in BATCH_SIZES:
        chunk = jobs[offset : offset + size]
        offset += size
        if selected and str(size) not in selected:
            continue
        print(f"Scoring batch {size}", flush=True)
        batches.append(
            run_batch(chunk, profile, candidate_summary, feedback_examples, endpoint)
        )
        print(
            f"batch {size} status={batches[-1]['status']} "
            f"ok={batches[-1].get('ok_count')} mae={batches[-1].get('mae')}"
        )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": PROFILE_KEY,
        "profile_model_name": profile.get("model_name"),
        "model": MODEL_NAME,
        "job_count": len(jobs),
        "note": (
            "Latest 60 by fit_updated_at. "
            "profile_jobs does not store the scoring model; caller says recent scores are grok-4.7. "
            "Batch scores are post _apply_fit_caps."
        ),
        "batches": batches,
    }
    for job in jobs:
        job["batch_timestamp"] = _serialize_value(job.get("batch_timestamp"))
        job["last_seen_at"] = _serialize_value(job.get("last_seen_at"))
    paths = write_artifacts(payload)
    for kind, path in paths.items():
        print(f"{kind}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
