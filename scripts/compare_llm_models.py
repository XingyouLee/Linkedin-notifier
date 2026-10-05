#!/usr/bin/env python3
"""Record production-equivalent JD evaluations from three LLM providers.

The job sample is read directly from Postgres. This script deliberately does
not scan LinkedIn, fetch descriptions over HTTP, start Airflow, or write to the
database. Its artifacts contain only model scores and reasons for later
third-party comparison.

Example:
    python scripts/compare_llm_models.py \
        --count 50 \
        --profile "Xingyou Li" \
        --models deepseek-flash gpt-6-sol grok-4.7 \
        --concurrency 3
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dags import fitting_notifier
from dags.runtime_utils import load_env


DEFAULT_MODELS = ["deepseek-flash", "gpt-6-sol", "grok-4.7"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record score/reason comparisons for stored job descriptions"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of stored job descriptions to evaluate (default: 50)",
    )
    parser.add_argument(
        "--profile",
        default="Xingyou Li",
        help="Active profile key used for prompt and job selection",
    )
    parser.add_argument(
        "--models",
        nargs="+",
        default=DEFAULT_MODELS,
        help="Model names to evaluate (default: deepseek-flash gpt-6-sol grok-4.7)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of jobs evaluated concurrently (default: 3)",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/llm-model-comparison",
        help="Directory for JSON, CSV, and Markdown artifacts",
    )
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("--count must be positive")
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")
    if not args.models:
        parser.error("--models must contain at least one model")
    if len({model.lower() for model in args.models}) != len(args.models):
        parser.error("--models must not contain duplicate model names")
    return args


def _connect_read_only(database_url: str):
    """Open a Postgres connection whose default transactions are read-only."""
    import psycopg

    return psycopg.connect(
        database_url,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on",
    )


def load_profile_and_jobs(
    database_url: str, profile_key: str, count: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load the profile and latest stored descriptions using SELECT only."""
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
                raise ValueError(
                    f"Profile {profile_key} has no candidate_summary_config"
                )

            cur.execute(
                """
                SELECT
                    j.id,
                    j.title,
                    j.company,
                    j.description,
                    j.batch_id,
                    b.timestamp AS batch_timestamp,
                    pj.last_seen_at
                FROM profile_jobs pj
                JOIN jobs j ON j.id = pj.job_id
                LEFT JOIN batches b ON b.id = j.batch_id
                WHERE pj.profile_id = %s
                  AND j.description IS NOT NULL
                  AND NULLIF(BTRIM(j.description), '') IS NOT NULL
                ORDER BY
                    b.timestamp DESC NULLS LAST,
                    j.batch_id DESC NULLS LAST,
                    pj.last_seen_at DESC NULLS LAST,
                    j.id DESC
                LIMIT %s
                """,
                (profile["id"], count),
            )
            jobs = [dict(row) for row in cur.fetchall()]

    if len(jobs) != count:
        raise ValueError(
            f"Only {len(jobs)} stored job descriptions available for profile "
            f"{profile_key}; required exactly {count}"
        )
    return dict(profile), jobs


def load_feedback_examples(
    database_url: str, profile_id: int, limit: int = 10
) -> list[dict[str, Any]]:
    """Read recent profile feedback for the production prompt."""
    with _connect_read_only(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    j.title, j.company,
                    pj.user_status AS status,
                    pj.user_note,
                    pj.fit_score,
                    pj.fit_decision
                FROM profile_jobs pj
                JOIN jobs j ON j.id = pj.job_id
                WHERE pj.profile_id = %s
                  AND pj.user_status IN ('applied', 'dismissed')
                  AND pj.fit_score IS NOT NULL
                ORDER BY pj.user_status_updated_at DESC NULLS LAST
                LIMIT %s
                """,
                (profile_id, limit),
            )
            return [
                {
                    "title": row.get("title"),
                    "company": row.get("company"),
                    "status": row.get("status"),
                    "user_note": row.get("user_note"),
                    "fit_score": row.get("fit_score"),
                    "fit_decision": row.get("fit_decision"),
                }
                for row in cur.fetchall()
            ]


def _normalize_chat_endpoint(endpoint: str) -> str:
    normalized = endpoint.strip().rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    if not normalized.endswith("/v1"):
        normalized += "/v1"
    return normalized + "/chat/completions"


def build_model_config(model_name: str) -> dict[str, Any]:
    """Build provider config without exposing credentials in artifact metadata."""
    lowered = model_name.lower()
    if lowered.startswith("deepseek-"):
        endpoint = os.getenv("Deepseek_API_ENDPOINT", "").strip()
        api_key = os.getenv("Deepseek_API_KEY", "").strip()
        if not endpoint or not api_key:
            raise ValueError(
                "DeepSeek requires Deepseek_API_ENDPOINT and Deepseek_API_KEY"
            )
        return {
            "name": model_name,
            "endpoints": [
                {
                    "name": "deepseek",
                    "request_url": _normalize_chat_endpoint(endpoint),
                    "api_key": api_key,
                    "api_type": "chat_completions",
                }
            ],
        }

    if lowered.startswith("grok-"):
        endpoint = os.getenv("Grok_API_ENDPOINT", "").strip()
        api_key = os.getenv("Grok_API_KEY", "").strip()
        if not endpoint or not api_key:
            raise ValueError("Grok requires Grok_API_ENDPOINT and Grok_API_KEY")
        return {
            "name": model_name,
            "endpoints": [
                {
                    "name": "grok",
                    "request_url": _normalize_chat_endpoint(endpoint),
                    "api_key": api_key,
                    "api_type": "chat_completions",
                }
            ],
        }

    if lowered.startswith("gpt-"):
        endpoints = fitting_notifier._parse_llm_endpoints_from_env()
        if not endpoints:
            raise ValueError("GPT requires a configured LLM_ENDPOINTS_JSON")
        return {"name": model_name, "endpoints": endpoints}

    raise ValueError(
        f"Unsupported model {model_name!r}; expected deepseek-*, gpt-*, or grok-*"
    )


def _reason_payload(match: dict[str, Any]) -> dict[str, Any]:
    experience = match.get("experience_check")
    if not isinstance(experience, dict):
        experience = {}
    language = match.get("language_check")
    if not isinstance(language, dict):
        language = {}
    skills = match.get("skills_match")
    if not isinstance(skills, dict):
        skills = {}
    risks = match.get("risk_factors")
    if not isinstance(risks, list):
        risks = []
    missing = skills.get("missing_critical_skills")
    if not isinstance(missing, list):
        missing = []
    return {
        "summary": match.get("summary") or "",
        "experience": experience.get("reason") or "",
        "language": language.get("impact") or "",
        "risks": risks,
        "missing_critical_skills": missing,
    }


def evaluate_model(
    job: dict[str, Any],
    prompt: str,
    model_config: dict[str, Any],
    candidate_summary: dict[str, Any],
    max_retries: int = 2,
) -> dict[str, Any]:
    """Evaluate one job/model pair and return comparison-safe fields."""
    started = time.monotonic()
    attempts = 0
    last_error: str | None = None
    for attempt in range(max_retries + 1):
        attempts = attempt + 1
        try:
            parsed, metadata = fitting_notifier._request_llm_json_with_fallback(
                endpoints=model_config["endpoints"],
                model_name=model_config["name"],
                prompt=prompt,
                return_model_metadata=True,
            )
            fitting_notifier._validate_llm_match_response(parsed)
            normalized = fitting_notifier._apply_fit_caps(
                parsed,
                job_title=job.get("title") or "",
                jd_text=job.get("description") or "",
                candidate_summary=candidate_summary,
            )
            return {
                "fit_score": normalized["fit_score"],
                "reason": _reason_payload(normalized),
                "status": "success",
                "error": None,
                "response_model": metadata.get("response_model_name"),
                "attempts": attempts,
                "latency_seconds": round(time.monotonic() - started, 3),
            }
        except RuntimeError as error:
            last_error = str(error)
            if not last_error.startswith("TRANSIENT_API::") or attempt >= max_retries:
                break
            time.sleep(min(2**attempt, 4))
        except Exception as error:
            last_error = f"{type(error).__name__}: {error}"
            break

    return {
        "fit_score": None,
        "reason": None,
        "status": "error",
        "error": last_error or "unknown_llm_error",
        "response_model": None,
        "attempts": attempts,
        "latency_seconds": round(time.monotonic() - started, 3),
    }


def evaluate_job(
    job: dict[str, Any],
    profile: dict[str, Any],
    candidate_summary: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
    model_configs: list[dict[str, Any]],
) -> dict[str, Any]:
    prompt = fitting_notifier._build_fit_prompt(
        job_title=job.get("title") or "",
        jd_text=job.get("description") or "",
        resume_text=profile["resume_text"],
        candidate_summary=candidate_summary,
        prompt_text=profile.get("fit_prompt_config"),
        feedback_examples=feedback_examples,
    )
    model_results = {}
    for model_config in model_configs:
        model_results[model_config["name"]] = evaluate_model(
            job, prompt, model_config, candidate_summary
        )
    return {
        "job_id": str(job["id"]),
        "title": job.get("title") or "",
        "company": job.get("company") or "",
        "batch_id": job.get("batch_id"),
        "batch_timestamp": _serialize_value(job.get("batch_timestamp")),
        "models": model_results,
    }


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def evaluate_jobs(
    jobs: list[dict[str, Any]],
    profile: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
    model_configs: list[dict[str, Any]],
    concurrency: int,
) -> list[dict[str, Any]]:
    candidate_summary = fitting_notifier._load_candidate_summary_config(
        profile["candidate_summary_config"]
    )
    results: list[dict[str, Any] | None] = [None] * len(jobs)
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        pending = {
            executor.submit(
                evaluate_job,
                job,
                profile,
                candidate_summary,
                feedback_examples,
                model_configs,
            ): index
            for index, job in enumerate(jobs)
        }
        for future in as_completed(pending):
            index = pending[future]
            results[index] = future.result()
            print(f"Evaluated {index + 1}/{len(jobs)}: {jobs[index].get('id')}")
    return [result for result in results if result is not None]


def _reason_text(reason: dict[str, Any] | None) -> str:
    if not reason:
        return ""
    parts = [reason.get("summary"), reason.get("experience"), reason.get("language")]
    risks = reason.get("risks") or []
    missing = reason.get("missing_critical_skills") or []
    if risks:
        parts.append("Risks: " + "; ".join(str(item) for item in risks))
    if missing:
        parts.append("Missing critical skills: " + "; ".join(str(item) for item in missing))
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def write_artifacts(
    output_dir: str,
    profile_key: str,
    model_names: list[str],
    jobs: list[dict[str, Any]],
) -> dict[str, str]:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = directory / f"comparison_{stamp}"
    payload = {
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile_key,
        "models": model_names,
        "count": len(jobs),
        "comparison": "score_and_reason_only",
        "jobs": jobs,
    }
    json_path = base.with_suffix(".json")
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    csv_path = base.with_suffix(".csv")
    fieldnames = ["job_id", "title", "company", "batch_id", "batch_timestamp"]
    for model in model_names:
        fieldnames.extend(
            [
                f"{model}_fit_score",
                f"{model}_reason",
                f"{model}_status",
                f"{model}_error",
            ]
        )
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for job in jobs:
            row = {key: job.get(key) for key in fieldnames[:5]}
            for model in model_names:
                result = job["models"].get(model) or {}
                row[f"{model}_fit_score"] = result.get("fit_score")
                row[f"{model}_reason"] = json.dumps(
                    result.get("reason"), ensure_ascii=False
                )
                row[f"{model}_status"] = result.get("status")
                row[f"{model}_error"] = result.get("error")
            writer.writerow(row)

    markdown_path = base.with_suffix(".md")
    with markdown_path.open("w", encoding="utf-8") as handle:
        handle.write("# JD Model Evaluation Records\n\n")
        handle.write(f"- Profile: `{profile_key}`\n")
        handle.write(f"- Jobs: {len(jobs)}\n")
        handle.write(f"- Models: {', '.join(f'`{model}`' for model in model_names)}\n")
        handle.write("- Scope: score and reason records only; no automatic comparison\n\n")
        for job in jobs:
            title = str(job.get("title") or "Untitled").replace("|", "\\|")
            company = str(job.get("company") or "Unknown").replace("|", "\\|")
            handle.write(f"## {title} — {company}\n\n")
            handle.write(f"Job ID: `{job['job_id']}`\n\n")
            for model in model_names:
                result = job["models"].get(model) or {}
                reason = _reason_text(result.get("reason"))
                handle.write(
                    f"- **{model}**: status `{result.get('status', 'unknown')}`; "
                    f"score `{result.get('fit_score')}`; "
                    f"reason: {reason or result.get('error') or 'N/A'}\n"
                )
            handle.write("\n")

    return {"json": str(json_path), "csv": str(csv_path), "markdown": str(markdown_path)}


def main() -> int:
    args = parse_args()
    load_env(override_if_missing=True)
    database_url = os.getenv("JOBS_DB_URL", "").strip()
    if not database_url:
        raise SystemExit("JOBS_DB_URL is required")

    model_configs = [build_model_config(model) for model in args.models]
    profile, jobs = load_profile_and_jobs(database_url, args.profile, args.count)
    feedback_examples = load_feedback_examples(database_url, profile["id"])
    print(
        f"Loaded {len(jobs)} stored JDs for {args.profile}; "
        f"recording {len(model_configs)} model results per JD"
    )
    results = evaluate_jobs(
        jobs, profile, feedback_examples, model_configs, args.concurrency
    )
    paths = write_artifacts(args.output_dir, args.profile, args.models, results)
    for kind, path in paths.items():
        print(f"{kind}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
