from __future__ import annotations

from airflow.sdk import dag, task

import ast
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
from datetime import datetime
import os
import pandas as pd
import re
import requests
import time
from dags import database
from dags import materials_launch
from dags.runtime_utils import df_to_xcom_records, load_env


load_env(override_if_missing=True)

LLM_API_ALERT_KEY = "llm_api_error"
MATCH_DECISION_ORDER = [
    "Not Recommended",
    "Weak Fit",
    "Moderate Fit",
    "Strong Fit",
]
MATCH_DECISION_RANK = {
    decision: rank for rank, decision in enumerate(MATCH_DECISION_ORDER)
}
SENIORITY_LEVELS = {
    "entry": 0,
    "junior": 1,
    "mid": 2,
    "senior": 3,
    "lead": 4,
    "architect": 5,
    "manager": 5,
    "staff": 6,
    "principal": 7,
    "director": 8,
    "unknown": -1,
}
SENIORITY_PATTERNS = [
    (re.compile(r"\bdirector\b", re.I), "director"),
    (re.compile(r"\bprincipal\b", re.I), "principal"),
    (re.compile(r"\bstaff\b", re.I), "staff"),
    (re.compile(r"\barchitect\b", re.I), "architect"),
    (re.compile(r"\bmanager\b", re.I), "manager"),
    (re.compile(r"\blead\b", re.I), "lead"),
    (re.compile(r"\bsenior\b", re.I), "senior"),
    (re.compile(r"\bmid(?:-level)?\b", re.I), "mid"),
    (re.compile(r"\bjunior\b", re.I), "junior"),
]
YEARS_RANGE_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:\+|plus)?\s*(?:-|to)\s*(\d+(?:\.\d+)?)\s*(?:years?|yrs?)",
    re.I,
)
YEARS_SINGLE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*\+?\s*(?:years?|yrs?)", re.I)
LOGGED_MODEL_NAME_RE = re.compile(r"model_name=([^\s|]+)")


def _build_match_task_result(
    jobs=None,
    *,
    api_error: bool = False,
    api_error_message: str | None = None,
    requeue_items=None,
    requeue_job_errors=None,
    stopped_early: bool = False,
    persisted_immediately: bool = False,
    finalize_counts=None,
):
    normalized_requeue_items = []
    seen_keys = set()
    for item in _iter_job_refs(requeue_items):
        item_key = _job_ref_key(item["profile_id"], item["job_id"])
        if item_key in seen_keys:
            continue
        seen_keys.add(item_key)
        normalized_requeue_items.append(item)

    return {
        "jobs": jobs or [],
        "api_error": api_error,
        "api_error_message": api_error_message,
        "requeue_items": normalized_requeue_items,
        "requeue_job_errors": requeue_job_errors or {},
        "stopped_early": stopped_early,
        "persisted_immediately": persisted_immediately,
        "finalize_counts": finalize_counts or {"done": 0, "failed": 0, "requeued": 0},
        "unprocessed_items": normalized_requeue_items,
    }


def _unwrap_match_task_result(match_result):
    if not match_result:
        return (
            [],
            False,
            None,
            [],
            {},
            False,
            False,
            {"done": 0, "failed": 0, "requeued": 0},
        )
    if isinstance(match_result, dict):
        requeue_items = match_result.get("requeue_items")
        if requeue_items is None:
            requeue_items = match_result.get("unprocessed_items") or []
        return (
            match_result.get("jobs") or [],
            bool(match_result.get("api_error")),
            match_result.get("api_error_message"),
            requeue_items,
            match_result.get("requeue_job_errors") or {},
            bool(match_result.get("stopped_early")),
            bool(match_result.get("persisted_immediately")),
            match_result.get("finalize_counts")
            or {"done": 0, "failed": 0, "requeued": 0},
        )
    return (
        match_result or [],
        False,
        None,
        [],
        {},
        False,
        False,
        {"done": 0, "failed": 0, "requeued": 0},
    )


def _build_job_match_result(
    profile_id: int,
    job_id: str,
    *,
    llm_match: str | None = None,
    llm_match_error: str | None = None,
    model_name: str | None = None,
):
    return {
        "profile_id": int(profile_id),
        "job_id": job_id,
        "llm_match": llm_match,
        "llm_match_error": llm_match_error,
        "model_name": model_name,
    }


def _job_ref_key(profile_id: int, job_id: str) -> str:
    return f"{int(profile_id)}:{str(job_id)}"


def _iter_job_refs(job_items):
    for item in job_items or []:
        if not isinstance(item, dict):
            continue
        profile_id = item.get("profile_id")
        job_id = item.get("job_id") or item.get("id")
        if profile_id is None or not job_id:
            continue
        yield {
            "profile_id": int(profile_id),
            "job_id": str(job_id),
        }


def _build_uniform_error_results(job_records, error_message: str):
    results = []
    for item in _iter_job_refs(job_records):
        results.append(
            _build_job_match_result(
                item["profile_id"],
                item["job_id"],
                llm_match_error=error_message,
            )
        )
    return results


def _is_transient_llm_http_status(status_code) -> bool:
    try:
        parsed_status_code = int(status_code)
    except (TypeError, ValueError):
        return False
    return (
        parsed_status_code == 408
        or parsed_status_code == 429
        or parsed_status_code >= 500
    )


def _strip_prefix(text: str, prefix: str) -> str:
    return text[len(prefix) :] if text.startswith(prefix) else text


def _summarize_api_errors(api_error_messages):
    normalized_messages = [
        str(message) for message in (api_error_messages or []) if message
    ]
    if not normalized_messages:
        return None
    if len(normalized_messages) == 1:
        return normalized_messages[0]
    return (
        f"transient_llm_api_errors count={len(normalized_messages)} "
        f"sample={normalized_messages[0]}"
    )


def _extract_logged_model_names(text: str | None) -> list[str]:
    model_names = []
    for model_name in LOGGED_MODEL_NAME_RE.findall(str(text or "")):
        if model_name not in model_names:
            model_names.append(model_name)
    return model_names


def _summarize_logged_model_names(
    text: str | None,
    fallback_model_name: str | None = None,
) -> str:
    model_names = _extract_logged_model_names(text)
    if model_names:
        return ",".join(model_names)
    return fallback_model_name or "unknown"


def _execute_prepared_fitting_items(
    prepared_items,
    *,
    concurrency: int,
    process_single_item,
    default_model_name_fn,
    handle_completed_event,
):
    fatal_events = []
    if not prepared_items:
        return fatal_events

    max_workers = max(1, int(concurrency or 1))
    prepared_iter = iter(prepared_items)
    stop_submitting = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        in_flight = {}

        def _submit_next() -> bool:
            try:
                prepared = next(prepared_iter)
            except StopIteration:
                return False

            future = executor.submit(process_single_item, prepared)
            in_flight[future] = prepared
            return True

        for _ in range(min(max_workers, len(prepared_items))):
            _submit_next()

        while in_flight:
            done, _ = wait(tuple(in_flight), return_when=FIRST_COMPLETED)
            completed_count = len(done)

            for future in done:
                prepared = in_flight.pop(future)
                try:
                    _, job_result = future.result()
                except Exception as error:
                    error_text = str(error)
                    error_model_name = _summarize_logged_model_names(
                        error_text,
                        default_model_name_fn(prepared),
                    )
                    if error_text.startswith("TRANSIENT_API::"):
                        handle_completed_event(
                            {
                                "kind": "transient_api_error",
                                "prepared": prepared,
                                "error_message": _strip_prefix(
                                    error_text,
                                    "TRANSIENT_API::",
                                ),
                                "model_name": error_model_name,
                            }
                        )
                        continue
                    if error_text.startswith("FATAL_API::"):
                        fatal_events.append(
                            {
                                "prepared": prepared,
                                "error_message": _strip_prefix(
                                    error_text,
                                    "FATAL_API::",
                                ),
                                "model_name": error_model_name,
                            }
                        )
                        continue

                    handle_completed_event(
                        {
                            "kind": "job_result",
                            "prepared": prepared,
                            "job_result": _build_job_match_result(
                                prepared["item"]["profile_id"],
                                prepared["item"]["job_id"],
                                llm_match_error=f"unexpected_job_error: {error}",
                                model_name=error_model_name,
                            ),
                        }
                    )
                    continue

                handle_completed_event(
                    {
                        "kind": "job_result",
                        "prepared": prepared,
                        "job_result": job_result,
                    }
                )

            if fatal_events:
                stop_submitting = True

            if not stop_submitting:
                for _ in range(completed_count):
                    if not _submit_next():
                        break

    return fatal_events


def _log_job_match_result(job_result):
    job_id = job_result.get("job_id")
    profile_id = job_result.get("profile_id")
    model_name = job_result.get("model_name") or "unknown"
    if not job_id or profile_id is None:
        return

    llm_error = job_result.get("llm_match_error")
    if llm_error:
        print(
            f"llm_result profile_id={profile_id} job_id={job_id} "
            f"model_name={model_name} status=error error={llm_error}"
        )
        return

    fit_score = None
    decision = None
    try:
        parsed_match = json.loads(job_result.get("llm_match") or "{}")
        fit_score = parsed_match.get("fit_score")
        decision = parsed_match.get("decision")
    except Exception:
        pass

    print(
        f"llm_result profile_id={profile_id} job_id={job_id} model_name={model_name} status=ok "
        f"fit_score={fit_score} decision={decision}"
    )


def _extract_output_text(response_json):
    output_text = response_json.get("output_text")
    if output_text:
        return output_text

    for item in response_json.get("output") or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            text = content.get("text")
            if text:
                return text
    return None


def _load_resume_text(
    *,
    resume_path: str | None = None,
    resume_text: str | None = None,
):
    if resume_text:
        return str(resume_text), None

    resume_candidates = []
    if resume_path:
        resume_candidates.append(os.path.abspath(os.path.expanduser(resume_path)))
    resume_candidates.extend(
        [
            os.path.abspath(os.path.join(os.path.dirname(__file__), "resume.md")),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resume.md")),
        ]
    )
    resume_error = None
    for resume_path in resume_candidates:
        try:
            with open(resume_path, "r", encoding="utf-8") as file_handle:
                return file_handle.read(), None
        except Exception as error:
            resume_error = error

    return None, f"resume_read_error: {resume_error}"


def _normalize_text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_number(value) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"-?\d+(?:\.\d+)?", str(value))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _to_json_number(value: float | None) -> int | float | None:
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return round(float(value), 1)


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in {"true", "1", "yes", "y", "on"}


def _is_test_mode_enabled() -> bool:
    return _coerce_bool(os.getenv("LINKEDIN_TEST_MODE"))


def _normalize_string_list(values) -> list[str]:
    if not values:
        return []
    if isinstance(values, str):
        values = [part.strip() for part in values.split(",")]
    normalized = []
    seen = set()
    for value in values:
        text = _normalize_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _normalize_language_level(value) -> str:
    normalized = (_normalize_text(value) or "unknown").lower()
    alias_map = {
        "none": "none",
        "no": "none",
        "basic": "basic",
        "beginner": "basic",
        "working": "working",
        "professional": "working",
        "fluent": "fluent",
        "native": "fluent",
        "unknown": "unknown",
    }
    return alias_map.get(normalized, "unknown")


def _infer_candidate_seniority_from_years(candidate_years: float | None) -> str:
    if candidate_years is None:
        return "unknown"
    if candidate_years < 1:
        return "entry"
    if candidate_years < 2:
        return "junior"
    if candidate_years < 5:
        return "mid"
    if candidate_years < 8:
        return "senior"
    return "lead"


def _normalize_candidate_seniority(
    value,
    *,
    candidate_years: float | None = None,
    fallback_to_years: bool = False,
) -> str:
    normalized = (_normalize_text(value) or "").lower()
    alias_map = {
        "entry": "entry",
        "entry level": "entry",
        "trainee": "entry",
        "intern": "entry",
        "junior": "junior",
        "mid": "mid",
        "mid-level": "mid",
        "regular": "mid",
        "senior": "senior",
        "lead": "lead",
        "staff": "staff",
        "principal": "principal",
        "architect": "architect",
        "manager": "manager",
        "director": "director",
        "unknown": "unknown",
    }
    if normalized in alias_map:
        return alias_map[normalized]
    if fallback_to_years:
        return _infer_candidate_seniority_from_years(candidate_years)
    return "unknown"


def _normalize_required_seniority(value) -> str:
    normalized = (_normalize_text(value) or "").lower()
    alias_map = {
        "entry": "entry",
        "entry level": "entry",
        "trainee": "entry",
        "intern": "entry",
        "junior": "junior",
        "mid": "mid",
        "mid-level": "mid",
        "regular": "mid",
        "senior": "senior",
        "lead": "lead",
        "architect": "architect",
        "manager": "manager",
        "staff": "staff",
        "principal": "principal",
        "director": "director",
    }
    return alias_map.get(normalized, "unknown")


def _infer_required_seniority(job_title: str, jd_text: str) -> str:
    title_text = _normalize_text(job_title) or ""
    jd_excerpt = _normalize_text(jd_text) or ""
    for source in (title_text, jd_excerpt):
        for pattern, label in SENIORITY_PATTERNS:
            if pattern.search(source):
                return label
    return "unknown"


def _extract_required_years(
    exp_requirement: str | None,
    job_title: str,
    jd_text: str,
) -> float | None:
    text = " ".join(
        part for part in [exp_requirement, job_title, jd_text] if _normalize_text(part)
    )
    values = []
    for match in YEARS_RANGE_RE.finditer(text):
        values.append(min(float(match.group(1)), float(match.group(2))))
    for match in YEARS_SINGLE_RE.finditer(text):
        values.append(float(match.group(1)))
    return max(values) if values else None


def _normalize_candidate_summary(summary_payload: dict) -> dict:
    if not isinstance(summary_payload, dict):
        raise ValueError("candidate_summary_not_object")

    candidate_years = _coerce_number(summary_payload.get("candidate_years"))
    if candidate_years is None:
        raise ValueError("candidate_summary_missing_candidate_years")

    candidate_seniority = _normalize_candidate_seniority(
        summary_payload.get("candidate_seniority"),
        candidate_years=candidate_years,
        fallback_to_years=False,
    )
    if candidate_seniority == "unknown":
        raise ValueError("candidate_summary_missing_candidate_seniority")

    language_signals = summary_payload.get("language_signals")
    if not isinstance(language_signals, dict):
        language_signals = {}

    normalized_summary = {
        "summary": _normalize_text(summary_payload.get("summary")),
        "target_roles": _normalize_string_list(summary_payload.get("target_roles")),
        "candidate_years": _to_json_number(candidate_years),
        "candidate_seniority": candidate_seniority,
        "core_skills": _normalize_string_list(summary_payload.get("core_skills")),
        "obvious_gaps": _normalize_string_list(summary_payload.get("obvious_gaps")),
        "language_signals": {
            "dutch_level": _normalize_language_level(
                language_signals.get("dutch_level")
            ),
            "english_level": _normalize_language_level(
                language_signals.get("english_level")
            ),
            "notes": _normalize_text(language_signals.get("notes"))
            or "Language evidence not explicitly summarized.",
        },
    }
    if not normalized_summary["summary"]:
        raise ValueError("candidate_summary_missing_summary")
    return normalized_summary


def _load_candidate_summary_config(candidate_summary_config) -> dict:
    if candidate_summary_config is None:
        raise ValueError("missing_candidate_summary_config")

    parsed_config = candidate_summary_config
    if isinstance(candidate_summary_config, str):
        text = _normalize_text(candidate_summary_config)
        if not text:
            raise ValueError("missing_candidate_summary_config")
        try:
            parsed_config = json.loads(text)
        except Exception as error:
            raise ValueError(f"candidate_summary_config_json_error: {error}") from error

    return _normalize_candidate_summary(parsed_config)


def _normalize_match_decision(decision) -> str:
    normalized = (_normalize_text(decision) or "").lower()
    alias_map = {
        "strong fit": "Strong Fit",
        "moderate fit": "Moderate Fit",
        "weak fit": "Weak Fit",
        "not recommended": "Not Recommended",
    }
    if normalized not in alias_map:
        raise ValueError("response_invalid_decision")
    return alias_map[normalized]


def _request_llm_json(
    *,
    request_url: str,
    api_key: str,
    model_name: str,
    prompt: str,
) -> dict:
    payload = {
        "model": model_name,
        "input": prompt,
    }
    response = requests.post(
        request_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()

    response_json = response.json()
    output_text = _extract_output_text(response_json)
    if not output_text:
        raise ValueError("response_missing_output_text")

    parsed = json.loads(output_text)
    if not isinstance(parsed, dict):
        raise ValueError("response_json_not_object")
    return parsed


def _parse_llm_endpoints_from_env() -> list[dict[str, str]]:
    endpoints_json = _normalize_text(os.getenv("LLM_ENDPOINTS_JSON"))
    endpoints: list[dict[str, str]] = []
    if endpoints_json:
        try:
            parsed = json.loads(endpoints_json)
        except Exception as error:
            raise ValueError(f"invalid_llm_endpoints_json: {error}") from error
        if not isinstance(parsed, list):
            raise ValueError("invalid_llm_endpoints_json: root must be a list")
        for index, entry in enumerate(parsed):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"invalid_llm_endpoints_json: entry {index} must be an object"
                )
            request_url = _normalize_text(entry.get("request_url") or entry.get("url"))
            api_key = _normalize_text(entry.get("api_key"))
            api_key_env = _normalize_text(entry.get("api_key_env"))
            name = _normalize_text(entry.get("name")) or f"endpoint_{index + 1}"
            if api_key is None and api_key_env:
                api_key = _normalize_text(os.getenv(api_key_env))
            if not request_url or not api_key:
                raise ValueError(
                    f"invalid_llm_endpoints_json: entry {index} requires request_url and api_key/api_key_env"
                )
            entry_dict: dict[str, str] = {
                    "name": name,
                    "request_url": request_url,
                    "api_key": api_key,
                }
            model = _normalize_text(entry.get("model"))
            if model:
                entry_dict["model"] = model
            endpoints.append(entry_dict)

    if endpoints:
        return endpoints

    request_url = _normalize_text(os.getenv("FITTING_REQUEST_URL"))
    api_key = _normalize_text(os.getenv("LLM_API_KEY")) or _normalize_text(
        os.getenv("GMN_API_KEY")
    )
    if request_url and api_key:
        return [
            {
                "name": "primary",
                "request_url": request_url,
                "api_key": api_key,
            }
        ]

    return []


def _request_llm_json_with_fallback(
    *,
    endpoints: list[dict[str, str]],
    model_name: str,
    prompt: str,
    return_metadata: bool = False,
) -> dict | tuple[dict, str]:
    transient_errors: list[str] = []
    fatal_errors: list[str] = []

    if not endpoints:
        raise RuntimeError("FATAL_API::no_llm_endpoints_available")

    for endpoint in endpoints:
        endpoint_name = (
            endpoint.get("name") or endpoint.get("request_url") or "endpoint"
        )
        effective_model_name = endpoint.get("model") or model_name
        try:
            parsed = _request_llm_json(
                request_url=endpoint["request_url"],
                api_key=endpoint["api_key"],
                model_name=effective_model_name,
                prompt=prompt,
            )
            if return_metadata:
                return parsed, effective_model_name
            return parsed
        except requests.HTTPError as error:
            status_code = (
                error.response.status_code if error.response is not None else "unknown"
            )
            message = (
                f"endpoint={endpoint_name} model_name={effective_model_name} "
                f"status={status_code} error={error}"
            )
            if _is_transient_llm_http_status(status_code):
                transient_errors.append(message)
                continue
            fatal_errors.append(message)
            continue
        except (requests.Timeout, requests.ConnectionError) as error:
            transient_errors.append(
                f"endpoint={endpoint_name} model_name={effective_model_name} error={error}"
            )
            continue
        except requests.RequestException as error:
            fatal_errors.append(
                f"endpoint={endpoint_name} model_name={effective_model_name} error={error}"
            )
            continue
        except ValueError as error:
            message = (
                f"endpoint={endpoint_name} model_name={effective_model_name} error={error}"
            )
            if str(error) == "response_missing_output_text":
                transient_errors.append(message)
                continue
            fatal_errors.append(message)
            continue

    if transient_errors and not fatal_errors:
        raise RuntimeError("TRANSIENT_API::" + " | ".join(transient_errors))

    all_errors = fatal_errors + transient_errors
    if all_errors:
        raise RuntimeError("FATAL_API::" + " | ".join(all_errors))

    raise RuntimeError("FATAL_API::no_llm_endpoints_available")


def _cap_decision(decision: str, max_decision: str | None) -> str:
    if not max_decision:
        return decision
    return MATCH_DECISION_ORDER[
        min(MATCH_DECISION_RANK[decision], MATCH_DECISION_RANK[max_decision])
    ]


def _append_unique_reason(reasons: list[str], reason: str | None) -> None:
    text = _normalize_text(reason)
    if not text or text in reasons:
        return
    reasons.append(text)


def _parse_structured_value(value):
    if isinstance(value, (dict, list)):
        return value
    text = _normalize_text(value)
    if not text or not (text.startswith("{") or text.startswith("[")):
        return None
    try:
        return json.loads(text)
    except Exception:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None


def _normalize_exp_requirement_text(exp_requirement) -> str:
    parsed = _parse_structured_value(exp_requirement)
    if parsed is not None:
        return _format_exp_requirement_for_discord(parsed)
    text = _normalize_text(exp_requirement)
    return text or "not specified"


def _apply_fit_caps(
    parsed_match: dict,
    *,
    job_title: str,
    jd_text: str,
    candidate_summary: dict,
) -> dict:
    if not isinstance(parsed_match, dict):
        raise ValueError("response_json_not_object")

    fit_score = _coerce_number(parsed_match.get("fit_score"))
    if fit_score is None:
        raise ValueError("response_invalid_fit_score")
    decision = _normalize_match_decision(parsed_match.get("decision"))

    language_check = parsed_match.get("language_check")
    if not isinstance(language_check, dict):
        language_check = {}
    experience_check = parsed_match.get("experience_check")
    if not isinstance(experience_check, dict):
        experience_check = {}

    candidate_years = _coerce_number(candidate_summary.get("candidate_years"))
    candidate_seniority = _normalize_candidate_seniority(
        candidate_summary.get("candidate_seniority"),
        candidate_years=candidate_years,
        fallback_to_years=False,
    )
    required_years = _coerce_number(experience_check.get("required_years"))
    exp_requirement = _normalize_exp_requirement_text(
        parsed_match.get("exp_requirement")
    )
    if required_years is None:
        required_years = _extract_required_years(exp_requirement, job_title, jd_text)

    gap_years = None
    if required_years is not None and candidate_years is not None:
        gap_years = max(0.0, required_years - candidate_years)

    required_seniority = _normalize_required_seniority(
        experience_check.get("seniority_required")
    )
    if required_seniority == "unknown":
        required_seniority = _infer_required_seniority(job_title, jd_text)

    candidate_level = SENIORITY_LEVELS.get(candidate_seniority)
    required_level = SENIORITY_LEVELS.get(required_seniority)
    seniority_gap = None
    if candidate_level is not None and required_level is not None:
        seniority_gap = required_level - candidate_level

    experience_blocker = _coerce_bool(experience_check.get("experience_blocker"))
    reasons = []
    _append_unique_reason(reasons, experience_check.get("reason"))

    score_cap = None
    decision_cap = None

    def _tighten_cap(new_score_cap: int, new_decision_cap: str | None = None) -> None:
        nonlocal score_cap, decision_cap
        score_cap = (
            new_score_cap if score_cap is None else min(score_cap, new_score_cap)
        )
        if new_decision_cap:
            if not decision_cap:
                decision_cap = new_decision_cap
            else:
                decision_cap = _cap_decision(decision_cap, new_decision_cap)

    language_blocker = _coerce_bool(language_check.get("language_blocker"))
    if language_blocker:
        _tighten_cap(40)

    high_seniority_required = required_seniority in {
        "senior",
        "lead",
        "architect",
        "manager",
        "staff",
        "principal",
        "director",
    }

    if gap_years is not None and gap_years >= 5:
        experience_blocker = True
        _tighten_cap(25, "Not Recommended")
        _append_unique_reason(
            reasons,
            f"Required experience exceeds candidate baseline by about {int(gap_years)} years.",
        )
    elif gap_years is not None and gap_years >= 3:
        experience_blocker = True
        _tighten_cap(45, "Weak Fit")
        _append_unique_reason(
            reasons,
            f"Required experience exceeds candidate baseline by about {int(gap_years)} years.",
        )

    if high_seniority_required and seniority_gap is not None and seniority_gap >= 1:
        experience_blocker = True
        _tighten_cap(40, "Weak Fit")
        _append_unique_reason(
            reasons,
            f"Role seniority '{required_seniority}' is above candidate seniority '{candidate_seniority}'.",
        )

    if experience_blocker and decision_cap is None:
        decision_cap = "Weak Fit"
    if experience_blocker and score_cap is None:
        score_cap = 40

    if score_cap is not None:
        fit_score = min(fit_score, float(score_cap))
    fit_score = max(0.0, min(100.0, fit_score))
    decision = _cap_decision(decision, decision_cap)

    severity = _normalize_text(experience_check.get("severity")) or "none"
    if gap_years is not None and gap_years >= 5:
        severity = "severe"
    elif experience_blocker or (gap_years is not None and gap_years >= 3):
        severity = "moderate"

    normalized_match = dict(parsed_match)
    normalized_match["fit_score"] = int(round(fit_score))
    normalized_match["decision"] = decision
    normalized_match["exp_requirement"] = exp_requirement
    normalized_match["candidate_summary"] = candidate_summary
    normalized_match["experience_check"] = {
        **experience_check,
        "required_years": _to_json_number(required_years),
        "candidate_years": _to_json_number(candidate_years),
        "gap_years": _to_json_number(gap_years),
        "seniority_required": required_seniority,
        "candidate_seniority": candidate_seniority,
        "experience_blocker": experience_blocker,
        "reason": "; ".join(reasons)
        if reasons
        else "No material experience blocker detected.",
        "severity": severity,
    }
    return normalized_match


def _build_fit_prompt(
    job_title: str,
    jd_text: str,
    resume_text: str,
    candidate_summary: dict,
    prompt_text: str | None = None,
) -> str:
    template = database._normalize_fit_prompt_text(prompt_text)
    replacements = {
        "{{job_title}}": str(job_title or "").strip() or "not provided",
        "{{job_description}}": str(jd_text or "").strip(),
        "{{candidate_resume}}": str(resume_text or "").strip(),
        "{{candidate_summary}}": json.dumps(candidate_summary, ensure_ascii=False),
    }

    prompt = template
    for placeholder, value in replacements.items():
        prompt = prompt.replace(placeholder, value)

    return prompt


def _request_llm_match(*, request_url: str, api_key: str, model_name: str, prompt: str):
    parsed = _request_llm_json(
        request_url=request_url,
        api_key=api_key,
        model_name=model_name,
        prompt=prompt,
    )
    if "fit_score" not in parsed or "decision" not in parsed:
        raise ValueError("response_missing_fit_fields")
    return parsed


def _parse_llm_match_payload(llm_match) -> dict:
    if isinstance(llm_match, dict):
        return llm_match
    if not llm_match:
        return {}
    try:
        parsed = json.loads(llm_match)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _has_experience_blocker(llm_match) -> bool:
    parsed_match = _parse_llm_match_payload(llm_match)
    experience_check = parsed_match.get("experience_check")
    if not isinstance(experience_check, dict):
        return False
    return _coerce_bool(experience_check.get("experience_blocker"))


def _has_successful_match_payload(job: dict | None) -> bool:
    if not isinstance(job, dict):
        return False
    if job.get("llm_match_error"):
        return False
    parsed_match = _parse_llm_match_payload(job.get("llm_match"))
    return bool(parsed_match)


def _filter_notification_jobs(job_records: list[dict]) -> list[dict]:
    if _is_test_mode_enabled():
        return [
            job for job in (job_records or []) if _has_successful_match_payload(job)
        ]
    return [
        job
        for job in (job_records or [])
        if not _has_experience_blocker(job.get("llm_match"))
    ]


def _format_exp_requirement_for_discord(exp_requirement) -> str:
    if exp_requirement is None:
        return "not specified"
    if isinstance(exp_requirement, dict):
        parts = []
        for key, value in exp_requirement.items():
            text = _normalize_text(value)
            if text is None:
                text = str(value).strip() if value is not None else ""
            if not text:
                continue
            label = str(key).replace("_", " ").strip()
            parts.append(f"{label}: {text}")
        return "; ".join(parts) if parts else "not specified"
    if isinstance(exp_requirement, list):
        parts = [_format_exp_requirement_for_discord(item) for item in exp_requirement]
        parts = [part for part in parts if part and part != "not specified"]
        return "; ".join(parts) if parts else "not specified"

    text = _normalize_text(exp_requirement)
    if not text:
        return "not specified"
    if text.startswith("{") or text.startswith("["):
        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            try:
                parsed = ast.literal_eval(text)
            except Exception:
                parsed = None
        if parsed is not None:
            return _format_exp_requirement_for_discord(parsed)
    return text


def _sort_notification_jobs(job_records: list[dict]) -> list[dict]:
    def _sort_key(job: dict):
        fit_score = _coerce_number((job or {}).get("fit_score"))
        normalized_score = fit_score if fit_score is not None else float("-inf")
        profile_id = _coerce_number((job or {}).get("profile_id"))
        job_id = _normalize_text((job or {}).get("id")) or ""
        return (-normalized_score, profile_id or float("inf"), job_id)

    return sorted(job_records or [], key=_sort_key)


def _send_discord_message(
    content: str,
    *,
    channel_id: str | None = None,
    webhook_url: str | None = None,
):
    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_ID")
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")

    try:
        if bot_token and channel_id:
            url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
            headers = {
                "Authorization": f"Bot {bot_token}",
                "Content-Type": "application/json",
            }
            response = requests.post(
                url, headers=headers, json={"content": content}, timeout=30
            )
        elif webhook_url:
            response = requests.post(webhook_url, json={"content": content}, timeout=30)
        else:
            return False, "Missing channel/webhook configuration for Discord delivery"

        response.raise_for_status()
        return True, None
    except Exception as error:
        return False, str(error)


def _build_discord_job_match_message(job: dict) -> str | None:
    profile_id = (
        int(job.get("profile_id")) if job.get("profile_id") is not None else None
    )
    job_id = job.get("id")
    if profile_id is None or not job_id:
        return None

    title = job.get("title") or "Unknown title"
    company = job.get("company") or "Unknown company"
    fit_score = job.get("fit_score")
    fit_decision = job.get("fit_decision")
    job_url = job.get("job_url") or ""
    exp_requirement = "not specified"
    profile_label = job.get("display_name") or job.get("profile_key") or profile_id

    llm_match = job.get("llm_match")
    if llm_match:
        try:
            parsed = llm_match if isinstance(llm_match, dict) else json.loads(llm_match)
            exp_requirement = _format_exp_requirement_for_discord(
                parsed.get("exp_requirement")
            )
        except Exception:
            pass

    materials_url = materials_launch.maybe_build_materials_launch_url(
        profile_id=profile_id,
        job_id=str(job_id),
    )

    lines = [
        "🎯 Job Match",
        f"Profile: {profile_label}",
        f"ID: {job_id}",
        f"Title: {title}",
        f"Company: {company}",
        f"Decision: {fit_decision}",
        f"Fit Score: {fit_score}",
        f"Exp Requirement: {exp_requirement}",
        f"URL: {job_url}",
    ]
    if materials_url:
        lines.append(f"Materials: {materials_url}")
    return "\n".join(lines)


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["linkedin_fitting_notifier"],
)
def linkedin_fitting_notifier():
    @task
    def claim_fitting_tasks():
        claimed = database.claim_pending_fitting_tasks()
        print(f"Fitting claim summary: claimed={len(claimed or [])}")
        return claimed

    @task
    def match_jobs_with_resume_llm(queue_items, batch_size=5):
        claimed_items = []
        seen_claims = set()
        for item in queue_items or []:
            if not isinstance(item, dict):
                continue
            profile_id = item.get("profile_id")
            job_id = item.get("job_id")
            if profile_id is None or not job_id:
                continue
            claim = {
                "profile_id": int(profile_id),
                "job_id": str(job_id),
                "attempts": int(item.get("attempts") or 0),
            }
            claim_key = _job_ref_key(claim["profile_id"], claim["job_id"])
            if claim_key in seen_claims:
                continue
            seen_claims.add(claim_key)
            claimed_items.append(claim)

        if not claimed_items:
            return _build_match_task_result([])

        print(
            f"Fitting stage progress: done=0 total={len(claimed_items)} queued={len(claimed_items)}"
        )

        load_env(override_if_missing=True)
        matched_jobs = []
        requeue_job_errors = {}
        api_error_messages = []
        resume_cache = {}
        candidate_summary_cache = {}
        profile_summary_errors = {}
        finalize_counts = {"done": 0, "failed": 0, "requeued": 0}
        finalized_item_keys = set()
        max_attempts = int(os.getenv("FITTING_MAX_ATTEMPTS", "3"))
        concurrency = max(
            1, int(os.getenv("FITTING_CONCURRENCY", str(batch_size or 5)))
        )

        def _persist_job_result(job_result: dict) -> None:
            save_df = pd.DataFrame([job_result])
            database.save_llm_matches(
                save_df[["profile_id", "job_id", "llm_match", "llm_match_error"]]
            )

        def _finalize_job_result(
            item: dict,
            job_result: dict | None = None,
            *,
            requeue_error: str | None = None,
        ) -> None:
            profile_id = int(item["profile_id"])
            job_id = str(item["job_id"])
            item_key = _job_ref_key(profile_id, job_id)
            if item_key in finalized_item_keys:
                return
            attempts = int(item.get("attempts") or 0)
            if requeue_error is not None:
                database.requeue_fitting_task(profile_id, job_id, error=requeue_error)
                finalized_item_keys.add(item_key)
                finalize_counts["requeued"] += 1
                return

            if (
                job_result
                and job_result.get("llm_match")
                and not job_result.get("llm_match_error")
            ):
                database.mark_fitting_done(profile_id, job_id)
                finalized_item_keys.add(item_key)
                finalize_counts["done"] += 1
                return

            error = "missing_llm_match"
            if job_result and job_result.get("llm_match_error"):
                error = str(job_result["llm_match_error"])
            retry = (attempts + 1) < max_attempts
            database.mark_fitting_failed(
                profile_id,
                job_id,
                error=error,
                retry=retry,
            )
            finalized_item_keys.add(item_key)
            finalize_counts["failed"] += 1

        def _default_prepared_model_name(prepared: dict) -> str:
            profile_record = prepared.get("profile_record") or {}
            return profile_record.get("model_name") or os.getenv(
                "FITTING_MODEL_NAME", "gpt-5.4"
            )

        def _record_transient_api_error(
            item,
            message: str,
            model_name: str | None = None,
        ):
            claim_key = _job_ref_key(item["profile_id"], item["job_id"])
            error_message = str(message)
            requeue_job_errors[claim_key] = error_message
            api_error_messages.append(error_message)
            _finalize_job_result(item, requeue_error=error_message)
            print(
                f"llm_result profile_id={item['profile_id']} job_id={item['job_id']} "
                f"model_name={model_name or 'unknown'} status=api_error_requeue error={error_message}"
            )

        def _process_single_item(prepared: dict) -> tuple[dict | None, dict | None]:
            item = prepared["item"]
            profile_id = item["profile_id"]
            job_id = item["job_id"]
            profile_record = prepared["profile_record"]
            job_title = prepared["job_title"]
            jd_text = prepared["jd_text"]
            resume_text = prepared["resume_text"]
            candidate_summary = prepared["candidate_summary"]

            base_prompt = _build_fit_prompt(
                job_title,
                jd_text,
                resume_text,
                candidate_summary,
                prompt_text=profile_record.get("fit_prompt_config"),
            )
            model_name = profile_record.get("model_name") or os.getenv(
                "FITTING_MODEL_NAME", "gpt-5.4"
            )

            parsed = None
            last_error = None
            used_model_name = model_name
            for attempt in range(3):
                prompt = base_prompt
                if attempt > 0:
                    prompt += " Previous output was invalid JSON. Return strictly valid JSON only."

                try:
                    parsed, used_model_name = _request_llm_json_with_fallback(
                        endpoints=llm_endpoints,
                        model_name=model_name,
                        prompt=prompt,
                        return_metadata=True,
                    )
                    parsed = _apply_fit_caps(
                        parsed,
                        job_title=job_title,
                        jd_text=jd_text,
                        candidate_summary=candidate_summary,
                    )
                    break
                except RuntimeError as error:
                    error_message = str(error)
                    if error_message.startswith("TRANSIENT_API::") and attempt < 2:
                        retry_model_name = _summarize_logged_model_names(
                            error_message,
                            model_name,
                        )
                        print(
                            f"llm_result profile_id={profile_id} job_id={job_id} "
                            f"model_name={retry_model_name} status=api_retry attempt={attempt + 1}/3 error={_strip_prefix(error_message, 'TRANSIENT_API::')}"
                        )
                        time.sleep(min(2**attempt, 4))
                        continue
                    raise
                except Exception as error:
                    last_error = str(error)

            if parsed is not None:
                return item, _build_job_match_result(
                    profile_id,
                    job_id,
                    llm_match=json.dumps(parsed, ensure_ascii=False),
                    model_name=used_model_name,
                )

            return item, _build_job_match_result(
                profile_id,
                job_id,
                llm_match_error=last_error or "invalid_json_response",
                model_name=used_model_name,
            )

        def _return_api_error(
            message: str,
            partial_results=None,
            retry_items=None,
            retry_job_errors=None,
            stopped_early: bool = True,
        ):
            error_message = str(message)
            partial_results = list(partial_results or [])
            processed_keys = {
                _job_ref_key(row["profile_id"], row["job_id"])
                for row in partial_results
                if row.get("profile_id") is not None and row.get("job_id")
            }
            retry_items = [
                item
                for item in _iter_job_refs(retry_items)
                if _job_ref_key(item["profile_id"], item["job_id"])
                not in processed_keys
            ]
            unprocessed_items = [
                item
                for item in claimed_items
                if _job_ref_key(item["profile_id"], item["job_id"])
                not in processed_keys
            ]
            requeue_items = []
            seen_keys = set()
            for item in retry_items + unprocessed_items:
                claim_key = _job_ref_key(item["profile_id"], item["job_id"])
                if claim_key in seen_keys:
                    continue
                seen_keys.add(claim_key)
                requeue_items.append(item)
            print(f"LLM API error detected, stop early: {error_message}")
            merged_requeue_job_errors = {}
            for item_key, job_error in (retry_job_errors or {}).items():
                if item_key in seen_keys:
                    merged_requeue_job_errors[item_key] = str(job_error)
            for item in requeue_items:
                claim_key = _job_ref_key(item["profile_id"], item["job_id"])
                merged_requeue_job_errors.setdefault(claim_key, error_message)
            for item in requeue_items:
                claim_key = _job_ref_key(item["profile_id"], item["job_id"])
                _finalize_job_result(
                    item,
                    requeue_error=merged_requeue_job_errors.get(
                        claim_key, error_message
                    ),
                )
            return _build_match_task_result(
                partial_results,
                api_error=True,
                api_error_message=error_message,
                requeue_items=requeue_items,
                requeue_job_errors=merged_requeue_job_errors,
                stopped_early=stopped_early,
                persisted_immediately=True,
                finalize_counts=finalize_counts,
            )

        try:
            llm_endpoints = _parse_llm_endpoints_from_env()
        except Exception as error:
            error_message = str(error)
            print(f"LLM API error detected before processing jobs: {error_message}")
            return _return_api_error(error_message, retry_items=claimed_items)

        if not llm_endpoints:
            error_message = "missing_llm_endpoints"
            print(f"LLM API error detected before processing jobs: {error_message}")
            return _return_api_error(error_message, retry_items=claimed_items)

        profile_ids = sorted({item["profile_id"] for item in claimed_items})
        profiles_df = database.get_profiles_by_ids(profile_ids)
        required_profile_columns = [
            "id",
            "profile_key",
            "display_name",
            "resume_path",
            "resume_text",
            "candidate_summary_config",
            "fit_prompt_config",
            "discord_channel_id",
            "discord_webhook_url",
            "model_name",
        ]
        for col in required_profile_columns:
            if col not in profiles_df.columns:
                profiles_df[col] = None
        safe_profiles_df = (
            profiles_df[required_profile_columns]
            .astype(object)
            .where(pd.notna(profiles_df[required_profile_columns]), None)
        )
        profiles_by_id = {
            int(record["id"]): record
            for record in safe_profiles_df.to_dict(orient="records")
            if record.get("id") is not None
        }

        for profile_id in profile_ids:
            profile_record = profiles_by_id.get(profile_id)
            if not profile_record:
                continue

            if profile_id not in resume_cache:
                resume_cache[profile_id] = _load_resume_text(
                    resume_path=profile_record.get("resume_path"),
                    resume_text=profile_record.get("resume_text"),
                )
            resume_text, resume_error = resume_cache[profile_id]
            if not resume_text:
                profile_summary_errors[profile_id] = resume_error or "resume_read_error"
                continue

            try:
                candidate_summary_cache[profile_id] = _load_candidate_summary_config(
                    profile_record.get("candidate_summary_config")
                )
            except Exception as error:
                profile_summary_errors[profile_id] = (
                    f"candidate_summary_config_error profile_id={profile_id} error={error}"
                )
                print(profile_summary_errors[profile_id])
                continue

        job_ids = sorted({item["job_id"] for item in claimed_items})
        jobs_df = database.get_jobs_by_ids(job_ids)
        if jobs_df is None or jobs_df.empty:
            error_results = _build_uniform_error_results(
                claimed_items,
                "missing_job_record",
            )
            for job_result in error_results:
                _persist_job_result(job_result)
            for item, job_result in zip(claimed_items, error_results):
                _finalize_job_result(item, job_result)
            for job_result in error_results:
                _log_job_match_result(job_result)
            return _build_match_task_result(
                error_results,
                persisted_immediately=True,
                finalize_counts=finalize_counts,
            )

        required_job_columns = ["id", "title", "description"]
        for col in required_job_columns:
            if col not in jobs_df.columns:
                jobs_df[col] = None

        safe_jobs_df = (
            jobs_df[required_job_columns]
            .astype(object)
            .where(pd.notna(jobs_df[required_job_columns]), None)
        )
        jobs_by_id = {
            str(record["id"]): record
            for record in safe_jobs_df.to_dict(orient="records")
            if record.get("id")
        }

        prepared_items = []
        for sequence_index, item in enumerate(claimed_items):
            profile_id = item["profile_id"]
            job_id = item["job_id"]
            profile_record = profiles_by_id.get(profile_id)
            if not profile_record:
                prepared_items.append(
                    {
                        "sequence_index": sequence_index,
                        "item": item,
                        "ready": False,
                        "job_result": _build_job_match_result(
                            profile_id,
                            job_id,
                            llm_match_error="missing_profile_record",
                        ),
                    }
                )
                continue

            job_record = jobs_by_id.get(job_id)
            if not job_record:
                prepared_items.append(
                    {
                        "sequence_index": sequence_index,
                        "item": item,
                        "ready": False,
                        "job_result": _build_job_match_result(
                            profile_id,
                            job_id,
                            llm_match_error="missing_job_record",
                        ),
                    }
                )
                continue

            resume_text, resume_error = resume_cache.get(profile_id, (None, None))
            if not resume_text:
                prepared_items.append(
                    {
                        "sequence_index": sequence_index,
                        "item": item,
                        "ready": False,
                        "job_result": _build_job_match_result(
                            profile_id,
                            job_id,
                            llm_match_error=resume_error or "resume_read_error",
                        ),
                    }
                )
                continue

            if profile_id in profile_summary_errors:
                prepared_items.append(
                    {
                        "sequence_index": sequence_index,
                        "item": item,
                        "ready": False,
                        "job_result": _build_job_match_result(
                            profile_id,
                            job_id,
                            llm_match_error=profile_summary_errors[profile_id],
                        ),
                    }
                )
                continue

            candidate_summary = candidate_summary_cache.get(profile_id)
            if not candidate_summary:
                prepared_items.append(
                    {
                        "sequence_index": sequence_index,
                        "item": item,
                        "ready": False,
                        "job_result": _build_job_match_result(
                            profile_id,
                            job_id,
                            llm_match_error="missing_candidate_summary_config",
                        ),
                    }
                )
                continue

            job_title = job_record.get("title") or ""
            jd_text = job_record.get("description") or ""
            if not jd_text.strip():
                prepared_items.append(
                    {
                        "sequence_index": sequence_index,
                        "item": item,
                        "ready": False,
                        "job_result": _build_job_match_result(
                            profile_id,
                            job_id,
                            llm_match_error="missing_job_description",
                        ),
                    }
                )
                continue

            prepared_items.append(
                {
                    "sequence_index": sequence_index,
                    "item": item,
                    "ready": True,
                    "profile_record": profile_record,
                    "job_title": job_title,
                    "jd_text": jd_text,
                    "resume_text": resume_text,
                    "candidate_summary": candidate_summary,
                }
            )

        for prepared in prepared_items:
            if prepared["ready"]:
                continue
            item = prepared["item"]
            job_result = prepared["job_result"]
            matched_jobs.append(job_result)
            _persist_job_result(job_result)
            _finalize_job_result(item, job_result)
            _log_job_match_result(job_result)

        ready_items = [prepared for prepared in prepared_items if prepared["ready"]]

        print(
            f"Starting LLM fitting with concurrency={concurrency} jobs={len(ready_items)}"
        )
        def _handle_completed_event(event):
            item = event["prepared"]["item"]
            if event["kind"] == "transient_api_error":
                _record_transient_api_error(
                    item,
                    event["error_message"],
                    event["model_name"],
                )
                return

            job_result = event["job_result"]
            matched_jobs.append(job_result)
            _persist_job_result(job_result)
            _finalize_job_result(item, job_result)
            _log_job_match_result(job_result)

        fatal_events = _execute_prepared_fitting_items(
            ready_items,
            concurrency=concurrency,
            process_single_item=_process_single_item,
            default_model_name_fn=_default_prepared_model_name,
            handle_completed_event=_handle_completed_event,
        )

        if fatal_events:
            fatal_messages = []
            fatal_retry_job_errors = dict(requeue_job_errors)
            fatal_retry_items = []
            for fatal_event in fatal_events:
                item = fatal_event["prepared"]["item"]
                fatal_error = fatal_event["error_message"]
                fatal_messages.append(fatal_error)
                fatal_retry_items.append(item)
                fatal_retry_job_errors[
                    _job_ref_key(item["profile_id"], item["job_id"])
                ] = fatal_error
                print(
                    f"llm_result profile_id={item['profile_id']} job_id={item['job_id']} "
                    f"model_name={fatal_event['model_name']} status=api_error error={fatal_error}"
                )

            return _return_api_error(
                _summarize_api_errors(fatal_messages) or fatal_messages[0],
                matched_jobs,
                retry_items=fatal_retry_items,
                retry_job_errors=fatal_retry_job_errors,
            )

        api_error_message = _summarize_api_errors(api_error_messages)
        requeue_items = [
            item
            for item in claimed_items
            if _job_ref_key(item["profile_id"], item["job_id"]) in requeue_job_errors
        ]
        print(
            "Inline fitting persistence summary: "
            f"processed={len(matched_jobs)} "
            f"done={finalize_counts['done']} "
            f"failed={finalize_counts['failed']} "
            f"requeued={finalize_counts['requeued']}"
        )
        print(
            "Fitting stage progress: "
            f"done={finalize_counts['done'] + finalize_counts['failed'] + finalize_counts['requeued']} "
            f"total={len(claimed_items)} "
            f"successful={finalize_counts['done']} failed={finalize_counts['failed']} requeued={finalize_counts['requeued']}"
        )
        return _build_match_task_result(
            matched_jobs,
            api_error=bool(requeue_job_errors),
            api_error_message=api_error_message,
            requeue_items=requeue_items,
            requeue_job_errors=requeue_job_errors,
            stopped_early=False,
            persisted_immediately=True,
            finalize_counts=finalize_counts,
        )

    @task.branch
    def branch_after_llm_match(match_result):
        (
            jobs,
            api_error,
            api_error_message,
            requeue_items,
            _,
            stopped_early,
            _,
            _,
        ) = _unwrap_match_task_result(match_result)
        if api_error:
            mode = "stop_early" if stopped_early else "continue_with_requeue"
            print(
                f"LLM API error detected after processing {len(jobs)} jobs. "
                f"requeued_jobs={len(requeue_items)} mode={mode} "
                f"Branching to notify + finalize path: {api_error_message}"
            )
            return ["notify_llm_api_error", "store_fitting_results"]
        print(f"LLM matching finished without API error. jobs={len(jobs)}")
        return "store_fitting_results"

    @task
    def store_fitting_results(match_result):
        (
            jobs_with_match_records,
            api_error,
            _,
            _,
            _,
            _,
            persisted_immediately,
            _,
        ) = _unwrap_match_task_result(match_result)
        jobs_with_match_records = jobs_with_match_records or []
        if not jobs_with_match_records:
            return 0

        if persisted_immediately:
            success_count = sum(
                1
                for row in jobs_with_match_records
                if row.get("llm_match") and not row.get("llm_match_error")
            )
            error_count = len(jobs_with_match_records) - success_count
            print(
                f"LLM results already persisted inline: total={len(jobs_with_match_records)} "
                f"success={success_count} error={error_count}"
            )
            if not api_error:
                for profile_id in {
                    int(row.get("profile_id"))
                    for row in jobs_with_match_records
                    if row.get("profile_id") is not None
                }:
                    database.resolve_active_alert(f"{LLM_API_ALERT_KEY}:{profile_id}")
            return len(jobs_with_match_records)

        save_df = pd.DataFrame(jobs_with_match_records)
        for column in ["profile_id", "job_id", "llm_match", "llm_match_error"]:
            if column not in save_df.columns:
                save_df[column] = None

        database.save_llm_matches(
            save_df[["profile_id", "job_id", "llm_match", "llm_match_error"]]
        )

        success_count = sum(
            1
            for row in jobs_with_match_records
            if row.get("llm_match") and not row.get("llm_match_error")
        )
        error_count = len(jobs_with_match_records) - success_count
        print(
            f"Persisted LLM results: total={len(jobs_with_match_records)} "
            f"success={success_count} error={error_count}"
        )
        if not api_error:
            for profile_id in {
                int(row.get("profile_id"))
                for row in jobs_with_match_records
                if row.get("profile_id") is not None
            }:
                database.resolve_active_alert(f"{LLM_API_ALERT_KEY}:{profile_id}")
        return len(jobs_with_match_records)

    @task
    def notify_llm_api_error(match_result):
        (
            jobs_with_match_records,
            api_error,
            api_error_message,
            requeue_items,
            _,
            stopped_early,
            _,
            _,
        ) = _unwrap_match_task_result(match_result)
        if not api_error:
            return {"alert_sent": False, "affected_jobs": 0}

        load_env()
        affected_profile_ids = sorted(
            {
                int(item["profile_id"])
                for item in list(_iter_job_refs(jobs_with_match_records))
                + list(_iter_job_refs(requeue_items))
            }
        )
        profiles_df = database.get_profiles_by_ids(affected_profile_ids)
        profiles_by_id = {
            int(record["id"]): record
            for record in profiles_df.to_dict(orient="records")
            if record.get("id") is not None
        }

        alert_sent = 0
        alert_suppressed = 0
        alert_failed = 0
        for profile_id in affected_profile_ids:
            alert_key = f"{LLM_API_ALERT_KEY}:{profile_id}"
            should_send = database.should_send_active_alert(
                alert_key,
                error=api_error_message or "llm_api_error",
            )
            if not should_send:
                alert_suppressed += 1
                print(
                    f"Suppressed duplicate LLM API alert for active outage profile_id={profile_id}."
                )
                continue

            profile_record = profiles_by_id.get(profile_id, {})
            profile_label = (
                profile_record.get("display_name")
                or profile_record.get("profile_key")
                or f"profile_{profile_id}"
            )
            processed_count = sum(
                1
                for row in jobs_with_match_records or []
                if int(row.get("profile_id") or -1) == profile_id
            )
            requeued_count = sum(
                1
                for row in _iter_job_refs(requeue_items)
                if row["profile_id"] == profile_id
            )
            alert_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            processed_jobs_label = (
                "Processed Jobs Before Stop"
                if stopped_early
                else "Processed Jobs This Run"
            )
            action_message = (
                "Action: fitting DAG stopped early and requeued pending jobs for the next trigger."
                if stopped_early
                else "Action: fitting DAG continued; transient API failures were requeued for the next trigger."
            )
            message = (
                "⚠️ LLM API Alert\n"
                f"Profile: {profile_label}\n"
                f"Time: {alert_time}\n"
                f"Error: {api_error_message or 'llm_api_error'}\n"
                f"{processed_jobs_label}: {processed_count}\n"
                f"Requeued Jobs: {requeued_count}\n"
                f"{action_message}"
            )
            sent, alert_error = _send_discord_message(
                message,
                channel_id=profile_record.get("discord_channel_id"),
                webhook_url=profile_record.get("discord_webhook_url"),
            )
            if sent:
                alert_sent += 1
            else:
                alert_failed += 1
                print(
                    f"Failed to send LLM API alert profile_id={profile_id}: {alert_error}"
                )
                database.resolve_active_alert(alert_key)

        return {
            "alert_sent": alert_sent,
            "alert_suppressed": alert_suppressed,
            "alert_failed": alert_failed,
            "affected_profiles": len(affected_profile_ids),
            "affected_jobs": len(jobs_with_match_records or []),
            "requeued_jobs": len(requeue_items or []),
            "stopped_early": stopped_early,
        }

    @task
    def finalize_fitting_queue(queue_items, match_result):
        (
            jobs_with_match_records,
            api_error,
            api_error_message,
            requeue_items,
            requeue_job_errors,
            _,
            persisted_immediately,
            finalize_counts,
        ) = _unwrap_match_task_result(match_result)
        if not queue_items:
            return {"done": 0, "failed": 0}

        if persisted_immediately:
            print(
                "Inline fitting finalization already completed: "
                f"done={int(finalize_counts.get('done') or 0)} "
                f"failed={int(finalize_counts.get('failed') or 0)} "
                f"requeued={int(finalize_counts.get('requeued') or 0)}"
            )
            return {
                "done": int(finalize_counts.get("done") or 0),
                "failed": int(finalize_counts.get("failed") or 0),
                "requeued": int(finalize_counts.get("requeued") or 0),
            }

        max_attempts = int(os.getenv("FITTING_MAX_ATTEMPTS", "3"))

        results = {}
        for row in jobs_with_match_records or []:
            profile_id = row.get("profile_id")
            job_id = row.get("job_id")
            if profile_id is None or not job_id:
                continue
            results[_job_ref_key(profile_id, job_id)] = {
                "llm_match": row.get("llm_match"),
                "llm_match_error": row.get("llm_match_error"),
            }

        done = 0
        failed = 0
        requeued = 0
        default_error = api_error_message or "missing_llm_match"
        requeue_item_keys = {
            _job_ref_key(item["profile_id"], item["job_id"])
            for item in _iter_job_refs(requeue_items)
        }
        for item in queue_items:
            profile_id = item.get("profile_id")
            job_id = str(item.get("job_id")) if item.get("job_id") else None
            if profile_id is None or not job_id:
                continue
            profile_id = int(profile_id)
            attempts = int(item.get("attempts") or 0)
            item_key = _job_ref_key(profile_id, job_id)
            result = results.get(item_key)
            if result and result.get("llm_match") and not result.get("llm_match_error"):
                database.mark_fitting_done(profile_id, job_id)
                done += 1
            elif item_key in requeue_item_keys:
                database.requeue_fitting_task(
                    profile_id,
                    job_id,
                    error=requeue_job_errors.get(item_key, default_error),
                )
                requeued += 1
            else:
                error = default_error if api_error else "missing_llm_match"
                if result and result.get("llm_match_error"):
                    error = result["llm_match_error"]
                retry = (attempts + 1) < max_attempts
                database.mark_fitting_failed(
                    profile_id,
                    job_id,
                    error=error,
                    retry=retry,
                )
                failed += 1

        return {"done": done, "failed": failed, "requeued": requeued}

    @task
    def select_jobs_for_notification():
        jobs_df = database.get_jobs_to_notify()
        print(f"Jobs eligible for notification: {len(jobs_df)}")
        if jobs_df is None or jobs_df.empty:
            return []

        notify_columns = [
            "profile_id",
            "profile_key",
            "display_name",
            "discord_channel_id",
            "discord_webhook_url",
            "id",
            "title",
            "company",
            "fit_score",
            "fit_decision",
            "job_url",
            "llm_match",
            "llm_match_error",
        ]
        for col in notify_columns:
            if col not in jobs_df.columns:
                jobs_df[col] = None

        eligible_records = df_to_xcom_records(jobs_df[notify_columns])
        filtered_records = _filter_notification_jobs(eligible_records)
        suppressed_count = len(eligible_records) - len(filtered_records)
        if suppressed_count > 0:
            if _is_test_mode_enabled():
                print(
                    f"Suppressed {suppressed_count} notification candidates because they lacked a successful fit payload in test mode."
                )
            else:
                print(
                    f"Suppressed {suppressed_count} notification candidates due to experience_blocker."
                )
        return filtered_records

    @task
    def notify_discord(jobs_to_notify):
        import json
        from datetime import datetime

        load_env()

        jobs_to_notify = _sort_notification_jobs(jobs_to_notify or [])
        eligible = len(jobs_to_notify)
        sent = 0
        failed = 0
        summary_sent = 0
        profile_summaries = {}

        for job in jobs_to_notify:
            profile_id = (
                int(job.get("profile_id"))
                if job.get("profile_id") is not None
                else None
            )
            job_id = job.get("id")
            profile_label = (
                job.get("display_name") or job.get("profile_key") or profile_id
            )

            if profile_id is None or not job_id:
                continue

            profile_summary = profile_summaries.setdefault(
                profile_id,
                {
                    "eligible": 0,
                    "sent": 0,
                    "failed": 0,
                    "display_name": profile_label,
                    "channel_id": job.get("discord_channel_id"),
                    "webhook_url": job.get("discord_webhook_url"),
                },
            )
            profile_summary["eligible"] += 1

            message = _build_discord_job_match_message(job)
            if not message:
                continue

            ok, error = _send_discord_message(
                message,
                channel_id=job.get("discord_channel_id"),
                webhook_url=job.get("discord_webhook_url"),
            )
            if ok:
                database.mark_job_notified(profile_id, job_id, status="sent")
                sent += 1
                profile_summary["sent"] += 1
            else:
                database.mark_job_notified(
                    profile_id,
                    job_id,
                    status="failed",
                    error=error,
                )
                failed += 1
                profile_summary["failed"] += 1
            time.sleep(1)

        summary_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for profile_id, profile_summary in profile_summaries.items():
            summary_message = (
                "━━━━━━━━━━━━━━━━━━━━\n"
                "📌 Fitting Notification Summary\n"
                f"Profile: {profile_summary['display_name']}\n"
                f"Time: {summary_time}\n"
                f"Eligible: {profile_summary['eligible']}\n"
                f"Sent: {profile_summary['sent']}\n"
                f"Failed: {profile_summary['failed']}\n"
                "━━━━━━━━━━━━━━━━━━━━"
            )

            delivered, summary_error = _send_discord_message(
                summary_message,
                channel_id=profile_summary.get("channel_id"),
                webhook_url=profile_summary.get("webhook_url"),
            )
            if delivered:
                summary_sent += 1
            else:
                print(
                    f"Failed to send summary message profile_id={profile_id}: {summary_error}"
                )

        return {
            "eligible": eligible,
            "sent": sent,
            "failed": failed,
            "summary_sent": summary_sent,
        }

    queue_items_task = claim_fitting_tasks()
    match_result_task = match_jobs_with_resume_llm(queue_items_task)
    branch_task = branch_after_llm_match(match_result_task)
    store_result_task = store_fitting_results(match_result_task)
    notify_llm_api_error_task = notify_llm_api_error(match_result_task)
    finalize_result_task = finalize_fitting_queue(queue_items_task, match_result_task)

    branch_task >> [notify_llm_api_error_task, store_result_task]
    store_result_task >> finalize_result_task

    jobs_to_notify_task = select_jobs_for_notification()
    finalize_result_task >> jobs_to_notify_task
    notify_discord(jobs_to_notify_task)


linkedin_fitting_notifier()
