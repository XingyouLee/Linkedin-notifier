from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from dags.runtime_utils import runtime_bool

JOB_REQUIRED_COLUMNS = ["id", "site", "job_url", "title", "company", "source_job_id"]
JD_QUEUE_COLUMNS = ["id", "job_url"]
LLM_RESULT_COLUMNS = ["id", "llm_match", "llm_match_error"]
PROFILE_JOB_COLUMNS = ["profile_id", "job_id", "search_config_id", "matched_term"]
PROFILE_JOB_RESULT_COLUMNS = ["profile_id", "job_id", "llm_match", "llm_match_error"]
SCHEMA_LOCK_KEY = 620240319001
DEFAULT_PROFILE_SOURCE_SIGNATURE = ("__default_profile__", 0, 0)
USER_INFO_DIR = Path(__file__).resolve().parent.parent / "user_info"
INCLUDE_USER_INFO_DIR = Path(__file__).resolve().parent.parent / "include" / "user_info"
BOOTSTRAP_EXISTING_JOBS_KEY = "existing_jobs_v1"
LINKEDIN_TEST_MODE_ENV_VAR = "LINKEDIN_TEST_MODE"
TEST_JOB_ID_PREFIX = "test-"
_SCHEMA_INITIALIZED = False
_PROFILE_SOURCE_SIGNATURE: tuple[str, int, int] | None = None
TEXT_RESUME_SUFFIXES = {".md", ".txt"}
MATCHER_CUTOVER_LABEL = "notifier->resume-matcher split"
DEFAULT_LINKEDIN_GEO_IDS = {
    "netherlands": "102890719",
}

DEFAULT_TITLE_EXCLUDE_KEYWORDS = ["senior", "medior"]
DEFAULT_COMPANY_BLACKLIST = [
    "Elevation Group",
    "Capgemini",
    "Jobster",
    "Sogeti",
    "CGI Nederland",
    "Mercor",
]

DEFAULT_FIT_PROMPT_TEXT = (
    "If output is not valid JSON, regenerate until valid. "
    "Do not include markdown. Do not include trailing commas. "
    "You are a strict job-fit evaluator for hiring in the Netherlands. "
    "Your priority is to reduce false positives. "
    "Prefer a false negative over a false positive when years of experience, seniority, ownership scope, language requirements, or core must-have skills are not convincingly aligned. "
    "Use the provided candidate summary as the canonical baseline for candidate_years, candidate_seniority, target role direction, core strengths, obvious gaps, and language signals. "
    "The raw resume remains the factual evidence source, but it must not be used to inflate the candidate above the candidate summary baseline. "
    "Do not convert internships, academic work, side projects, coursework, part-time school work, or short project exposure into multiple years of full-time industry seniority. "
    "Treat the job title as part of the requirement context because seniority, language, and scope requirements often appear in the title or metadata rather than in a clean JD paragraph. "
    "Read the entire supplied text carefully, including title-like fragments, metadata-like fragments, bullet lists, and short snippets. "
    "If the supplied job text contains any explicit year range or seniority cue anywhere, extract it. "
    "If the supplied job text is vague, sparse, or ambiguous, do not invent a favorable interpretation. "
    "Uncertainty should make you more conservative, not more optimistic. "
    "Decision policy: "
    "1. If the job explicitly requires Dutch or fluent Dutch, set language_blocker = true and score harshly. "
    "2. Be conservative on years of experience and seniority. "
    "3. Extract both required_years and seniority_required whenever the title or JD provides any credible signal. "
    "4. Titles such as Senior, Lead, Staff, Principal, Architect, Manager, Director, and Mid-Senior should be treated as meaningful requirement signals unless the JD explicitly contradicts them. "
    "5. Generic titles such as Software Engineer, Developer, Engineer, or Python Developer are not enough for a favorable result unless the responsibilities and scope clearly match the candidate. "
    "6. Required skills, years of experience, seniority, language requirements, ownership scope, and domain relevance matter more than superficial keyword overlap. "
    "7. If the JD asks for 4+ years, strong independent ownership, mentoring, architecture responsibility, or cross-team technical leadership, be very skeptical for an early-career candidate. "
    "8. If the JD asks for 5+ years, senior ownership, lead-level accountability, or platform or architecture leadership, a favorable recommendation should be rare and requires unusually strong contradictory evidence in the JD itself. "
    "9. Before finalizing a Strong Fit or Moderate Fit, actively audit for false-positive risk: unaddressed experience gap, seniority mismatch, Dutch requirement, vague title-only overlap, or missing must-have skills. If any of those remain material, downgrade. "
    "exp_requirement must be a single plain-English line, never a JSON object, array, Python dict, or key-value dump. "
    "It should summarize the best evidence you found about JD experience and seniority requirements, for example explicit years, senior title, or that the JD does not state a reliable requirement. "
    "If not explicitly stated, set exp_requirement to a short plain-English sentence such as 'No explicit years requirement found; title and scope are the main signals.' "
    "Return ONLY valid JSON. No explanations outside JSON. No markdown. No comments. "
    "JSON structure: "
    "{"
    '"fit_score": 0-100, '
    '"decision": "Strong Fit | Moderate Fit | Weak Fit | Not Recommended", '
    '"exp_requirement": "one-line JD experience requirement", '
    '"candidate_summary": {"summary": "2-4 concise lines", "target_roles": [], "candidate_years": number, "candidate_seniority": "entry | junior | mid | senior | lead | staff | principal | architect | manager | director | unknown", "core_skills": [], "obvious_gaps": [], "language_signals": {"dutch_level": "none | basic | working | fluent | unknown", "english_level": "none | basic | working | fluent | unknown", "notes": "short explanation"}}, '
    '"language_check": {"dutch_required": true/false, "language_blocker": true/false, "impact": "short explanation"}, '
    '"experience_check": {"required_years": number|null, "candidate_years": number|null, "gap_years": number|null, "seniority_required": "entry | junior | mid | senior | lead | staff | principal | architect | manager | director | unknown", "candidate_seniority": "entry | junior | mid | senior | lead | staff | principal | architect | manager | director | unknown", "experience_blocker": true/false, "reason": "short explanation", "severity": "none | minor | moderate | severe"}, '
    '"skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []}, '
    '"risk_factors": [], '
    '"summary": "3-5 concise lines"'
    "} "
    "Candidate Summary: <<<{{candidate_summary}}>>> "
    "Job Title: <<<{{job_title}}>>> "
    "Job Description: <<<{{job_description}}>>> "
    "Candidate Resume: <<<{{candidate_resume}}>>>"
)


def _get_positive_int_env(var_name: str, default: int) -> int:
    try:
        value = int(os.getenv(var_name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(1, value)


def _coerce_nonnegative_int(value, default: int) -> int:
    try:
        coerced = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, coerced)


def _default_linkedin_geo_id(location) -> str | None:
    normalized_location = str(location or "").strip().lower()
    if not normalized_location:
        return None
    return DEFAULT_LINKEDIN_GEO_IDS.get(normalized_location)


def _coerce_geo_id(value) -> str | None:
    if value is None:
        return None
    normalized_value = str(value).strip()
    return normalized_value or None


def _resolve_db_url() -> str:
    env_db_url = os.getenv("JOBS_DB_URL", "").strip()
    if env_db_url:
        return env_db_url

    raise ValueError("JOBS_DB_URL must be set for the business database connection")


def get_db_url() -> str:
    return _resolve_db_url()


def _connect(row_factory=None, autocommit: bool = False):
    return psycopg.connect(get_db_url(), row_factory=row_factory, autocommit=autocommit)


def _ensure_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> pd.DataFrame:
    normalized_df = df.copy()
    for col in required_columns:
        if col not in normalized_df.columns:
            normalized_df[col] = None
    return normalized_df


def _coerce_fit_score(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if not score == score:
        return None
    return int(round(max(0.0, min(100.0, score))))


def _coerce_fit_decision(value) -> Optional[str]:
    decision = str(value or "").strip()
    return decision or None


def _extract_fit_fields(
    llm_match: Optional[str],
) -> tuple[Optional[int], Optional[str]]:
    if not llm_match:
        return None, None
    try:
        parsed = json.loads(llm_match)
        if not isinstance(parsed, dict):
            return None, None
        return (
            _coerce_fit_score(parsed.get("fit_score")),
            _coerce_fit_decision(parsed.get("decision")),
        )
    except Exception:
        return None, None


def _default_resume_path_candidates() -> list[str]:
    dags_dir = Path(__file__).resolve().parent
    return [
        str((dags_dir / "resume.md").resolve()),
        str((dags_dir.parent / "resume.md").resolve()),
    ]


def _resolve_default_resume_path() -> Optional[str]:
    configured_path = os.getenv("RESUME_PATH")
    candidates = [configured_path] if configured_path else []
    candidates.extend(_default_resume_path_candidates())
    for candidate in candidates:
        if not candidate:
            continue
        resolved = Path(candidate).expanduser().resolve()
        if resolved.exists():
            return str(resolved)
    return configured_path


def _parse_terms(terms_input) -> list[str]:
    if isinstance(terms_input, str):
        terms = [term.strip() for term in terms_input.split(",") if term.strip()]
    else:
        terms = [str(term).strip() for term in (terms_input or []) if str(term).strip()]
    return terms


def _coerce_bool(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False
    return default


def is_test_mode_enabled() -> bool:
    return runtime_bool(LINKEDIN_TEST_MODE_ENV_VAR, False)


def _profile_mode_clause(alias: str = "p") -> str:
    expected_flag = "TRUE" if is_test_mode_enabled() else "FALSE"
    return (
        f"{alias}.is_active = TRUE "
        f"AND COALESCE({alias}.is_test_profile, FALSE) = {expected_flag}"
    )


def _serialize_candidate_summary_config(candidate_summary_config) -> str | None:
    if candidate_summary_config is None:
        return None
    if isinstance(candidate_summary_config, str):
        return candidate_summary_config
    return json.dumps(candidate_summary_config, ensure_ascii=False)


def _normalize_fit_prompt_text(prompt_text) -> str:
    normalized_prompt = str(prompt_text or "").strip()
    if not normalized_prompt:
        normalized_prompt = DEFAULT_FIT_PROMPT_TEXT

    if "{{job_title}}" not in normalized_prompt:
        normalized_prompt += " Job Title: <<<{{job_title}}>>>"
    if "{{job_description}}" not in normalized_prompt:
        normalized_prompt += " Job Description: <<<{{job_description}}>>>"
    if "{{candidate_resume}}" not in normalized_prompt:
        normalized_prompt += " Candidate Resume: <<<{{candidate_resume}}>>>"
    if "{{candidate_summary}}" not in normalized_prompt:
        normalized_prompt += " Candidate Summary: <<<{{candidate_summary}}>>>"

    return normalized_prompt


def _resolve_profiles_config_path() -> Path:
    configured_path = os.getenv("PROFILE_CONFIG_PATH")
    if configured_path:
        return Path(configured_path).expanduser().resolve()

    candidate_paths = [
        (INCLUDE_USER_INFO_DIR / "profiles.json").resolve(),
        (USER_INFO_DIR / "profiles.json").resolve(),
    ]
    for candidate_path in candidate_paths:
        if candidate_path.exists():
            return candidate_path

    return candidate_paths[0]


def _resolve_profile_resume_path(
    resume_path: Optional[str], *, base_dir: Path
) -> Optional[str]:
    if not resume_path:
        return resume_path
    resolved = Path(str(resume_path)).expanduser()
    if not resolved.is_absolute():
        resolved = (base_dir / resolved).resolve()
    return str(resolved)


def _load_resume_text_from_path(resume_path: Optional[str]) -> Optional[str]:
    if not resume_path:
        return None

    resolved = Path(str(resume_path)).expanduser()
    if not resolved.exists() or not resolved.is_file():
        return None

    if resolved.suffix.lower() not in TEXT_RESUME_SUFFIXES:
        return None

    for encoding in ("utf-8", "utf-8-sig"):
        try:
            content = resolved.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
        except OSError:
            return None
        if content.strip():
            return content
    return None


def _load_profile_configs_from_file(config_path: Path) -> list[dict[str, Any]]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"PROFILE_CONFIG_PATH does not exist: {config_path}"
        )

    with config_path.open("r", encoding="utf-8") as file_handle:
        raw_configs = json.load(file_handle)

    if not isinstance(raw_configs, list):
        raise ValueError(f"Profile config must be a JSON array: {config_path}")

    base_dir = config_path.parent
    profile_configs = []
    for profile_config in raw_configs:
        if not isinstance(profile_config, dict):
            continue
        normalized_profile = dict(profile_config)
        normalized_profile["resume_path"] = _resolve_profile_resume_path(
            profile_config.get("resume_path"),
            base_dir=base_dir,
        )
        if not normalized_profile.get("resume_text"):
            normalized_profile["resume_text"] = _load_resume_text_from_path(
                normalized_profile.get("resume_path")
            )
        profile_configs.append(normalized_profile)
    return profile_configs


def _collect_matcher_cutover_validation_errors(
    profile_configs: list[dict[str, Any]],
) -> list[str]:
    errors: list[str] = []
    for profile_config in profile_configs or []:
        if not profile_config.get("is_active", True):
            continue
        if not profile_config.get("search_configs"):
            continue

        profile_key = str(profile_config.get("profile_key") or "").strip() or "<unknown>"
        display_name = (
            str(profile_config.get("display_name") or "").strip() or profile_key
        )
        resume_path = str(profile_config.get("resume_path") or "").strip()
        resume_text = str(profile_config.get("resume_text") or "").strip()

        if not resume_path:
            errors.append(
                f"{profile_key} ({display_name}): resume_path is required for {MATCHER_CUTOVER_LABEL}"
            )
            continue

        suffix = Path(resume_path).suffix.lower()
        if suffix not in TEXT_RESUME_SUFFIXES:
            allowed = ", ".join(sorted(TEXT_RESUME_SUFFIXES))
            errors.append(
                f"{profile_key} ({display_name}): resume_path {resume_path} must use one of {allowed}"
            )

        if not resume_text:
            errors.append(
                f"{profile_key} ({display_name}): canonical resume_text is empty after source hydration"
            )

    return errors


def _validate_matcher_cutover_profiles(profile_configs: list[dict[str, Any]]) -> None:
    errors = _collect_matcher_cutover_validation_errors(profile_configs)
    if not errors:
        return

    bullet_list = "\n".join(f"- {error}" for error in errors)
    raise ValueError(
        f"{MATCHER_CUTOVER_LABEL} validation failed.\n"
        "Active notifier-owned profiles must use markdown/txt resumes and produce canonical resume_text.\n"
        "Fix the following profiles before running sync_profiles_from_source(force=True):\n"
        f"{bullet_list}"
    )


def _compute_profile_source_signature(config_path: Path) -> tuple[str, int, int]:
    if not config_path.exists():
        return DEFAULT_PROFILE_SOURCE_SIGNATURE
    stat_result = config_path.stat()
    return (str(config_path), int(stat_result.st_mtime_ns), int(stat_result.st_size))


def _default_profile_config() -> dict[str, Any]:
    results_default = _get_positive_int_env(
        "SCAN_RESULTS_PER_TERM",
        _get_positive_int_env("SCAN_RESULTS_WANTED", 10),
    )
    return {
        "profile_key": os.getenv("DEFAULT_PROFILE_KEY", "default"),
        "display_name": os.getenv("DEFAULT_PROFILE_NAME", "Default Profile"),
        "bootstrap_existing_jobs": True,
        "resume_path": _resolve_default_resume_path(),
        "resume_text": None,
        "candidate_summary_config": os.getenv("DEFAULT_CANDIDATE_SUMMARY_JSON")
        or os.getenv("CANDIDATE_SUMMARY_JSON"),
        "fit_prompt_text": _normalize_fit_prompt_text(None),
        "discord_channel_id": os.getenv("DISCORD_CHANNEL_ID"),
        "discord_webhook_url": os.getenv("DISCORD_WEBHOOK_URL"),
        "model_name": os.getenv("FITTING_MODEL_NAME", "gpt-5.4"),
        "is_active": True,
        "is_test_profile": False,
        "search_configs": [
            {
                "name": os.getenv("DEFAULT_SEARCH_CONFIG_NAME", "default"),
                "location": os.getenv("SCAN_LOCATION", "Netherlands"),
                "geo_id": os.getenv("SCAN_GEO_ID")
                or _default_linkedin_geo_id(
                    os.getenv("SCAN_LOCATION", "Netherlands")
                ),
                "distance": _get_positive_int_env("SCAN_DISTANCE", 25),
                "hours_old": _get_positive_int_env("SCAN_HOURS_OLD", 168),
                "results_per_term": results_default,
                "is_active": True,
                "terms": _parse_terms(os.getenv("SCAN_SEARCH_TERMS", ""))
                or [os.getenv("SCAN_SEARCH_TERM", "Data Engineer")],
            }
        ],
    }


def _coerce_profile_configs(profile_configs) -> list[dict[str, Any]]:
    normalized_profiles = []
    for profile_config in profile_configs or []:
        if not isinstance(profile_config, dict):
            continue
        profile_key = str(profile_config.get("profile_key") or "").strip()
        if not profile_key:
            continue
        profile_is_active = _coerce_bool(
            profile_config.get("active")
            if "active" in profile_config
            else profile_config.get("is_active"),
            True,
        )
        bootstrap_existing_jobs = _coerce_bool(
            profile_config.get("bootstrap_existing_jobs"),
            False,
        )
        is_test_profile = _coerce_bool(
            profile_config.get("test_mode_only")
            if "test_mode_only" in profile_config
            else profile_config.get("is_test_profile"),
            False,
        )
        search_configs = []
        for search_config in profile_config.get("search_configs") or []:
            if not isinstance(search_config, dict):
                continue
            config_name = (
                str(search_config.get("name") or "default").strip() or "default"
            )
            terms = _parse_terms(search_config.get("terms"))
            if not terms:
                continue
            location = str(search_config.get("location") or "Netherlands").strip()
            if not location:
                location = "Netherlands"
            raw_geo_id = (
                search_config.get("geo_id")
                if "geo_id" in search_config
                else search_config.get("geoId")
            )
            geo_id = (
                _coerce_geo_id(raw_geo_id)
                if raw_geo_id is not None
                else _default_linkedin_geo_id(location)
            )
            if "results_per_term" not in search_config or search_config.get(
                "results_per_term"
            ) is None:
                raise ValueError(
                    "search_config.results_per_term is required for "
                    f"profile={profile_key} config={config_name}"
                )
            search_configs.append(
                {
                    "name": config_name,
                    "location": location,
                    "geo_id": geo_id,
                    "distance": int(search_config.get("distance") or 25),
                    "hours_old": int(search_config.get("hours_old") or 168),
                    "results_per_term": _coerce_nonnegative_int(
                        search_config.get("results_per_term"),
                        10,
                    ),
                    "is_active": _coerce_bool(
                        search_config.get("active")
                        if "active" in search_config
                        else search_config.get("is_active"),
                        True,
                    ),
                    "terms": terms,
                }
            )
        if not search_configs:
            continue
        normalized_profiles.append(
            {
                "profile_key": profile_key,
                "display_name": profile_config.get("display_name") or profile_key,
                "bootstrap_existing_jobs": bootstrap_existing_jobs,
                "resume_path": profile_config.get("resume_path"),
                "resume_text": profile_config.get("resume_text"),
                "candidate_summary_config": profile_config.get("candidate_summary")
                or profile_config.get("candidate_summary_config"),
                "fit_prompt_text": _normalize_fit_prompt_text(
                    profile_config.get("fit_prompt")
                    or profile_config.get("fit_prompt_text")
                    or profile_config.get("fit_prompt_config")
                ),
                "discord_channel_id": profile_config.get("discord_channel_id"),
                "discord_webhook_url": profile_config.get("discord_webhook_url"),
                "model_name": profile_config.get("model_name")
                or os.getenv("FITTING_MODEL_NAME", "gpt-5.4"),
                "is_active": profile_is_active,
                "is_test_profile": is_test_profile,
                "search_configs": search_configs,
            }
        )
    return normalized_profiles


def _sync_profile_configs(
    cursor,
    profile_configs,
    *,
    already_normalized: bool = False,
) -> int:
    normalized_profiles = (
        list(profile_configs or [])
        if already_normalized
        else _coerce_profile_configs(profile_configs)
    )
    synced_profiles = 0
    for profile_config in normalized_profiles:
        cursor.execute(
            """
            INSERT INTO profiles (
                profile_key,
                display_name,
                resume_path,
                resume_text,
                candidate_summary_config,
                fit_prompt_config,
                discord_channel_id,
                discord_webhook_url,
                model_name,
                is_active,
                is_test_profile,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(profile_key) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                resume_path = EXCLUDED.resume_path,
                resume_text = EXCLUDED.resume_text,
                candidate_summary_config = EXCLUDED.candidate_summary_config,
                fit_prompt_config = EXCLUDED.fit_prompt_config,
                discord_channel_id = EXCLUDED.discord_channel_id,
                discord_webhook_url = EXCLUDED.discord_webhook_url,
                model_name = EXCLUDED.model_name,
                is_active = EXCLUDED.is_active,
                is_test_profile = EXCLUDED.is_test_profile,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """,
            (
                profile_config["profile_key"],
                profile_config["display_name"],
                profile_config.get("resume_path"),
                profile_config.get("resume_text"),
                _serialize_candidate_summary_config(
                    profile_config.get("candidate_summary_config")
                ),
                profile_config.get("fit_prompt_text"),
                profile_config.get("discord_channel_id"),
                profile_config.get("discord_webhook_url"),
                profile_config.get("model_name"),
                profile_config.get("is_active", True),
                profile_config.get("is_test_profile", False),
            ),
        )
        profile_id = cursor.fetchone()[0]
        synced_profiles += 1

        for search_config in profile_config["search_configs"]:
            cursor.execute(
                """
                INSERT INTO search_configs (
                    profile_id,
                    name,
                    location,
                    geo_id,
                    distance,
                    hours_old,
                    results_per_term,
                    is_active,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(profile_id, name) DO UPDATE SET
                    location = EXCLUDED.location,
                    geo_id = EXCLUDED.geo_id,
                    distance = EXCLUDED.distance,
                    hours_old = EXCLUDED.hours_old,
                    results_per_term = EXCLUDED.results_per_term,
                    is_active = EXCLUDED.is_active,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (
                    profile_id,
                    search_config["name"],
                    search_config.get("location"),
                    search_config.get("geo_id"),
                    search_config.get("distance"),
                    search_config.get("hours_old"),
                    search_config.get("results_per_term"),
                    search_config.get("is_active", True),
                ),
            )
            search_config_id = cursor.fetchone()[0]
            cursor.execute(
                "DELETE FROM search_terms WHERE search_config_id = %s",
                (search_config_id,),
            )
            cursor.executemany(
                """
                INSERT INTO search_terms (search_config_id, term)
                VALUES (%s, %s)
                ON CONFLICT(search_config_id, term) DO NOTHING
                """,
                [(search_config_id, term) for term in search_config["terms"]],
            )

        cursor.execute(
            """
            UPDATE search_configs
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE profile_id = %s
              AND NOT (name = ANY(%s))
            """,
            (
                profile_id,
                [
                    search_config["name"]
                    for search_config in profile_config["search_configs"]
                ],
            ),
        )

    return synced_profiles


def _backfill_profile_jobs_from_existing_jobs(cursor, profile_id: int):
    cursor.execute(
        """
        INSERT INTO profile_jobs (
            profile_id,
            job_id,
            discovered_at,
            last_seen_at,
            fit_status,
            fit_attempts,
            fit_last_error,
            fit_updated_at,
            llm_match,
            llm_match_error,
            fit_score,
            fit_decision,
            notified_at,
            notify_status,
            notify_error
        )
        SELECT
            %s,
            j.id,
            COALESCE(j.fit_updated_at, CURRENT_TIMESTAMP),
            CURRENT_TIMESTAMP,
            j.fit_status,
            COALESCE(j.fit_attempts, 0),
            j.fit_last_error,
            j.fit_updated_at,
            j.llm_match,
            j.llm_match_error,
            j.fit_score,
            j.fit_decision,
            j.notified_at,
            j.notify_status,
            j.notify_error
        FROM jobs j
        WHERE j.id NOT LIKE 'test-%'
          AND j.source_job_id IS NULL
        ON CONFLICT(profile_id, job_id) DO NOTHING
        """,
        (profile_id,),
    )


def _backfill_default_profile_jobs(cursor, profile_key: str):
    cursor.execute(
        "SELECT id FROM profiles WHERE profile_key = %s",
        (profile_key,),
    )
    row = cursor.fetchone()
    if not row:
        return
    _backfill_profile_jobs_from_existing_jobs(cursor, int(row[0]))


def _deactivate_missing_profiles(cursor, active_profile_keys: list[str]):
    if active_profile_keys:
        cursor.execute(
            """
            UPDATE profiles
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE NOT (profile_key = ANY(%s))
            """,
            (active_profile_keys,),
        )
        return

    cursor.execute(
        """
        UPDATE profiles
        SET is_active = FALSE,
            updated_at = CURRENT_TIMESTAMP
        """
    )


def _bootstrap_flagged_profiles(
    cursor, normalized_profiles: list[dict[str, Any]]
) -> int:
    flagged_profile_keys = [
        profile_config["profile_key"]
        for profile_config in normalized_profiles
        if profile_config.get("bootstrap_existing_jobs")
        and not profile_config.get("is_test_profile", False)
    ]
    if not flagged_profile_keys:
        return 0

    cursor.execute(
        "SELECT id, profile_key FROM profiles WHERE profile_key = ANY(%s)",
        (flagged_profile_keys,),
    )
    profile_rows = cursor.fetchall() or []
    if not profile_rows:
        return 0

    profile_ids = [int(row[0]) for row in profile_rows]
    cursor.execute(
        """
        SELECT profile_id
        FROM profile_bootstrap_state
        WHERE bootstrap_key = %s
          AND profile_id = ANY(%s)
        """,
        (BOOTSTRAP_EXISTING_JOBS_KEY, profile_ids),
    )
    completed_profile_ids = {
        int(row[0]) for row in (cursor.fetchall() or []) if row and row[0] is not None
    }

    bootstrapped_profiles = 0
    for profile_id, _profile_key in profile_rows:
        normalized_profile_id = int(profile_id)
        if normalized_profile_id in completed_profile_ids:
            continue
        _backfill_profile_jobs_from_existing_jobs(cursor, normalized_profile_id)
        cursor.execute(
            """
            INSERT INTO profile_bootstrap_state (
                profile_id,
                bootstrap_key,
                completed_at
            )
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(profile_id, bootstrap_key) DO NOTHING
            """,
            (normalized_profile_id, BOOTSTRAP_EXISTING_JOBS_KEY),
        )
        bootstrapped_profiles += 1

    return bootstrapped_profiles


def _sync_profiles_from_source(cursor, *, force: bool = False) -> int:
    global _PROFILE_SOURCE_SIGNATURE

    config_path = _resolve_profiles_config_path()
    if os.getenv("PROFILE_CONFIG_PATH") and not config_path.exists():
        raise FileNotFoundError(
            f"PROFILE_CONFIG_PATH does not exist: {config_path}"
        )
    source_signature = _compute_profile_source_signature(config_path)
    if not force and source_signature == _PROFILE_SOURCE_SIGNATURE:
        return 0

    if config_path.exists():
        profile_configs = _load_profile_configs_from_file(config_path)
        normalized_profiles = _coerce_profile_configs(profile_configs)
        if force:
            _validate_matcher_cutover_profiles(normalized_profiles)
        synced_profiles = _sync_profile_configs(
            cursor,
            normalized_profiles,
            already_normalized=True,
        )
        _deactivate_missing_profiles(
            cursor,
            [profile_config["profile_key"] for profile_config in normalized_profiles],
        )
        _bootstrap_flagged_profiles(cursor, normalized_profiles)
        _PROFILE_SOURCE_SIGNATURE = source_signature
        return synced_profiles

    default_profile_config = _default_profile_config()
    synced_profiles = _sync_profile_configs(cursor, [default_profile_config])
    _deactivate_missing_profiles(cursor, [default_profile_config["profile_key"]])
    _backfill_default_profile_jobs(cursor, default_profile_config["profile_key"])
    _PROFILE_SOURCE_SIGNATURE = source_signature
    return synced_profiles


def sync_profiles_from_source(force: bool = False) -> int:
    """Sync profile/search config state from user_info or fallback env defaults."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            return _sync_profiles_from_source(cursor, force=force)


def init_db():
    """Ensure required tables/indexes exist without mutating hot business rows."""
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return

    with _connect() as conn:
        with conn.cursor() as cursor:
            # Serialize schema bootstrap across concurrent task processes and avoid
            # deadlocks with concurrent read-heavy DAG tasks.
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_KEY,))
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    site TEXT,
                    job_url TEXT,
                    title TEXT,
                    company TEXT,
                    source_job_id TEXT,
                    batch_id BIGINT,
                    description TEXT,
                    description_error TEXT,
                    llm_match TEXT,
                    llm_match_error TEXT,
                    fit_score INTEGER,
                    fit_decision TEXT,
                    notified_at TIMESTAMP,
                    notify_status TEXT,
                    notify_error TEXT,
                    fit_status TEXT,
                    fit_attempts INTEGER DEFAULT 0,
                    fit_last_error TEXT,
                    fit_updated_at TIMESTAMP,
                    FOREIGN KEY (batch_id) REFERENCES batches (id)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS jd_queue (
                    job_id TEXT PRIMARY KEY,
                    job_url TEXT,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs (id)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_state (
                    alert_key TEXT PRIMARY KEY,
                    is_active BOOLEAN NOT NULL DEFAULT FALSE,
                    last_error TEXT,
                    first_seen_at TIMESTAMP,
                    last_seen_at TIMESTAMP,
                    last_sent_at TIMESTAMP,
                    resolved_at TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS profiles (
                    id BIGSERIAL PRIMARY KEY,
                    profile_key TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    resume_path TEXT,
                    resume_text TEXT,
                    candidate_summary_config TEXT,
                    fit_prompt_config TEXT,
                    discord_channel_id TEXT,
                    discord_webhook_url TEXT,
                    model_name TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    is_test_profile BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                ALTER TABLE jobs
                ADD COLUMN IF NOT EXISTS source_job_id TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE profiles
                ADD COLUMN IF NOT EXISTS candidate_summary_config TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE profiles
                ADD COLUMN IF NOT EXISTS fit_prompt_config TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE profiles
                ADD COLUMN IF NOT EXISTS is_test_profile BOOLEAN NOT NULL DEFAULT FALSE
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS search_configs (
                    id BIGSERIAL PRIMARY KEY,
                    profile_id BIGINT NOT NULL REFERENCES profiles (id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    location TEXT,
                    geo_id TEXT,
                    distance INTEGER,
                    hours_old INTEGER,
                    results_per_term INTEGER,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (profile_id, name)
                )
                """
            )

            cursor.execute(
                """
                ALTER TABLE search_configs
                ADD COLUMN IF NOT EXISTS location TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE search_configs
                ADD COLUMN IF NOT EXISTS geo_id TEXT
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS search_terms (
                    id BIGSERIAL PRIMARY KEY,
                    search_config_id BIGINT NOT NULL REFERENCES search_configs (id) ON DELETE CASCADE,
                    term TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (search_config_id, term)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS profile_jobs (
                    profile_id BIGINT NOT NULL REFERENCES profiles (id) ON DELETE CASCADE,
                    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
                    search_config_id BIGINT REFERENCES search_configs (id) ON DELETE SET NULL,
                    matched_term TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    llm_match TEXT,
                    llm_match_error TEXT,
                    fit_score INTEGER,
                    fit_decision TEXT,
                    notified_at TIMESTAMP,
                    notify_status TEXT,
                    notify_error TEXT,
                    fit_status TEXT,
                    fit_attempts INTEGER DEFAULT 0,
                    fit_last_error TEXT,
                    fit_updated_at TIMESTAMP,
                    user_status TEXT DEFAULT 'new',
                    user_note TEXT,
                    user_status_updated_at TIMESTAMP,
                    PRIMARY KEY (profile_id, job_id)
                )
                """
            )

            cursor.execute(
                """
                ALTER TABLE profile_jobs
                ADD COLUMN IF NOT EXISTS user_status TEXT DEFAULT 'new'
                """
            )

            cursor.execute(
                """
                ALTER TABLE profile_jobs
                ADD COLUMN IF NOT EXISTS user_note TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE profile_jobs
                ADD COLUMN IF NOT EXISTS user_status_updated_at TIMESTAMP
                """
            )

            cursor.execute(
                """
                UPDATE profile_jobs
                SET user_status = 'new'
                WHERE user_status IS NOT NULL
                  AND user_status <> ''
                  AND user_status NOT IN ('new', 'saved', 'dismissed', 'applied')
                """
            )

            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'profile_jobs_user_status_check'
                          AND conrelid = 'profile_jobs'::regclass
                    ) THEN
                        ALTER TABLE profile_jobs
                        ADD CONSTRAINT profile_jobs_user_status_check
                        CHECK (
                            user_status IS NULL
                            OR user_status = ''
                            OR user_status IN ('new', 'saved', 'dismissed', 'applied')
                        );
                    END IF;
                END $$;
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS profile_bootstrap_state (
                    profile_id BIGINT NOT NULL REFERENCES profiles (id) ON DELETE CASCADE,
                    bootstrap_key TEXT NOT NULL,
                    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (profile_id, bootstrap_key)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_runs (
                    id BIGSERIAL PRIMARY KEY,
                    profile_id BIGINT NOT NULL REFERENCES profiles (id) ON DELETE CASCADE,
                    dag_id TEXT,
                    dag_run_id TEXT,
                    run_type TEXT NOT NULL DEFAULT 'discord_notification',
                    source TEXT NOT NULL DEFAULT 'airflow',
                    status TEXT NOT NULL DEFAULT 'started',
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    eligible_count INTEGER NOT NULL DEFAULT 0,
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    skipped_count INTEGER NOT NULL DEFAULT 0,
                    summary_status TEXT,
                    summary_error TEXT,
                    error TEXT,
                    backfill_key TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_run_jobs (
                    id BIGSERIAL PRIMARY KEY,
                    run_id BIGINT NOT NULL REFERENCES notification_runs (id) ON DELETE CASCADE,
                    profile_id BIGINT NOT NULL,
                    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
                    fit_score INTEGER,
                    fit_decision TEXT,
                    notify_status TEXT NOT NULL DEFAULT 'pending',
                    notify_error TEXT,
                    notified_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (run_id, profile_id, job_id),
                    FOREIGN KEY (profile_id, job_id) REFERENCES profile_jobs (profile_id, job_id) ON DELETE CASCADE
                )
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_fit_status
                ON jobs (fit_status)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jd_queue_status
                ON jd_queue (status)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_profiles_is_active
                ON profiles (is_active)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_profiles_active_test_profile
                ON profiles (is_active, is_test_profile)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_search_configs_profile_active
                ON search_configs (profile_id, is_active)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_search_terms_config_id
                ON search_terms (search_config_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_profile_jobs_fit_status
                ON profile_jobs (fit_status)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_profile_jobs_notify_status
                ON profile_jobs (notified_at, fit_status)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_profile_jobs_job_id
                ON profile_jobs (job_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_notification_runs_profile_started
                ON notification_runs (profile_id, started_at DESC)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_notification_runs_dag_run_id
                ON notification_runs (dag_run_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_notification_run_jobs_run_id
                ON notification_run_jobs (run_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_notification_run_jobs_profile_job
                ON notification_run_jobs (profile_id, job_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_notification_run_jobs_run_status
                ON notification_run_jobs (run_id, notify_status)
                """
            )

            _sync_profiles_from_source(cursor)
    _SCHEMA_INITIALIZED = True


def save_jobs(jobs_df: pd.DataFrame):
    """
    Saves a pandas DataFrame of jobs to the database.
    Assigns a new batch_id to this group.
    Updates existing jobs if the ID already exists (UPSERT).
    """
    if jobs_df.empty:
        return

    init_db()

    filtered_df = _ensure_columns(jobs_df, JOB_REQUIRED_COLUMNS)[JOB_REQUIRED_COLUMNS]

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO batches DEFAULT VALUES RETURNING id")
            batch_id = cursor.fetchone()[0]

            safe_df = filtered_df.copy()
            safe_df["source_job_id"] = (
                safe_df["source_job_id"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace({"": None})
            )
            records = [
                (*row, batch_id)
                for row in safe_df.itertuples(index=False, name=None)
            ]

            written_count = 0
            for record in records:
                cursor.execute(
                    """
                    INSERT INTO jobs (
                        id,
                        site,
                        job_url,
                        title,
                        company,
                        source_job_id,
                        batch_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        site = COALESCE(NULLIF(BTRIM(EXCLUDED.site), ''), jobs.site),
                        job_url = COALESCE(NULLIF(BTRIM(EXCLUDED.job_url), ''), jobs.job_url),
                        title = COALESCE(NULLIF(BTRIM(EXCLUDED.title), ''), jobs.title),
                        company = COALESCE(NULLIF(BTRIM(EXCLUDED.company), ''), jobs.company),
                        source_job_id = COALESCE(
                            NULLIF(BTRIM(EXCLUDED.source_job_id), ''),
                            jobs.source_job_id
                        ),
                        batch_id = EXCLUDED.batch_id
                    """,
                    record,
                )
                written_count += cursor.rowcount

    if written_count > 0:
        print(f"✅ Upserted {written_count} jobs in Batch {batch_id}.")
    else:
        print(f"No job rows changed during Batch {batch_id}.")


def upsert_profile_configs(profile_configs) -> int:
    """Upsert profile metadata and search configurations."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            return _sync_profile_configs(cursor, profile_configs)


def get_active_search_configs() -> list[dict[str, Any]]:
    """Return active search configs with their owning profile and terms."""
    sync_profiles_from_source()
    profile_mode_clause = _profile_mode_clause("p")

    query = f"""
        SELECT
            p.id AS profile_id,
            p.profile_key,
            p.display_name,
            p.resume_path,
            p.resume_text,
            p.discord_channel_id,
            p.discord_webhook_url,
            p.model_name,
            COALESCE(p.is_test_profile, FALSE) AS is_test_profile,
            c.id AS search_config_id,
            c.name AS search_config_name,
            c.location,
            c.geo_id,
            c.distance,
            c.hours_old,
            c.results_per_term,
            t.term
        FROM profiles p
        JOIN search_configs c ON c.profile_id = p.id
        JOIN search_terms t ON t.search_config_id = c.id
        WHERE {profile_mode_clause}
          AND c.is_active = TRUE
        ORDER BY p.id ASC, c.id ASC, t.id ASC
    """

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    configs_by_id: dict[int, dict[str, Any]] = {}
    ordered_configs: list[dict[str, Any]] = []
    for row in rows:
        search_config_id = row["search_config_id"]
        config = configs_by_id.get(search_config_id)
        if config is None:
            config = {
                "profile_id": row["profile_id"],
                "profile_key": row["profile_key"],
                "display_name": row["display_name"],
                "resume_path": row["resume_path"],
                "resume_text": row["resume_text"],
                "discord_channel_id": row["discord_channel_id"],
                "discord_webhook_url": row["discord_webhook_url"],
                "model_name": row["model_name"]
                or os.getenv("FITTING_MODEL_NAME", "gpt-5.4"),
                "is_test_profile": bool(row.get("is_test_profile")),
                "search_config_id": search_config_id,
                "search_config_name": row["search_config_name"],
                "location": row["location"]
                or os.getenv("SCAN_LOCATION", "Netherlands"),
                "geo_id": row.get("geo_id")
                or _default_linkedin_geo_id(
                    row["location"] or os.getenv("SCAN_LOCATION", "Netherlands")
                ),
                "distance": row["distance"]
                if row["distance"] is not None
                else _get_positive_int_env("SCAN_DISTANCE", 25),
                "hours_old": row["hours_old"]
                if row["hours_old"] is not None
                else _get_positive_int_env("SCAN_HOURS_OLD", 168),
                "results_per_term": row["results_per_term"]
                if row["results_per_term"] is not None
                else _get_positive_int_env("SCAN_RESULTS_PER_TERM", 10),
                "terms": [],
            }
            configs_by_id[search_config_id] = config
            ordered_configs.append(config)
        if row["term"]:
            config["terms"].append(str(row["term"]).strip())

    return ordered_configs


def upsert_profile_jobs(profile_jobs_df: pd.DataFrame) -> int:
    """Create or refresh profile-job links without resetting fit state."""
    if profile_jobs_df is None or profile_jobs_df.empty:
        return 0

    init_db()

    link_df = _ensure_columns(profile_jobs_df, PROFILE_JOB_COLUMNS)[
        PROFILE_JOB_COLUMNS
    ].copy()
    link_df = link_df.dropna(subset=["profile_id", "job_id"])
    if link_df.empty:
        return 0

    link_df["profile_id"] = link_df["profile_id"].astype(int)
    link_df["job_id"] = link_df["job_id"].astype(str)
    link_df["matched_term"] = (
        link_df["matched_term"].fillna("").astype(str).str.strip().replace({"": None})
    )
    link_df = link_df.drop_duplicates(subset=["profile_id", "job_id"], keep="first")
    safe_link_df = link_df.astype(object).where(pd.notna(link_df), None)
    records = list(safe_link_df.itertuples(index=False, name=None))

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO profile_jobs (
                    profile_id,
                    job_id,
                    search_config_id,
                    matched_term,
                    discovered_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(profile_id, job_id) DO UPDATE SET
                    search_config_id = COALESCE(EXCLUDED.search_config_id, profile_jobs.search_config_id),
                    matched_term = COALESCE(
                        NULLIF(BTRIM(EXCLUDED.matched_term), ''),
                        profile_jobs.matched_term
                    ),
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                records,
            )

    return len(records)


def enqueue_jd_requests(jobs_df: pd.DataFrame):
    """Upsert pending JD requests for the LinkedIn jobPosting API worker."""
    if jobs_df is None or jobs_df.empty:
        return 0

    init_db()

    if "id" not in jobs_df.columns or "job_url" not in jobs_df.columns:
        return 0

    queue_df = _ensure_columns(jobs_df, JD_QUEUE_COLUMNS)[JD_QUEUE_COLUMNS]
    records = [
        (job_id, job_url)
        for job_id, job_url in queue_df.itertuples(index=False, name=None)
    ]

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO jd_queue (job_id, job_url, status, attempts, updated_at, error)
                VALUES (%s, %s, 'pending', 0, CURRENT_TIMESTAMP, NULL)
                ON CONFLICT(job_id) DO UPDATE SET
                    job_url=EXCLUDED.job_url,
                    status='pending',
                    updated_at=CASE
                        WHEN jd_queue.status = 'pending' THEN jd_queue.updated_at
                        ELSE CURRENT_TIMESTAMP
                    END,
                    error=NULL
                """,
                records,
            )

    return len(records)


def enqueue_fitting_requests(jobs_df: pd.DataFrame):
    """Mark profile jobs as pending for one-time fitting."""
    if jobs_df is None or jobs_df.empty:
        return 0

    init_db()

    if "profile_id" not in jobs_df.columns or "job_id" not in jobs_df.columns:
        return 0

    filtered_df = jobs_df[["profile_id", "job_id"]].copy().dropna()
    if filtered_df.empty:
        return 0

    filtered_df["profile_id"] = filtered_df["profile_id"].astype(int)
    filtered_df["job_id"] = filtered_df["job_id"].astype(str)
    records = list(filtered_df.drop_duplicates().itertuples(index=False, name=None))

    with _connect() as conn:
        with conn.cursor() as cursor:
            queued = 0
            for profile_id, job_id in records:
                cursor.execute(
                    """
                    UPDATE profile_jobs pj
                    SET fit_status = 'pending_fit',
                        fit_updated_at = CURRENT_TIMESTAMP,
                        fit_last_error = NULL,
                        fit_attempts = COALESCE(pj.fit_attempts, 0)
                    FROM jobs j
                    WHERE pj.profile_id = %s
                      AND pj.job_id = %s
                      AND j.id = pj.job_id
                      AND j.description IS NOT NULL
                      AND pj.llm_match IS NULL
                      AND COALESCE(pj.fit_status, '') NOT IN (
                           'pending_fit', 'fitting', 'fit_done', 'notified', 'fit_failed', 'notify_failed'
                       )
                    """,
                    (profile_id, job_id),
                )
                queued += cursor.rowcount

    return queued


def claim_pending_fitting_tasks(limit: int = None) -> List[Dict[str, Any]]:
    """Atomically claim pending fitting tasks for processing."""
    sync_profiles_from_source()
    stale_minutes = _get_positive_int_env("FITTING_CLAIM_STALE_MINUTES", 30)
    profile_mode_clause = _profile_mode_clause("p")

    select_query = f"""
        SELECT pj.profile_id, pj.job_id, COALESCE(pj.fit_attempts, 0) AS attempts
        FROM profile_jobs pj
        JOIN jobs j ON j.id = pj.job_id
        JOIN profiles p ON p.id = pj.profile_id
        WHERE (
                pj.fit_status = 'pending_fit'
                OR (
                    pj.fit_status = 'fitting'
                    AND COALESCE(
                        pj.fit_updated_at,
                        TIMESTAMP '1970-01-01 00:00:00'
                    ) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                )
              )
          AND {profile_mode_clause}
          AND j.description IS NOT NULL
          AND pj.llm_match IS NULL
        ORDER BY COALESCE(pj.fit_updated_at, TIMESTAMP '1970-01-01 00:00:00') ASC
        FOR UPDATE SKIP LOCKED
    """
    params: List[Any] = [stale_minutes]
    if limit is not None and int(limit) > 0:
        select_query += " LIMIT %s"
        params.append(int(limit))

    with _connect(row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(select_query, params)
                rows = cursor.fetchall()
                if not rows:
                    return []

                cursor.executemany(
                    """
                    UPDATE profile_jobs
                    SET fit_status = 'fitting',
                        fit_updated_at = CURRENT_TIMESTAMP
                    WHERE profile_id = %s
                      AND job_id = %s
                    """,
                    [(row["profile_id"], row["job_id"]) for row in rows],
                )

    return [
        {
            "profile_id": row["profile_id"],
            "job_id": row["job_id"],
            "attempts": row["attempts"],
        }
        for row in rows
    ]


def mark_fitting_done(profile_id: int, job_id: str):
    """Mark fitting task as completed."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE profile_jobs
                SET fit_status = 'fit_done',
                    fit_updated_at = CURRENT_TIMESTAMP,
                    fit_last_error = NULL
                WHERE profile_id = %s
                  AND job_id = %s
                """,
                (profile_id, job_id),
            )


def mark_fitting_failed(
    profile_id: int,
    job_id: str,
    error: str = "",
    retry: bool = True,
):
    """Mark fitting task as failed and optionally requeue."""
    init_db()

    status = "pending_fit" if retry else "fit_failed"
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE profile_jobs
                SET fit_status = %s,
                    fit_attempts = COALESCE(fit_attempts, 0) + 1,
                    fit_last_error = %s,
                    fit_updated_at = CURRENT_TIMESTAMP
                WHERE profile_id = %s
                  AND job_id = %s
                """,
                (status, error, profile_id, job_id),
            )


def requeue_fitting_task(profile_id: int, job_id: str, error: str = ""):
    """Return an unprocessed fitting task to pending without spending an attempt."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE profile_jobs
                SET fit_status = 'pending_fit',
                    llm_match_error = NULL,
                    fit_last_error = %s,
                    fit_updated_at = CURRENT_TIMESTAMP
                WHERE profile_id = %s
                  AND job_id = %s
                """,
                (error, profile_id, job_id),
            )


def should_send_active_alert(alert_key: str, error: Optional[str] = None) -> bool:
    """Send once per active outage; alert can reopen after resolve_active_alert."""
    init_db()

    with _connect(row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT is_active
                    FROM alert_state
                    WHERE alert_key = %s
                    FOR UPDATE
                    """,
                    (alert_key,),
                )
                row = cursor.fetchone()

                if not row or not row["is_active"]:
                    cursor.execute(
                        """
                        INSERT INTO alert_state (
                            alert_key,
                            is_active,
                            last_error,
                            first_seen_at,
                            last_seen_at,
                            last_sent_at,
                            resolved_at
                        )
                        VALUES (
                            %s,
                            TRUE,
                            %s,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            NULL
                        )
                        ON CONFLICT(alert_key) DO UPDATE SET
                            is_active = TRUE,
                            last_error = EXCLUDED.last_error,
                            first_seen_at = CURRENT_TIMESTAMP,
                            last_seen_at = CURRENT_TIMESTAMP,
                            last_sent_at = CURRENT_TIMESTAMP,
                            resolved_at = NULL
                        """,
                        (alert_key, error),
                    )
                    return True

                cursor.execute(
                    """
                    UPDATE alert_state
                    SET last_error = %s,
                        last_seen_at = CURRENT_TIMESTAMP,
                        resolved_at = NULL
                    WHERE alert_key = %s
                    """,
                    (error, alert_key),
                )
                return False


def resolve_active_alert(alert_key: str):
    """Mark an active alert as recovered so the next outage can notify again."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE alert_state
                SET is_active = FALSE,
                    resolved_at = CURRENT_TIMESTAMP,
                    last_seen_at = CURRENT_TIMESTAMP
                WHERE alert_key = %s
                  AND is_active = TRUE
                """,
                (alert_key,),
            )


def count_jd_queue_status(job_ids: List[str], status: str) -> int:
    """Count how many jobs in given ids are in target status."""
    if not job_ids:
        return 0
    init_db()

    query = "SELECT COUNT(*) FROM jd_queue WHERE status = %s AND job_id = ANY(%s)"

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (status, job_ids))
            count = cursor.fetchone()[0]

    return count


def get_jobs_needing_jd(max_attempts: int = 3) -> pd.DataFrame:
    """Fetch jobs that still need JD scraping and are eligible for retry."""
    init_db()
    stale_minutes = _get_positive_int_env("JD_CLAIM_STALE_MINUTES", 30)
    profile_mode_clause = _profile_mode_clause("p")

    query = f"""
        SELECT j.id, j.site, j.job_url, j.title, j.company, j.source_job_id,
               pj.profile_id,
               q.status AS jd_status,
               COALESCE(q.attempts, 0) AS jd_attempts
        FROM jobs j
        JOIN profile_jobs pj ON pj.job_id = j.id
        JOIN profiles p ON p.id = pj.profile_id
        LEFT JOIN jd_queue q ON q.job_id = j.id
        WHERE j.description IS NULL
          AND NULLIF(TRIM(COALESCE(j.job_url, '')), '') IS NOT NULL
          AND {profile_mode_clause}
          AND (
                q.job_id IS NULL
                OR q.status = 'pending'
                OR (q.status = 'failed' AND COALESCE(q.attempts, 0) < %s)
                OR (
                    q.status = 'processing'
                    AND COALESCE(
                        q.updated_at,
                        TIMESTAMP '1970-01-01 00:00:00'
                    ) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                )
                OR (q.status = 'done' AND j.description IS NULL)
              )
        ORDER BY
            COALESCE(q.updated_at, TIMESTAMP '1970-01-01 00:00:00') ASC,
            COALESCE(j.batch_id, 0) DESC,
            j.id ASC
    """

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (int(max_attempts), stale_minutes))
            rows = cursor.fetchall()

    return pd.DataFrame(rows)


def get_profile_jobs_ready_for_fitting() -> pd.DataFrame:
    """Fetch profile jobs that already have JD and still need to enter fitting."""
    sync_profiles_from_source()
    profile_mode_clause = _profile_mode_clause("p")

    query = f"""
        SELECT pj.profile_id, pj.job_id
        FROM profile_jobs pj
        JOIN jobs j ON j.id = pj.job_id
        JOIN profiles p ON p.id = pj.profile_id
        WHERE j.description IS NOT NULL
          AND {profile_mode_clause}
          AND pj.llm_match IS NULL
          AND COALESCE(pj.fit_status, '') NOT IN (
                'pending_fit', 'fitting', 'fit_done', 'notified', 'fit_failed', 'notify_failed'
              )
        ORDER BY
            COALESCE(pj.fit_updated_at, TIMESTAMP '1970-01-01 00:00:00') ASC,
            pj.profile_id ASC,
            pj.job_id ASC
    """

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return pd.DataFrame(rows)


def get_jobs_ready_for_fitting() -> pd.DataFrame:
    """Compatibility wrapper for profile-based fitting queue selection."""
    return get_profile_jobs_ready_for_fitting()


def get_profiles_by_ids(profile_ids: List[int]) -> pd.DataFrame:
    """Fetch profiles by id list."""
    if not profile_ids:
        return pd.DataFrame()
    sync_profiles_from_source()

    normalized_ids = [int(profile_id) for profile_id in profile_ids]
    query = "SELECT * FROM profiles WHERE id = ANY(%s)"

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (normalized_ids,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def save_llm_matches(jobs_df: pd.DataFrame):
    """Persist LLM fit results back to profile job rows."""
    if jobs_df is None or jobs_df.empty:
        return

    init_db()

    update_df = jobs_df.copy()
    if "profile_id" not in update_df.columns or "job_id" not in update_df.columns:
        return

    update_df = _ensure_columns(update_df, PROFILE_JOB_RESULT_COLUMNS)

    records = []
    for row in update_df[PROFILE_JOB_RESULT_COLUMNS].itertuples(index=False, name=None):
        profile_id, job_id, llm_match, llm_match_error = row
        fit_score, fit_decision = _extract_fit_fields(llm_match)
        records.append(
            (
                llm_match,
                llm_match_error,
                fit_score,
                fit_decision,
                int(profile_id),
                str(job_id),
            )
        )

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE profile_jobs
                SET llm_match = %s,
                    llm_match_error = %s,
                    fit_score = %s,
                    fit_decision = %s
                WHERE profile_id = %s
                  AND job_id = %s
                """,
                records,
            )


def seed_default_profile_scan_filters(profile_id: int) -> None:
    """Populate default portal filters for a profile when portal tables exist."""
    if not _portal_tables_exist():
        print(
            "Django portal tables are missing; skip default scan filter seeding. "
            "Run Django migrations to enable portal-managed filters."
        )
        return
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM portal_profiletitleexcludekeyword WHERE profile_id = %s",
                (profile_id,),
            )
            title_count = int((cursor.fetchone() or [0])[0] or 0)
            if title_count == 0:
                cursor.executemany(
                    """
                    INSERT INTO portal_profiletitleexcludekeyword (profile_id, keyword, created_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(profile_id, keyword) DO NOTHING
                    """,
                    [(profile_id, keyword) for keyword in DEFAULT_TITLE_EXCLUDE_KEYWORDS],
                )
            cursor.execute(
                "SELECT COUNT(*) FROM portal_profilecompanyblacklist WHERE profile_id = %s",
                (profile_id,),
            )
            company_count = int((cursor.fetchone() or [0])[0] or 0)
            if company_count == 0:
                cursor.executemany(
                    """
                    INSERT INTO portal_profilecompanyblacklist (profile_id, company, created_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(profile_id, company) DO NOTHING
                    """,
                    [(profile_id, company) for company in DEFAULT_COMPANY_BLACKLIST],
                )


def ensure_web_portal_schema():
    """Compatibility hook for portal settings; Django owns portal migrations."""
    init_db()
    if not _portal_tables_exist():
        print(
            "Django portal tables are missing; portal-managed filters are disabled. "
            "Run Django migrations to enable them."
        )


PORTAL_TABLE_NAMES = [
    "portal_profilenotificationpreference",
    "portal_profiletitleexcludekeyword",
    "portal_profilecompanyblacklist",
]


def _missing_portal_tables() -> list[str]:
    """Return portal tables that are not present yet. Django owns these migrations."""
    missing = []
    with _connect() as conn:
        with conn.cursor() as cursor:
            for table in PORTAL_TABLE_NAMES:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = %s
                    )
                    """,
                    (table,),
                )
                if not cursor.fetchone()[0]:
                    missing.append(table)
    return missing


def _portal_tables_exist() -> bool:
    """Return whether Django-managed portal tables are available."""
    return not _missing_portal_tables()


def get_profile_scan_filters() -> dict[int, dict[str, list[str]]]:
    """Return per-profile scan filters configured by the Django portal."""
    missing_tables = _missing_portal_tables()
    if missing_tables:
        print(
            "Django portal tables are missing; use built-in scan filters only. "
            f"Missing portal tables: {', '.join(missing_tables)}"
        )
        return {}

    filters: dict[int, dict[str, list[str]]] = {}
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT p.id
                FROM profiles p
                WHERE p.is_active = TRUE
                  AND NOT EXISTS (
                    SELECT 1 FROM portal_profiletitleexcludekeyword f
                    WHERE f.profile_id = p.id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM portal_profilecompanyblacklist f
                    WHERE f.profile_id = p.id
                  )
                """
            )
            profiles_to_seed = [int(row["id"]) for row in cursor.fetchall()]
    for profile_id in profiles_to_seed:
        seed_default_profile_scan_filters(profile_id)

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT profile_id, keyword
                FROM portal_profiletitleexcludekeyword
                ORDER BY profile_id, keyword
                """
            )
            for row in cursor.fetchall():
                filters.setdefault(int(row["profile_id"]), {"title_keywords": [], "companies": []})[
                    "title_keywords"
                ].append(row["keyword"])
            cursor.execute(
                """
                SELECT profile_id, company
                FROM portal_profilecompanyblacklist
                ORDER BY profile_id, company
                """
            )
            for row in cursor.fetchall():
                filters.setdefault(int(row["profile_id"]), {"title_keywords": [], "companies": []})[
                    "companies"
                ].append(row["company"])
    return filters


def get_active_notification_profiles() -> pd.DataFrame:
    """Return active profiles that should receive fitting notification summaries."""
    sync_profiles_from_source()
    profile_mode_clause = _profile_mode_clause("p")
    pref_join = ""
    pref_column = "TRUE AS discord_enabled"
    if _portal_tables_exist():
        pref_join = """
        LEFT JOIN portal_profilenotificationpreference pref
          ON pref.profile_id = p.id
        """
        pref_column = "COALESCE(pref.discord_enabled, TRUE) AS discord_enabled"

    query = f"""
        SELECT
            p.id AS profile_id,
            p.profile_key,
            p.display_name,
            p.discord_channel_id,
            p.discord_webhook_url,
            {pref_column}
        FROM profiles p
        {pref_join}
        WHERE {profile_mode_clause}
          AND p.is_active = TRUE
        ORDER BY p.id ASC
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def get_jobs_to_notify() -> pd.DataFrame:
    """Get unnotified fit results that are ready to notify per profile."""
    sync_profiles_from_source()
    profile_mode_clause = _profile_mode_clause("p")
    if is_test_mode_enabled():
        decision_filter = """
          AND pj.llm_match IS NOT NULL
          AND NULLIF(TRIM(COALESCE(pj.llm_match_error, '')), '') IS NULL
        """
    else:
        decision_filter = """
          AND pj.fit_decision IN ('Strong Fit', 'Moderate Fit')
        """

    pref_join = ""
    pref_column = "TRUE AS discord_enabled"
    if _portal_tables_exist():
        pref_join = """
        LEFT JOIN portal_profilenotificationpreference pref
          ON pref.profile_id = p.id
        """
        pref_column = "COALESCE(pref.discord_enabled, TRUE) AS discord_enabled"

    query = f"""
        SELECT
            pj.profile_id,
            p.profile_key,
            p.display_name,
            p.discord_channel_id,
            p.discord_webhook_url,
            {pref_column},
            p.model_name,
            COALESCE(p.is_test_profile, FALSE) AS is_test_profile,
            j.id,
            j.title,
            j.company,
            j.job_url,
            j.batch_id,
            pj.fit_score,
            pj.fit_decision,
            pj.llm_match,
            pj.notified_at,
            pj.notify_status,
            pj.notify_error,
            pj.fit_status
        FROM profile_jobs pj
        JOIN jobs j ON j.id = pj.job_id
        JOIN profiles p ON p.id = pj.profile_id
        {pref_join}
        WHERE {profile_mode_clause}
          AND pj.notified_at IS NULL
          AND pj.fit_status IN ('fit_done', 'notify_failed')
          {decision_filter}
        ORDER BY pj.profile_id ASC, COALESCE(j.batch_id, 0) DESC, pj.fit_score DESC
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def mark_job_notified(
    profile_id: int,
    job_id: str,
    status: str = "sent",
    error: Optional[str] = None,
):
    """Mark notification status for one profile job."""
    init_db()
    with _connect() as conn:
        with conn.cursor() as cursor:
            if status == "sent":
                cursor.execute(
                    """
                    UPDATE profile_jobs
                    SET notified_at = CURRENT_TIMESTAMP,
                        notify_status = 'sent',
                        notify_error = NULL,
                        fit_status = 'notified'
                    WHERE profile_id = %s
                      AND job_id = %s
                    """,
                    (profile_id, job_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE profile_jobs
                    SET notify_status = %s,
                        notify_error = %s,
                        fit_status = 'notify_failed'
                    WHERE profile_id = %s
                      AND job_id = %s
                    """,
                    (status, error, profile_id, job_id),
                )



def create_notification_run(
    profile_id: int,
    *,
    dag_id: Optional[str] = None,
    dag_run_id: Optional[str] = None,
    source: str = "airflow",
    started_at: Optional[Any] = None,
    backfill_key: Optional[str] = None,
) -> int:
    """Create or fetch a durable notification run row."""
    init_db()
    with _connect() as conn:
        with conn.cursor() as cursor:
            if backfill_key:
                cursor.execute(
                    """
                    INSERT INTO notification_runs (
                        profile_id, dag_id, dag_run_id, source, started_at, backfill_key
                    )
                    VALUES (%s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP), %s)
                    ON CONFLICT (backfill_key) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (profile_id, dag_id, dag_run_id, source, started_at, backfill_key),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO notification_runs (
                        profile_id, dag_id, dag_run_id, source, started_at
                    )
                    VALUES (%s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP))
                    RETURNING id
                    """,
                    (profile_id, dag_id, dag_run_id, source, started_at),
                )
            return int(cursor.fetchone()[0])


def add_notification_run_jobs(run_id: int, jobs: Iterable[dict[str, Any]]) -> int:
    """Add pending job attempts to a notification run idempotently."""
    init_db()
    records = []
    for job in jobs or []:
        profile_id = job.get("profile_id")
        job_id = job.get("job_id") or job.get("id")
        if profile_id is None or not job_id:
            continue
        records.append(
            (
                int(run_id),
                int(profile_id),
                str(job_id),
                job.get("fit_score"),
                job.get("fit_decision"),
            )
        )
    if not records:
        return 0
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO notification_run_jobs (
                    run_id, profile_id, job_id, fit_score, fit_decision, notify_status
                )
                VALUES (%s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (run_id, profile_id, job_id) DO UPDATE SET
                    fit_score = EXCLUDED.fit_score,
                    fit_decision = EXCLUDED.fit_decision,
                    updated_at = CURRENT_TIMESTAMP
                """,
                records,
            )
    return len(records)


def claim_job_for_notification(profile_id: int, job_id: str, run_id: Optional[int] = None) -> bool:
    """Atomically claim one profile job before sending a notification."""
    init_db()
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE profile_jobs
                SET notify_status = 'notifying',
                    notify_error = NULL,
                    fit_status = 'notifying'
                WHERE profile_id = %s
                  AND job_id = %s
                  AND notified_at IS NULL
                  AND fit_status IN ('fit_done', 'notify_failed')
                RETURNING profile_id, job_id
                """,
                (int(profile_id), str(job_id)),
            )
            claimed = cursor.fetchone() is not None
            if not claimed and run_id is not None:
                mark_notification_run_job(
                    int(run_id),
                    int(profile_id),
                    str(job_id),
                    status="skipped",
                    error="already_claimed_or_notified",
                    cursor=cursor,
                )
            return claimed


def mark_notification_run_job(
    run_id: int,
    profile_id: int,
    job_id: str,
    *,
    status: str,
    error: Optional[str] = None,
    cursor=None,
) -> None:
    """Update durable per-run job status."""
    query = """
        UPDATE notification_run_jobs
        SET notify_status = %s,
            notify_error = %s,
            notified_at = CASE WHEN %s = 'sent' THEN CURRENT_TIMESTAMP ELSE notified_at END,
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s
          AND profile_id = %s
          AND job_id = %s
    """
    params = (status, error, status, int(run_id), int(profile_id), str(job_id))
    if cursor is not None:
        cursor.execute(query, params)
        return
    init_db()
    with _connect() as conn:
        with conn.cursor() as own_cursor:
            own_cursor.execute(query, params)


def mark_job_notification_skipped(
    profile_id: int,
    job_id: str,
    error: str = "discord_disabled_or_missing_config",
) -> None:
    """Mark a job as skipped without making it terminal/notified."""
    init_db()
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE profile_jobs
                SET notify_status = 'skipped',
                    notify_error = %s,
                    fit_status = 'fit_done'
                WHERE profile_id = %s
                  AND job_id = %s
                  AND notified_at IS NULL
                """,
                (error, int(profile_id), str(job_id)),
            )


def finalize_notification_run(
    run_id: int,
    *,
    summary_status: Optional[str] = None,
    summary_error: Optional[str] = None,
    error: Optional[str] = None,
) -> dict[str, Any]:
    """Finalize run counts/status from notification_run_jobs."""
    init_db()
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS eligible_count,
                    COUNT(*) FILTER (WHERE notify_status = 'sent') AS sent_count,
                    COUNT(*) FILTER (WHERE notify_status = 'failed') AS failed_count,
                    COUNT(*) FILTER (WHERE notify_status = 'skipped') AS skipped_count
                FROM notification_run_jobs
                WHERE run_id = %s
                """,
                (int(run_id),),
            )
            counts = dict(cursor.fetchone() or {})
            eligible = int(counts.get("eligible_count") or 0)
            sent = int(counts.get("sent_count") or 0)
            failed = int(counts.get("failed_count") or 0)
            skipped = int(counts.get("skipped_count") or 0)
            if eligible == 0:
                status = "completed_zero_results"
            elif failed > 0 and sent + skipped > 0:
                status = "partial_failed"
            elif failed > 0:
                status = "failed"
            else:
                status = "completed"
            cursor.execute(
                """
                UPDATE notification_runs
                SET status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    eligible_count = %s,
                    sent_count = %s,
                    failed_count = %s,
                    skipped_count = %s,
                    summary_status = %s,
                    summary_error = %s,
                    error = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    status,
                    eligible,
                    sent,
                    failed,
                    skipped,
                    summary_status,
                    summary_error,
                    error,
                    int(run_id),
                ),
            )
            return {
                "run_id": int(run_id),
                "status": status,
                "eligible_count": eligible,
                "sent_count": sent,
                "failed_count": failed,
                "skipped_count": skipped,
            }


def backfill_notification_runs() -> dict[str, int]:
    """Backfill historical notification run tables from notified_at minute buckets."""
    init_db()
    created_runs = 0
    inserted_jobs = 0
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    pj.profile_id,
                    date_trunc('minute', pj.notified_at) AS run_minute,
                    MIN(pj.notified_at) AS started_at,
                    MAX(pj.notified_at) AS completed_at
                FROM profile_jobs pj
                WHERE pj.notified_at IS NOT NULL
                GROUP BY pj.profile_id, run_minute
                ORDER BY run_minute ASC, pj.profile_id ASC
                """
            )
            buckets = cursor.fetchall()
            for bucket in buckets:
                profile_id = int(bucket["profile_id"])
                run_minute = bucket["run_minute"]
                backfill_key = f"profile:{profile_id}:minute:{run_minute:%Y%m%d%H%M}"
                cursor.execute(
                    """
                    INSERT INTO notification_runs (
                        profile_id, source, status, started_at, completed_at, backfill_key
                    )
                    VALUES (%s, 'backfill_minute_bucket', 'completed', %s, %s, %s)
                    ON CONFLICT (backfill_key) DO NOTHING
                    RETURNING id
                    """,
                    (profile_id, bucket["started_at"], bucket["completed_at"], backfill_key),
                )
                row = cursor.fetchone()
                if row:
                    created_runs += 1
                    run_id = int(row["id"])
                else:
                    cursor.execute(
                        "SELECT id FROM notification_runs WHERE backfill_key = %s",
                        (backfill_key,),
                    )
                    run_id = int(cursor.fetchone()["id"])
                cursor.execute(
                    """
                    INSERT INTO notification_run_jobs (
                        run_id, profile_id, job_id, fit_score, fit_decision,
                        notify_status, notify_error, notified_at
                    )
                    SELECT
                        %s,
                        pj.profile_id,
                        pj.job_id,
                        pj.fit_score,
                        pj.fit_decision,
                        COALESCE(NULLIF(pj.notify_status, ''), 'sent'),
                        pj.notify_error,
                        pj.notified_at
                    FROM profile_jobs pj
                    WHERE pj.profile_id = %s
                      AND date_trunc('minute', pj.notified_at) = %s
                      AND pj.notified_at IS NOT NULL
                    ON CONFLICT (run_id, profile_id, job_id) DO NOTHING
                    """,
                    (run_id, profile_id, run_minute),
                )
                inserted_jobs += cursor.rowcount
                cursor.execute(
                    """
                    UPDATE notification_runs nr
                    SET eligible_count = counts.eligible_count,
                        sent_count = counts.sent_count,
                        failed_count = counts.failed_count,
                        skipped_count = counts.skipped_count,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT
                            COUNT(*) AS eligible_count,
                            COUNT(*) FILTER (WHERE notify_status = 'sent') AS sent_count,
                            COUNT(*) FILTER (WHERE notify_status = 'failed') AS failed_count,
                            COUNT(*) FILTER (WHERE notify_status = 'skipped') AS skipped_count
                        FROM notification_run_jobs
                        WHERE run_id = %s
                    ) counts
                    WHERE nr.id = %s
                    """,
                    (run_id, run_id),
                )
    return {"created_runs": created_runs, "inserted_jobs": inserted_jobs}


def save_jd_result(
    job_id: str,
    description: Optional[str] = None,
    description_error: Optional[str] = None,
):
    """Persist one JD fetch result from the LinkedIn jobPosting API worker."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            if description:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET description = %s, description_error = NULL
                    WHERE id = %s
                    """,
                    (description, job_id),
                )
                cursor.execute(
                    """
                    UPDATE jd_queue
                    SET status = 'done', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP, error = NULL
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
            else:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET description = NULL, description_error = %s
                    WHERE id = %s
                    """,
                    (description_error, job_id),
                )
                cursor.execute(
                    """
                    UPDATE jd_queue
                    SET status = 'failed', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP, error = %s
                    WHERE job_id = %s
                    """,
                    (description_error, job_id),
                )


def claim_pending_jd_requests(
    limit: int = 10,
    job_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Atomically claim pending JD requests for one worker run."""
    init_db()
    stale_minutes = _get_positive_int_env("JD_CLAIM_STALE_MINUTES", 30)
    profile_mode_clause = _profile_mode_clause("p")
    normalized_job_ids = [str(job_id) for job_id in (job_ids or []) if job_id]
    if job_ids is not None and not normalized_job_ids:
        return pd.DataFrame(columns=["job_id", "job_url"])

    select_query = f"""
        SELECT q.job_id, q.job_url, j.source_job_id
        FROM jd_queue q
        JOIN jobs j ON j.id = q.job_id
        WHERE (
                q.status = 'pending'
                OR (
                    q.status = 'processing'
                    AND COALESCE(
                        q.updated_at,
                        TIMESTAMP '1970-01-01 00:00:00'
                    ) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                )
              )
          AND EXISTS (
                SELECT 1
                FROM profile_jobs pj
                JOIN profiles p ON p.id = pj.profile_id
                WHERE pj.job_id = q.job_id
                  AND {profile_mode_clause}
              )
    """
    params: List[Any] = [stale_minutes]
    if normalized_job_ids:
        select_query += " AND q.job_id = ANY(%s)"
        params.append(normalized_job_ids)

    select_query += """
        ORDER BY q.updated_at ASC
        FOR UPDATE SKIP LOCKED
    """
    if limit and int(limit) > 0:
        select_query += " LIMIT %s"
        params.append(int(limit))

    with _connect(row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(select_query, params)
                rows = cursor.fetchall()
                if not rows:
                    return pd.DataFrame(columns=["job_id", "job_url"])

                cursor.executemany(
                    """
                    UPDATE jd_queue
                    SET status = 'processing',
                        updated_at = CURRENT_TIMESTAMP,
                        error = NULL
                    WHERE job_id = %s
                    """,
                    [(row["job_id"],) for row in rows],
                )

    return pd.DataFrame(rows)


def get_jobs_by_ids(job_ids: List[str]) -> pd.DataFrame:
    """Fetch jobs by id list."""
    if not job_ids:
        return pd.DataFrame()
    init_db()

    query = "SELECT * FROM jobs WHERE id = ANY(%s)"

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (job_ids,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def get_pending_jd_requests(limit: int = 10) -> pd.DataFrame:
    """Fetch pending jd queue items for external worker."""
    init_db()
    query = """
        SELECT q.job_id, q.job_url
        FROM jd_queue q
        WHERE q.status = 'pending'
        ORDER BY q.updated_at ASC
        LIMIT %s
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def get_latest_batch_jobs() -> pd.DataFrame:
    """Retrieves only the jobs from the most recent batch."""
    query = """
        SELECT * FROM jobs
        WHERE batch_id = (SELECT MAX(id) FROM batches)
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully on Postgres")
