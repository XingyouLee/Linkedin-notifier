from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
import os
import pandas as pd
import random
import re
import time
from html import unescape
from urllib.parse import urlencode

import requests
from dags import database
from dags.runtime_utils import df_to_xcom_records, load_env


load_env()

SCAN_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
SCAN_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

SCAN_TAG_RE = re.compile(r"<[^>]+>")
SCAN_WHITESPACE_RE = re.compile(r"\s+")
SCAN_JOB_URL_RE = re.compile(r'href="([^"]*?/jobs/view/[^"]+)"')
SCAN_TITLE_RE = re.compile(r"<h3[^>]*>(.*?)</h3>", re.S)
SCAN_COMPANY_RE = re.compile(r"<h4[^>]*>(.*?)</h4>", re.S)
SCAN_JOB_ID_RE = re.compile(r"-(\d+)\?")
SCAN_ITEM_RE = re.compile(r"<li>(.*?)</li>", re.S)
RAW_SCAN_JOB_ID_RE = re.compile(r"\d+")
TEST_JOB_ID_PREFIX = "test-"
PERSISTED_SCAN_JOB_ID_RE = re.compile(rf"(?:{re.escape(TEST_JOB_ID_PREFIX)})?(\d+)")


def _get_nonnegative_int_env(var_name: str, default: int) -> int:
    try:
        value = int(os.getenv(var_name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(0, value)


def _default_scan_results_per_term() -> int:
    return _get_nonnegative_int_env("SCAN_RESULTS_PER_TERM", 10)


def _coerce_bool(value, default: bool = False) -> bool:
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


def _is_test_mode_enabled() -> bool:
    return _coerce_bool(os.getenv("LINKEDIN_TEST_MODE"), False)


def _test_mode_cap(var_name: str, default: int) -> int | None:
    """Return a positive row cap only while LINKEDIN_TEST_MODE is enabled."""
    if not _is_test_mode_enabled():
        return None
    raw_value = os.getenv(var_name) or os.getenv("LINKEDIN_TEST_MAX_JOBS")
    try:
        cap = int(raw_value) if raw_value is not None else int(default)
    except (TypeError, ValueError):
        cap = int(default)
    return max(1, cap)


def _apply_test_mode_cap(records: list[dict], *, var_name: str, default: int, label: str) -> list[dict]:
    cap = _test_mode_cap(var_name, default)
    if cap is None or len(records) <= cap:
        return records
    print(
        f"Test mode cap applied for {label}: keeping {cap} of {len(records)} "
        f"records ({var_name} or LINKEDIN_TEST_MAX_JOBS)."
    )
    return records[:cap]


def _is_test_profile(value) -> bool:
    if isinstance(value, dict):
        if "is_test_profile" in value:
            return _coerce_bool(value.get("is_test_profile"), False)
        if "test_mode_only" in value:
            return _coerce_bool(value.get("test_mode_only"), False)
    return _coerce_bool(value, False)


def _apply_title_keyword_filter(jobs_df: pd.DataFrame, profile_filters: dict[int, dict[str, list[str]]]) -> pd.DataFrame:
    if jobs_df is None or jobs_df.empty or "profile_id" not in jobs_df.columns:
        return jobs_df
    filtered = jobs_df.copy()
    keep_mask = pd.Series(True, index=filtered.index)
    title_col = filtered.get("title", pd.Series("", index=filtered.index)).fillna("").astype(str).str.lower()
    for profile_id, config in (profile_filters or {}).items():
        keywords = [str(keyword).strip().lower() for keyword in config.get("title_keywords", []) if str(keyword).strip()]
        if not keywords:
            continue
        profile_mask = filtered["profile_id"].astype(str) == str(profile_id)
        keyword_mask = pd.Series(False, index=filtered.index)
        for keyword in keywords:
            keyword_mask = keyword_mask | title_col.str.contains(re.escape(keyword), na=False)
        keep_mask = keep_mask & ~(profile_mask & keyword_mask)
    return filtered[keep_mask]


def _apply_company_blacklist_filter(jobs_df: pd.DataFrame, profile_filters: dict[int, dict[str, list[str]]]) -> pd.DataFrame:
    if jobs_df is None or jobs_df.empty or "profile_id" not in jobs_df.columns:
        return jobs_df
    filtered = jobs_df.copy()
    keep_mask = pd.Series(True, index=filtered.index)
    company_col = filtered.get("company", pd.Series("", index=filtered.index)).fillna("").astype(str).str.strip().str.lower()
    for profile_id, config in (profile_filters or {}).items():
        companies = {str(company).strip().lower() for company in config.get("companies", []) if str(company).strip()}
        if not companies:
            continue
        profile_mask = filtered["profile_id"].astype(str) == str(profile_id)
        company_mask = company_col.isin(companies)
        keep_mask = keep_mask & ~(profile_mask & company_mask)
    return filtered[keep_mask]


def _normalize_source_job_id(source_job_id) -> str | None:
    if source_job_id is None:
        return None
    normalized = str(source_job_id).strip()
    if not normalized:
        return None
    if RAW_SCAN_JOB_ID_RE.fullmatch(normalized):
        return normalized
    match = PERSISTED_SCAN_JOB_ID_RE.fullmatch(normalized)
    if match:
        return match.group(1)
    return None


def _transform_scan_job_identity(
    job_id: str,
    *,
    is_test_profile: bool,
    source_job_id=None,
) -> tuple[str, str | None]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return "", None

    if RAW_SCAN_JOB_ID_RE.fullmatch(normalized_job_id):
        if is_test_profile:
            raw_job_id = _normalize_source_job_id(source_job_id) or normalized_job_id
            return f"{TEST_JOB_ID_PREFIX}{raw_job_id}", raw_job_id
        return normalized_job_id, None

    match = PERSISTED_SCAN_JOB_ID_RE.fullmatch(normalized_job_id)
    if match:
        raw_job_id = _normalize_source_job_id(source_job_id) or match.group(1)
        return normalized_job_id, raw_job_id

    return "", None


def _build_scan_config(
    results_wanted: int, hours_old: int, distance: int | None = None
) -> dict:
    if results_wanted is None:
        results_wanted = _default_scan_results_per_term()
    else:
        results_wanted = max(0, int(results_wanted))
    return {
        "request_page_size": int(os.getenv("SCAN_REQUEST_PAGE_SIZE", "10")),
        "http_max_retries": max(0, int(os.getenv("SCAN_HTTP_MAX_RETRIES", "6"))),
        "http_base_delay_sec": float(os.getenv("SCAN_HTTP_BASE_DELAY_SEC", "2.0")),
        "http_max_delay_sec": float(os.getenv("SCAN_HTTP_MAX_DELAY_SEC", "60.0")),
        "http_jitter_sec": float(os.getenv("SCAN_HTTP_JITTER_SEC", "1.5")),
        "between_req_min_sec": float(os.getenv("SCAN_BETWEEN_REQUESTS_MIN_SEC", "0.8")),
        "between_req_max_sec": float(os.getenv("SCAN_BETWEEN_REQUESTS_MAX_SEC", "1.8")),
        "between_terms_delay_sec": float(
            os.getenv("SCAN_BETWEEN_TERMS_DELAY_SEC", "8.0")
        ),
        "request_timeout_sec": int(os.getenv("SCAN_REQUEST_TIMEOUT_SEC", "45")),
        "results_per_term": int(results_wanted),
        "hours_old": int(hours_old),
        "distance": int(
            distance if distance is not None else os.getenv("SCAN_DISTANCE", "25")
        ),
    }


def _clean_scan_text(raw: str) -> str:
    if not raw:
        return ""
    text = unescape(raw)
    text = SCAN_TAG_RE.sub(" ", text)
    text = SCAN_WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _extract_scan_job_id(job_url: str) -> str:
    if not job_url:
        return ""
    match = SCAN_JOB_ID_RE.search(job_url)
    return match.group(1) if match else ""


def _build_scan_params(
    term: str,
    start: int,
    *,
    location: str | None,
    geo_id: str | None,
    distance: int,
    hours_old: int,
) -> dict:
    params = {
        "keywords": term,
        "distance": str(distance),
        "start": str(start),
    }
    if location:
        params["location"] = str(location).strip()
    if geo_id:
        params["geoId"] = str(geo_id).strip()
    if hours_old > 0:
        params["f_TPR"] = f"r{hours_old * 3600}"
    return params


def _build_scan_headers(
    term: str,
    *,
    location: str | None,
    geo_id: str | None,
) -> dict[str, str]:
    referer_params = {"keywords": term}
    if location:
        referer_params["location"] = str(location).strip()
    if geo_id:
        referer_params["geoId"] = str(geo_id).strip()
    return {
        "User-Agent": SCAN_USER_AGENT,
        "Accept-Language": os.getenv("SCAN_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
        "Referer": f"https://www.linkedin.com/jobs/search/?{urlencode(referer_params)}",
    }


def _parse_scan_items(html_text: str) -> list[dict]:
    rows = []
    for item in SCAN_ITEM_RE.findall(html_text):
        url_match = SCAN_JOB_URL_RE.search(item)
        title_match = SCAN_TITLE_RE.search(item)
        company_match = SCAN_COMPANY_RE.search(item)
        if not url_match or not title_match or not company_match:
            continue

        job_url = unescape(url_match.group(1))
        job_id = _extract_scan_job_id(job_url)
        if not job_id:
            continue

        rows.append(
            {
                "id": job_id,
                "title": _clean_scan_text(title_match.group(1)),
                "company": _clean_scan_text(company_match.group(1)),
                "job_url": job_url,
                "site": "linkedin",
            }
        )
    return rows


def _scan_fetch_page(
    session: requests.Session,
    term: str,
    start: int,
    *,
    scan_config: dict,
) -> list[dict]:
    for attempt in range(scan_config["http_max_retries"] + 1):
        try:
            response = session.get(
                SCAN_API_URL,
                params=_build_scan_params(
                    term=term,
                    start=start,
                    location=scan_config.get("location"),
                    geo_id=scan_config.get("geo_id"),
                    distance=scan_config["distance"],
                    hours_old=scan_config["hours_old"],
                ),
                timeout=scan_config["request_timeout_sec"],
                headers=_build_scan_headers(
                    term=term,
                    location=scan_config.get("location"),
                    geo_id=scan_config.get("geo_id"),
                ),
            )
        except requests.RequestException as error:
            if attempt >= scan_config["http_max_retries"]:
                print(
                    f"term={term}, start={start}, network_error={error}, "
                    f"attempt={attempt + 1}/{scan_config['http_max_retries'] + 1} -> stop page"
                )
                return []

            backoff = min(
                scan_config["http_max_delay_sec"],
                scan_config["http_base_delay_sec"] * (2**attempt),
            )
            sleep_sec = backoff + random.uniform(0, scan_config["http_jitter_sec"])
            print(
                f"term={term}, start={start}, network_error={error}, "
                f"retry_in={sleep_sec:.1f}s, attempt={attempt + 1}/{scan_config['http_max_retries'] + 1}"
            )
            time.sleep(sleep_sec)
            continue

        status_code = response.status_code
        if status_code == 429 or status_code >= 500:
            if attempt >= scan_config["http_max_retries"]:
                print(
                    f"term={term}, start={start}, status={status_code}, "
                    f"attempt={attempt + 1}/{scan_config['http_max_retries'] + 1} -> stop page"
                )
                return []

            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                sleep_sec = float(retry_after)
            else:
                backoff = min(
                    scan_config["http_max_delay_sec"],
                    scan_config["http_base_delay_sec"] * (2**attempt),
                )
                sleep_sec = backoff + random.uniform(0, scan_config["http_jitter_sec"])

            print(
                f"term={term}, start={start}, status={status_code}, "
                f"retry_in={sleep_sec:.1f}s, attempt={attempt + 1}/{scan_config['http_max_retries'] + 1}"
            )
            time.sleep(max(0.0, sleep_sec))
            continue

        if status_code >= 400:
            print(f"term={term}, start={start}, status={status_code} -> stop page")
            return []

        return _parse_scan_items(response.text)

    return []


def _scan_one_term(
    session: requests.Session,
    term: str,
    target_count: int,
    *,
    scan_config: dict,
) -> list[dict]:
    import math

    expected_pages = math.ceil(target_count / 25)
    term_rows = []
    seen_ids = set()
    start = 0
    request_count = 0

    while len(term_rows) < target_count:
        page_rows = _scan_fetch_page(
            session=session,
            term=term,
            start=start,
            scan_config=scan_config,
        )
        request_count += 1

        if not page_rows:
            print(
                f"term={term}, request={request_count}, start={start}, fetched=0 -> stop"
            )
            break

        added = 0
        for row in page_rows:
            if row["id"] in seen_ids:
                continue
            seen_ids.add(row["id"])
            row["search_term"] = term
            term_rows.append(row)
            added += 1
            if len(term_rows) >= target_count:
                break

        logical_page = math.ceil(len(term_rows) / 25) if term_rows else 1
        print(
            f"term={term}, request={request_count}, logical_page~{logical_page}/{expected_pages}, "
            f"start={start}, fetched={len(page_rows)}, added={added}, total={len(term_rows)}"
        )

        if added == 0:
            break

        start += scan_config["request_page_size"]
        if scan_config["between_req_max_sec"] > 0:
            delay_min = min(
                scan_config["between_req_min_sec"], scan_config["between_req_max_sec"]
            )
            delay_max = max(
                scan_config["between_req_min_sec"], scan_config["between_req_max_sec"]
            )
            time.sleep(random.uniform(delay_min, delay_max))

    return term_rows


def _collect_scan_rows(search_configs: list[dict]) -> list[dict]:
    all_rows = []
    with requests.Session() as session:
        for config_index, search_config in enumerate(search_configs):
            terms = [term for term in (search_config.get("terms") or []) if term]
            if not terms:
                continue
            profile_label = (
                search_config.get("profile_key")
                or search_config.get("display_name")
                or search_config.get("profile_id")
            )
            results_per_term = search_config.get("results_per_term")
            if results_per_term is None:
                raise ValueError(
                    "search_config.results_per_term is required for "
                    f"profile={profile_label} config={search_config.get('search_config_name')}"
                )
            scan_config = _build_scan_config(
                results_wanted=results_per_term,
                hours_old=search_config.get("hours_old") or 168,
                distance=search_config.get("distance") or 25,
            )
            scan_config["location"] = search_config.get("location") or "Netherlands"
            scan_config["geo_id"] = search_config.get("geo_id")

            print(
                f"Scanning profile={profile_label} config={search_config.get('search_config_name')} "
                f"terms={terms} location={scan_config['location']} hours_old={scan_config['hours_old']} "
                f"results_per_term={scan_config['results_per_term']} distance={scan_config['distance']} "
                f"geo_id={scan_config.get('geo_id')}"
            )

            if scan_config["results_per_term"] <= 0:
                print(
                    f"Skipping scan for profile={profile_label} "
                    f"config={search_config.get('search_config_name')} because "
                    "results_per_term=0"
                )
                continue

            for term_index, term in enumerate(terms):
                term_rows = _scan_one_term(
                    session=session,
                    term=term,
                    target_count=scan_config["results_per_term"],
                    scan_config=scan_config,
                )
                for row in term_rows:
                    row["profile_id"] = search_config.get("profile_id")
                    row["search_config_id"] = search_config.get("search_config_id")
                    row["search_term"] = term
                    row["is_test_profile"] = _is_test_profile(search_config)
                all_rows.extend(term_rows)

                if (
                    term_index < len(terms) - 1
                    and scan_config["between_terms_delay_sec"] > 0
                ):
                    print(
                        f"profile={profile_label} term={term} done, "
                        f"cooldown_before_next_term={scan_config['between_terms_delay_sec']:.1f}s"
                    )
                    time.sleep(scan_config["between_terms_delay_sec"])

            if (
                config_index < len(search_configs) - 1
                and scan_config["between_terms_delay_sec"] > 0
            ):
                time.sleep(scan_config["between_terms_delay_sec"])
    return all_rows


def _normalize_and_save_scan_rows(all_rows: list[dict]) -> dict:
    def _normalize_optional(series: pd.Series) -> pd.Series:
        normalized = series.fillna("").astype(str).str.strip()
        return normalized.mask(normalized == "", None)

    jobs_df = pd.DataFrame(all_rows or [])
    raw_count = len(jobs_df)

    if raw_count == 0:
        return {
            "scanned_raw": 0,
            "scanned_unique": 0,
            "scanned_duplicates": 0,
            "scanned_invalid_ids": 0,
        }

    normalized_df = pd.DataFrame(
        {
            "id": jobs_df.get("id", pd.Series(index=jobs_df.index, dtype="object"))
            .fillna("")
            .astype(str)
            .str.strip(),
            "title": _normalize_optional(
                jobs_df.get("title", pd.Series(index=jobs_df.index, dtype="object"))
            ),
            "company": _normalize_optional(
                jobs_df.get("company", pd.Series(index=jobs_df.index, dtype="object"))
            ),
            "job_url": _normalize_optional(
                jobs_df.get("job_url", pd.Series(index=jobs_df.index, dtype="object"))
            ),
            "site": _normalize_optional(
                jobs_df.get("site", pd.Series(index=jobs_df.index, dtype="object"))
            ),
            "search_term": _normalize_optional(
                jobs_df.get(
                    "search_term", pd.Series(index=jobs_df.index, dtype="object")
                )
            ),
            "profile_id": pd.to_numeric(
                jobs_df.get(
                    "profile_id", pd.Series(index=jobs_df.index, dtype="object")
                ),
                errors="coerce",
            ),
            "search_config_id": pd.to_numeric(
                jobs_df.get(
                    "search_config_id", pd.Series(index=jobs_df.index, dtype="object")
                ),
                errors="coerce",
            ),
            "is_test_profile": jobs_df.get(
                "is_test_profile",
                pd.Series(False, index=jobs_df.index, dtype="object"),
            ).map(_is_test_profile),
            "source_job_id": jobs_df.get(
                "source_job_id", pd.Series(index=jobs_df.index, dtype="object")
            ),
        }
    )
    transformed_identities = normalized_df.apply(
        lambda row: _transform_scan_job_identity(
            row["id"],
            is_test_profile=_is_test_profile(row["is_test_profile"]),
            source_job_id=row.get("source_job_id"),
        ),
        axis=1,
        result_type="expand",
    )
    normalized_df["id"] = transformed_identities[0]
    normalized_df["source_job_id"] = transformed_identities[1]
    normalized_df["site"] = normalized_df["site"].fillna("linkedin")
    normalized_df = normalized_df[
        normalized_df["id"].map(
            lambda value: bool(PERSISTED_SCAN_JOB_ID_RE.fullmatch(str(value or "").strip()))
        )
    ]
    normalized_count = len(normalized_df)
    invalid_id_count = raw_count - normalized_count

    if normalized_count == 0:
        print("No valid job ids found after normalization.")
        return {
            "scanned_raw": raw_count,
            "scanned_unique": 0,
            "scanned_duplicates": 0,
            "scanned_invalid_ids": invalid_id_count,
        }

    per_term_stats = (
        normalized_df[["search_term", "id"]]
        .drop_duplicates(subset=["search_term", "id"])
        .groupby("search_term", dropna=False)["id"]
        .nunique()
        .sort_values(ascending=False)
        .to_dict()
    )

    normalized_df = normalized_df.assign(
        quality_score=(
            normalized_df["job_url"].notna().astype(int)
            + normalized_df["title"].notna().astype(int)
            + normalized_df["company"].notna().astype(int)
            + normalized_df["search_term"].notna().astype(int)
        ),
        title_len=normalized_df["title"].fillna("").str.len(),
        company_len=normalized_df["company"].fillna("").str.len(),
    )
    deduped_df = (
        normalized_df.sort_values(
            by=["id", "quality_score", "title_len", "company_len"],
            ascending=[True, False, False, False],
        )
        .drop_duplicates(subset=["id"], keep="first")
        .drop(
            columns=[
                "quality_score",
                "title_len",
                "company_len",
                "search_term",
                "is_test_profile",
            ]
        )
        .reset_index(drop=True)
    )
    unique_count = len(deduped_df)
    duplicate_count = normalized_count - unique_count

    print(f"Per-term unique stats: {per_term_stats}")
    print(
        "Merged id stats: "
        f"total={raw_count}, valid={normalized_count}, unique={unique_count}, "
        f"duplicates={duplicate_count}, invalid_ids={invalid_id_count}"
    )

    database.save_jobs(
        deduped_df[["id", "site", "job_url", "title", "company", "source_job_id"]]
    )

    profile_job_df = (
        normalized_df[["profile_id", "search_config_id", "id", "search_term"]]
        .dropna(subset=["profile_id", "id"])
        .sort_values(by=["profile_id", "id", "search_config_id"])
        .drop_duplicates(subset=["profile_id", "id"], keep="first")
        .rename(columns={"id": "job_id", "search_term": "matched_term"})
        .reset_index(drop=True)
    )
    if not profile_job_df.empty:
        profile_job_df["profile_id"] = profile_job_df["profile_id"].astype(int)
        if profile_job_df["search_config_id"].notna().any():
            profile_job_df["search_config_id"] = profile_job_df[
                "search_config_id"
            ].astype("Int64")
    linked_count = database.upsert_profile_jobs(profile_job_df)

    per_profile_stats = (
        profile_job_df.groupby("profile_id")["job_id"].nunique().to_dict()
        if not profile_job_df.empty
        else {}
    )
    print(f"Per-profile discovered job stats: {per_profile_stats}")

    return {
        "scanned_raw": raw_count,
        "scanned_unique": unique_count,
        "scanned_duplicates": duplicate_count,
        "scanned_invalid_ids": invalid_id_count,
        "profile_job_links": linked_count,
    }


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="0 */12 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["linkedin_notifier"],
)
def linkedin_notifier():
    @task(retries=1, retry_delay=timedelta(minutes=2))
    def scan_and_save_jobs():
        """Scrape LinkedIn jobs for every active profile search config."""
        search_configs = database.get_active_search_configs()
        if not search_configs:
            print("No active profile search configs found. Skip scan stage.")
            return {
                "scanned_raw": 0,
                "scanned_unique": 0,
                "search_configs": 0,
                "profiles": 0,
            }

        profile_count = len({config["profile_id"] for config in search_configs})
        print(
            f"Scanning LinkedIn jobs for {profile_count} profiles across "
            f"{len(search_configs)} active search configs."
        )

        all_rows = _collect_scan_rows(search_configs)

        if not all_rows:
            print("No jobs returned by scripts scraper.")
            return {
                "scanned_raw": 0,
                "scanned_unique": 0,
                "search_configs": len(search_configs),
                "profiles": profile_count,
            }

        all_rows = _apply_test_mode_cap(
            all_rows,
            var_name="LINKEDIN_TEST_MAX_SCAN_ROWS",
            default=10,
            label="scan rows",
        )
        scan_stats = _normalize_and_save_scan_rows(all_rows)
        print(
            "Scan stage progress: "
            f"done={scan_stats.get('scanned_unique', 0)} "
            f"total={scan_stats.get('scanned_unique', 0)} "
            f"profile_job_links={scan_stats.get('profile_job_links', 0)}"
        )

        return {
            "scan_source": "scripts_guest_api",
            "search_configs": len(search_configs),
            "profiles": profile_count,
            **scan_stats,
        }

    @task
    def filter_jobs():
        jd_max_attempts = int(os.getenv("JD_MAX_ATTEMPTS", "3"))
        df = database.get_jobs_needing_jd(max_attempts=jd_max_attempts)
        blocked_companies = database.DEFAULT_COMPANY_BLACKLIST
        if df is None or df.empty:
            print("No jobs currently need JD. Skip JD filtering stage.")
            return []

        if "company" not in df.columns or "title" not in df.columns:
            print(
                "Missing company/title columns in JD backlog query. Skip JD filtering stage."
            )
            return []

        filter_columns = ["id", "site", "job_url", "title", "company", "profile_id"]
        for col in filter_columns:
            if col not in df.columns:
                df[col] = None

        filtered_df = df[filter_columns].copy()
        if _is_test_mode_enabled():
            filtered_records = df_to_xcom_records(filtered_df)
            total_records = len(filtered_records)
            filtered_records = _apply_test_mode_cap(
                filtered_records,
                var_name="LINKEDIN_TEST_MAX_JD_JOBS",
                default=10,
                label="JD backlog",
            )
            print(
                f"JD backlog candidates in test mode: {len(filtered_records)} "
                f"of {total_records} "
                f"(production pre-JD filters bypassed, max_attempts={jd_max_attempts})"
            )
            return filtered_records

        company_col = filtered_df["company"].fillna("").astype(str).str.strip()
        title_col = filtered_df["title"].fillna("").astype(str).str.lower()
        default_title_pattern = "|".join(
            re.escape(keyword) for keyword in database.DEFAULT_TITLE_EXCLUDE_KEYWORDS
        )
        filtered_df = filtered_df[
            ~company_col.isin(blocked_companies)
            & ~title_col.str.contains(default_title_pattern, na=False)
        ]
        profile_filters = database.get_profile_scan_filters()
        before_profile_filters = len(filtered_df)
        filtered_df = _apply_title_keyword_filter(filtered_df, profile_filters)
        after_title_filter = len(filtered_df)
        filtered_df = _apply_company_blacklist_filter(filtered_df, profile_filters)
        print(
            "Profile scan filters suppressed "
            f"title={before_profile_filters - after_title_filter} "
            f"company={after_title_filter - len(filtered_df)}"
        )
        filtered_records = df_to_xcom_records(filtered_df)
        print(
            f"JD backlog candidates after filters: {len(filtered_records)} "
            f"(max_attempts={jd_max_attempts})"
        )
        return filtered_records

    @task
    def normalize_job_records(job_records):
        if not job_records:
            return []
        return [
            {
                "id": record.get("id"),
                "job_url": record.get("job_url"),
            }
            for record in job_records
            if record.get("id")
        ]

    @task.branch
    def branch_after_filter(job_records):
        count = len(job_records or [])
        if count == 0:
            print("No jobs currently need JD. Skip JD stage and continue fitting sync.")
            return "finish_no_new_jobs"
        print(f"Found {count} jobs needing JD after filtering. Continue JD stage.")
        return "enqueue_jd_requests"

    @task
    def enqueue_jd_requests(job_records):
        jobs_df = pd.DataFrame(job_records or [])
        queued = database.enqueue_jd_requests(jobs_df)
        print(f"Queued {queued} JD requests for in-DAG worker")
        return [j.get("id") for j in (job_records or []) if j.get("id")]

    @task(task_id="run_jd_worker")
    def run_jd_worker(job_ids, worker_batch_size=5, max_loops=20, idle_loop_limit=2):
        import math
        import time

        from dags.jd_api_worker import run_once

        job_ids = job_ids or []
        if not job_ids:
            return {"processed": 0, "done": 0, "failed": 0, "pending": 0, "total": 0}

        worker_batch_size = int(
            os.getenv("JD_WORKER_BATCH_SIZE", str(worker_batch_size))
        )
        idle_loop_limit = int(
            os.getenv("JD_WORKER_IDLE_LOOP_LIMIT", str(idle_loop_limit))
        )
        worker_batch_size = max(1, worker_batch_size)
        idle_loop_limit = max(1, idle_loop_limit)

        total = len(job_ids)
        min_loops_needed = math.ceil(total / worker_batch_size) + idle_loop_limit
        max_loops = int(
            os.getenv("JD_WORKER_MAX_LOOPS", str(max(max_loops, min_loops_needed)))
        )
        if max_loops <= 0:
            max_loops = min_loops_needed

        total_processed = 0
        idle_loops = 0

        print(
            f"JD worker config: batch_size={worker_batch_size}, "
            f"max_loops={max_loops}, idle_loop_limit={idle_loop_limit}, "
            f"min_loops_needed={min_loops_needed}, total={total}"
        )

        for _ in range(max_loops):
            done_count = database.count_jd_queue_status(job_ids, "done")
            failed_count = database.count_jd_queue_status(job_ids, "failed")
            pending_count = total - done_count - failed_count
            print(
                f"JD queue progress(before): done={done_count}, failed={failed_count}, "
                f"pending={pending_count}, total={total}"
            )
            print(
                "JD stage progress: "
                f"done={done_count + failed_count} total={total} "
                f"successful={done_count} failed={failed_count} in_progress={pending_count}"
            )

            if pending_count <= 0:
                break

            processed = run_once(limit=worker_batch_size, job_ids=job_ids)
            total_processed += processed

            if processed == 0:
                idle_loops += 1
                if idle_loops >= idle_loop_limit:
                    break
                time.sleep(2)
                continue

            idle_loops = 0
            time.sleep(random.randint(1, 3))

        done_count = database.count_jd_queue_status(job_ids, "done")
        failed_count = database.count_jd_queue_status(job_ids, "failed")
        pending_count = total - done_count - failed_count
        result = {
            "processed": total_processed,
            "done": done_count,
            "failed": failed_count,
            "pending": pending_count,
            "total": total,
        }
        print(
            "JD stage final progress: "
            f"done={done_count + failed_count} total={total} "
            f"successful={done_count} failed={failed_count} remaining={pending_count}"
        )

        if pending_count > 0:
            raise RuntimeError(
                "JD worker stopped with pending jobs: "
                f"{result}. Increase JD_WORKER_MAX_LOOPS (suggested >= {min_loops_needed}) "
                f"or JD_WORKER_BATCH_SIZE (current {worker_batch_size})."
            )

        return result

    @task
    def enqueue_fitting_tasks():
        jobs_df = database.get_profile_jobs_ready_for_fitting()
        if jobs_df is None or jobs_df.empty:
            return {"queued": 0}

        fitting_cap = _test_mode_cap("LINKEDIN_TEST_MAX_FIT_JOBS", 10)
        if fitting_cap is not None and len(jobs_df) > fitting_cap:
            print(
                f"Test mode cap applied for fitting queue: keeping {fitting_cap} "
                f"of {len(jobs_df)} rows (LINKEDIN_TEST_MAX_FIT_JOBS or LINKEDIN_TEST_MAX_JOBS)."
            )
            jobs_df = jobs_df.head(fitting_cap).copy()

        queued = database.enqueue_fitting_requests(jobs_df)
        print(f"Queued {queued} fitting requests")
        print(f"Fitting stage progress: done=0 total={queued} queued={queued}")
        return {"queued": queued}

    # Pipeline wiring (all task calls at bottom)
    scan_meta = scan_and_save_jobs()

    filtered_job_records = filter_jobs()
    scan_meta >> filtered_job_records
    normalized_job_records = normalize_job_records(filtered_job_records)
    branch_next = branch_after_filter(normalized_job_records)
    finish_no_new_jobs = EmptyOperator(task_id="finish_no_new_jobs")
    jd_stage_complete = EmptyOperator(
        task_id="jd_stage_complete",
        trigger_rule="none_failed_min_one_success",
    )

    branch_next >> finish_no_new_jobs

    queued_job_ids = enqueue_jd_requests(normalized_job_records)
    branch_next >> queued_job_ids
    jd_worker_meta = run_jd_worker(queued_job_ids)
    fitting_enqueue_meta = enqueue_fitting_tasks()

    finish_no_new_jobs >> jd_stage_complete
    jd_worker_meta >> jd_stage_complete
    jd_stage_complete >> fitting_enqueue_meta

    trigger_fitting_notifier = TriggerDagRunOperator(
        task_id="trigger_fitting_notifier",
        trigger_dag_id="linkedin_fitting_notifier",
        conf={"source_dag_run_id": "{{ dag_run.run_id }}"},
        wait_for_completion=False,
    )
    fitting_enqueue_meta >> trigger_fitting_notifier


linkedin_notifier()
