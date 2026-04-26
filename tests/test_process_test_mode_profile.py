from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PROCESS_SOURCE = (REPO_ROOT / "dags" / "process.py").read_text(encoding="utf-8")
DATABASE_SOURCE = (REPO_ROOT / "dags" / "database.py").read_text(encoding="utf-8")


def _slice(source: str, start_marker: str, end_marker: str) -> str:
    start = source.index(start_marker)
    end = source.index(end_marker, start)
    return source[start:end]


BUILD_SCAN_CONFIG_SOURCE = _slice(
    PROCESS_SOURCE,
    "def _build_scan_config(",
    "def _clean_scan_text(",
)
FILTER_JOBS_SOURCE = _slice(
    PROCESS_SOURCE,
    "def filter_jobs():",
    "def normalize_job_records(job_records):",
)
BACKFILL_SOURCE = _slice(
    DATABASE_SOURCE,
    "def _backfill_profile_jobs_from_existing_jobs(",
    "def _backfill_default_profile_jobs(",
)
BOOTSTRAP_FLAGGED_SOURCE = _slice(
    DATABASE_SOURCE,
    "def _bootstrap_flagged_profiles(",
    "def _sync_profiles_from_source(",
)
GET_JOBS_NEEDING_JD_SOURCE = _slice(
    DATABASE_SOURCE,
    "def get_jobs_needing_jd(",
    "def get_profile_jobs_ready_for_fitting(",
)
GET_PROFILE_JOBS_READY_SOURCE = _slice(
    DATABASE_SOURCE,
    "def get_profile_jobs_ready_for_fitting(",
    "def get_jobs_ready_for_fitting(",
)
CLAIM_PENDING_FITTING_SOURCE = _slice(
    DATABASE_SOURCE,
    "def claim_pending_fitting_tasks(",
    "def mark_fitting_done(",
)
GET_JOBS_TO_NOTIFY_SOURCE = _slice(
    DATABASE_SOURCE,
    "def get_jobs_to_notify(",
    "def mark_job_notified(",
)


def test_process_scan_config_uses_profile_results_per_term_without_env_override():
    assert re.search(
        r'"results_per_term"\s*:\s*int\(results_wanted\)',
        BUILD_SCAN_CONFIG_SOURCE,
    )
    assert "SCAN_RESULTS_PER_TERM" not in BUILD_SCAN_CONFIG_SOURCE


def test_process_source_rewrites_test_ids_and_preserves_raw_source_job_id():
    assert "source_job_id" in PROCESS_SOURCE
    assert "test-" in PROCESS_SOURCE
    assert '.str.fullmatch(r"[0-9]+", na=False)' not in PROCESS_SOURCE


def test_filter_jobs_checks_test_mode_before_applying_production_company_and_title_filters():
    assert "blocked_companies" in FILTER_JOBS_SOURCE
    assert "DEFAULT_TITLE_EXCLUDE_KEYWORDS" in FILTER_JOBS_SOURCE
    assert "DEFAULT_COMPANY_BLACKLIST" in FILTER_JOBS_SOURCE

    test_mode_markers = [
        marker
        for marker in ["LINKEDIN_TEST_MODE", "is_linkedin_test_mode", "is_test_mode"]
        if marker in FILTER_JOBS_SOURCE
    ]
    assert test_mode_markers, "expected filter_jobs to branch on test mode"

    production_filter_index = FILTER_JOBS_SOURCE.index("~company_col.isin(blocked_companies)")
    assert min(FILTER_JOBS_SOURCE.index(marker) for marker in test_mode_markers) < production_filter_index


def test_database_source_declares_test_mode_helpers_and_raw_source_job_id_fields():
    assert "def is_test_mode_enabled()" in DATABASE_SOURCE
    assert "def _profile_mode_clause(" in DATABASE_SOURCE
    assert "is_test_profile" in DATABASE_SOURCE
    assert "source_job_id" in DATABASE_SOURCE


def test_database_mode_aware_selectors_guard_dormant_test_rows_after_mode_off():
    selector_sources = {
        "get_jobs_needing_jd": GET_JOBS_NEEDING_JD_SOURCE,
        "get_profile_jobs_ready_for_fitting": GET_PROFILE_JOBS_READY_SOURCE,
        "claim_pending_fitting_tasks": CLAIM_PENDING_FITTING_SOURCE,
        "get_jobs_to_notify": GET_JOBS_TO_NOTIFY_SOURCE,
    }

    for selector_name, selector_source in selector_sources.items():
        assert "_profile_mode_clause" in selector_source, (
            f"{selector_name} is missing mode-aware dormant test-row gating"
        )


def test_database_bootstrap_source_excludes_test_rows_and_test_profiles():
    assert "INSERT INTO profile_jobs" in BACKFILL_SOURCE
    assert "NOT LIKE 'test-%'" in BACKFILL_SOURCE
    assert "source_job_id IS NULL" in BACKFILL_SOURCE
    assert 'not profile_config.get("is_test_profile", False)' in BOOTSTRAP_FLAGGED_SOURCE


def test_process_source_has_per_profile_filter_helpers():
    assert "def _apply_title_keyword_filter" in PROCESS_SOURCE
    assert "def _apply_company_blacklist_filter" in PROCESS_SOURCE
    assert "database.get_profile_scan_filters()" in FILTER_JOBS_SOURCE
    assert "profile_id" in FILTER_JOBS_SOURCE


def test_profile_title_keyword_filter_applies_per_profile():
    import pandas as pd
    import re

    namespace = {"pd": pd, "re": re}
    helper_source = _slice(
        PROCESS_SOURCE,
        "def _apply_title_keyword_filter",
        "def _normalize_source_job_id",
    )
    exec(helper_source, namespace)

    jobs = pd.DataFrame(
        [
            {
                "id": "1",
                "profile_id": 1,
                "title": "Senior Data Engineer",
                "company": "GoodCo",
            },
            {
                "id": "2",
                "profile_id": 2,
                "title": "Senior Data Engineer",
                "company": "GoodCo",
            },
            {
                "id": "3",
                "profile_id": 1,
                "title": "Junior Data Engineer",
                "company": "GoodCo",
            },
        ]
    )

    filtered = namespace["_apply_title_keyword_filter"](
        jobs, {1: {"title_keywords": ["Senior"], "companies": []}}
    )

    assert filtered["id"].tolist() == ["2", "3"]


def test_profile_company_blacklist_filter_applies_per_profile():
    import pandas as pd
    import re

    namespace = {"pd": pd, "re": re}
    helper_source = _slice(
        PROCESS_SOURCE,
        "def _apply_company_blacklist_filter",
        "def _normalize_source_job_id",
    )
    exec(helper_source, namespace)

    jobs = pd.DataFrame(
        [
            {
                "id": "1",
                "profile_id": 1,
                "title": "Data Engineer",
                "company": "BadCo",
            },
            {
                "id": "2",
                "profile_id": 2,
                "title": "Data Engineer",
                "company": "BadCo",
            },
            {
                "id": "3",
                "profile_id": 1,
                "title": "Data Engineer",
                "company": "GoodCo",
            },
        ]
    )

    filtered = namespace["_apply_company_blacklist_filter"](
        jobs, {1: {"title_keywords": [], "companies": ["BadCo"]}}
    )

    assert filtered["id"].tolist() == ["2", "3"]


def test_get_jobs_needing_jd_does_not_use_invalid_distinct_order_by():
    assert "SELECT DISTINCT" not in GET_JOBS_NEEDING_JD_SOURCE
    assert "COALESCE(q.updated_at" in GET_JOBS_NEEDING_JD_SOURCE
    assert "ORDER BY" in GET_JOBS_NEEDING_JD_SOURCE
