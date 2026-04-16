from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PROCESS_SOURCE = (REPO_ROOT / "dags" / "process.py").read_text(encoding="utf-8")
DATABASE_SOURCE = (REPO_ROOT / "dags" / "database.py").read_text(encoding="utf-8")
PROFILES = json.loads(
    (REPO_ROOT / "include" / "user_info" / "profiles.json").read_text(
        encoding="utf-8"
    )
)


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


def test_profiles_json_declares_one_dormant_test_profile_with_exact_scan_shape():
    test_profiles = [
        profile
        for profile in PROFILES
        if profile.get("test_mode_only") or profile.get("is_test_profile")
    ]

    assert len(test_profiles) == 1
    test_profile = test_profiles[0]
    search_configs = test_profile["search_configs"]

    assert test_profile["bootstrap_existing_jobs"] is False
    assert test_profile["discord_channel_id"] == "1476129860450779147"
    assert len(search_configs) == 1
    assert search_configs[0]["results_per_term"] == 20
    assert len(search_configs[0]["terms"]) == 2


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
    assert "senior" in FILTER_JOBS_SOURCE
    assert "medior" in FILTER_JOBS_SOURCE

    test_mode_markers = [
        marker
        for marker in ["LINKEDIN_TEST_MODE", "is_linkedin_test_mode", "is_test_mode"]
        if marker in FILTER_JOBS_SOURCE
    ]
    assert test_mode_markers, "expected filter_jobs to branch on test mode"

    production_filter_index = FILTER_JOBS_SOURCE.index("~company_col.isin(blocked_companies)")
    assert min(FILTER_JOBS_SOURCE.index(marker) for marker in test_mode_markers) < production_filter_index


def test_database_source_declares_test_profile_and_raw_source_job_id_fields():
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
        assert (
            "is_test_profile" in selector_source or "test-%" in selector_source
        ), f"{selector_name} is missing test-row/profile gating"


def test_database_bootstrap_source_excludes_test_rows_from_formal_backfill():
    assert "INSERT INTO profile_jobs" in BACKFILL_SOURCE
    assert "test-%" in BACKFILL_SOURCE or "is_test_profile" in BACKFILL_SOURCE
