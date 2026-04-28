import re
import json
from pathlib import Path

import pandas as pd
import pytest

from dags import database
from dags import jd_api_worker


class DummyCursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 1
        self._rows = []
        self._next_fetch = (1,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - stub
        return False

    def execute(self, sql, params=None):
        self.calls.append(("execute", sql, params))
        self.last_sql = sql
        self.last_params = params
        if "RETURNING id" in sql:
            self._next_fetch = (42,)

    def executemany(self, sql, params_seq):
        self.calls.append(("executemany", sql, list(params_seq)))

    def fetchone(self):
        return self._next_fetch

    def fetchall(self):
        return list(self._rows)


class DummyTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - stub
        return False


class DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - stub
        return False

    def cursor(self):
        return self._cursor

    def transaction(self):
        return DummyTransaction()


def _patch_connect(monkeypatch, cursor):
    def _dummy_connect(*args, **kwargs):
        return DummyConnection(cursor)

    monkeypatch.setattr(database, "_connect", _dummy_connect)
    monkeypatch.setattr(database, "init_db", lambda **kwargs: None)
    monkeypatch.setattr(database, "sync_profiles_from_source", lambda force=False: 0)



def test_extract_fit_fields_rejects_non_numeric_score():
    payload = json.dumps({"fit_score": "Not Recommended", "decision": "Not Recommended"})

    assert database._extract_fit_fields(payload) == (None, "Not Recommended")


def test_save_llm_matches_does_not_write_decision_into_score(monkeypatch):
    cursor = DummyCursor()
    _patch_connect(monkeypatch, cursor)
    df = pd.DataFrame(
        [
            {
                "profile_id": 2,
                "job_id": "4406173144",
                "llm_match": json.dumps(
                    {"fit_score": "Not Recommended", "decision": "Not Recommended"}
                ),
                "llm_match_error": None,
            }
        ]
    )

    database.save_llm_matches(df)

    executemany_call = next(call for call in cursor.calls if call[0] == "executemany")
    records = executemany_call[2]
    assert records == [
        (
            json.dumps({"fit_score": "Not Recommended", "decision": "Not Recommended"}),
            None,
            None,
            "Not Recommended",
            2,
            "4406173144",
        )
    ]

def test_save_jobs_performs_upsert(monkeypatch):
    cursor = DummyCursor()
    _patch_connect(monkeypatch, cursor)
    df = pd.DataFrame(
        [
            {
                "id": "100",
                "site": "linkedin",
                "job_url": "https://example.com/100",
                "title": "Data Engineer",
                "company": "Codex",
            }
        ]
    )

    database.save_jobs(df)

    job_insert = next(
        sql
        for kind, sql, _ in cursor.calls
        if kind == "execute" and "INSERT INTO jobs" in sql
    )
    assert "ON CONFLICT(id) DO UPDATE SET" in job_insert
    assert (
        "job_url = COALESCE(NULLIF(BTRIM(EXCLUDED.job_url), ''), jobs.job_url)"
        in job_insert
    )


def test_coerce_profile_configs_preserves_zero_results_per_term():
    profiles = database._coerce_profile_configs(
        [
            {
                "profile_key": "zero-scan",
                "search_configs": [
                    {
                        "name": "default",
                        "location": "Netherlands",
                        "distance": 25,
                        "hours_old": 24,
                        "results_per_term": 0,
                        "terms": ["data engineer"],
                    }
                ],
            }
        ]
    )

    assert profiles[0]["search_configs"][0]["results_per_term"] == 0


def test_coerce_profile_configs_defaults_netherlands_geo_id():
    profiles = database._coerce_profile_configs(
        [
            {
                "profile_key": "nl-default-geo",
                "search_configs": [
                    {
                        "name": "default",
                        "location": "Netherlands",
                        "distance": 25,
                        "hours_old": 24,
                        "results_per_term": 10,
                        "terms": ["data engineer"],
                    }
                ],
            }
        ]
    )

    assert profiles[0]["search_configs"][0]["geo_id"] == "102890719"


def test_coerce_profile_configs_preserves_explicit_geo_id():
    profiles = database._coerce_profile_configs(
        [
            {
                "profile_key": "explicit-geo",
                "search_configs": [
                    {
                        "name": "default",
                        "location": "Germany",
                        "geo_id": "90000001",
                        "distance": 25,
                        "hours_old": 24,
                        "results_per_term": 10,
                        "terms": ["python"],
                    }
                ],
            }
        ]
    )

    assert profiles[0]["search_configs"][0]["geo_id"] == "90000001"


def test_coerce_profile_configs_requires_results_per_term():
    try:
        database._coerce_profile_configs(
            [
                {
                    "profile_key": "missing-results",
                    "search_configs": [
                        {
                            "name": "default",
                            "location": "Netherlands",
                            "distance": 25,
                            "hours_old": 24,
                            "terms": ["data engineer"],
                        }
                    ],
                }
            ]
        )
        assert False, "Expected ValueError for missing results_per_term"
    except ValueError as error:
        assert "results_per_term is required" in str(error)


def test_coerce_profile_configs_rejects_none_results_per_term():
    try:
        database._coerce_profile_configs(
            [
                {
                    "profile_key": "none-results",
                    "search_configs": [
                        {
                            "name": "default",
                            "location": "Netherlands",
                            "distance": 25,
                            "hours_old": 24,
                            "results_per_term": None,
                            "terms": ["data engineer"],
                        }
                    ],
                }
            ]
        )
        assert False, "Expected ValueError for None results_per_term"
    except ValueError as error:
        assert "results_per_term is required" in str(error)


def test_load_profile_configs_from_file_hydrates_resume_text_from_markdown(tmp_path):
    resume_dir = tmp_path / "resume"
    resume_dir.mkdir(parents=True, exist_ok=True)
    resume_path = resume_dir / "candidate.md"
    resume_path.write_text("# Candidate\n\nExperience bullets", encoding="utf-8")

    profiles_path = tmp_path / "profiles.json"
    profiles_path.write_text(
        json.dumps(
            [
                {
                    "profile_key": "default",
                    "display_name": "Default",
                    "resume_path": "resume/candidate.md",
                    "resume_text": None,
                }
            ]
        ),
        encoding="utf-8",
    )

    profiles = database._load_profile_configs_from_file(profiles_path)

    assert len(profiles) == 1
    assert profiles[0]["resume_path"] == str(resume_path.resolve())
    assert "Experience bullets" in (profiles[0].get("resume_text") or "")


def test_file_backed_profiles_do_not_persist_model_name(monkeypatch):
    monkeypatch.setenv("FITTING_MODEL_NAME", "gpt-5.5")

    profiles = database._coerce_profile_configs(
        [
            {
                "profile_key": "candidate",
                "display_name": "Candidate",
                "resume_text": "Resume",
                "search_configs": [
                    {
                        "name": "default",
                        "terms": ["data engineer"],
                        "results_per_term": 1,
                    }
                ],
            },
            {
                "profile_key": "explicit",
                "display_name": "Explicit",
                "resume_text": "Resume",
                "model_name": "profile-model",
                "search_configs": [
                    {
                        "name": "default",
                        "terms": ["data engineer"],
                        "results_per_term": 1,
                    }
                ],
            },
        ]
    )

    assert profiles[0]["model_name"] is None
    assert profiles[1]["model_name"] is None


def test_get_active_search_configs_preserves_zero_results_per_term(monkeypatch):
    cursor = DummyCursor()
    cursor._rows = [
        {
            "profile_id": 1,
            "profile_key": "zero-scan",
            "display_name": "Zero Scan",
            "resume_path": None,
            "resume_text": None,
            "discord_channel_id": None,
            "discord_webhook_url": None,
            "model_name": "gpt-5.4",
            "search_config_id": 10,
            "search_config_name": "default",
            "location": "Netherlands",
            "geo_id": None,
            "distance": 25,
            "hours_old": 24,
            "results_per_term": 0,
            "term": "data engineer",
        }
    ]
    _patch_connect(monkeypatch, cursor)

    configs = database.get_active_search_configs()

    assert len(configs) == 1
    assert configs[0]["results_per_term"] == 0
    assert configs[0]["geo_id"] == "102890719"


def test_collect_scan_rows_requires_results_per_term_with_clear_profile_context():
    source = (Path(__file__).resolve().parents[1] / "dags" / "process.py").read_text()

    profile_label_match = re.search(
        r"profile_label\s*=\s*\(\s*search_config.get\(\"profile_key\"\)",
        source,
    )
    results_guard_match = re.search(
        r"results_per_term\s*=\s*search_config.get\(\"results_per_term\"\)",
        source,
    )

    assert profile_label_match is not None
    assert results_guard_match is not None
    assert profile_label_match.start() < results_guard_match.start()
    assert "search_config.results_per_term is required for " in source


def test_claim_pending_fitting_tasks_only_reclaims_stale_fitting(monkeypatch):
    cursor = DummyCursor()
    cursor._rows = [{"profile_id": 7, "job_id": "10", "attempts": 0}]
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setenv("FITTING_CLAIM_STALE_MINUTES", "15")

    result = database.claim_pending_fitting_tasks()

    assert result == [{"profile_id": 7, "job_id": "10", "attempts": 0}]
    select_sql = cursor.calls[0][1]
    select_params = cursor.calls[0][2]
    assert "fit_status = 'pending_fit'" in select_sql
    assert "fit_status = 'fitting'" in select_sql
    assert "CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')" in select_sql
    assert select_params[0] == 15


def test_claim_pending_jd_requests_scopes_to_requested_job_ids(monkeypatch):
    cursor = DummyCursor()
    cursor._rows = [{"job_id": "job-1", "job_url": "https://example.com/job-1"}]
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setenv("JD_CLAIM_STALE_MINUTES", "20")

    claimed = database.claim_pending_jd_requests(limit=2, job_ids=["job-1", "job-2"])

    assert claimed.to_dict(orient="records") == [
        {"job_id": "job-1", "job_url": "https://example.com/job-1"}
    ]
    select_sql = cursor.calls[0][1]
    select_params = cursor.calls[0][2]
    update_sql = cursor.calls[1][1]
    assert "q.job_id = ANY(%s)" in select_sql
    assert select_params[0] == 20
    assert select_params[1] == ["job-1", "job-2"]
    assert select_params[2] == 2
    assert "SET status = 'processing'" in update_sql


def test_jd_api_run_once_forwards_job_ids_to_claim_api(monkeypatch):
    captured = {}

    def fake_claim_pending_jd_requests(limit, job_ids=None):
        captured["limit"] = limit
        captured["job_ids"] = job_ids
        return pd.DataFrame(columns=["job_id", "job_url"])

    monkeypatch.setattr(
        database, "claim_pending_jd_requests", fake_claim_pending_jd_requests
    )

    processed = jd_api_worker.run_once(limit=3, job_ids=["a", "b"])

    assert processed == 0
    assert captured == {"limit": 3, "job_ids": ["a", "b"]}


def test_jd_api_extract_description_reads_description_rich_block():
    html = """
    <div class="description__text description__text--rich">
      <p>Build resilient Python data pipelines for analytics and machine learning workflows.</p>
    </div>
    <ul class="description__job-criteria-list">
      <li>Seniority level Associate</li>
      <li>Employment type Full-time</li>
    </ul>
    """

    extracted = jd_api_worker.extract_description(html)

    assert extracted is not None
    assert "Python data pipelines" in extracted
    assert "Seniority level Associate" in extracted


def test_jd_api_extract_description_reads_job_criteria_when_description_missing():
    html = """
    <ul class="description__job-criteria-list">
      <li>Seniority level Associate</li>
      <li>Employment type Full-time</li>
      <li>Job function Information Technology</li>
    </ul>
    """

    extracted = jd_api_worker.extract_description(html)

    assert extracted is not None
    assert "Seniority level Associate" in extracted
    assert "Job function Information Technology" in extracted


def test_jd_api_extract_description_keeps_nested_div_text():
    html = """
    <div class="description__text description__text--rich">
      <div>
        <p>Line one about backend services and data pipelines.</p>
      </div>
      <div>
        <p>Line two about PostgreSQL, Python, and production APIs.</p>
      </div>
    </div>
    """

    extracted = jd_api_worker.extract_description(html)

    assert extracted is not None
    assert "backend services and data pipelines" in extracted
    assert "PostgreSQL, Python, and production APIs" in extracted


def test_jd_api_extract_description_combines_description_and_criteria():
    html = """
    <div class="description__text description__text--rich">
      <p><strong>Achter elke succesvolle collectie staat data die klopt.</strong></p>
      <p>Als Product Data Quality Specialist ben je verantwoordelijk voor de kwaliteit, volledigheid en actualiteit van onze productdata.</p>
      <p><strong>Wat ga je doen?</strong></p>
      <ul>
        <li>Je beheert, verrijkt en optimaliseert productdata.</li>
        <li>Je onderzoekt structurele oorzaken van datakwaliteitsproblemen.</li>
      </ul>
    </div>
    <ul class="description__job-criteria-list">
      <li class="description__job-criteria-item">Seniority level Associate</li>
      <li class="description__job-criteria-item">Employment type Part-time</li>
    </ul>
    """

    extracted = jd_api_worker.extract_description(html)

    assert extracted is not None
    assert "Product Data Quality Specialist" in extracted
    assert "optimaliseert productdata" in extracted
    assert "Seniority level Associate" in extracted


def test_normalize_fit_prompt_text_appends_placeholders_when_missing():
    prompt_text = database._normalize_fit_prompt_text(
        "Return JSON with fit_score and decision only."
    )

    assert "{{candidate_summary}}" in prompt_text
    assert "{{job_title}}" in prompt_text
    assert "{{job_description}}" in prompt_text
    assert "{{candidate_resume}}" in prompt_text
    assert "{{candidate_summary}}" in prompt_text


def test_default_fit_prompt_text_contains_candidate_summary_contract():
    prompt_text = database._normalize_fit_prompt_text(None)

    assert '"candidate_summary": {' in prompt_text
    assert '"experience_blocker": true/false' in prompt_text


def test_default_fit_prompt_text_contains_output_schema():
    prompt_text = database._normalize_fit_prompt_text(None)

    assert '"fit_score": 0-100' in prompt_text
    assert '"candidate_summary": {' in prompt_text
    assert '"experience_blocker": true/false' in prompt_text
    assert (
        '"decision": "Strong Fit | Moderate Fit | Weak Fit | Not Recommended"'
        in prompt_text
    )


def test_coerce_profile_configs_supports_active_alias():
    profiles = database._coerce_profile_configs(
        [
            {
                "profile_key": "george",
                "display_name": "George",
                "active": False,
                "bootstrap_existing_jobs": True,
                "candidate_summary": {
                    "summary": "Backend candidate.",
                    "target_roles": ["Backend Engineer"],
                    "candidate_years": 2,
                    "candidate_seniority": "junior",
                    "core_skills": ["Python"],
                    "obvious_gaps": [],
                    "language_signals": {
                        "dutch_level": "basic",
                        "english_level": "fluent",
                        "notes": "Good English.",
                    },
                },
                "search_configs": [
                    {
                        "name": "default",
                        "location": "Netherlands",
                        "active": True,
                        "distance": 25,
                        "hours_old": 72,
                        "results_per_term": 10,
                        "terms": ["Python Engineer"],
                    }
                ],
            }
        ]
    )

    assert len(profiles) == 1
    assert profiles[0]["is_active"] is False
    assert profiles[0]["bootstrap_existing_jobs"] is True
    assert profiles[0]["candidate_summary_config"]["candidate_years"] == 2
    assert profiles[0]["search_configs"][0]["location"] == "Netherlands"
    assert profiles[0]["search_configs"][0]["is_active"] is True


def test_sync_profiles_from_source_deactivates_using_normalized_profiles(monkeypatch):
    captured = {
        "synced": None,
        "deactivated_keys": None,
        "bootstrapped": None,
    }

    monkeypatch.setattr(
        database,
        "_resolve_profiles_config_path",
        lambda: database.INCLUDE_USER_INFO_DIR / "profiles.json",
    )
    monkeypatch.setattr(
        database,
        "_compute_profile_source_signature",
        lambda config_path: ("test", 1, 1),
    )
    monkeypatch.setattr(
        database,
        "_load_profile_configs_from_file",
        lambda config_path: [{"profile_key": "broken-profile"}],
    )
    monkeypatch.setattr(database, "_PROFILE_SOURCE_SIGNATURE", None)

    normalized_profiles = [
        {
            "profile_key": "valid-profile",
            "bootstrap_existing_jobs": True,
            "search_configs": [],
        }
    ]
    monkeypatch.setattr(
        database,
        "_coerce_profile_configs",
        lambda profile_configs: normalized_profiles,
    )

    def fake_sync_profile_configs(cursor, profile_configs, already_normalized=False):
        captured["synced"] = (profile_configs, already_normalized)
        return len(profile_configs)

    monkeypatch.setattr(database, "_sync_profile_configs", fake_sync_profile_configs)
    monkeypatch.setattr(
        database,
        "_deactivate_missing_profiles",
        lambda cursor, active_profile_keys: captured.update(
            {"deactivated_keys": active_profile_keys}
        ),
    )
    monkeypatch.setattr(
        database,
        "_bootstrap_flagged_profiles",
        lambda cursor, profiles: captured.update({"bootstrapped": profiles}),
    )

    result = database._sync_profiles_from_source(cursor=object(), force=True)

    assert result == 1
    assert captured["synced"] == (normalized_profiles, True)
    assert captured["deactivated_keys"] == ["valid-profile"]
    assert captured["bootstrapped"] == normalized_profiles


def test_bootstrap_profiles_skips_legacy_source_when_profiles_exist(monkeypatch):
    cursor = DummyCursor()
    cursor._next_fetch = (2,)

    def fail_load(*args, **kwargs):
        raise AssertionError("profiles.json should not be read when DB has profiles")

    monkeypatch.setattr(database, "_load_profile_configs_from_file", fail_load)

    assert database._bootstrap_profiles_from_source_if_empty(cursor) == 0


def test_runtime_profile_reads_do_not_sync_profiles_json(monkeypatch):
    cursor = DummyCursor()
    cursor._rows = [
        {
            "profile_id": 1,
            "profile_key": "candidate",
            "display_name": "Candidate",
            "resume_path": None,
            "resume_text": "Resume",
            "discord_channel_id": "123",
            "discord_webhook_url": None,
            "model_name": None,
            "is_test_profile": False,
            "search_config_id": 10,
            "search_config_name": "default",
            "location": "Netherlands",
            "geo_id": "102890719",
            "distance": 25,
            "hours_old": 24,
            "results_per_term": 1,
            "term": "data engineer",
        }
    ]

    def fail_sync(*args, **kwargs):
        raise AssertionError("runtime reads must not sync profiles.json")

    def _dummy_connect(*args, **kwargs):
        return DummyConnection(cursor)

    monkeypatch.setattr(database, "init_db", lambda **kwargs: None)
    monkeypatch.setattr(database, "_connect", _dummy_connect)
    monkeypatch.setattr(database, "sync_profiles_from_source", fail_sync)

    configs = database.get_active_search_configs()

    assert configs[0]["profile_key"] == "candidate"
    assert configs[0]["terms"] == ["data engineer"]


def test_runtime_profile_query_functions_are_db_authoritative():
    source = (Path(__file__).resolve().parents[1] / "dags" / "database.py").read_text()
    runtime_functions = [
        ("def get_active_search_configs(", "def upsert_profile_jobs("),
        ("def claim_pending_fitting_tasks(", "def mark_fitting_done("),
        ("def get_profile_jobs_ready_for_fitting(", "def get_jobs_ready_for_fitting("),
        ("def get_profiles_by_ids(", "def save_llm_matches("),
        ("def get_active_notification_profiles(", "def get_jobs_to_notify("),
        ("def get_jobs_to_notify(", "def mark_job_notified("),
    ]

    for start, end in runtime_functions:
        body = source[source.index(start) : source.index(end)]
        assert "sync_profiles_from_source()" not in body


def test_get_profile_scan_filters_returns_empty_when_portal_tables_missing(monkeypatch):
    monkeypatch.setattr(
        database,
        "_missing_portal_tables",
        lambda: ["portal_profiletitleexcludekeyword"],
    )

    assert database.get_profile_scan_filters() == {}


def test_sync_profiles_from_source_does_not_require_portal_tables(monkeypatch):
    called = {"sync": False}

    monkeypatch.setattr(database, "init_db", lambda **kwargs: None)
    monkeypatch.setattr(
        database,
        "_missing_portal_tables",
        lambda: ["portal_profiletitleexcludekeyword"],
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(database, "_connect", lambda *args, **kwargs: Conn())

    def fake_sync(cursor, force=False):
        called["sync"] = True
        return 3

    monkeypatch.setattr(database, "_sync_profiles_from_source", fake_sync)

    assert database.sync_profiles_from_source(force=True) == 3
    assert called["sync"] is True


def test_seed_default_profile_scan_filters_skips_when_portal_tables_missing(monkeypatch):
    monkeypatch.setattr(database, "_portal_tables_exist", lambda: False)

    def fail_connect(*args, **kwargs):
        raise AssertionError("should not connect when portal tables are missing")

    monkeypatch.setattr(database, "_connect", fail_connect)

    database.seed_default_profile_scan_filters(123)


def test_seed_default_profile_scan_filters_writes_created_at(monkeypatch):
    cursor = DummyCursor()
    cursor._next_fetch = (0,)
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setattr(database, "_portal_tables_exist", lambda: True)

    database.seed_default_profile_scan_filters(590)

    executemany_sql = [call[1] for call in cursor.calls if call[0] == "executemany"]
    assert len(executemany_sql) == 2
    assert "portal_profiletitleexcludekeyword (profile_id, keyword, created_at)" in executemany_sql[0]
    assert "portal_profilecompanyblacklist (profile_id, company, created_at)" in executemany_sql[1]
    assert "CURRENT_TIMESTAMP" in executemany_sql[0]
    assert "CURRENT_TIMESTAMP" in executemany_sql[1]

def test_claim_job_for_notification_sets_notifying_before_send(monkeypatch):
    cursor = DummyCursor()
    cursor._next_fetch = (7, "job-1")
    _patch_connect(monkeypatch, cursor)

    claimed = database.claim_job_for_notification(7, "job-1", run_id=42)

    assert claimed is True
    sql = cursor.calls[0][1]
    params = cursor.calls[0][2]
    assert "SET notify_status = 'notifying'" in sql
    assert "AND notified_at IS NULL" in sql
    assert "fit_status IN ('fit_done', 'notify_failed')" in sql
    assert params == (7, "job-1")


def test_add_notification_run_jobs_is_idempotent_per_run_job(monkeypatch):
    cursor = DummyCursor()
    _patch_connect(monkeypatch, cursor)

    count = database.add_notification_run_jobs(
        42,
        [
            {
                "profile_id": 7,
                "id": "job-1",
                "fit_score": 82,
                "fit_decision": "Strong Fit",
            }
        ],
    )

    assert count == 1
    call = next(call for call in cursor.calls if call[0] == "executemany")
    assert "ON CONFLICT (run_id, profile_id, job_id) DO UPDATE" in call[1]
    assert call[2] == [(42, 7, "job-1", 82, "Strong Fit")]


def test_notification_run_schema_is_created_by_airflow_init_db():
    source = (Path(__file__).resolve().parents[1] / "dags" / "database.py").read_text()

    assert "CREATE TABLE IF NOT EXISTS notification_runs" in source
    assert "CREATE TABLE IF NOT EXISTS notification_run_jobs" in source
    assert "idx_notification_runs_profile_started" in source
    assert "idx_notification_run_jobs_profile_job" in source


def test_get_active_notification_profiles_selects_discord_enabled_when_portal_exists(monkeypatch):
    cursor = DummyCursor()
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setattr(database, "_portal_tables_exist", lambda: True)

    database.get_active_notification_profiles()

    sql = cursor.calls[-1][1]
    assert "portal_profilenotificationpreference" in sql
    assert "discord_enabled" in sql


def test_get_active_notification_profiles_defaults_discord_enabled_without_portal(monkeypatch):
    cursor = DummyCursor()
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setattr(database, "_portal_tables_exist", lambda: False)

    database.get_active_notification_profiles()

    sql = cursor.calls[-1][1]
    assert "TRUE AS discord_enabled" in sql
    assert "portal_profilenotificationpreference" not in sql


def test_get_jobs_to_notify_selects_discord_enabled_when_portal_exists(monkeypatch):
    cursor = DummyCursor()
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setattr(database, "_portal_tables_exist", lambda: True)
    monkeypatch.setattr(database, "is_test_mode_enabled", lambda: False)

    database.get_jobs_to_notify()

    sql = cursor.calls[-1][1]
    assert "portal_profilenotificationpreference" in sql
    assert "discord_enabled" in sql


def test_profile_job_user_workspace_schema_contract_is_declared():
    source = (Path(__file__).resolve().parents[1] / "dags" / "database.py").read_text()

    assert "user_status TEXT DEFAULT 'new'" in source
    assert "user_note TEXT" in source
    assert "user_status_updated_at TIMESTAMP" in source
    assert "profile_jobs_user_status_check" in source
    assert "user_status IN ('new', 'saved', 'dismissed', 'applied')" in source
