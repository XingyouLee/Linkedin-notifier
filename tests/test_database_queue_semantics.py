import asyncio

import pandas as pd

from dags import database
from dags import jd_api_worker
from dags import jd_playwright_worker


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
    monkeypatch.setattr(database, "init_db", lambda: None)
    monkeypatch.setattr(database, "sync_profiles_from_source", lambda force=False: 0)


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


def test_run_once_forwards_job_ids_to_claim_api(monkeypatch):
    captured = {}

    def fake_claim_pending_jd_requests(limit, job_ids=None):
        captured["limit"] = limit
        captured["job_ids"] = job_ids
        return pd.DataFrame(columns=["job_id", "job_url"])

    monkeypatch.setattr(
        database, "claim_pending_jd_requests", fake_claim_pending_jd_requests
    )

    processed = asyncio.run(jd_playwright_worker.run_once(limit=3, job_ids=["a", "b"]))

    assert processed == 0
    assert captured == {"limit": 3, "job_ids": ["a", "b"]}


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


def test_jd_api_extract_description_prefers_description_markup():
    html = """
    <div class="jobs-description">
      <section>
        <div>
          <p>Build resilient Python data pipelines for analytics and machine learning workflows.</p>
        </div>
      </section>
    </div>
    """

    extracted = jd_api_worker.extract_description(html)

    assert extracted is not None
    assert "Python data pipelines" in extracted


def test_jd_api_extract_description_keeps_nested_div_text():
    html = """
    <div class="show-more-less-html__markup">
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
