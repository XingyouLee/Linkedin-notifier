import asyncio

import pandas as pd

from dags import database
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
        sql for kind, sql, _ in cursor.calls if kind == "execute" and "INSERT INTO jobs" in sql
    )
    assert "ON CONFLICT(id) DO UPDATE SET" in job_insert
    assert "job_url = COALESCE(NULLIF(BTRIM(EXCLUDED.job_url), ''), jobs.job_url)" in job_insert


def test_claim_pending_fitting_tasks_only_reclaims_stale_fitting(monkeypatch):
    cursor = DummyCursor()
    cursor._rows = [{"job_id": "10", "attempts": 0}]
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setenv("FITTING_CLAIM_STALE_MINUTES", "15")

    result = database.claim_pending_fitting_tasks()

    assert result == [{"job_id": "10", "attempts": 0}]
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

    monkeypatch.setattr(database, "claim_pending_jd_requests", fake_claim_pending_jd_requests)

    processed = asyncio.run(jd_playwright_worker.run_once(limit=3, job_ids=["a", "b"]))

    assert processed == 0
    assert captured == {"limit": 3, "job_ids": ["a", "b"]}
