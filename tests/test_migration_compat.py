import argparse
import sqlite3

import pytest

from scripts import migrate_sqlite_to_postgres
from scripts import run_jobs_sql


def test_fetch_sqlite_rows_backfills_missing_columns_with_none():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE jobs (id TEXT, site TEXT)")
    conn.execute(
        "INSERT INTO jobs (id, site) VALUES (?, ?)",
        ("1", "linkedin"),
    )

    rows = migrate_sqlite_to_postgres._fetch_sqlite_rows(
        conn,
        "jobs",
        ["id", "site", "job_url", "fit_status"],
    )

    assert rows == [("1", "linkedin", None, None)]


def test_parse_args_requires_explicit_pg_url_when_jobs_db_url_missing(monkeypatch):
    monkeypatch.delenv("JOBS_DB_URL", raising=False)
    monkeypatch.setattr("sys.argv", ["migrate_sqlite_to_postgres.py"])

    args = migrate_sqlite_to_postgres._parse_args()

    assert args.pg_url is None


def test_resolve_db_config_requires_jobs_db_url_or_cli_overrides(monkeypatch, tmp_path):
    monkeypatch.delenv("JOBS_DB_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_jobs_sql, "_load_local_env", lambda: {})

    with pytest.raises(SystemExit, match="JOBS_DB_URL"):
        run_jobs_sql._resolve_db_config(argparse.Namespace(db=None, user=None))
