import sqlite3

from scripts import migrate_sqlite_to_postgres


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
