#!/usr/bin/env python3
import argparse
import os
import sqlite3
from pathlib import Path
from typing import List, Sequence, Tuple

import psycopg


def _table_exists_sqlite(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    sanitized_table = table_name.replace("'", "''")
    cursor = conn.execute(f"PRAGMA table_info('{sanitized_table}')")
    return [row[1] for row in cursor.fetchall()]


def _fetch_sqlite_rows(
    conn: sqlite3.Connection,
    table_name: str,
    columns: Sequence[str],
) -> List[Tuple]:
    available_columns = set(_get_table_columns(conn, table_name))
    select_parts = [
        col if col in available_columns else f"NULL AS {col}"
        for col in columns
    ]
    col_sql = ", ".join(select_parts)
    sanitized_table = table_name.replace("'", "''")
    rows = conn.execute(f"SELECT {col_sql} FROM {sanitized_table}").fetchall()
    return [tuple(row[column] for column in columns) for row in rows]


def _copy_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn: psycopg.Connection,
    table_name: str,
    columns: Sequence[str],
    conflict_target: str,
) -> int:
    if not _table_exists_sqlite(sqlite_conn, table_name):
        print(f"Skip {table_name}: source table not found in SQLite.")
        return 0

    rows = _fetch_sqlite_rows(sqlite_conn, table_name, columns)
    if not rows:
        print(f"Skip {table_name}: no rows.")
        return 0

    col_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {table_name} ({col_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT {conflict_target} DO NOTHING"
    )

    with pg_conn.cursor() as cursor:
        cursor.executemany(sql, rows)

    print(f"Copied {len(rows)} rows into {table_name}.")
    return len(rows)


def _sync_batches_sequence(pg_conn: psycopg.Connection):
    with pg_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT setval(
                pg_get_serial_sequence('batches', 'id'),
                COALESCE((SELECT MAX(id) FROM batches), 1),
                (SELECT COUNT(*) > 0 FROM batches)
            )
            """
        )


def _parse_args():
    repo_root = Path(__file__).resolve().parents[1]
    default_sqlite_path = repo_root / "include" / "jobs.db"
    default_pg_url = os.getenv("JOBS_DB_URL")

    parser = argparse.ArgumentParser(
        description="One-time migration from local SQLite jobs.db to Postgres jobsdb."
    )
    parser.add_argument(
        "--sqlite-path",
        default=str(default_sqlite_path),
        help="Path to source SQLite file.",
    )
    parser.add_argument(
        "--pg-url",
        default=default_pg_url,
        help="Target Postgres DSN. Pass explicitly or set JOBS_DB_URL.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    pg_url = (args.pg_url or "").strip()

    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")

    if not pg_url:
        raise SystemExit("Set JOBS_DB_URL or pass --pg-url before running migration.")

    os.environ["JOBS_DB_URL"] = pg_url
    from dags import database as pg_database  # noqa: WPS433

    pg_database.init_db()

    with sqlite3.connect(str(sqlite_path)) as sqlite_conn:
        sqlite_conn.row_factory = sqlite3.Row
        with psycopg.connect(pg_url) as pg_conn:
            _copy_table(
                sqlite_conn,
                pg_conn,
                "batches",
                ["id", "timestamp"],
                "(id)",
            )
            _copy_table(
                sqlite_conn,
                pg_conn,
                "jobs",
                [
                    "id",
                    "site",
                    "job_url",
                    "title",
                    "company",
                    "batch_id",
                    "description",
                    "description_error",
                    "llm_match",
                    "llm_match_error",
                    "fit_score",
                    "fit_decision",
                    "notified_at",
                    "notify_status",
                    "notify_error",
                    "fit_status",
                    "fit_attempts",
                    "fit_last_error",
                    "fit_updated_at",
                ],
                "(id)",
            )
            _copy_table(
                sqlite_conn,
                pg_conn,
                "jd_queue",
                [
                    "job_id",
                    "job_url",
                    "status",
                    "attempts",
                    "created_at",
                    "updated_at",
                    "error",
                ],
                "(job_id)",
            )
            _sync_batches_sequence(pg_conn)

    print("✅ Migration complete.")


if __name__ == "__main__":
    main()
