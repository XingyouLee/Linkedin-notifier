#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from dags.database import get_db_url
from dags.runtime_utils import load_env


DEFAULT_SINCE = "2026-03-27 00:14:42"


@dataclass(frozen=True)
class Scope:
    since: str
    until: str | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reset JD and fitting state for jobs whose jd_queue rows were completed "
            "inside a time window. Defaults to dry-run."
        )
    )
    parser.add_argument(
        "--since",
        default=DEFAULT_SINCE,
        help=f"Inclusive lower bound for jd_queue.updated_at. Default: {DEFAULT_SINCE}",
    )
    parser.add_argument(
        "--until",
        default=None,
        help="Exclusive upper bound for jd_queue.updated_at.",
    )
    parser.add_argument(
        "--clear-notify",
        action="store_true",
        help="Also clear notified_at / notify_status / notify_error on affected profile_jobs.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="How many affected jobs to print in dry-run summary. Default: 10.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the reset. Without this flag the script only prints a dry-run summary.",
    )
    return parser.parse_args()


def _connect() -> psycopg.Connection[Any]:
    load_env()
    return psycopg.connect(get_db_url(), row_factory=dict_row)


def _scope_sql(scope: Scope) -> tuple[str, list[Any]]:
    clauses = ["q.status = 'done'", "q.updated_at >= %s"]
    params: list[Any] = [scope.since]
    if scope.until:
        clauses.append("q.updated_at < %s")
        params.append(scope.until)
    return " AND ".join(clauses), params


def _collect_summary(
    conn: psycopg.Connection[Any],
    scope: Scope,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    where_sql, params = _scope_sql(scope)

    summary_query = f"""
        WITH affected_jobs AS (
            SELECT DISTINCT q.job_id
            FROM jd_queue q
            WHERE {where_sql}
        ),
        fit_counts AS (
            SELECT COALESCE(pj.fit_status, 'NULL') AS fit_status, COUNT(*) AS cnt
            FROM profile_jobs pj
            JOIN affected_jobs a ON a.job_id = pj.job_id
            GROUP BY 1
        ),
        notify_counts AS (
            SELECT
                COUNT(*) FILTER (WHERE pj.notified_at IS NOT NULL) AS notified_rows,
                COUNT(*) FILTER (WHERE pj.notify_status IS NOT NULL) AS notify_status_rows
            FROM profile_jobs pj
            JOIN affected_jobs a ON a.job_id = pj.job_id
        )
        SELECT
            (SELECT COUNT(*) FROM affected_jobs) AS affected_jobs,
            (
                SELECT COUNT(*)
                FROM profile_jobs pj
                JOIN affected_jobs a ON a.job_id = pj.job_id
            ) AS affected_profile_jobs,
            (
                SELECT COUNT(*)
                FROM jobs j
                JOIN affected_jobs a ON a.job_id = j.id
                WHERE j.description IS NOT NULL
            ) AS jobs_with_description,
            (
                SELECT COUNT(*)
                FROM jobs j
                JOIN affected_jobs a ON a.job_id = j.id
                WHERE COALESCE(j.description, '') ILIKE 'Seniority level %Employment type %Job function %'
            ) AS criteria_like_jobs,
            COALESCE((SELECT json_object_agg(fit_status, cnt) FROM fit_counts), '{{}}'::json) AS fit_status_counts,
            (
                SELECT row_to_json(notify_counts)
                FROM notify_counts
            ) AS notify_counts
    """

    sample_query = f"""
        WITH affected_jobs AS (
            SELECT DISTINCT q.job_id
            FROM jd_queue q
            WHERE {where_sql}
        )
        SELECT
            j.id AS job_id,
            q.updated_at,
            j.title,
            LENGTH(COALESCE(j.description, '')) AS description_length,
            LEFT(COALESCE(j.description, ''), 160) AS description_preview
        FROM affected_jobs a
        JOIN jobs j ON j.id = a.job_id
        JOIN jd_queue q ON q.job_id = j.id
        ORDER BY q.updated_at ASC, j.id ASC
        LIMIT %s
    """

    with conn.cursor() as cursor:
        cursor.execute(summary_query, params)
        summary_row = cursor.fetchone() or {}
        cursor.execute(sample_query, [*params, max(1, sample_limit)])
        sample_rows = cursor.fetchall()

    return {
        "scope": {
            "since": scope.since,
            "until": scope.until,
        },
        "affected_jobs": int(summary_row.get("affected_jobs") or 0),
        "affected_profile_jobs": int(summary_row.get("affected_profile_jobs") or 0),
        "jobs_with_description": int(summary_row.get("jobs_with_description") or 0),
        "criteria_like_jobs": int(summary_row.get("criteria_like_jobs") or 0),
        "fit_status_counts": dict(summary_row.get("fit_status_counts") or {}),
        "notify_counts": dict(summary_row.get("notify_counts") or {}),
        "sample_jobs": list(sample_rows),
    }


def _print_summary(summary: dict[str, Any], *, clear_notify: bool, execute: bool) -> None:
    mode = "execute" if execute else "dry-run"
    print(f"Mode: {mode}")
    print(f"Window since: {summary['scope']['since']}")
    print(f"Window until: {summary['scope']['until'] or 'open-ended'}")
    print(f"Clear notify fields: {clear_notify}")
    print()
    print(f"Affected jobs: {summary['affected_jobs']}")
    print(f"Affected profile_jobs: {summary['affected_profile_jobs']}")
    print(f"Jobs with description right now: {summary['jobs_with_description']}")
    print(f"Criteria-like metadata descriptions: {summary['criteria_like_jobs']}")
    print()
    print("Affected fit_status distribution:")
    for status, count in sorted(summary["fit_status_counts"].items()):
        print(f"  {status}: {count}")
    print()
    print("Affected notify counts:")
    for key, count in sorted(summary["notify_counts"].items()):
        print(f"  {key}: {count}")

    sample_rows = summary.get("sample_jobs") or []
    if sample_rows:
        print()
        print("Sample affected jobs:")
        for row in sample_rows:
            print(
                f"  {row['updated_at']}  job_id={row['job_id']}  "
                f"len={row['description_length']}  title={row['title']}"
            )
            print(f"    {row['description_preview']}")


def _apply_reset(
    conn: psycopg.Connection[Any],
    scope: Scope,
    *,
    clear_notify: bool,
) -> dict[str, int]:
    where_sql, params = _scope_sql(scope)
    profile_job_assignments = [
        "llm_match = NULL",
        "llm_match_error = NULL",
        "fit_score = NULL",
        "fit_decision = NULL",
        "fit_attempts = 0",
        "fit_last_error = NULL",
        "fit_status = 'pending_fit'",
        "fit_updated_at = CURRENT_TIMESTAMP",
    ]
    if clear_notify:
        profile_job_assignments.extend(
            [
                "notified_at = NULL",
                "notify_status = NULL",
                "notify_error = NULL",
            ]
        )

    jobs_update = f"""
        WITH affected_jobs AS (
            SELECT DISTINCT q.job_id
            FROM jd_queue q
            WHERE {where_sql}
        )
        UPDATE jobs j
        SET description = NULL,
            description_error = NULL
        FROM affected_jobs a
        WHERE j.id = a.job_id
    """
    jd_queue_update = f"""
        WITH affected_jobs AS (
            SELECT DISTINCT q.job_id
            FROM jd_queue q
            WHERE {where_sql}
        )
        UPDATE jd_queue q
        SET status = 'pending',
            attempts = 0,
            updated_at = CURRENT_TIMESTAMP,
            error = NULL
        FROM affected_jobs a
        WHERE q.job_id = a.job_id
    """
    profile_jobs_update = f"""
        WITH affected_jobs AS (
            SELECT DISTINCT q.job_id
            FROM jd_queue q
            WHERE {where_sql}
        )
        UPDATE profile_jobs pj
        SET {", ".join(profile_job_assignments)}
        FROM affected_jobs a
        WHERE pj.job_id = a.job_id
    """

    results: dict[str, int] = {}
    with conn.cursor() as cursor:
        cursor.execute(jobs_update, params)
        results["jobs_updated"] = int(cursor.rowcount or 0)
        cursor.execute(jd_queue_update, params)
        results["jd_queue_updated"] = int(cursor.rowcount or 0)
        cursor.execute(profile_jobs_update, params)
        results["profile_jobs_updated"] = int(cursor.rowcount or 0)
    return results


def main() -> None:
    args = _parse_args()
    scope = Scope(since=args.since, until=args.until)

    with _connect() as conn:
        summary = _collect_summary(conn, scope, sample_limit=args.sample_limit)
        _print_summary(summary, clear_notify=args.clear_notify, execute=args.execute)

        if not args.execute:
            print()
            print("Dry run only. No changes were written.")
            return

        if summary["affected_jobs"] == 0:
            print()
            print("No rows matched the window. Nothing to reset.")
            return

        results = _apply_reset(conn, scope, clear_notify=args.clear_notify)
        conn.commit()

    print()
    print("Reset complete:")
    for key, value in sorted(results.items()):
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
