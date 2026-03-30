#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dags.database import get_db_url  # noqa: E402


RESETTABLE_FIELDS = [
    "fit_status = 'pending_fit'",
    "llm_match = NULL",
    "llm_match_error = NULL",
    "fit_score = NULL",
    "fit_decision = NULL",
    "fit_attempts = 0",
    "fit_last_error = NULL",
    "fit_updated_at = CURRENT_TIMESTAMP",
    "notified_at = NULL",
    "notify_status = NULL",
    "notify_error = NULL",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Reset fitting results for a batch of profile_jobs so LLM scoring can be re-run."
    )
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument(
        "--batch-id",
        type=int,
        help="Specific batch id to reset.",
    )
    target_group.add_argument(
        "--latest",
        action="store_true",
        help="Reset the latest batch. If no target is provided, latest is used by default.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be reset without committing changes.",
    )
    parser.add_argument(
        "--set-pending-fit",
        action="store_true",
        help="Deprecated: pending_fit is now the default reset behavior.",
    )
    parser.add_argument(
        "--set-null",
        action="store_true",
        help="Reset fit_status to NULL instead of pending_fit. Use this only if another pipeline step will enqueue fitting later.",
    )
    return parser.parse_args()


def _get_target_batch_id(
    conn: psycopg.Connection, requested_batch_id: int | None
) -> int:
    if requested_batch_id is not None:
        row = conn.execute(
            "SELECT id, timestamp FROM batches WHERE id = %s",
            (requested_batch_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Batch {requested_batch_id} not found.")
        return int(row["id"])

    row = conn.execute(
        "SELECT id, timestamp FROM batches ORDER BY timestamp DESC, id DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise ValueError("No batches found.")
    return int(row["id"])


def _get_batch_summary(conn: psycopg.Connection, batch_id: int):
    batch_row = conn.execute(
        """
        SELECT b.id, b.timestamp, COUNT(j.id) AS job_count
        FROM batches b
        LEFT JOIN jobs j ON j.batch_id = b.id
        WHERE b.id = %s
        GROUP BY b.id, b.timestamp
        """,
        (batch_id,),
    ).fetchone()
    if not batch_row:
        raise ValueError(f"Batch {batch_id} not found.")

    status_rows = conn.execute(
        """
        SELECT
            p.profile_key,
            pj.fit_status,
            COUNT(*) AS cnt
        FROM profile_jobs pj
        JOIN profiles p ON p.id = pj.profile_id
        JOIN jobs j ON j.id = pj.job_id
        WHERE j.batch_id = %s
        GROUP BY p.profile_key, pj.fit_status
        ORDER BY p.profile_key, pj.fit_status NULLS FIRST
        """,
        (batch_id,),
    ).fetchall()

    total_profile_jobs_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM profile_jobs pj
        JOIN jobs j ON j.id = pj.job_id
        WHERE j.batch_id = %s
        """,
        (batch_id,),
    ).fetchone()

    return batch_row, status_rows, int(total_profile_jobs_row["cnt"])


def _print_batch_summary(batch_row, status_rows, total_profile_jobs: int) -> None:
    print(f"Batch id: {batch_row['id']}")
    print(f"Batch timestamp: {batch_row['timestamp']}")
    print(f"Jobs in batch: {batch_row['job_count']}")
    print(f"profile_jobs rows in batch: {total_profile_jobs}")
    print()
    print("Current fit_status distribution:")
    for row in status_rows:
        fit_status = row["fit_status"] if row["fit_status"] is not None else "NULL"
        print(
            f"  profile={row['profile_key']:<12} fit_status={fit_status:<12} count={row['cnt']}"
        )


def _reset_batch(
    conn: psycopg.Connection,
    batch_id: int,
    *,
    set_null: bool = False,
) -> int:
    assignments = list(RESETTABLE_FIELDS)
    if set_null:
        assignments[0] = "fit_status = NULL"
        assignments[7] = "fit_updated_at = NULL"

    sql = f"""
        UPDATE profile_jobs
        SET {", ".join(assignments)}
        WHERE job_id IN (
            SELECT id
            FROM jobs
            WHERE batch_id = %s
        )
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (batch_id,))
        return cursor.rowcount


def main():
    args = _parse_args()
    requested_batch_id = args.batch_id

    with psycopg.connect(get_db_url(), row_factory=dict_row) as conn:
        batch_id = _get_target_batch_id(conn, requested_batch_id)
        batch_row, status_rows, total_profile_jobs = _get_batch_summary(conn, batch_id)

        _print_batch_summary(batch_row, status_rows, total_profile_jobs)

        if total_profile_jobs == 0:
            print("\nNo profile_jobs found for this batch. Nothing to reset.")
            return

        if args.dry_run:
            print("\nDry run only. No changes were written.")
            return

        updated_rows = _reset_batch(
            conn,
            batch_id,
            set_null=args.set_null,
        )
        conn.commit()

    print()
    print(
        f"Reset complete. Updated {updated_rows} profile_jobs rows for batch {batch_id}."
    )
    if args.set_null:
        print("All fitting results and notification state for that batch were cleared.")
    else:
        print(
            "All fitting results were cleared and the batch was re-queued as pending_fit."
        )


if __name__ == "__main__":
    main()
