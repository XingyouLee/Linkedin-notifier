from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote_plus

import pandas as pd
import psycopg
from psycopg.rows import dict_row

JOB_REQUIRED_COLUMNS = ["id", "site", "job_url", "title", "company"]
JD_QUEUE_COLUMNS = ["id", "job_url"]
LLM_RESULT_COLUMNS = ["id", "llm_match", "llm_match_error"]
SCHEMA_LOCK_KEY = 620240319001
_SCHEMA_INITIALIZED = False


def _get_positive_int_env(var_name: str, default: int) -> int:
    try:
        value = int(os.getenv(var_name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(1, value)


def _resolve_db_url() -> str:
    env_db_url = os.getenv("JOBS_DB_URL")
    if env_db_url:
        return env_db_url

    host = os.getenv("JOBS_DB_HOST")
    if not host:
        host = "postgres" if os.path.exists("/.dockerenv") else "127.0.0.1"

    port = os.getenv("JOBS_DB_PORT", "5432")
    user = os.getenv("JOBS_DB_USER", "postgres")
    password = os.getenv("JOBS_DB_PASSWORD", "postgres")
    db_name = os.getenv("JOBS_DB_NAME", "jobsdb")

    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(db_name)}"
    )


def get_db_url() -> str:
    return _resolve_db_url()


def _connect(row_factory=None, autocommit: bool = False):
    return psycopg.connect(get_db_url(), row_factory=row_factory, autocommit=autocommit)


def _ensure_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> pd.DataFrame:
    normalized_df = df.copy()
    for col in required_columns:
        if col not in normalized_df.columns:
            normalized_df[col] = None
    return normalized_df


def _extract_fit_fields(
    llm_match: Optional[str],
) -> tuple[Optional[int], Optional[str]]:
    if not llm_match:
        return None, None
    try:
        parsed = json.loads(llm_match)
        return parsed.get("fit_score"), parsed.get("decision")
    except Exception:
        return None, None


def init_db():
    """Ensure required tables/indexes exist without mutating hot business rows."""
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return

    with _connect() as conn:
        with conn.cursor() as cursor:
            # Serialize schema bootstrap across concurrent task processes.
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_KEY,))
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    site TEXT,
                    job_url TEXT,
                    title TEXT,
                    company TEXT,
                    batch_id BIGINT,
                    description TEXT,
                    description_error TEXT,
                    llm_match TEXT,
                    llm_match_error TEXT,
                    fit_score INTEGER,
                    fit_decision TEXT,
                    notified_at TIMESTAMP,
                    notify_status TEXT,
                    notify_error TEXT,
                    fit_status TEXT,
                    fit_attempts INTEGER DEFAULT 0,
                    fit_last_error TEXT,
                    fit_updated_at TIMESTAMP,
                    FOREIGN KEY (batch_id) REFERENCES batches (id)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS jd_queue (
                    job_id TEXT PRIMARY KEY,
                    job_url TEXT,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs (id)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_state (
                    alert_key TEXT PRIMARY KEY,
                    is_active BOOLEAN NOT NULL DEFAULT FALSE,
                    last_error TEXT,
                    first_seen_at TIMESTAMP,
                    last_seen_at TIMESTAMP,
                    last_sent_at TIMESTAMP,
                    resolved_at TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_fit_status
                ON jobs (fit_status)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jd_queue_status
                ON jd_queue (status)
                """
            )
    _SCHEMA_INITIALIZED = True


def save_jobs(jobs_df: pd.DataFrame):
    """
    Saves a pandas DataFrame of jobs (from JobSpy) to the database.
    Assigns a new batch_id to this group.
    Updates existing jobs if the ID already exists (UPSERT).
    """
    if jobs_df.empty:
        return

    init_db()

    filtered_df = _ensure_columns(jobs_df, JOB_REQUIRED_COLUMNS)[JOB_REQUIRED_COLUMNS]

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO batches DEFAULT VALUES RETURNING id")
            batch_id = cursor.fetchone()[0]

            records = [
                (*row, batch_id)
                for row in filtered_df.itertuples(index=False, name=None)
            ]

            written_count = 0
            for record in records:
                cursor.execute(
                    """
                    INSERT INTO jobs (id, site, job_url, title, company, batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        site = COALESCE(NULLIF(BTRIM(EXCLUDED.site), ''), jobs.site),
                        job_url = COALESCE(NULLIF(BTRIM(EXCLUDED.job_url), ''), jobs.job_url),
                        title = COALESCE(NULLIF(BTRIM(EXCLUDED.title), ''), jobs.title),
                        company = COALESCE(NULLIF(BTRIM(EXCLUDED.company), ''), jobs.company),
                        batch_id = EXCLUDED.batch_id
                    """,
                    record,
                )
                written_count += cursor.rowcount

    if written_count > 0:
        print(
            f"✅ Upserted {written_count} jobs in Batch {batch_id}."
        )
    else:
        print(f"No job rows changed during Batch {batch_id}.")


def enqueue_jd_requests(jobs_df: pd.DataFrame):
    """Upsert pending JD requests for OpenClaw worker."""
    if jobs_df is None or jobs_df.empty:
        return 0

    init_db()

    if "id" not in jobs_df.columns or "job_url" not in jobs_df.columns:
        return 0

    queue_df = _ensure_columns(jobs_df, JD_QUEUE_COLUMNS)[JD_QUEUE_COLUMNS]
    records = [
        (job_id, job_url)
        for job_id, job_url in queue_df.itertuples(index=False, name=None)
    ]

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO jd_queue (job_id, job_url, status, attempts, updated_at, error)
                VALUES (%s, %s, 'pending', 0, CURRENT_TIMESTAMP, NULL)
                ON CONFLICT(job_id) DO UPDATE SET
                    job_url=EXCLUDED.job_url,
                    status='pending',
                    updated_at=CASE
                        WHEN jd_queue.status = 'pending' THEN jd_queue.updated_at
                        ELSE CURRENT_TIMESTAMP
                    END,
                    error=NULL
                """,
                records,
            )

    return len(records)


def enqueue_fitting_requests(jobs_df: pd.DataFrame):
    """Mark jobs as pending for one-time fitting at job level."""
    if jobs_df is None or jobs_df.empty:
        return 0

    init_db()

    if "id" not in jobs_df.columns:
        return 0

    filtered_df = jobs_df.copy()
    if "description" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["description"].notna()]

    if filtered_df.empty:
        return 0

    job_ids = filtered_df["id"].astype(str).unique().tolist()

    with _connect() as conn:
        with conn.cursor() as cursor:
            queued = 0
            for job_id in job_ids:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET fit_status = 'pending_fit',
                        fit_updated_at = CURRENT_TIMESTAMP,
                        fit_last_error = NULL,
                        fit_attempts = COALESCE(fit_attempts, 0)
                    WHERE id = %s
                      AND description IS NOT NULL
                      AND llm_match IS NULL
                      AND COALESCE(fit_status, '') NOT IN (
                          'pending_fit', 'fitting', 'fit_done', 'notified', 'fit_failed', 'notify_failed'
                      )
                    """,
                    (job_id,),
                )
                queued += cursor.rowcount

    return queued


def claim_pending_fitting_tasks(limit: int = None) -> List[Dict[str, Any]]:
    """Atomically claim pending fitting tasks for processing."""
    init_db()
    stale_minutes = _get_positive_int_env("FITTING_CLAIM_STALE_MINUTES", 30)

    select_query = """
        SELECT id AS job_id, COALESCE(fit_attempts, 0) AS attempts
        FROM jobs
        WHERE (
                fit_status = 'pending_fit'
                OR (
                    fit_status = 'fitting'
                    AND COALESCE(
                        fit_updated_at,
                        TIMESTAMP '1970-01-01 00:00:00'
                    ) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                )
              )
          AND description IS NOT NULL
          AND llm_match IS NULL
        ORDER BY COALESCE(fit_updated_at, TIMESTAMP '1970-01-01 00:00:00') ASC
        FOR UPDATE SKIP LOCKED
    """
    params: List[Any] = [stale_minutes]
    if limit is not None and int(limit) > 0:
        select_query += " LIMIT %s"
        params.append(int(limit))

    with _connect(row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(select_query, params)
                rows = cursor.fetchall()
                if not rows:
                    return []

                cursor.executemany(
                    """
                    UPDATE jobs
                    SET fit_status = 'fitting',
                        fit_updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    [(row["job_id"],) for row in rows],
                )

    return [{"job_id": row["job_id"], "attempts": row["attempts"]} for row in rows]


def mark_fitting_done(job_id: str):
    """Mark fitting task as completed."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET fit_status = 'fit_done',
                    fit_updated_at = CURRENT_TIMESTAMP,
                    fit_last_error = NULL
                WHERE id = %s
                """,
                (job_id,),
            )


def mark_fitting_failed(
    job_id: str,
    error: str = "",
    retry: bool = True,
):
    """Mark fitting task as failed and optionally requeue."""
    init_db()

    status = "pending_fit" if retry else "fit_failed"
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET fit_status = %s,
                    fit_attempts = COALESCE(fit_attempts, 0) + 1,
                    fit_last_error = %s,
                    fit_updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (status, error, job_id),
            )


def requeue_fitting_task(job_id: str, error: str = ""):
    """Return an unprocessed fitting task to pending without spending an attempt."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET fit_status = 'pending_fit',
                    llm_match_error = NULL,
                    fit_last_error = %s,
                    fit_updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (error, job_id),
            )


def should_send_active_alert(alert_key: str, error: Optional[str] = None) -> bool:
    """Send once per active outage; alert can reopen after resolve_active_alert."""
    init_db()

    with _connect(row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT is_active
                    FROM alert_state
                    WHERE alert_key = %s
                    FOR UPDATE
                    """,
                    (alert_key,),
                )
                row = cursor.fetchone()

                if not row or not row["is_active"]:
                    cursor.execute(
                        """
                        INSERT INTO alert_state (
                            alert_key,
                            is_active,
                            last_error,
                            first_seen_at,
                            last_seen_at,
                            last_sent_at,
                            resolved_at
                        )
                        VALUES (
                            %s,
                            TRUE,
                            %s,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            NULL
                        )
                        ON CONFLICT(alert_key) DO UPDATE SET
                            is_active = TRUE,
                            last_error = EXCLUDED.last_error,
                            first_seen_at = CURRENT_TIMESTAMP,
                            last_seen_at = CURRENT_TIMESTAMP,
                            last_sent_at = CURRENT_TIMESTAMP,
                            resolved_at = NULL
                        """,
                        (alert_key, error),
                    )
                    return True

                cursor.execute(
                    """
                    UPDATE alert_state
                    SET last_error = %s,
                        last_seen_at = CURRENT_TIMESTAMP,
                        resolved_at = NULL
                    WHERE alert_key = %s
                    """,
                    (error, alert_key),
                )
                return False


def resolve_active_alert(alert_key: str):
    """Mark an active alert as recovered so the next outage can notify again."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE alert_state
                SET is_active = FALSE,
                    resolved_at = CURRENT_TIMESTAMP,
                    last_seen_at = CURRENT_TIMESTAMP
                WHERE alert_key = %s
                  AND is_active = TRUE
                """,
                (alert_key,),
            )


def count_jd_queue_status(job_ids: List[str], status: str) -> int:
    """Count how many jobs in given ids are in target status."""
    if not job_ids:
        return 0
    init_db()

    query = "SELECT COUNT(*) FROM jd_queue WHERE status = %s AND job_id = ANY(%s)"

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (status, job_ids))
            count = cursor.fetchone()[0]

    return count


def get_jobs_needing_jd(max_attempts: int = 3) -> pd.DataFrame:
    """Fetch jobs that still need JD scraping and are eligible for retry."""
    init_db()
    stale_minutes = _get_positive_int_env("JD_CLAIM_STALE_MINUTES", 30)

    query = """
        SELECT j.id, j.site, j.job_url, j.title, j.company,
               q.status AS jd_status,
               COALESCE(q.attempts, 0) AS jd_attempts
        FROM jobs j
        LEFT JOIN jd_queue q ON q.job_id = j.id
        WHERE j.description IS NULL
          AND NULLIF(TRIM(COALESCE(j.job_url, '')), '') IS NOT NULL
          AND (
                q.job_id IS NULL
                OR q.status = 'pending'
                OR (q.status = 'failed' AND COALESCE(q.attempts, 0) < %s)
                OR (
                    q.status = 'processing'
                    AND COALESCE(
                        q.updated_at,
                        TIMESTAMP '1970-01-01 00:00:00'
                    ) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                )
                OR (q.status = 'done' AND j.description IS NULL)
              )
        ORDER BY
            COALESCE(q.updated_at, TIMESTAMP '1970-01-01 00:00:00') ASC,
            COALESCE(j.batch_id, 0) DESC,
            j.id ASC
    """

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (int(max_attempts), stale_minutes))
            rows = cursor.fetchall()

    return pd.DataFrame(rows)


def get_jobs_ready_for_fitting() -> pd.DataFrame:
    """Fetch jobs that already have JD and still need to enter fitting."""
    init_db()

    query = """
        SELECT id
        FROM jobs
        WHERE description IS NOT NULL
          AND llm_match IS NULL
          AND COALESCE(fit_status, '') NOT IN (
                'pending_fit', 'fitting', 'fit_done', 'notified', 'fit_failed', 'notify_failed'
              )
        ORDER BY
            COALESCE(fit_updated_at, TIMESTAMP '1970-01-01 00:00:00') ASC,
            COALESCE(batch_id, 0) DESC,
            id ASC
    """

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return pd.DataFrame(rows)


def save_llm_matches(jobs_df: pd.DataFrame):
    """Persist LLM fit results back to jobs table."""
    if jobs_df is None or jobs_df.empty:
        return

    init_db()

    update_df = jobs_df.copy()
    if "id" not in update_df.columns:
        return

    update_df = _ensure_columns(update_df, LLM_RESULT_COLUMNS)

    records = []
    for row in update_df[LLM_RESULT_COLUMNS].itertuples(index=False, name=None):
        job_id, llm_match, llm_match_error = row
        fit_score, fit_decision = _extract_fit_fields(llm_match)
        records.append((llm_match, llm_match_error, fit_score, fit_decision, job_id))

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE jobs
                SET llm_match = %s,
                    llm_match_error = %s,
                    fit_score = %s,
                    fit_decision = %s
                WHERE id = %s
                """,
                records,
            )


def get_jobs_to_notify(limit: int = 10) -> pd.DataFrame:
    """Get unnotified fit results that are ready to notify."""
    init_db()
    query = """
        SELECT *
        FROM jobs
        WHERE fit_decision IN ('Strong Fit', 'Moderate Fit')
          AND notified_at IS NULL
          AND fit_status IN ('fit_done', 'notify_failed')
        ORDER BY batch_id DESC, fit_score DESC
        LIMIT %s
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def mark_job_notified(job_id: str, status: str = "sent", error: Optional[str] = None):
    """Mark notification status for one job."""
    init_db()
    with _connect() as conn:
        with conn.cursor() as cursor:
            if status == "sent":
                cursor.execute(
                    """
                    UPDATE jobs
                    SET notified_at = CURRENT_TIMESTAMP,
                        notify_status = 'sent',
                        notify_error = NULL,
                        fit_status = 'notified'
                    WHERE id = %s
                    """,
                    (job_id,),
                )
            else:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET notify_status = %s,
                        notify_error = %s,
                        fit_status = 'notify_failed'
                    WHERE id = %s
                    """,
                    (status, error, job_id),
                )


def save_jd_result(
    job_id: str,
    description: Optional[str] = None,
    description_error: Optional[str] = None,
):
    """Persist one JD scrape result from OpenClaw worker."""
    init_db()

    with _connect() as conn:
        with conn.cursor() as cursor:
            if description:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET description = %s, description_error = NULL
                    WHERE id = %s
                    """,
                    (description, job_id),
                )
                cursor.execute(
                    """
                    UPDATE jd_queue
                    SET status = 'done', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP, error = NULL
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
            else:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET description = NULL, description_error = %s
                    WHERE id = %s
                    """,
                    (description_error, job_id),
                )
                cursor.execute(
                    """
                    UPDATE jd_queue
                    SET status = 'failed', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP, error = %s
                    WHERE job_id = %s
                    """,
                    (description_error, job_id),
                )


def claim_pending_jd_requests(
    limit: int = 10,
    job_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Atomically claim pending JD requests for one worker run."""
    init_db()
    stale_minutes = _get_positive_int_env("JD_CLAIM_STALE_MINUTES", 30)
    normalized_job_ids = [str(job_id) for job_id in (job_ids or []) if job_id]
    if job_ids is not None and not normalized_job_ids:
        return pd.DataFrame(columns=["job_id", "job_url"])

    select_query = """
        SELECT q.job_id, q.job_url
        FROM jd_queue q
        WHERE (
                q.status = 'pending'
                OR (
                    q.status = 'processing'
                    AND COALESCE(
                        q.updated_at,
                        TIMESTAMP '1970-01-01 00:00:00'
                    ) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                )
              )
    """
    params: List[Any] = [stale_minutes]
    if normalized_job_ids:
        select_query += " AND q.job_id = ANY(%s)"
        params.append(normalized_job_ids)

    select_query += """
        ORDER BY q.updated_at ASC
        FOR UPDATE SKIP LOCKED
    """
    if limit and int(limit) > 0:
        select_query += " LIMIT %s"
        params.append(int(limit))

    with _connect(row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(select_query, params)
                rows = cursor.fetchall()
                if not rows:
                    return pd.DataFrame(columns=["job_id", "job_url"])

                cursor.executemany(
                    """
                    UPDATE jd_queue
                    SET status = 'processing',
                        updated_at = CURRENT_TIMESTAMP,
                        error = NULL
                    WHERE job_id = %s
                    """,
                    [(row["job_id"],) for row in rows],
                )

    return pd.DataFrame(rows)


def get_jobs_by_ids(job_ids: List[str]) -> pd.DataFrame:
    """Fetch jobs by id list."""
    if not job_ids:
        return pd.DataFrame()
    init_db()

    query = "SELECT * FROM jobs WHERE id = ANY(%s)"

    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (job_ids,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def get_pending_jd_requests(limit: int = 10) -> pd.DataFrame:
    """Fetch pending jd queue items for external worker."""
    init_db()
    query = """
        SELECT q.job_id, q.job_url
        FROM jd_queue q
        WHERE q.status = 'pending'
        ORDER BY q.updated_at ASC
        LIMIT %s
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def get_latest_batch_jobs() -> pd.DataFrame:
    """Retrieves only the jobs from the most recent batch."""
    query = """
        SELECT * FROM jobs
        WHERE batch_id = (SELECT MAX(id) FROM batches)
    """
    with _connect(row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully on Postgres")
