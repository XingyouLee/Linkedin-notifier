import sqlite3
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
import os
import json

def _resolve_db_path() -> str:
    env_db_path = os.getenv("JOBS_DB_PATH")
    if env_db_path:
        return env_db_path

    candidate_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "include", "jobs.db")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "include", "jobs.db")),
        "/usr/local/airflow/include/jobs.db",
    ]

    for candidate in candidate_paths:
        candidate_dir = os.path.dirname(candidate)
        if os.path.isdir(candidate_dir):
            return candidate

    return candidate_paths[0]


DB_PATH = _resolve_db_path()

def init_db():
    """Initializes the database and creates/migrates tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create a table to track each scrape batch
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create the table with the requested fields + batch_id
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            site TEXT,
            job_url TEXT,
            title TEXT,
            company TEXT,
            batch_id INTEGER,
            description TEXT,
            description_error TEXT,
            llm_match TEXT,
            llm_match_error TEXT,
            fit_score INTEGER,
            fit_decision TEXT,
            notified_at DATETIME,
            notify_status TEXT,
            notify_error TEXT,
            fit_status TEXT,
            fit_attempts INTEGER DEFAULT 0,
            fit_last_error TEXT,
            fit_updated_at DATETIME,
            FOREIGN KEY (batch_id) REFERENCES batches (id)
        )
    """)

    # Queue table for OpenClaw-driven JD scraping
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jd_queue (
            job_id TEXT PRIMARY KEY,
            job_url TEXT,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            error TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (id)
        )
    """)

    # Legacy table cleanup: fitting state is tracked directly on jobs.
    cursor.execute("DROP TABLE IF EXISTS fitting_queue")

    # Lightweight migration for existing databases
    cursor.execute("PRAGMA table_info(jobs)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    if "description" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN description TEXT")
    if "description_error" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN description_error TEXT")
    if "llm_match" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN llm_match TEXT")
    if "llm_match_error" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN llm_match_error TEXT")
    if "fit_score" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN fit_score INTEGER")
    if "fit_decision" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN fit_decision TEXT")
    if "notified_at" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN notified_at DATETIME")
    if "notify_status" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN notify_status TEXT")
    if "notify_error" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN notify_error TEXT")
    if "fit_status" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN fit_status TEXT")
    if "fit_attempts" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN fit_attempts INTEGER DEFAULT 0")
    if "fit_last_error" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN fit_last_error TEXT")
    if "fit_updated_at" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN fit_updated_at DATETIME")

    cursor.execute(
        """
        UPDATE jobs
        SET fit_status = CASE
            WHEN notified_at IS NOT NULL THEN 'notified'
            WHEN llm_match IS NOT NULL THEN 'fit_done'
            WHEN llm_match_error IS NOT NULL THEN 'fit_failed'
            ELSE fit_status
        END
        WHERE fit_status IS NULL
        """
    )

    conn.commit()
    conn.close()

def save_jobs(jobs_df: pd.DataFrame):
    """
    Saves a pandas DataFrame of jobs (from JobSpy) to the database.
    Assigns a new batch_id to this group.
    Updates existing jobs if the ID already exists (UPSERT).
    """
    if jobs_df.empty:
        return

    init_db()
        
    # Ensure dataframe has the required columns, fill missing with None
    required_cols = ['id', 'site', 'job_url', 'title', 'company']
    for col in required_cols:
        if col not in jobs_df.columns:
            jobs_df[col] = None
            
    # Filter only the columns we care about
    filtered_df = jobs_df[required_cols]
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Create a new batch record
    cursor.execute("INSERT INTO batches DEFAULT VALUES")
    batch_id = cursor.lastrowid
    
    # 2. Extract records and append batch_id to each tuple
    records = []
    for row in filtered_df.itertuples(index=False, name=None):
        records.append((*row, batch_id))
    
    # 3. Insert only if the exact 'id' doesn't exist already
    cursor.executemany("""
        INSERT INTO jobs (id, site, job_url, title, company, batch_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
    """, records)
    
    inserted_count = cursor.rowcount
    
    # Optional: If no jobs were inserted, delete the empty batch to keep the DB clean
    if inserted_count == 0:
        cursor.execute("DELETE FROM batches WHERE id = ?", (batch_id,))
    
    conn.commit()
    conn.close()
    
    if inserted_count > 0:
        print(f"✅ Added {inserted_count} new jobs to the database in Batch {batch_id}.")
    else:
        print("No new jobs to add (all were duplicates).")

def enqueue_jd_requests(jobs_df: pd.DataFrame):
    """Upsert pending JD requests for OpenClaw worker."""
    if jobs_df is None or jobs_df.empty:
        return 0

    init_db()

    if "id" not in jobs_df.columns or "job_url" not in jobs_df.columns:
        return 0

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    records = []
    for row in jobs_df[["id", "job_url"]].itertuples(index=False, name=None):
        job_id, job_url = row
        records.append((job_id, job_url))

    cursor.executemany(
        """
        INSERT INTO jd_queue (job_id, job_url, status, attempts, updated_at, error)
        VALUES (?, ?, 'pending', 0, CURRENT_TIMESTAMP, NULL)
        ON CONFLICT(job_id) DO UPDATE SET
            job_url=excluded.job_url,
            status='pending',
            attempts=0,
            updated_at=CURRENT_TIMESTAMP,
            error=NULL
        """,
        records,
    )

    conn.commit()
    conn.close()
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

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    queued = 0
    for job_id in job_ids:
        cursor.execute(
            """
            UPDATE jobs
            SET fit_status = 'pending_fit',
                fit_updated_at = CURRENT_TIMESTAMP,
                fit_last_error = NULL,
                fit_attempts = COALESCE(fit_attempts, 0)
            WHERE id = ?
              AND description IS NOT NULL
              AND COALESCE(fit_status, '') NOT IN ('pending_fit', 'fitting', 'fit_done', 'notified')
            """,
            (job_id,),
        )
        queued += cursor.rowcount

    conn.commit()
    conn.close()
    return queued


def claim_pending_fitting_tasks(limit: int = 10) -> List[Dict[str, Any]]:
    """Atomically claim pending fitting tasks for processing."""
    init_db()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            """
            SELECT id AS job_id, COALESCE(fit_attempts, 0) AS attempts
            FROM jobs
            WHERE fit_status = 'pending_fit'
              AND description IS NOT NULL
            ORDER BY COALESCE(fit_updated_at, '1970-01-01 00:00:00') ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        if not rows:
            cursor.execute("COMMIT")
            return []

        cursor.executemany(
            """
            UPDATE jobs
            SET fit_status = 'fitting',
                fit_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [(row["job_id"],) for row in rows],
        )
        cursor.execute("COMMIT")

        return [
            {
                "job_id": row["job_id"],
                "attempts": row["attempts"],
            }
            for row in rows
        ]
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def mark_fitting_done(job_id: str):
    """Mark fitting task as completed."""
    init_db()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE jobs
        SET fit_status = 'fit_done',
            fit_updated_at = CURRENT_TIMESTAMP,
            fit_last_error = NULL
        WHERE id = ?
        """,
        (job_id,),
    )
    conn.commit()
    conn.close()


def mark_fitting_failed(
    job_id: str,
    error: str = "",
    retry: bool = True,
):
    """Mark fitting task as failed and optionally requeue."""
    init_db()

    status = "pending_fit" if retry else "fit_failed"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE jobs
        SET fit_status = ?,
            fit_attempts = COALESCE(fit_attempts, 0) + 1,
            fit_last_error = ?,
            fit_updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (status, error, job_id),
    )
    conn.commit()
    conn.close()


def count_jd_queue_status(job_ids: List[str], status: str) -> int:
    """Count how many jobs in given ids are in target status."""
    if not job_ids:
        return 0

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    placeholders = ",".join(["?"] * len(job_ids))
    query = f"SELECT COUNT(*) FROM jd_queue WHERE status = ? AND job_id IN ({placeholders})"
    cursor.execute(query, (status, *job_ids))
    count = cursor.fetchone()[0]

    conn.close()
    return count


def save_llm_matches(jobs_df: pd.DataFrame):
    """Persist LLM fit results back to jobs table."""
    if jobs_df is None or jobs_df.empty:
        return

    init_db()

    update_df = jobs_df.copy()
    if "id" not in update_df.columns:
        return

    if "llm_match" not in update_df.columns:
        update_df["llm_match"] = None
    if "llm_match_error" not in update_df.columns:
        update_df["llm_match_error"] = None

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    records = []
    for row in update_df[["id", "llm_match", "llm_match_error"]].itertuples(index=False, name=None):
        job_id, llm_match, llm_match_error = row
        fit_score = None
        fit_decision = None
        if llm_match:
            try:
                parsed = json.loads(llm_match)
                fit_score = parsed.get("fit_score")
                fit_decision = parsed.get("decision")
            except Exception:
                pass
        records.append((llm_match, llm_match_error, fit_score, fit_decision, job_id))

    cursor.executemany(
        """
        UPDATE jobs
        SET llm_match = ?,
            llm_match_error = ?,
            fit_score = ?,
            fit_decision = ?
        WHERE id = ?
        """,
        records,
    )

    conn.commit()
    conn.close()


def get_jobs_to_notify(limit: int = 10) -> pd.DataFrame:
    """Get unnotified fit results that are ready to notify."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT *
        FROM jobs
        WHERE fit_decision IN ('Strong Fit', 'Moderate Fit')
          AND notified_at IS NULL
          AND fit_status IN ('fit_done', 'notify_failed')
        ORDER BY batch_id DESC, fit_score DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


def mark_job_notified(job_id: str, status: str = "sent", error: str = None):
    """Mark notification status for one job."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if status == "sent":
        cursor.execute(
            """
            UPDATE jobs
            SET notified_at = CURRENT_TIMESTAMP,
                notify_status = 'sent',
                notify_error = NULL,
                fit_status = 'notified'
            WHERE id = ?
            """,
            (job_id,),
        )
    else:
        cursor.execute(
            """
            UPDATE jobs
            SET notify_status = ?,
                notify_error = ?,
                fit_status = 'notify_failed'
            WHERE id = ?
            """,
            (status, error, job_id),
        )

    conn.commit()
    conn.close()


def get_all_jobs() -> pd.DataFrame:
    """Retrieves all jobs from the database as a pandas DataFrame."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM jobs", conn)
    conn.close()
    return df

def save_jd_result(job_id: str, description: str = None, description_error: str = None):
    """Persist one JD scrape result from OpenClaw worker."""
    init_db()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if description:
        cursor.execute(
            """
            UPDATE jobs
            SET description = ?, description_error = NULL
            WHERE id = ?
            """,
            (description, job_id),
        )
        cursor.execute(
            """
            UPDATE jd_queue
            SET status = 'done', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP, error = NULL
            WHERE job_id = ?
            """,
            (job_id,),
        )
    else:
        cursor.execute(
            """
            UPDATE jobs
            SET description = NULL, description_error = ?
            WHERE id = ?
            """,
            (description_error, job_id),
        )
        cursor.execute(
            """
            UPDATE jd_queue
            SET status = 'failed', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP, error = ?
            WHERE job_id = ?
            """,
            (description_error, job_id),
        )

    conn.commit()
    conn.close()


def get_jobs_by_ids(job_ids: List[str]) -> pd.DataFrame:
    """Fetch jobs by id list."""
    if not job_ids:
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    placeholders = ",".join(["?"] * len(job_ids))
    query = f"SELECT * FROM jobs WHERE id IN ({placeholders})"
    df = pd.read_sql_query(query, conn, params=job_ids)
    conn.close()
    return df


def get_pending_jd_requests(limit: int = 10) -> pd.DataFrame:
    """Fetch pending jd queue items for external worker."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT q.job_id, q.job_url
        FROM jd_queue q
        WHERE q.status = 'pending'
        ORDER BY q.updated_at ASC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


def get_latest_batch_jobs() -> pd.DataFrame:
    """Retrieves only the jobs from the most recent batch."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT * FROM jobs 
        WHERE batch_id = (SELECT MAX(id) FROM batches)
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

if __name__ == "__main__":
    init_db()
    print("Database initialized successfully at jobs.db")
