import sqlite3
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
import os

# Point DB to the include directory so it is shared between Astro docker container and host machine
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "include", "jobs.db"))

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
            llm_match TEXT,
            llm_match_error TEXT,
            FOREIGN KEY (batch_id) REFERENCES batches (id)
        )
    """)

    # Lightweight migration for existing databases
    cursor.execute("PRAGMA table_info(jobs)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    if "llm_match" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN llm_match TEXT")
    if "llm_match_error" not in existing_cols:
        cursor.execute("ALTER TABLE jobs ADD COLUMN llm_match_error TEXT")

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
        records.append(row)

    cursor.executemany(
        """
        UPDATE jobs
        SET llm_match = ?,
            llm_match_error = ?
        WHERE id = ?
        """,
        [(llm_match, llm_match_error, job_id) for job_id, llm_match, llm_match_error in records],
    )

    conn.commit()
    conn.close()


def get_all_jobs() -> pd.DataFrame:
    """Retrieves all jobs from the database as a pandas DataFrame."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM jobs", conn)
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
