LinkedIn Notifier (Airflow + Astro)
==================================

This project runs a two-DAG pipeline:

1. `linkedin_notifier` (`/Users/levi/Linkedin-notifier/dags/process.py`)
   - Scan LinkedIn jobs via scripts guest API scraper (`scripts/linkedin_public_jobs_scraper.py`)
   - Save new jobs into Postgres
   - Queue JD scraping, run Playwright JD worker (`dags/jd_playwright_worker.py`), enqueue fitting tasks
   - Trigger `linkedin_fitting_notifier`

2. `linkedin_fitting_notifier` (`/Users/levi/Linkedin-notifier/dags/fitting_notifier.py`)
   - Claim fitting tasks (`pending_fit` plus stale recoverable `fitting`)
   - Run LLM fitting
   - Save match result + score + decision
   - Finalize queue status
   - Send Discord notification for `Strong Fit` / `Moderate Fit`


Run locally
-----------

1. Start local Airflow:
   - `astro dev start`
2. Trigger scan DAG:
   - `astro dev run dags trigger linkedin_notifier`
3. Or trigger fitting DAG directly:
   - `astro dev run dags trigger linkedin_fitting_notifier`


Environment variables
---------------------

For Astro local runs, keep runtime vars in:

- `/Users/levi/Linkedin-notifier/dags/.env`
- `/Users/levi/Linkedin-notifier/.env` (for host CLI tooling)

Ensure both files are covered by version-control and Docker ignores so secrets never leak.

Common vars:

- `JOBS_DB_URL`: business DB DSN (for DAG tasks), e.g. `postgresql://jobs_app:jobs_pass@postgres:5432/jobsdb`
- `SCAN_RESULTS_WANTED`: scan target count
- `SCAN_SEARCH_TERMS`: comma-separated terms (optional)
- `SCAN_HOURS_OLD`, `SCAN_DISTANCE`
- `JD_WORKER_BATCH_SIZE`, `JD_WORKER_MAX_LOOPS`, `JD_WORKER_IDLE_LOOP_LIMIT`
- `JD_CLAIM_STALE_MINUTES`: reclaim stalled JD worker leases after this many minutes
- `FITTING_MAX_ATTEMPTS`
- `FITTING_CLAIM_STALE_MINUTES`: reclaim stalled fitting leases after this many minutes
- `FITTING_MODEL_NAME`
- `GMN_API_KEY`
- `DISCORD_BOT_TOKEN` or `DISCORD_WEBHOOK_URL`


Data storage
------------

- Business data is stored in Postgres (`jobsdb`)
- Main tables:
  - `jobs`
  - `batches`
  - `jd_queue`
  - `fitting_queue` (legacy table, currently not used by DAG flow)


Migrate existing SQLite data (one-time)
--------------------------------------

1. Ensure `jobsdb` exists in your local Postgres container.
2. Run migration from host (requires `psycopg[binary]` installed) or from Airflow container:
   - Host: `python scripts/migrate_sqlite_to_postgres.py --sqlite-path include/jobs.db --pg-url postgresql://postgres:postgres@127.0.0.1:5432/jobsdb`
   - Container: `docker exec "$(docker ps --filter 'name=scheduler-1' --format '{{.Names}}' | head -n1)" python /usr/local/airflow/scripts/migrate_sqlite_to_postgres.py --sqlite-path /usr/local/airflow/include/jobs.db --pg-url postgresql://postgres:postgres@postgres:5432/jobsdb`
3. Set `JOBS_DB_URL` in `/Users/levi/Linkedin-notifier/dags/.env` for Airflow runtime.


Notes
-----

- Job id normalization is required to keep DB dedupe stable (`id` is stored as numeric string).
- Notifications run after fitting finalization, so newly finished jobs are not skipped.
