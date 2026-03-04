LinkedIn Notifier (Airflow + Astro)
==================================

This project runs a two-DAG pipeline:

1. `linkedin_notifier` (`/Users/levi/Linkedin-notifier/dags/process.py`)
   - Scan LinkedIn jobs via scripts guest API scraper (`scripts/linkedin_public_jobs_scraper.py`)
   - Save new jobs into SQLite
   - Queue JD scraping, run Playwright JD worker (`dags/jd_playwright_worker.py`), enqueue fitting tasks
   - Trigger `linkedin_fitting_notifier`

2. `linkedin_fitting_notifier` (`/Users/levi/Linkedin-notifier/dags/fitting_notifier.py`)
   - Claim fitting tasks (`pending_fit` and recoverable `fitting`)
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

(`.dockerignore` excludes root `.env`, so scheduler may not see it.)

Common vars:

- `SCAN_RESULTS_WANTED`: scan target count
- `SCAN_SEARCH_TERMS`: comma-separated terms (optional)
- `SCAN_HOURS_OLD`, `SCAN_DISTANCE`
- `JD_WORKER_BATCH_SIZE`, `JD_WORKER_MAX_LOOPS`, `JD_WORKER_IDLE_LOOP_LIMIT`
- `FITTING_MAX_ATTEMPTS`
- `FITTING_MODEL_NAME`
- `GMN_API_KEY`
- `DISCORD_BOT_TOKEN` or `DISCORD_WEBHOOK_URL`


Data storage
------------

- SQLite DB path defaults to `/Users/levi/Linkedin-notifier/include/jobs.db`
- Main tables:
  - `jobs`
  - `batches`
  - `jd_queue`


Notes
-----

- Job id normalization is required to keep DB dedupe stable (`id` is stored as numeric string).
- Notifications run after fitting finalization, so newly finished jobs are not skipped.
