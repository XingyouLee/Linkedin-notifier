LinkedIn Notifier (Airflow + Astro)
==================================

This project runs a two-DAG pipeline:

1. `linkedin_notifier` (`/Users/levi/Linkedin-notifier/dags/process.py`)
   - Scan LinkedIn jobs via scripts guest API scraper (`scripts/linkedin_public_jobs_scraper.py`)
   - Save canonical jobs into Postgres
   - Link matched jobs to one or more active profiles/search configs
   - Queue JD fetching, run LinkedIn `jobPosting` API worker (`dags/jd_api_worker.py`), enqueue fitting tasks
   - Trigger `linkedin_fitting_notifier`

2. `linkedin_fitting_notifier` (`/Users/levi/Linkedin-notifier/dags/fitting_notifier.py`)
   - Claim profile-job fitting tasks (`pending_fit` plus stale recoverable `fitting`)
   - Run LLM fitting with the owning profile resume
   - Save profile-specific match result + score + decision
   - Finalize queue status
   - Send Discord notification for `Strong Fit` / `Moderate Fit` to the owning profile channel/webhook


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
- `PROFILE_CONFIG_PATH`: optional override for profile config file; defaults to `include/user_info/profiles.json`
- `SCAN_REQUEST_PAGE_SIZE`, `SCAN_BETWEEN_REQUESTS_MIN_SEC`, `SCAN_BETWEEN_REQUESTS_MAX_SEC`
- `SCAN_BETWEEN_TERMS_DELAY_SEC`, `SCAN_HTTP_MAX_RETRIES`, `SCAN_HTTP_BASE_DELAY_SEC`
- `SCAN_HTTP_MAX_DELAY_SEC`, `SCAN_HTTP_JITTER_SEC`, `SCAN_REQUEST_TIMEOUT_SEC`
- `JD_WORKER_BATCH_SIZE`, `JD_WORKER_MAX_LOOPS`, `JD_WORKER_IDLE_LOOP_LIMIT`
- `JD_CLAIM_STALE_MINUTES`: reclaim stalled JD worker leases after this many minutes
- `FITTING_MAX_ATTEMPTS`
- `FITTING_CLAIM_STALE_MINUTES`: reclaim stalled fitting leases after this many minutes
- `GMN_API_KEY`
- `DISCORD_BOT_TOKEN` (used with per-profile Discord channel ids)
- `DEFAULT_PROFILE_KEY`, `DEFAULT_PROFILE_NAME`, `RESUME_PATH` (compatibility bootstrap for single-user mode)


Multi-user config
-----------------

- The runtime now stores user-specific state in Postgres: `profiles`, `search_configs`, `search_terms`, and `profile_jobs`.
- Canonical job data stays shared in `jobs`; JD scraping stays shared in `jd_queue`; fit results and notifications are tracked per profile in `profile_jobs`.
- Put user config in `include/user_info/profiles.json` and resumes in `include/user_info/resume/`.
- DAG runs auto-sync the profile config JSON into Postgres at runtime; no separate manual sync script is required.
- The loader checks `include/user_info/profiles.json` first so Airflow containers can see the config during local Astro runs.
- `resume_path` values in the profile config are resolved relative to that file, so `resume/xingyouli.md` maps to `include/user_info/resume/xingyouli.md`.
- Each profile can have its own `active` flag, optional `bootstrap_existing_jobs` one-time migration flag, `resume_path`, Discord destination, model name, full `fit_prompt` template, and one or more search configs with distinct terms.
- Set `bootstrap_existing_jobs: true` only for the legacy profile that should inherit pre-multi-user `jobs` history; leave it `false` for newly added users.
- Search config supports `location` again, but not `geo_id`; the current scan flow sends `keywords`, `location`, `distance`, `start`, and `f_TPR`.


Data storage
------------

- Business data is stored in Postgres (`jobsdb`)
- Main tables:
  - `batches`
  - `jobs`
  - `jd_queue`
  - `profiles`
  - `search_configs`
  - `search_terms`
  - `profile_jobs`
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
- Canonical jobs are shared globally, but discovery / fitting / notification state is tracked per profile.
- Notifications run after fitting finalization, so newly finished jobs are not skipped.
