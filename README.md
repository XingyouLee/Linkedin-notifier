LinkedIn Notifier
=================

## Branch status: Deprecated

This branch (`feature/material-generation-flow`) is deprecated and should not be used as the base for new materials-generation work.

- Reason: the recent materials-generation exploration on this line has been abandoned.
- Next step: start the replacement work from a fresh branch off `main`.
- Guidance: keep this branch only as historical reference; do not continue feature development here.

This repository currently has three supported surfaces:

- An Astro/Airflow pipeline that scans LinkedIn jobs, fetches JD details, runs profile-specific fitting, and sends notifications.
- A lightweight materials web app that serves signed generation links for tailored resume and cover-letter previews/downloads.
- A standalone macOS SwiftUI app that reads the shared business database for a read-only operator view.

Only these two surfaces are part of the active repository workflow.

## Repository layout

- `dags/`: Airflow DAGs and shared pipeline logic
- `scripts/`: operational scripts such as scraping, migrations, and local reset helpers
- `include/`: runtime seed data, resumes, and profile configuration
- `apps/macos/LinkedinNotifierApp/`: standalone macOS SwiftUI client
- `webapp/`: lightweight signed-link materials generation web surface
- `tests/`: Python regression coverage

## Pipeline overview

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


## Run locally

1. Start local Airflow:
   - `astro dev start`
2. Trigger scan DAG:
   - `astro dev run dags trigger linkedin_notifier`
3. Or trigger fitting DAG directly:
   - `astro dev run dags trigger linkedin_fitting_notifier`

4. Open the materials web app locally at `http://localhost:8000/materials?token=...` after a signed link is generated.

## macOS app

The SwiftUI app under [`apps/macos/LinkedinNotifierApp/`](/Users/levi/Linkedin-notifier/apps/macos/LinkedinNotifierApp/) is a separate read-only client for the cloud-hosted stack.

- Open [`Package.swift`](/Users/levi/Linkedin-notifier/apps/macos/LinkedinNotifierApp/Package.swift) in Xcode to run it locally.
- Run `swift test --package-path apps/macos/LinkedinNotifierApp` for Swift package tests.
- Use `./apps/macos/LinkedinNotifierApp/scripts/package_app.sh` to produce a local `.app` bundle.

## Environment variables

For Astro local runs, keep runtime vars in:

- `/Users/levi/Linkedin-notifier/dags/.env`
- `/Users/levi/Linkedin-notifier/.env` (for host CLI tooling)

Ensure both files are covered by version-control and Docker ignores so secrets never leak.

`JOBS_DB_URL` is the only source of truth for the business database connection. Local Astro/Airflow runs and cloud deployments should normally point to the same shared business database. If you need to fall back to a local or emergency database, change `JOBS_DB_URL` explicitly.

Common vars:

- `JOBS_DB_URL`: business DB DSN (for DAG tasks), e.g. `postgresql://jobs_app:jobs_pass@db.example.com:5432/jobsdb`
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


## Multi-user config

- The runtime now stores user-specific state in Postgres: `profiles`, `search_configs`, `search_terms`, and `profile_jobs`.
- Canonical job data stays shared in `jobs`; JD scraping stays shared in `jd_queue`; fit results and notifications are tracked per profile in `profile_jobs`.
- Put user config in `include/user_info/profiles.json` and resumes in `include/user_info/resume/`.
- DAG runs auto-sync the profile config JSON into Postgres at runtime; no separate manual sync script is required.
- The loader checks `include/user_info/profiles.json` first so Airflow containers can see the config during local Astro runs.
- `resume_path` values in the profile config are resolved relative to that file, so `resume/xingyouli.md` maps to `include/user_info/resume/xingyouli.md`.
- Each profile can have its own `active` flag, optional `bootstrap_existing_jobs` one-time migration flag, `resume_path`, Discord destination, model name, full `fit_prompt` template, and one or more search configs with distinct terms.
- Set `bootstrap_existing_jobs: true` only for the legacy profile that should inherit pre-multi-user `jobs` history; leave it `false` for newly added users.
- Search config supports `location` again, but not `geo_id`; the current scan flow sends `keywords`, `location`, `distance`, `start`, and `f_TPR`.


## Data storage

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


## Migrate existing SQLite data (one-time)

1. Decide which Postgres database should receive the migrated business data.
2. Set `JOBS_DB_URL` to that target, or pass `--pg-url` explicitly.
3. Run migration from host (requires `psycopg[binary]` installed) or from Airflow container:
   - Host: `JOBS_DB_URL=postgresql://jobs_app:jobs_pass@db.example.com:5432/jobsdb python scripts/migrate_sqlite_to_postgres.py --sqlite-path include/jobs.db`
   - Container: `docker exec -e JOBS_DB_URL="$JOBS_DB_URL" "$(docker ps --filter 'name=scheduler-1' --format '{{.Names}}' | head -n1)" python /usr/local/airflow/scripts/migrate_sqlite_to_postgres.py --sqlite-path /usr/local/airflow/include/jobs.db`
4. Keep `JOBS_DB_URL` aligned between `/Users/levi/Linkedin-notifier/.env` and your local Airflow runtime env so host tooling and DAG runs talk to the same business database by default.


## Notes

- Job id normalization is required to keep DB dedupe stable (`id` is stored as numeric string).
- Canonical jobs are shared globally, but discovery / fitting / notification state is tracked per profile.

## Zeabur deployment

Deploy this repo to Zeabur as **one Docker app service plus two Postgres databases**:

1. `linkedin-notifier`
   - Build from the root-level `Dockerfile`
   - Uses the image default command, which runs:
     - `airflow db migrate`
     - `airflow scheduler`
     - `airflow dag-processor`
     - `airflow triggerer`
     - `airflow api-server`
   - Expose port `8080`
2. `postgres-airflow`
   - Airflow metadata database only
3. `postgres-jobs`
   - Business database used by the DAGs through `JOBS_DB_URL`

Important notes:

- Do **not** split Airflow into multiple Zeabur app services for this setup.
- Set `CLOUD_DEPLOYMENT=1` on the Docker app service.
- Set `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` to the Airflow metadata Postgres.
- Set `JOBS_DB_URL` to the separate business Postgres.
- Set `AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`.
- Set `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` to one or more `<username>:<role>` pairs such as `admin:admin`.
- Set `AIRFLOW_ADMIN_PASSWORD` if you want a fixed Airflow login password from env.
- Set `AIRFLOW__API__BASE_URL=http://127.0.0.1:8080`.
- Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/`.
- Set `AIRFLOW__API_AUTH__JWT_SECRET` to the same long random secret for the whole app service.
- `PROFILE_CONFIG_PATH` should point to `/usr/local/airflow/include/user_info/profiles.json` unless you deliberately move the config.
- `DISCORD_BOT_TOKEN` should be configured globally if Discord delivery is enabled.
- `DISCORD_CHANNEL_ID` is only a fallback; profile-specific channel ids from config/database still take precedence.
- `DISCORD_WEBHOOK_URL` can be left empty if you only use bot-token delivery.
- If `AIRFLOW_ADMIN_PASSWORD` is unset, Airflow login uses a generated password file under `/usr/local/airflow/simple_auth_manager_passwords.json.generated`.
