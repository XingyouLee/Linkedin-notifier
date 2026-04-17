# LinkedIn Notifier (Airflow + Astro)

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

## Notifier / Resume Matcher repository boundary

The ongoing two-repo split treats this repository as the long-term home for **LinkedIn Notifier** only. `apps/resume-matcher/` is a migration-era subtree while the extraction is finalized; new notifier work should stay in the root Airflow tree.

Notifier-owned responsibilities:

- Airflow scanning, JD ingestion, fitting, and Discord notifications
- `include/user_info/` source resumes + `profiles.json`
- shared Postgres schema/migrations and `sync_profiles_from_source(force=True)` backfills
- launch-token generation via `RESUME_MATCHER_BASE_URL` + `MATERIALS_LINK_SECRET`

Resume Matcher becomes the separate deployable app that owns `/launch`, workspace sessions, editing flows, PDF generation, and its own `RESUME_MATCHER_SESSION_SECRET`. The matcher-side fallback to `MATERIALS_LINK_SECRET` is transitional only and should be removed after cutover.

Before cutover, refresh canonical profile content with `sync_profiles_from_source(force=True)` and keep notifier-owned source resumes in `.md` or `.txt`; those are the formats this repo currently hydrates into `profiles.resume_text`.

## Run locally

1. Start local Airflow:
   - `astro dev start`
2. Trigger scan DAG:
   - `astro dev run dags trigger linkedin_notifier`
3. Or trigger fitting DAG directly:
   - `astro dev run dags trigger linkedin_fitting_notifier`

## Environment variables

For Astro local runs, keep runtime vars in:

- `/Users/levi/Linkedin-notifier/dags/.env`
- `/Users/levi/Linkedin-notifier/.env` (for host CLI tooling)

Ensure both files are covered by version-control and Docker ignores so secrets never leak.

`JOBS_DB_URL` is the only source of truth for the business database connection. Local Astro/Airflow runs and cloud deployments should normally point to the same shared business database. If you need to fall back to a local or emergency database, change `JOBS_DB_URL` explicitly.

Common vars:

- `JOBS_DB_URL`: business DB DSN (for DAG tasks), e.g. `postgresql://jobs_app:jobs_pass@db.example.com:5432/jobsdb`
- `PROFILE_CONFIG_PATH`: optional override for profile config file; defaults to `include/user_info/profiles.json`
- `RESUME_MATCHER_BASE_URL`: public Resume Matcher base URL used for Discord deep links (`/launch?token=...`)
- `MATERIALS_LINK_SECRET`: shared HMAC secret used to sign launch tokens that Resume Matcher verifies
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
- Calibration notes for the 2026-04-16 experience-filtering pass live in `include/user_info/fit-calibration-2026-04-16.md`; use that document when tightening `Moderate Fit` prompt behavior without harming junior/plausible-mid recall.
- Set `bootstrap_existing_jobs: true` only for the legacy profile that should inherit pre-multi-user `jobs` history; leave it `false` for newly added users.
- Search config supports both `location` and optional `geo_id`; the scan flow now sends `keywords`, `location`, `geoId`, `distance`, `start`, and `f_TPR`.
- For Netherlands-wide searches, use LinkedIn `geo_id` `102890719` unless you have a more specific regional geoId to target.

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
