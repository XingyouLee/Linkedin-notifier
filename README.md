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
- DB-authoritative profile/search/prompt/Discord configuration in Postgres
- legacy `include/user_info/profiles.json` import/bootstrap support
- shared Postgres schema/migrations and explicit legacy backfills
- launch-token generation via `RESUME_MATCHER_BASE_URL` + `MATERIALS_LINK_SECRET`

Resume Matcher becomes the separate deployable app that owns `/launch`, workspace sessions, editing flows, PDF generation, and its own `RESUME_MATCHER_SESSION_SECRET`. The matcher-side fallback to `MATERIALS_LINK_SECRET` is transitional only and should be removed after cutover.

Profile configuration is now DB-authoritative. `include/user_info/profiles.json` is retained only for one-time bootstrap/import of legacy rows; normal DAG runtime reads do not sync it back into Postgres.

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
- `PROFILE_CONFIG_PATH`: optional legacy import/bootstrap file; defaults to `include/user_info/profiles.json` and is not auto-synced during normal DAG runtime once profiles exist in DB
- `RESUME_MATCHER_BASE_URL`: public Resume Matcher base URL used for Discord deep links (`/launch?token=...`)
- `MATERIALS_LINK_SECRET`: shared HMAC secret used to sign launch tokens that Resume Matcher verifies
- `SCAN_REQUEST_PAGE_SIZE`, `SCAN_BETWEEN_REQUESTS_MIN_SEC`, `SCAN_BETWEEN_REQUESTS_MAX_SEC`
- `SCAN_BETWEEN_TERMS_DELAY_SEC`, `SCAN_HTTP_MAX_RETRIES`, `SCAN_HTTP_BASE_DELAY_SEC`
- `SCAN_HTTP_MAX_DELAY_SEC`, `SCAN_HTTP_JITTER_SEC`, `SCAN_REQUEST_TIMEOUT_SEC`
- `JD_WORKER_BATCH_SIZE`, `JD_WORKER_MAX_LOOPS`, `JD_WORKER_IDLE_LOOP_LIMIT`
- `JD_CLAIM_STALE_MINUTES`: reclaim stalled JD worker leases after this many minutes
- `FITTING_MAX_ATTEMPTS`
- `FITTING_CLAIM_STALE_MINUTES`: reclaim stalled fitting leases after this many minutes
- `FITTING_MODEL_NAME`: default LLM model for fitting; only a per-endpoint `model` in `LLM_ENDPOINTS_JSON` overrides it
- `LLM_ENDPOINTS_JSON`: JSON array of LLM endpoints, including provider API keys, e.g. `[{"name":"nc","request_url":"https://nowcoding.ai/v1/responses","api_key_env":"NC_API_KEY"}]`
- `DISCORD_BOT_TOKEN` (used with per-profile Discord channel ids)
- `DEFAULT_PROFILE_KEY`, `DEFAULT_PROFILE_NAME`, `RESUME_PATH` (compatibility bootstrap only when the profiles table is empty and no legacy profile config file exists)

## Airflow test mode

`LINKEDIN_TEST_MODE=true` means running the real Airflow DAGs against the test-only profile rows. It is **not** the same as running `pytest`, and it is **not** the same as the helper script `scripts/verify_notification_runs_test_mode.py`. Use it when you want an end-to-end smoke run through the actual scheduler/tasks.

Recommended smoke-test env:

```env
LINKEDIN_TEST_MODE=true
LINKEDIN_TEST_MAX_JOBS=5
LINKEDIN_TEST_MAX_SCAN_ROWS=5
LINKEDIN_TEST_MAX_JD_JOBS=5
LINKEDIN_TEST_MAX_FIT_JOBS=5
```

Then trigger the real DAG:

```bash
airflow dags trigger linkedin_notifier
```

Expected behavior:

- `linkedin_notifier` only selects profiles marked `test_mode_only` / `is_test_profile`.
- Scan/filter/JD/fitting paths still execute as real Airflow tasks.
- Test-mode caps keep the smoke run small instead of processing the full test backlog.
- `linkedin_notifier` should trigger `linkedin_fitting_notifier`.
- `linkedin_fitting_notifier` should complete and create a notification run, including a `completed_zero_results` run when no jobs qualify.

Cap variables:

- `LINKEDIN_TEST_MAX_JOBS`: fallback cap used by test-mode stages when a stage-specific cap is not set.
- `LINKEDIN_TEST_MAX_SCAN_ROWS`: max scanned rows kept before saving jobs/profile links.
- `LINKEDIN_TEST_MAX_JD_JOBS`: max JD backlog records passed into the in-DAG JD worker.
- `LINKEDIN_TEST_MAX_FIT_JOBS`: max fitting tasks queued/claimed in test mode.
- `LINKEDIN_TEST_MAX_NOTIFY_JOBS`: max jobs sent to Discord in test mode (default 3); all successful LLM payloads are eligible regardless of fit decision.

The helper script remains useful for a deterministic DB/Django contract check:

```bash
LINKEDIN_TEST_MODE=true python scripts/verify_notification_runs_test_mode.py
```

For a real Airflow smoke run, use the bounded trigger helper from the project root or Airflow container:

```bash
LINKEDIN_TEST_MODE=true \
LINKEDIN_TEST_MAX_JOBS=3 \
LINKEDIN_TEST_MAX_SCAN_ROWS=5 \
LINKEDIN_TEST_MAX_JD_JOBS=5 \
LINKEDIN_TEST_MAX_FIT_JOBS=5 \
LINKEDIN_TEST_MAX_NOTIFY_JOBS=3 \
./scripts/run_airflow_test_mode_smoke.sh
```

The smoke script now:
- Guards `LINKEDIN_TEST_MODE=true` (exits with code 2 if not set).
- Diagnoses Discord config: warns if `DISCORD_BOT_TOKEN` is missing.
- Fails fast on `REQUIRE_DISCORD_VERIFICATION=true` when Discord config is incomplete (exit code 3).
- Triggers `linkedin_notifier` with full test-mode conf.
- Polls Airflow for both the scan DAG and the auto-triggered fitting DAG (configurable via `SMOKE_MAX_WAIT_SECONDS` and `SMOKE_POLL_INTERVAL`).
- Prints a pass/fail summary with exit code 0 (all stages succeeded) or 1 (any stage failed or timed out).
- Does **not** manually trigger `linkedin_fitting_notifier`: the scan DAG triggers it automatically. Triggering both can cause database DDL/queue deadlocks in local Airflow.

This submits the actual `linkedin_notifier` DAG. It is intentionally separate from `pytest`: use it when you need production-like pipeline evidence against the test-only profile rows.

## Multi-user config

- The runtime now stores user-specific state in Postgres: `profiles`, `search_configs`, `search_terms`, and `profile_jobs`.
- Canonical job data stays shared in `jobs`; JD scraping stays shared in `jd_queue`; fit results and notifications are tracked per profile in `profile_jobs`.
- Treat Postgres as the source of truth. Edit profile names, resumes, prompts, search terms, scan filters, and Discord destinations through DB/WebUI surfaces.
- `include/user_info/profiles.json` is legacy/bootstrap input only. Airflow imports it automatically only when the `profiles` table is empty; otherwise DAG runtime never syncs it and will not overwrite WebUI/DB edits.
- To intentionally re-import legacy profile JSON, run `database.sync_profiles_from_source(force=True)` from a controlled maintenance shell. Omitted `resume_text` preserves the existing DB resume so WebUI resume edits are not clobbered.
- Resume markdown files are no longer the runtime source of truth. Store and edit real CV content in `profiles.resume_text` through Django WebUI/DB maintenance.
- Each profile can have its own `active` flag, optional `bootstrap_existing_jobs` one-time migration flag, DB-backed `resume_text`, Discord destination, full `fit_prompt` template, and one or more search configs with distinct terms.
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
- `PROFILE_CONFIG_PATH` is optional in cloud runtime; set it only if you need legacy empty-DB bootstrap/import from a non-default file.
- `DISCORD_BOT_TOKEN` should be configured globally if Discord delivery is enabled.
- `DISCORD_CHANNEL_ID` is only a fallback; profile-specific channel ids from config/database still take precedence.
- `DISCORD_WEBHOOK_URL` can be left empty if you only use bot-token delivery.
- If `AIRFLOW_ADMIN_PASSWORD` is unset, Airflow login uses a generated password file under `/usr/local/airflow/simple_auth_manager_passwords.json.generated`.
