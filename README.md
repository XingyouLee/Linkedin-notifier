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

The normal local/VPS runtime is Docker Compose. It starts the Airflow app plus
two Postgres databases so local deployment matches production topology:

- `postgres-airflow`: Airflow metadata only
- `postgres-jobs`: business data used by the DAGs through `JOBS_DB_URL`

```bash
docker compose up -d
```

Then open Airflow at <http://127.0.0.1:8080>.

If you intentionally want compose to use external databases, set
`COMPOSE_AIRFLOW_DB_URL` and/or `COMPOSE_JOBS_DB_URL`. Plain `JOBS_DB_URL` is
still used by host CLI tooling, but compose defaults to its own local
`postgres-jobs` service unless `COMPOSE_JOBS_DB_URL` is set.

### Astro local development

Do not use local Astro as a smoke-test runner. Starting the local scheduler can
resume old scheduled DAG runs. For safe local DAG validation, use:

```bash
astro dev pytest .astro/test_dag_integrity_default.py --args "-q"
```

Real end-to-end smoke runs belong in the manual GitHub Actions workflow described
in [Airflow test mode](#airflow-test-mode).

## Environment variables

For Astro local runs, keep runtime vars in:

- `/Users/levi/Linkedin-notifier/dags/.env`
- `/Users/levi/Linkedin-notifier/.env` (for host CLI tooling)

Ensure both files are covered by version-control and Docker ignores so secrets never leak.

`JOBS_DB_URL` is the only source of truth for the business database connection in host tooling and Airflow runtime. Docker Compose supplies a local default through `COMPOSE_JOBS_DB_URL`; Zeabur should point `JOBS_DB_URL` to the remote jobs Postgres service.

Common vars:

- `JOBS_DB_URL`: business DB DSN (for DAG tasks), e.g. `postgresql://jobs_app:jobs_pass@db.example.com:5432/jobsdb`
- `COMPOSE_JOBS_DB_URL`: optional compose-only override; defaults to `postgresql://jobs_app:jobs_pass@postgres-jobs:5432/jobsdb`
- `COMPOSE_AIRFLOW_DB_URL`: optional compose-only Airflow metadata DB override; defaults to `postgresql+psycopg2://airflow:airflow@postgres-airflow:5432/airflow`
- `PROFILE_CONFIG_PATH`: optional legacy import/bootstrap file; defaults to `include/user_info/profiles.json` and is not auto-synced during normal DAG runtime once profiles exist in DB
- `RESUME_MATCHER_BASE_URL`: public Resume Matcher base URL used for Discord deep links (`/launch?token=...`)
- `MATERIALS_LINK_SECRET`: shared HMAC secret used to sign launch tokens that Resume Matcher verifies
- `SCAN_REQUEST_PAGE_SIZE`, `SCAN_BETWEEN_REQUESTS_MIN_SEC`, `SCAN_BETWEEN_REQUESTS_MAX_SEC`
- `SCAN_BETWEEN_TERMS_DELAY_SEC`, `SCAN_HTTP_MAX_RETRIES`, `SCAN_HTTP_BASE_DELAY_SEC`
- `SCAN_HTTP_MAX_DELAY_SEC`, `SCAN_HTTP_JITTER_SEC`, `SCAN_REQUEST_TIMEOUT_SEC`
- `JD_WORKER_BATCH_SIZE`, `JD_WORKER_MAX_LOOPS`, `JD_WORKER_IDLE_LOOP_LIMIT`
- `JD_CLAIM_STALE_MINUTES`: reclaim stalled JD worker leases after this many minutes
- `FITTING_MAX_ATTEMPTS`
- `FITTING_CLAIM_LIMIT`: optional max profile-job fitting tasks claimed per fitting DAG run; leave unset to use the built-in default of `1000`, set `0` only when intentionally processing the full backlog in one run
- `FITTING_CLAIM_STALE_MINUTES`: reclaim stalled fitting leases after this many minutes
- `FITTING_MODEL_NAME`: default LLM model for fitting; only a per-endpoint `model` in `LLM_ENDPOINTS_JSON` overrides it
- `LLM_ENDPOINTS_JSON`: JSON array of LLM endpoints, including provider API keys, e.g. `[{"name":"nc","request_url":"https://nowcoding.ai/v1/responses","api_key_env":"NC_API_KEY"}]`
- `DISCORD_BOT_TOKEN` (used with per-profile Discord channel ids)
- `DEFAULT_PROFILE_KEY`, `DEFAULT_PROFILE_NAME`, `RESUME_PATH` (compatibility bootstrap only when the profiles table is empty and no legacy profile config file exists)

## Airflow test mode

> **Real smoke runs are GitHub Actions only. Running `scripts/run_airflow_test_mode_smoke.sh` locally is forbidden.** The script refuses to run unless `GITHUB_ACTIONS=true` is present (set automatically by the runner).

`LINKEDIN_TEST_MODE=true` means running the real Airflow DAGs against the test-only profile rows. It is **not** the same as running `pytest`, and it is **not** the same as the helper script `scripts/verify_notification_runs_test_mode.py`.

### Local validation (permitted)

Use `pytest` and `astro dev pytest` only — no scheduler is started:

```bash
# Unit tests (no Airflow runtime needed)
pytest tests -q

# DAG integrity / parse check via Astro Runtime container
astro dev pytest .astro/test_dag_integrity_default.py --args "-q"

# DB/Django contract check (no live DAGs)
LINKEDIN_TEST_MODE=true python scripts/verify_notification_runs_test_mode.py
```

### Real smoke runs (GitHub Actions only)

Trigger the `Smoke test (manual only)` workflow via GitHub Actions → **Actions** → **Smoke test (manual only)** → **Run workflow**.

The workflow builds this repository's Airflow Docker image, runs the smoke script
inside that image, and uses the Airflow CLI only to insert/poll a manual DAG run
through the configured deployed Airflow metadata database. It does **not** run a
local Astro scheduler.

Required GitHub repository secrets (set under Settings → Secrets → Actions):

| Secret | Purpose |
|---|---|
| `JOBS_DB_URL` | Business DB connection string |
| `AIRFLOW_METADATA_DB_URL` | Deployed Airflow metadata DB, exposed to the container as `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| `LLM_ENDPOINTS_JSON` | JSON array of LLM provider endpoints |
| `NC_API_KEY` | Provider API key (or equivalent for your provider) |
| `FITTING_MODEL_NAME` | Default LLM model name for fitting |
| `DISCORD_BOT_TOKEN` | Discord bot token for notification delivery |

Workflow dispatch inputs (all optional, defaults are intentionally tiny):

| Input | Default | Description |
|---|---|---|
| `max_jobs` | `3` | `LINKEDIN_TEST_MAX_JOBS` |
| `max_scan_rows` | `5` | `LINKEDIN_TEST_MAX_SCAN_ROWS` |
| `max_jd_jobs` | `5` | `LINKEDIN_TEST_MAX_JD_JOBS` |
| `max_fit_jobs` | `3` | `LINKEDIN_TEST_MAX_FIT_JOBS` |
| `max_notify_jobs` | `3` | `LINKEDIN_TEST_MAX_NOTIFY_JOBS` |

Expected behavior:

- `linkedin_notifier` only selects profiles marked `test_mode_only` / `is_test_profile`.
- Scan/filter/JD/fitting paths still execute as real Airflow tasks.
- Test-mode caps keep the smoke run small instead of processing the full test backlog.
- `linkedin_notifier` triggers `linkedin_fitting_notifier` automatically; the smoke script does **not** trigger it separately (doing so risks DDL/queue deadlocks).
- `linkedin_fitting_notifier` should complete and create a notification run, including a `completed_zero_results` run when no jobs qualify.

The smoke script:
- Refuses to run outside GitHub Actions (exit code 4).
- Guards `LINKEDIN_TEST_MODE=true` (exit code 2).
- Requires `JOBS_DB_URL` and `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` so it cannot fall back to a local metadata DB (exit code 5).
- Refuses to trigger if `linkedin_notifier` or `linkedin_fitting_notifier` already has active queued/running/scheduled runs (exit code 6).
- Diagnoses Discord config: warns if `DISCORD_BOT_TOKEN` is missing.
- The GitHub workflow sets `REQUIRE_DISCORD_VERIFICATION=true`, so missing Discord config fails fast (exit code 3).
- Polls Airflow for both the scan DAG and the auto-triggered fitting DAG (configurable via `SMOKE_MAX_WAIT_SECONDS` and `SMOKE_POLL_INTERVAL`).
- Prints a pass/fail summary with exit code 0 (all stages succeeded) or 1 (any stage failed or timed out).

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

- Airflow metadata is stored in a dedicated Postgres database (`airflow`)
- Business data is stored in a separate Postgres database (`jobsdb`)
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

Deploy this repo to Zeabur as **one Docker app service plus two Postgres databases**.

The preferred deployment flow is:

1. Push changes to GitHub.
2. GitHub Actions `CI` runs unit tests, DAG integrity, Swift tests, and Docker build.
3. On `main` or `zeabur-airflow-deploy`, CI publishes the Docker image to GHCR:
   - immutable SHA tag: `ghcr.io/xingyoulee/linkedin-notifier:<commit-sha>` when the owner/repo are lowercased by GHCR
   - branch tag: `ghcr.io/xingyoulee/linkedin-notifier:main` or `:zeabur-airflow-deploy`
   - `:latest` only for `main`
4. Zeabur should pull the published GHCR image instead of building/running local Astro.

If the repository owner casing differs, use the lowercase GHCR package name shown in the `Publish image to GHCR` job output.
If the GHCR package is private, configure Zeabur with GHCR registry credentials or make the package public before pointing Zeabur at the image.

Zeabur should run **one Docker app service plus two Postgres databases**. Zeabur does not use the root Docker Compose file directly; create the equivalent services in Zeabur and set the app env vars to the two Zeabur Postgres URLs.

1. `linkedin-notifier`
   - Build from the root-level `Dockerfile`
   - Uses the image default command, which runs:
     - `airflow db check`
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
- Do **not** bake either database into the Docker image. The databases are persistent external services.
- Set `CLOUD_DEPLOYMENT=1` on the Docker app service.
- Set `PORT=8080`.
- Set `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` to the Airflow metadata Postgres.
- Set `JOBS_DB_URL` to the separate business Postgres.
- Set `AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`.
- Set `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` to one or more `<username>:<role>` pairs such as `admin:admin`.
- Set `AIRFLOW_ADMIN_PASSWORD` if you want a fixed Airflow login password from env.
- Set `AIRFLOW__API__BASE_URL` to the public Airflow app URL, for example `https://your-airflow-zeabur-domain`.
- Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/`.
- Set `AIRFLOW__LOGGING__BASE_LOG_FOLDER=/usr/local/airflow/logs`.
- Add a Zeabur volume on the Docker app service mounted at `/usr/local/airflow/logs`. Without this volume, Airflow task logs live on the ephemeral container filesystem and disappear after restart/redeploy.
- Set Zeabur's custom HTTP health check path to `/api/v2/version` after a local smoke test confirms that endpoint returns 2xx without login for the pinned Airflow runtime.
- Set `AIRFLOW__API_AUTH__JWT_SECRET` to the same long random secret for the whole app service.
- `PROFILE_CONFIG_PATH` is optional in cloud runtime; set it only if you need legacy empty-DB bootstrap/import from a non-default file.
- `DISCORD_BOT_TOKEN` should be configured globally if Discord delivery is enabled.
- `DISCORD_CHANNEL_ID` is only a fallback; profile-specific channel ids from config/database still take precedence.
- `DISCORD_WEBHOOK_URL` can be left empty if you only use bot-token delivery.
- You do not need to set `FITTING_CLAIM_LIMIT` on Zeabur for normal operation. The Airflow code defaults to claiming at most 1000 fitting tasks per run.
- If `AIRFLOW_ADMIN_PASSWORD` is unset, Airflow login uses a generated password file under `/usr/local/airflow/simple_auth_manager_passwords.json.generated`.

### Zeabur operations checklist

Use this checklist when Airflow becomes unreachable or after changing the deployment:

1. Confirm the Docker app service has a persistent volume mounted at `/usr/local/airflow/logs`.
2. Confirm the app env includes `AIRFLOW__LOGGING__BASE_LOG_FOLDER=/usr/local/airflow/logs`, `PORT=8080`, the public `AIRFLOW__API__BASE_URL`, the internal `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, and both Postgres URLs.
3. Set the custom HTTP health check path to `/api/v2/version`. If Airflow is reachable but suspicious, also open `/api/v2/monitor/health` to inspect metadata database, scheduler, triggerer, and DAG processor status.
4. Before redeploying to recover the UI, copy the current Zeabur runtime logs and check the persisted Airflow task logs under `/usr/local/airflow/logs`; redeploying replaces the running container.
