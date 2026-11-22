# LinkedIn Notifier (Airflow + Astro)

Scans LinkedIn public job search, scores each job against each profile's resume with an LLM, and sends good matches to Discord.

## Pipeline

1. `linkedin_notifier` (`dags/process.py`)
   - Scans the LinkedIn guest search API for each active search config. Every scan covers a fixed recent 24h window first, then the config's `hours_old` lookback (default 120h).
   - Upserts canonical jobs into `jobs` and links them to profiles in `profile_jobs`.
   - Fetches missing job descriptions (`dags/jd_api_worker.py`), queues fitting, and triggers the fitting DAG.
2. `linkedin_fitting_notifier` (`dags/fitting_notifier.py`)
   - Claims `pending_fit` work (plus stale `fitting` leases).
   - Calls the LLM with the profile resume and JD, applies conservative post-checks (experience gap, seniority, language), and stores the result per profile.
   - Sends `Strong Fit` / `Moderate Fit` jobs to the profile's Discord channel.

## Data and configuration

- Business data lives in its own Postgres (`JOBS_DB_URL`), separate from Airflow metadata.
- Shared tables: `jobs`, `jd_queue`, `batches`. Per-profile tables: `profiles`, `search_configs`, `search_terms`, `profile_jobs`.
- **Postgres is the source of truth** for profiles, resumes (`profiles.resume_text`), prompts, search terms and Discord destinations. Edit them through the Django web UI.
- `include/user_info/profiles.json` is only imported when the `profiles` table is empty. To re-import on purpose, run `database.sync_profiles_from_source(force=True)`; an omitted `resume_text` keeps the DB resume.
- Fitting LLM endpoints come only from active rows in Django's `llm_endpoints` table; drag order in `/admin/` is the retry order. A DB failure or no active rows stops fitting with a config error. `LLM_ENDPOINTS_JSON` is ignored by the DAG and only used by local comparison scripts and the Resume Matcher fallback.
- For Netherlands-wide searches, use `geo_id` `102890719`.
- Prompt calibration notes: `include/user_info/fit-calibration-2026-04-16.md`.

`apps/resume-matcher/` is a migration-era subtree being split into its own repo; new notifier work belongs in the root Airflow tree. Discord links into it are signed with `MATERIALS_LINK_SECRET` and point at `RESUME_MATCHER_BASE_URL`.

## Run locally

```bash
docker compose up -d
```

This starts Airflow plus two Postgres services (`postgres-airflow`, `postgres-jobs`) at <http://127.0.0.1:8080>. Set `COMPOSE_AIRFLOW_DB_URL` / `COMPOSE_JOBS_DB_URL` to use external databases instead.

Runtime env goes in `dags/.env` (Airflow) and `.env` (host tooling); both are git- and Docker-ignored.

Don't use `astro dev start` as a smoke runner: the scheduler can resume old scheduled runs.

## Tests

```bash
pytest tests -q
astro dev pytest .astro/test_dag_integrity_default.py --args "-q"
LINKEDIN_TEST_MODE=true python scripts/verify_notification_runs_test_mode.py
```

Real end-to-end smoke runs only happen in the manual GitHub Actions workflow **Smoke test (manual only)**. `scripts/run_airflow_test_mode_smoke.sh` refuses to run outside GitHub Actions. It runs the real DAGs against `test_mode_only` profiles with small caps (workflow inputs `max_jobs`, `max_scan_rows`, `max_jd_jobs`, `max_fit_jobs`, `max_notify_jobs`), refuses to start if either DAG already has an active run, and requires these repo secrets: `JOBS_DB_URL`, `AIRFLOW_METADATA_DB_URL`, `DISCORD_BOT_TOKEN`, `FITTING_MODEL_NAME`, plus any provider keys referenced by endpoint `api_key_env` (e.g. `DEEPSEEK_API_KEY`, `OPENAI_API_KEY`).

## Environment variables

See `.env.zeabur.example` for a full cloud example. The ones that matter:

| Variable | Purpose |
|---|---|
| `JOBS_DB_URL` | Business DB DSN. Required; the only DB source for DAGs and host tooling. |
| `FITTING_MODEL_NAME` | Default model when an endpoint row has no `model`. |
| `FITTING_MAX_ATTEMPTS` | DAG attempts per job before it is marked `fit_failed`. |
| `FITTING_CLAIM_LIMIT` | Max fitting tasks per run (default 1000, `0` = unlimited). |
| `FITTING_CONCURRENCY` | Parallel LLM calls. |
| `FITTING_CLAIM_STALE_MINUTES`, `JD_CLAIM_STALE_MINUTES` | Reclaim stalled leases (default 30). |
| `JD_WORKER_BATCH_SIZE`, `JD_WORKER_MAX_LOOPS`, `JD_WORKER_IDLE_LOOP_LIMIT`, `JD_MAX_ATTEMPTS` | JD fetch worker tuning. |
| `SCAN_*` | Scan paging, delays, retries and timeouts. |
| `DISCORD_BOT_TOKEN` | Discord delivery; `DISCORD_CHANNEL_ID` / `DISCORD_WEBHOOK_URL` are fallbacks only. |
| `RESUME_MATCHER_BASE_URL`, `MATERIALS_LINK_SECRET` | Resume Matcher launch links. |
| `PROFILE_CONFIG_PATH` | Legacy bootstrap file (default `include/user_info/profiles.json`). |
| `DEFAULT_PROFILE_KEY`, `DEFAULT_PROFILE_NAME`, `RESUME_PATH` | Legacy bootstrap when no profiles and no config file exist. |

## Zeabur deployment

One Docker app service plus two Zeabur Postgres services (Airflow metadata, business data). Don't split Airflow into several services and don't bake databases into the image.

Deploy flow: push to `zeabur-airflow-deploy` (or `main`) → CI runs tests, DAG integrity and the Docker build → publishes `ghcr.io/xingyoulee/linkedin-notifier:<sha>` (plus branch tag, and `:latest` on `main`) → updates the Zeabur service to the SHA tag. CI needs `ZEABUR_TOKEN`, `ZEABUR_SERVICE_ID`, `ZEABUR_ENVIRONMENT_ID` and optionally `ZEABUR_WORKSPACE`. If the GHCR package is private, give Zeabur registry credentials.

The image runs `airflow db migrate`, then scheduler, dag-processor, triggerer and api-server in one container on port `8080`. Copy env from `.env.zeabur.example`, and:

- Mount a volume at `/usr/local/airflow/logs` (with `AIRFLOW__LOGGING__BASE_LOG_FOLDER` pointing there), or task logs vanish on redeploy.
- Keep `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/`.
- Use the same `AIRFLOW__API_AUTH__JWT_SECRET` for the whole service.
- Set `AIRFLOW_ADMIN_PASSWORD` for a fixed login; otherwise the password is generated into `/usr/local/airflow/simple_auth_manager_passwords.json.generated`.
- Health check path: `/api/v2/version`. For deeper status, open `/api/v2/monitor/health`.

If Airflow becomes unreachable, copy the Zeabur runtime logs and check `/usr/local/airflow/logs` **before** redeploying, since a redeploy replaces the container.

## One-off: migrate legacy SQLite

```bash
JOBS_DB_URL=postgresql://jobs_app:jobs_pass@db.example.com:5432/jobsdb python scripts/migrate_sqlite_to_postgres.py --sqlite-path include/jobs.db
```
