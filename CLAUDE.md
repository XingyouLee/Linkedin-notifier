# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Local Airflow / Astro
- Start local Astro/Airflow: `astro dev start`
- Trigger the scan pipeline DAG: `astro dev run dags trigger linkedin_notifier`
- Trigger the fitting/notification DAG: `astro dev run dags trigger linkedin_fitting_notifier`

### Tests
- Run all pytest tests: `pytest`
- Run one test file: `pytest tests/test_fitting_notifier.py`
- Run one test case: `pytest tests/test_fitting_notifier.py::test_apply_fit_caps_blocks_senior_title_even_without_exp_years`
- Run Astro DAG import/parsing validation: `pytest .astro/test_dag_integrity_default.py`

### One-off scripts
- Migrate legacy SQLite data into Postgres: `JOBS_DB_URL=postgresql://jobs_app:jobs_pass@db.example.com:5432/jobsdb python scripts/migrate_sqlite_to_postgres.py --sqlite-path include/jobs.db`

## Architecture

This repo is an Astro/Airflow project for a LinkedIn job ingestion and scoring pipeline backed by Postgres.

### End-to-end flow
There are two main DAGs:

1. `linkedin_notifier` in `dags/process.py`
   - Loads/syncs profile config from JSON into Postgres.
   - Scans LinkedIn public jobs search results for each active profile search config.
   - Normalizes and upserts canonical jobs into the shared `jobs` table.
   - Creates per-profile job associations.
   - Enqueues missing job descriptions in `jd_queue`.
   - Runs the JD fetch worker inline and then enqueues fitting work.
   - Triggers the fitting DAG.

2. `linkedin_fitting_notifier` in `dags/fitting_notifier.py`
   - Claims pending or stale per-profile fitting work.
   - Builds a prompt from the profile resume, profile candidate summary, job title, and JD text.
   - Calls the LLM, applies conservative post-processing caps, and persists profile-specific fit results.
   - Finalizes queue state.
   - Sends Discord notifications only for jobs that pass the notification filter.

### Core modules
- `dags/database.py` is the real data layer and schema owner. It:
  - Resolves the Postgres connection.
  - Initializes/migrates schema.
  - Syncs profile/search config JSON into relational tables.
  - Owns queue-claiming and result-persistence logic.
- `database.py` at repo root is only a compatibility shim that re-exports `dags.database`; prefer importing `dags.database` directly.
- `dags/jd_api_worker.py` fetches LinkedIn `jobPosting` HTML, extracts the description text, and stores JD results.
- `dags/runtime_utils.py` loads env vars from both repo-root and `dags/` `.env` files and converts pandas values into XCom-safe records.

### Data model
The runtime is multi-profile, but job discovery data is partly shared:
- Shared canonical data: `jobs`, `jd_queue`, `batches`
- Multi-profile state: `profiles`, `search_configs`, `search_terms`, `profile_jobs`
- `fitting_queue` exists as a legacy table and is currently not the active DAG flow according to the README

The important split is:
- `jobs` stores the canonical job record once
- `profile_jobs` stores per-profile fitting/notification state and outcomes

### Multi-profile config
- Runtime profile config lives in `include/user_info/profiles.json` by default.
- Resume paths in that JSON are resolved relative to the config file.
- DAG runs auto-sync config JSON into Postgres; there is no separate sync command.
- Legacy single-user bootstrap still exists through env vars like `DEFAULT_PROFILE_*` and `RESUME_PATH`, but the current design centers on profile-driven config.

### Fitting logic
The fitting pipeline is intentionally conservative:
- The default fit prompt is defined in `dags/database.py`.
- `dags/fitting_notifier.py` adds post-LLM policy checks for experience gaps, seniority mismatch, and language blockers.
- Notifications are filtered from persisted results rather than raw model output.

When changing fit behavior, inspect both prompt construction and `_apply_fit_caps()` / notification filtering so changes stay consistent.

### Scanning and JD fetching
- Job scanning in `dags/process.py` hits LinkedIn guest search endpoints and parses returned HTML snippets.
- JD fetching in `dags/jd_api_worker.py` is queue-based and reclaim-aware; stale in-progress jobs can be reclaimed via DB claim logic.
- Queue semantics for both JD and fitting work are tested in `tests/test_database_queue_semantics.py`.

## Testing notes
- Unit tests stub Airflow, Playwright, and psycopg imports in `tests/conftest.py`, so many tests can run without a live Airflow runtime or database.
- `.astro/test_dag_integrity_default.py` is the DAG import safety check used to catch parse-time errors.
- If you change schema/profile sync behavior, also inspect `tests/test_migration_compat.py` and queue semantics tests.

## Environment
- Runtime env files are expected at `.env` and/or `dags/.env`.
- `JOBS_DB_URL` is required for the business database connection. Legacy split `JOBS_DB_HOST`, `JOBS_DB_PORT`, `JOBS_DB_USER`, `JOBS_DB_PASSWORD`, and `JOBS_DB_NAME` fallbacks should not be relied on.
- Important runtime vars called out by the repo README include `JOBS_DB_URL`, `PROFILE_CONFIG_PATH`, scan tuning vars, JD/fitting stale-claim vars, `GMN_API_KEY`, Discord credentials, and `AIRFLOW_ADMIN_PASSWORD` for fixed SimpleAuthManager login bootstrapping.
- The Docker image installs Playwright Chromium during build (`Dockerfile`).
- Zeabur deployment is a single Docker app service plus two Postgres databases: one for Airflow metadata (`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`) and one for business data (`JOBS_DB_URL`).
- In the single-container cloud deployment, keep `AIRFLOW__API__BASE_URL=http://127.0.0.1:8080` and `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/`; do not point them at old multi-service hostnames.

### Code Review 工作流

如果要求code review，使用 codex 和 git diff 进行 code review，分析 codex 的反馈后，自行修改代码，然后将新的 diff 使用 codex-reply 和相同的 threadId 发给 codex，继续下一轮 review。如此循环，最多 3 轮，直到没有关键问题。
