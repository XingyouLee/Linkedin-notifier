# Zeabur Deployment (Airflow 3)

This repository now includes a standard Docker build and a Zeabur-friendly startup script for running the existing Airflow DAGs without Docker Compose or Astronomer-managed infrastructure.

## Recommended Layout

Use one Zeabur service built from the repo root `Dockerfile`.

The container entrypoint is [`scripts/start_airflow_service.sh`](/Users/levi/Linkedin-notifier/scripts/start_airflow_service.sh). With the default `AIRFLOW_SERVICE_ROLE=all-in-one`, it starts:

- `airflow db migrate`
- `airflow scheduler`
- `airflow dag-processor`
- `airflow api-server`

This is the recommended Zeabur shape because Zeabur expects long-running services to expose an HTTP port. Airflow's standalone `scheduler` and `dag-processor` processes do not expose one, so splitting them into separate Zeabur services is awkward and not the default path here.

## Databases

You need two Postgres connection strings:

1. Airflow metadata database
   - Env: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
   - Purpose: stores DAG run metadata, task state, scheduler state, auth metadata
2. Business database
   - Env: `JOBS_DB_URL`
   - Purpose: stores `jobs`, `profile_jobs`, `jd_queue`, `profiles`, and related application tables

These can live on the same Zeabur Postgres service if you use separate database names.

## Required Environment Variables

Set these on the Zeabur service:

- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- `JOBS_DB_URL`
- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`
  Format: `username:role`, for example `levi:admin` or `levi:admin,readonly:viewer`

Also set the application secrets you already use locally:

- `GMN_API_KEY` or `LLM_API_KEY` plus `FITTING_REQUEST_URL`
- `DISCORD_BOT_TOKEN` and/or `DISCORD_WEBHOOK_URL`
- `PROFILE_CONFIG_PATH` if you are not using the repo-default profile file

Recommended Airflow-related values:

- `AIRFLOW_SERVICE_ROLE=all-in-one`
- `AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`
- `AIRFLOW__CORE__LOAD_EXAMPLES=False`

Optional but recommended:

- `AIRFLOW__CORE__FERNET_KEY`
  Use a stable value if you plan to store encrypted Airflow connections or variables.

## Profiles and Resumes

By default the image contains:

- [`include/user_info/profiles.json`](/Users/levi/Linkedin-notifier/include/user_info/profiles.json)
- [`include/user_info/resume/xingyouli.md`](/Users/levi/Linkedin-notifier/include/user_info/resume/xingyouli.md)
- [`include/user_info/resume/georgegu.md`](/Users/levi/Linkedin-notifier/include/user_info/resume/georgegu.md)

That means no extra Zeabur volume is required if you are happy baking profile config and resumes into the image.

If you want to manage them outside git:

- upload them through Zeabur Config Editor or a mounted volume
- set `PROFILE_CONFIG_PATH` to the external `profiles.json`
- keep each profile's `resume_path` valid relative to that file

## Health and Runtime Behavior

- The API server listens on Zeabur's injected `PORT`.
- The startup script exits the container if the scheduler, dag processor, or API server exits unexpectedly.
- For Zeabur health checks, use the API service itself. A simple path such as `/api/v2/monitor/health` is suitable for checking that the HTTP surface is up.

## Deploy Steps

1. Create a Zeabur Postgres database for Airflow metadata.
2. Create or reuse a Zeabur Postgres database for `JOBS_DB_URL`.
3. Create one Zeabur Git service from this repository root.
4. Let Zeabur build the root `Dockerfile`.
5. Set the required environment variables.
6. Deploy.
7. Open the service URL and log in with the credentials from `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`.
   Airflow auto-generates the password for each configured user on first startup and prints it in the container logs.

## Notes

- The current DAG schedule remains unchanged. [`linkedin_notifier`](/Users/levi/Linkedin-notifier/dags/process.py#L482) still runs every 12 hours and triggers [`linkedin_fitting_notifier`](/Users/levi/Linkedin-notifier/dags/process.py#L739).
- The macOS Swift app under [`apps/macos`](/Users/levi/Linkedin-notifier/apps/macos) is still a local operator tool. It is not part of the Zeabur deployment.
