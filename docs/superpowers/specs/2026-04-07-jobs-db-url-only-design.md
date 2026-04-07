# Jobs DB URL Only Design

## Summary

The business database connection must be determined only by `JOBS_DB_URL`.
If `JOBS_DB_URL` points at the cloud database, both local and cloud runtimes use that cloud database.
If `JOBS_DB_URL` points at a local or fallback database, both local and cloud runtimes use that target instead.

The repository must stop inferring or constructing a business database DSN from local defaults such as `127.0.0.1`, Docker hostnames, or split `JOBS_DB_*` variables.

## Goals

- Make `JOBS_DB_URL` the single source of truth for the business database connection.
- Ensure removing a local business Postgres container does not affect the runtime when `JOBS_DB_URL` points elsewhere.
- Keep local and cloud behavior identical for business database resolution.
- Fail fast with a clear error when `JOBS_DB_URL` is missing.

## Non-Goals

- Changing the Airflow metadata database setup.
- Removing `postgres-airflow` from local Docker Compose.
- Changing the Swift macOS app's direct database access model.
- Adding automatic failover or fallback database discovery.

## Current Problem

`dags/database.py` currently accepts `JOBS_DB_URL`, but when it is missing it builds a fallback PostgreSQL DSN from local defaults and split environment variables.
That behavior makes the business database ambiguous:

- A local run can silently connect to `127.0.0.1:5432/jobsdb`.
- A containerized run can silently connect to `postgres:5432/jobsdb`.
- The effective database can differ between local and cloud even when the operator intended one shared business database.

This is the opposite of the desired operating model, where the business database target is always explicit and controlled by one variable.

## Proposed Design

### Connection Resolution

Update `dags/database.py` so `get_db_url()` and its internal resolver do the following:

1. Read `JOBS_DB_URL`.
2. Return it unchanged when it is set to a non-empty value.
3. Raise a `ValueError` with an explicit message when it is missing or empty.

The resolver will no longer:

- Inspect `CLOUD_DEPLOYMENT` to decide whether missing configuration is allowed.
- Choose `postgres` or `127.0.0.1` based on Docker detection.
- Build a DSN from `JOBS_DB_HOST`, `JOBS_DB_PORT`, `JOBS_DB_USER`, `JOBS_DB_PASSWORD`, or `JOBS_DB_NAME`.

### Local Docker Behavior

Keep `docker-compose.yml` focused on local Airflow infrastructure.
`postgres-airflow` remains the Airflow metadata database service.
The business database remains external to Compose and is selected only through `JOBS_DB_URL`.

That means:

- Deleting or stopping any local business Postgres setup does not affect the DAG runtime as long as `JOBS_DB_URL` still points to a reachable database.
- Using a local emergency business database still works, but only after explicitly changing `JOBS_DB_URL`.

### Documentation and Operator Guidance

Update operator-facing docs to describe this rule plainly:

- `JOBS_DB_URL` always chooses the business database.
- Local and cloud deployments should normally use the same shared business database.
- A local or fallback business database is an explicit override for emergency use, not an automatic default.
- `postgres-airflow` is only for Airflow metadata and should not be confused with the business database.

## Affected Areas

- `dags/database.py`
- `tests/test_cloud_runtime_config.py`
- `README.md`
- `CLAUDE.md`
- Any scripts or docs that currently imply a default local business Postgres target

## Testing Strategy

Add or update tests to cover these cases:

- `get_db_url()` returns `JOBS_DB_URL` unchanged when set.
- `get_db_url()` raises a clear error when `JOBS_DB_URL` is absent.
- The error is the same regardless of `CLOUD_DEPLOYMENT` or Docker-related conditions.
- Existing env-loading behavior still preserves an already injected `JOBS_DB_URL`.

Documentation updates do not require new runtime tests beyond the configuration behavior above.

## Risks and Mitigations

- Risk: an older local workflow may have relied on implicit localhost defaults.
  Mitigation: the new error message must clearly instruct the operator to set `JOBS_DB_URL`.
- Risk: docs may still imply that a local jobs database container exists by default.
  Mitigation: update all operator-facing setup text in the same change.

## Implementation Outline

1. Remove fallback DSN construction from `dags/database.py`.
2. Simplify tests to require explicit `JOBS_DB_URL`.
3. Update docs and script help text to match the new explicit configuration model.
