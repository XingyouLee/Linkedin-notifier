# Notifier ↔ Resume Matcher integration contract

## Purpose

This document freezes the contract between the **linkedin-notifier** repo and the extracted **resume-matcher** repo during the 2026-04-17 split. The goal is to keep launch/material generation working while removing any requirement for Resume Matcher to read notifier checkout paths directly.

## Ownership boundaries

### linkedin-notifier owns

- Airflow DAGs, scan/fitting orchestration, and Discord delivery
- `include/user_info/profiles.json` plus notifier-owned source resumes under `include/user_info/resume/`
- Postgres schema/migrations for `jobs`, `profile_jobs`, `profiles`, and related launch tables
- canonical profile backfills via `dags/database.py::sync_profiles_from_source(force=True)`
- launch-token creation via `dags/materials_launch.py`

### resume-matcher owns

- `/launch` and workspace/session initialization
- hidden workspace state, resume/cover-letter editing, and PDF export
- frontend/backend deployment, health checks, and browser/PDF runtime concerns
- `RESUME_MATCHER_SESSION_SECRET` provisioning for workspace cookies

### Shared surface (and nothing broader)

- `JOBS_DB_URL`
- `RESUME_MATCHER_BASE_URL`
- `MATERIALS_LINK_SECRET`
- `RESUME_MATCHER_SESSION_SECRET`
- the Postgres query contract described below

`RESUME_MATCHER_SESSION_SECRET` is matcher-owned. Resume Matcher may temporarily fall back to `MATERIALS_LINK_SECRET` during migration, but that fallback is transitional and should be removed after cutover verification.

## Launch flow contract

1. linkedin-notifier builds `/launch?token=...` links with `build_materials_launch_url(...)`.
2. The launch token payload contains only `profile_id`, `job_id`, and `exp`, signed with `MATERIALS_LINK_SECRET`.
3. Resume Matcher verifies that token with the same HMAC semantics before reading shared DB state.
4. Resume Matcher reads launch context from Postgres only; it must not require notifier checkout paths after cutover.
5. Resume Matcher creates its own workspace session cookie using `RESUME_MATCHER_SESSION_SECRET`.

Token expiry currently defaults to 7 days on both sides. Any future change must keep notifier creation and matcher verification in parity.

## Shared Postgres query contract

Resume Matcher currently initializes launch state from a single query shaped like:

- `profile_jobs.profile_id`
- `profile_jobs.job_id`
- `profiles.display_name`
- `profiles.resume_path`
- `profiles.resume_text`
- `jobs.title`
- `jobs.company`
- `jobs.job_url`
- `jobs.description`

Required behavior:

- the `(profile_id, job_id)` pair must resolve to exactly one live launch context
- `jobs.description` must be populated before launch succeeds
- `profiles.resume_text` is the canonical cross-repo resume content
- `profiles.resume_path` is migration-only metadata until path fallback is removed from Resume Matcher

## Canonical resume-content contract

linkedin-notifier is the source of truth for profile-source resumes. During this split:

- notifier-owned profile resumes are expected to be `.md` or `.txt`
- `sync_profiles_from_source(force=True)` refreshes `profiles.resume_text` from `profiles.json` and the source resume files
- if a profile source is not readable as text, `profiles.resume_text` will not be hydrated by notifier and cutover should be blocked until the source is converted
- direct uploads inside Resume Matcher remain matcher-local and do not change notifier ownership of profile-source resumes

## Deployment contract

### linkedin-notifier runtime requirements

- `JOBS_DB_URL`
- `RESUME_MATCHER_BASE_URL`
- `MATERIALS_LINK_SECRET`
- `PROFILE_CONFIG_PATH` when `profiles.json` is relocated

### resume-matcher runtime requirements

- `JOBS_DB_URL`
- `MATERIALS_LINK_SECRET`
- `RESUME_MATCHER_SESSION_SECRET`
- matcher-local frontend/PDF/browser env such as `FRONTEND_BASE_URL`

## Cutover checklist

Before deleting `apps/resume-matcher/` from this repo, verify all of the following:

1. `sync_profiles_from_source(force=True)` succeeds against the live notifier config.
2. Active launchable profiles have canonical `profiles.resume_text` content.
3. Resume Matcher launches work without access to `include/user_info/`.
4. `RESUME_MATCHER_SESSION_SECRET` is explicitly configured in the matcher deployment.
5. The matcher-side `MATERIALS_LINK_SECRET` fallback is tracked for later cleanup, not treated as permanent architecture.
