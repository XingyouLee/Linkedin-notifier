# Notifier ↔ Resume Matcher integration contract

## Ownership

- `linkedin-notifier` owns `profiles.json`, source resume files, Postgres schema/migrations, and launch-token issuance.
- `resume-matcher` consumes shared DB rows and shared secrets/env as a read-only integration surface.
- This migration explicitly removes any requirement for Resume Matcher to read `include/user_info` or notifier checkout paths.

## Shared runtime contract

- Database: `JOBS_DB_URL`
- Notifier → matcher base URL: `RESUME_MATCHER_BASE_URL`
- Launch token signing secret: `MATERIALS_LINK_SECRET`
- Matcher workspace-session secret: `RESUME_MATCHER_SESSION_SECRET`

## Launch data contract

Resume Matcher launch relies on the notifier-owned query shape:

- `profile_jobs.profile_id`
- `profile_jobs.job_id`
- `profiles.display_name`
- `profiles.resume_path`
- `profiles.resume_text`
- `jobs.title`
- `jobs.company`
- `jobs.job_url`
- `jobs.description`

`profiles.resume_text` is the canonical launch resume payload during this split. `profiles.resume_path` remains notifier-owned metadata only and must not be treated as a readable filesystem path by Resume Matcher.

## Cutover rule for notifier-owned profile resumes

- During this migration, notifier-owned profile-source resumes must be `.md` or `.txt`.
- `sync_profiles_from_source(force=True)` is the forced backfill gate before matcher cutover.
- The forced sync must fail if any active notifier-owned profile:
  - uses a non-text source extension, or
  - does not hydrate non-empty `profiles.resume_text`

This rule keeps canonical resume extraction inside the notifier repo and avoids importing matcher-only document parsing dependencies into the notifier runtime.

## Token / session semantics

- Notifier signs Discord launch tokens with `MATERIALS_LINK_SECRET`.
- Matcher verifies launch tokens with the same secret.
- Matcher workspace sessions must use `RESUME_MATCHER_SESSION_SECRET`; relying on `MATERIALS_LINK_SECRET` as a session fallback is migration-only behavior to remove.

## Operational validation

Before cutting matcher over to the extracted repo:

1. Run `sync_profiles_from_source(force=True)` from notifier.
2. Confirm no active profile fails the markdown/txt + canonical `resume_text` gate.
3. Confirm notifier-generated launch links still point at `RESUME_MATCHER_BASE_URL`.
4. Confirm matcher launch succeeds using shared DB state only.
