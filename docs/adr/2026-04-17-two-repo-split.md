# ADR 2026-04-17: split linkedin-notifier and resume-matcher into separate repositories

- Status: Accepted
- Date: 2026-04-17

## Context

The current monorepo contains two separately deployable products:

- root Airflow infrastructure for LinkedIn scanning/fitting/notification
- `apps/resume-matcher/` for launch workflows, editing, and PDF export

That arrangement hides an important boundary problem: Resume Matcher still contains monorepo-era fallback logic for notifier-owned repo paths and shared env discovery, while operators already deploy the two surfaces with different runtime concerns.

## Decision

Adopt a contract-first split:

1. linkedin-notifier remains the owner of source resumes, `profiles.json`, shared DB schema/migrations, and launch-link generation
2. resume-matcher becomes its own repo and consumes launch/profile/job context via shared Postgres plus explicit env/secrets
3. `profiles.resume_text` becomes the canonical cross-repo resume content for notifier-owned profiles
4. matcher workspace sessions use `RESUME_MATCHER_SESSION_SECRET`; fallback to `MATERIALS_LINK_SECRET` is migration-only
5. path-based resume resolution inside Resume Matcher is transitional and must be removed once DB-backed canonical content is verified

## Consequences

### Positive

- Each deployable app gets a single obvious repo root, Dockerfile, and README
- notifier remains the only owner of notifier-specific data ingestion and schema decisions
- matcher can evolve its frontend/PDF runtime without carrying Airflow deployment assumptions

### Negative / tradeoffs

- The split requires temporary compatibility logic until cutover evidence is collected
- Launch behavior now depends on a carefully documented DB contract rather than implicit monorepo proximity
- Operators must provision one additional explicit secret (`RESUME_MATCHER_SESSION_SECRET`) instead of relying on shared fallback behavior

## Rejected alternative

### Extract first, harden later

Rejected because it would turn local monorepo coupling into a live cross-repo failure mode before token, DB, and resume-content contracts are frozen.

## Follow-up requirements

- keep launch-token parity tests on both sides of the split
- verify `sync_profiles_from_source(force=True)` before path fallback removal
- delete matcher code that reads notifier checkout paths after cutover evidence is complete
