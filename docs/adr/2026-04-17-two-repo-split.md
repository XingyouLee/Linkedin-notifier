# ADR 2026-04-17: split notifier and matcher with notifier-owned canonical resume backfill

## Status
Accepted

## Context
- The current monorepo contains two deployable surfaces with different runtime concerns.
- Resume Matcher launch must keep working after extraction into its own repo.
- The notifier repo already owns `profiles.json`, source resume files, and the shared Postgres schema used by launch flows.

## Decision
- Split `linkedin-notifier` and `resume-matcher` into separate repositories.
- Keep notifier as the owner of profile-source resumes and canonical resume hydration into `profiles.resume_text`.
- Treat `.md` / `.txt` as the only supported notifier-owned profile-source resume formats during this migration.
- Gate cutover on `sync_profiles_from_source(force=True)` so active profiles fail early when canonical `resume_text` is missing or the source extension is not text-based.

## Consequences
### Positive
- Resume Matcher can launch from shared DB state without notifier checkout paths or shared volumes.
- Operators get a deterministic cutover gate with profile-specific failure output.
- Notifier avoids importing matcher-only document parsing dependencies for this migration.

### Negative
- Non-text notifier-owned profile resumes must be converted before cutover.
- The forced-sync gate can block deployment until profile data is normalized.

## Rejected alternatives
- Let Resume Matcher keep reading notifier `include/user_info` paths after extraction — rejected because it preserves cross-repo filesystem coupling.
- Add PDF/DOC/DOCX parsing to notifier during this split — rejected because current live notifier-owned profiles already use markdown and the matcher parser stack should stay matcher-local for now.
