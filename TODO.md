# TODO

## JD Scraping Pipeline Improvements

- Add fallback mode for JD scraping:
  - Primary path: Playwright worker (`dags/openclaw_jd_worker.py`)
  - Fallback path: OpenClaw browser-tool based rescue for failed jobs
- Proposed flow:
  1. Worker marks failed rows with `status='failed'` and clear error reason
  2. Add `needs_manual`/`fallback_needed` flag for queue rows
  3. Optional rescue job processes only fallback-marked rows
  4. Write rescued descriptions back to `jobs.description`
- Keep current Playwright queue flow as the default production path.
