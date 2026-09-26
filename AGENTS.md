# Repository Guidelines

## Project Structure & Module Organization
This repository combines an Astro/Airflow pipeline with a local macOS SwiftUI app. Core DAGs live in `dags/`: `process.py` handles job discovery and queueing, and `fitting_notifier.py` scores jobs and sends notifications. Shared database logic lives in `dags/database.py`; the root `database.py` is only a compatibility shim. Utility scripts live in `scripts/`, test coverage lives in `tests/`, and runtime data/config samples live under `include/`. The macOS client is in `apps/macos/LinkedinNotifierApp/` with sources in `Sources/` and SwiftPM tests in `Tests/`.

## Build, Test, and Development Commands
Use `astro dev start` to boot the local Airflow stack. Trigger the main DAGs with `astro dev run dags trigger linkedin_notifier` and `astro dev run dags trigger linkedin_fitting_notifier`. Run Python tests with `pytest`, or target one file such as `pytest tests/test_fitting_notifier.py`. Validate DAG imports with `pytest .astro/test_dag_integrity_default.py`. For the macOS app, run `swift test --package-path apps/macos/LinkedinNotifierApp` and package a local app bundle with `./apps/macos/LinkedinNotifierApp/scripts/package_app.sh`.

## Coding Style & Naming Conventions
Follow existing patterns instead of introducing new structure. Python uses 4-space indentation, snake_case for functions/modules, and focused helper functions near the DAG that owns them. Keep imports grouped as standard library, third-party, then local modules. Swift code uses PascalCase for types, camelCase for properties and methods, and small view/view-model files under `Sources/LinkedinNotifierApp/`. No formatter config is checked in, so keep changes consistent with surrounding code.

## Testing Guidelines
Add or update tests whenever DAG behavior, queue semantics, schema sync, or notification policy changes. Python tests belong in `tests/test_*.py`; prefer descriptive names such as `test_apply_fit_caps_downgrades_large_experience_gap`. Swift app tests belong in `apps/macos/LinkedinNotifierApp/Tests/LinkedinNotifierAppTests/`. Keep tests narrow and deterministic; stub external services instead of calling live LinkedIn, Airflow, or LLM endpoints.

## Commit & Pull Request Guidelines
Recent history favors short imperative commit subjects: `Fix ...`, `Add ...`, `Document ...`, `Collapse ...`. Keep commits scoped to one concern. Pull requests should state which DAGs, scripts, or app screens changed, list any new environment variables or migration steps, and include screenshots for SwiftUI UI changes.

## Security & Configuration Tips
Never commit real secrets from `.env`, `dags/.env`, or profile data in `include/user_info/`. Treat `JOBS_DB_URL`, API keys, Discord tokens, and resume/profile files as sensitive. When changing runtime config, document required variables in `README.md` and keep local-only values out of version control.

## Project-Specific Agent Operating Notes

### JD Model Comparisons
For fitting model comparisons, follow `docs/LLM_MODEL_COMPARISON.md`: read stored JDs directly from the remote database using SELECT-only connections, run the user-requested models with the existing production prompt, save scores and reasons, and provide the requested comparison. Use local Python only. Do not write or run tests, use Docker (including one-off containers), start Astro/Airflow, trigger DAGs, scrape LinkedIn, write remote database records, or send notifications. This workflow is separate from Real Airflow Test Mode below. Address missing Python dependencies in a local virtual environment. Deliver actual fitting results; script preparation alone is not a completed comparison.

### Real Airflow Test Mode
When the user says `test`, `test mode`, or asks to "run the real test" in this repository, do **not** interpret that as only `pytest` or mocked unit tests. Here, test mode means running the actual Airflow pipeline with `LINKEDIN_TEST_MODE=true` against the configured jobs database and the `LinkedIn Test Mode` profile.

Expected behavior for real test mode:
- Use the real Airflow DAG path, preferably through `scripts/run_airflow_test_mode_smoke.sh`.
- Pass test-mode settings through DAG run `--conf` so scheduler/task processes see them, not just the shell that triggers the DAG.
- Keep the test profile synthetic, but use the same Discord delivery destination as Xingyou Li so the user can verify messages in Discord.
- Test-mode notification should send at most `LINKEDIN_TEST_MAX_NOTIFY_JOBS` jobs, default 3.
- Test-mode notification should send successful LLM payloads regardless of Strong/Moderate/Weak/Not Recommended decision, and messages must be clearly marked as TEST.
- A real test-mode pass should include evidence from the Airflow DAG run and, when Discord delivery is in scope, evidence that Discord delivery was attempted/sent. Unit tests alone are not enough.

### Model / Tool Division of Labor
For future planning and implementation work in this repository:
- GPT/Codex owns planning, product/architecture judgment, implementation, integration decisions, and final synthesis.
- Codex remains responsible for verification and deciding whether the result is safe to ship.
