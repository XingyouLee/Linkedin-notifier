# Engineering Review: LinkedIn Notifier — 2026-04-27

## 1. Architecture Assessment

### Is the current architecture sound?

The two-DAG split (`linkedin_notifier` for scan+JD, `linkedin_fitting_notifier` for fit+notify) is appropriate. The TriggerDagRunOperator bridge with test-mode conf forwarding is clean. The queue-based JD fetching with claim semantics is a reasonable pattern for single-container deployment.

**Weak points:**

- **No CI/CD pipeline**: `.github/workflows/ci.yml` does not exist. Zero automated checks on push or PR — no lint, no test run, no build. Every code change goes to deployment untested.

- **`database.py` is 2,629 lines**: Conflates ~7 responsibilities: schema DDL, profile config sync, job CRUD, JD queue management, fitting queue management, notification run management, alert state management, scan filter management. This is the single largest maintenance risk.

- **Dual schema ownership is brittle**: `init_db()` creates tables with `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. Django models use `managed = False`. Schema changes require careful coordination — a DDL change must be applied in `database.py`'s `init_db()`, the Django model updated to match, and a manual migration created. No migration history or rollback path.

- **Encapsulation violations**: `fitting_notifier.py` directly calls private `database._normalize_fit_prompt_text` (line 997).

- **Concern separation**: `fitting_notifier.py` (2,371 lines) couples fitting logic, cap enforcement, Discord message formatting, and Discord delivery into one file.

### How to break up database.py

1. `dags/schema.py` — `init_db()` and all schema DDL (advisory lock pattern)
2. `dags/profile_sync.py` — Profile config loading, parsing, syncing, bootstrap
3. `dags/jobs_repo.py` — Job CRUD operations
4. `dags/jd_queue.py` — JD queue operations
5. `dags/fitting_queue.py` — Fitting queue operations
6. `dags/notifications_repo.py` — Notification run management
7. `dags/scan_filters.py` — `seed_default_profile_scan_filters`, portal table management
8. `dags/database.py` — becomes a thin re-export facade for backward compatibility

Effort: **M** (2-3 days with careful testing)

---

## 2. Tech Debt Inventory

### Most urgent tech debt

1. **No CI pipeline** — No `.github/workflows/ci.yml`. No linting, no testing, no build check.
2. **2,629-line database.py** — As described above.
3. **Swallowed exceptions** — 5 instances of `except Exception: pass`:
   - `dags/runtime_utils.py:31-32,42-43` — `.env` file loading silently swallows all errors
   - `dags/runtime_utils.py:74-75` — `runtime_conf_value()` catches all exceptions
   - `dags/fitting_notifier.py:357-358` — `_log_job_match_result()` silently catches JSON parse failures
   - `dags/fitting_notifier.py:1283-1284` — `_build_discord_job_match_message()` silently catches JSON parse errors

4. **Hardcoded values**:
   - `docker-compose.yml:65` — `FITTING_CONCURRENCY: 8` — no rationale
   - `dags/fitting_notifier.py:1371` — `FITTING_MAX_ATTEMPTS` default "3"
   - `dags/materials_launch.py:12` — `DEFAULT_TOKEN_TTL_SECONDS = 7 days`

5. **Logging quality is poor**: `print()` everywhere instead of Python `logging`. No log levels, no structured format, no integration with Airflow's logging infrastructure.

6. **Type annotations are inconsistent**: Mix of `Optional` and `| None`. Several functions lack return type annotations.

### Error handling patterns

- LLM calling pipeline has a good layered retry pattern: `_request_llm_json_with_fallback` iterates endpoints, `_process_single_item` retries 3 times for transient API errors.
- Discord sending catches all via `except Exception` — reasonable for a notification pathway.
- Claim functions use `FOR UPDATE SKIP LOCKED` — correct concurrency pattern.
- **Gap**: No health check or alert for JD queue backlogs or fitting queue backlogs. The `alert_state` table exists but only used for LLM API errors.

### Test coverage gaps

**Strengths**: `test_fitting_notifier_policy.py` tests `_apply_fit_caps()` well; `test_database_queue_semantics.py` tests queue claiming; `test_migration_compat.py` tests schema idempotency.

**Critical gaps:**
- No tests for Discord message flow
- No tests for Django views or web portal
- No integration tests for the full scan→JD→fit→notify pipeline
- No tests for `materials_launch` HMAC token verification
- No tests for `process.py` scan logic with mock HTTP endpoints

---

## 3. Scalability Analysis

### 2 profiles to 20 profiles — what breaks first

1. **Scan HTTP rate limiting**: 20 profiles × 3 configs × 3 terms = 180 sequential term scans. LinkedIn likely to throttle past 50+ requests in a short window.

2. **Single-container CPU/memory**: `FITTING_CONCURRENCY=8` via `ThreadPoolExecutor`. 20 profiles × 5 jobs each = 100 concurrent LLM calls. Single-container Astro runtime may OOM.

3. **Database row growth**: ~3,600 new `profile_jobs` rows/day for 20 profiles. After 6 months: ~650K rows. Existing indexes are appropriate but JOIN queries will slow down without tuning.

### N+1 queries

- `save_jobs()` (database.py:1240-1265) does individual UPSERT per row — O(n) round trips.
- `enqueue_fitting_requests()` (database.py:1472-1477) does row-by-row UPDATE in a for-loop — O(n) round trips.
- `get_active_search_configs()` uses a single JOIN query — correct.

### Connection pooling

Every function calls `_connect()` creating a new `psycopg.connect()`. No connection pooling. At 8 concurrent fitting tasks, connection count could spike to 30-50. At current scale this is acceptable, but `psycopg_pool` should be introduced.

### Rate limiting

- Scan backoff logic (process.py:280-358) is well-constructed with exponential backoff, jitter, and Retry-After handling.
- **Missing**: No per-domain rate limiter (token bucket or sliding window).
- **Missing**: No randomness in request timing beyond configurable range.

---

## 4. Security Review

### Django settings (`linkedin-notifier-web/notifier_web/settings.py`)

- **`SECRET_KEY`** (line 21): Falls back to `"django-insecure-local-dev-only"`. No validation that production has a non-default value.
- **`DEBUG`** (line 22): Defaults to `"true"`. Leaks stack traces on error pages in production if not overridden.
- **`ALLOWED_HOSTS`** (line 23): Defaults to `"localhost,127.0.0.1,0.0.0.0"`. Must be restricted in production.
- **`CSRF_TRUSTED_ORIGINS`** (line 24): Defaults to empty string. No CSRF origin validation in production by default.
- **Session security**: No `SESSION_COOKIE_SECURE` or `CSRF_COOKIE_SECURE` visible (defaults to False).

### HMAC token generation (`dags/materials_launch.py`)

- Uses `hmac.new(secret, payload, hashlib.sha256)` with `compare_digest` — correct construction.
- Token format: `base64url(payload).base64url(signature)` — clean design.
- **Risk**: `MATERIALS_LINK_SECRET` env var. If leaked, tokens can be forged.

### SQL injection

- `_profile_mode_clause()` uses f-string interpolation but the interpolated value is a boolean constant (TRUE/FALSE), not user input. **Safe but fragile.**
- All other dynamic SQL uses parameterized queries (`%s` placeholders). **Safe.**

### XSS in Django

- Django auto-escaping protects template variables.
- **Risk**: `services.py:178-186` — `enrich_job()` returns raw LLM JSON output. If rendered with `|safe` filter in a template, this is an XSS vector.

### Test mode isolation

- `_profile_mode_clause()` correctly filters profiles based on `is_test_profile`. **Correct isolation.**
- `get_jobs_to_notify()` uses a different filter in test mode: skips qualification check allowing any fit decision to notify. Intentional but means test mode notifications include Weak Fit results.

---

## 5. Reliability and Resilience

### LinkedIn rate limiting / HTML changes

- Scan retry logic is solid with exponential backoff, jitter, and Retry-After support.
- **Risk**: HTML structure changes will silently return empty results. No alert when scan yield drops.
- **Risk**: `JD_API_URL` is hardcoded. If LinkedIn changes this endpoint, JD fetching silently fails.
- **Recommendation**: Add scan yield telemetry. If yield ratio drops below 50%, trigger alert.

### LLM endpoint failure handling

- `_request_llm_json_with_fallback()` iterates endpoints, distinguishing transient vs fatal errors. Well-designed.
- 3-attempt retry with exponential backoff is appropriate.
- **Missing**: No circuit breaker. If all endpoints fail, every task retries 3 times, generating 300+ API calls before giving up.

### Discord API failure handling

- `_send_discord_message()` returns `(bool, str | None)`. Failed sends are logged, jobs marked `notify_failed` but remain eligible for retry. **Resilient.**
- **Missing**: No alert when Discord API fails consistently.

### Database connection failures

- Every function reconnects via `_connect()`. No connection pool, no health check. Transient postgres restart crashes entire DAG run.
- `init_db()` uses `pg_advisory_xact_lock` to serialize schema bootstrapping — good.
- **Missing**: No retry on database connection failures.

### Partial pipeline failure recovery

- **Good**: Fitting queue uses `FOR UPDATE SKIP LOCKED` — atomic claim. Stale claims reclaimed after 30 min.
- **Good**: JD queue uses similar stale-claim pattern.
- **Good**: Notifications are idempotent via `ON CONFLICT`.
- **Gap**: If JD worker stops with pending jobs, it raises `RuntimeError`, killing the DAG run.

### Retry mechanisms summary

| Stage | Retry? | Mechanism |
|-------|--------|-----------|
| Scan | Yes | 6 retries, exponential backoff |
| JD | Yes | Per-job via `JD_MAX_ATTEMPTS`, queue reclaim for stale |
| Fitting | Yes | Per-job via `FITTING_MAX_ATTEMPTS` (default 3), endpoint fallback |
| Notification | No explicit | `notify_failed` status, eligible for future runs |
| DB connection | **None** | — |

---

## 6. CI/CD and DevOps

### CI pipeline — DOES NOT EXIST

No `.github/workflows/ci.yml`. This is the single most critical infrastructure gap.

### Docker

Minimal Dockerfile based on `astrocrpublic.azurecr.io/runtime:3.1-13`. No additional Python packages or system dependencies. Fine for current needs.

### Deployment rollback

No documented rollback strategy. A bad deployment requires force-pushing the previous commit. DB DDL in `init_db()` has no rollback path.

### Monitoring and alerting

Essentially none beyond `print()` statements:
- No structured logging
- No metrics on scan yield, JD success rate, fitting latency, notification delivery rate
- `alert_state` table only used for LLM API errors, not connected to external alerting
- No health check endpoint for Django portal
- No database connection pool monitoring

### Backup strategy

**None documented.** Business data (jobs, profile config, fit results, notification state) is in Postgres. If lost:
- Jobs/JD: rescannable
- Fit results: permanently lost (LLM calls must be re-made)
- Profile config: recoverable from JSON files

---

## 7. Recommendations

### Top 5 Engineering Improvements

**1. Establish CI pipeline — Effort: S, Risk: Low**
Create `.github/workflows/ci.yml` that runs linting (ruff/flake8), and `pytest` on push/PR. Include DAG import test.

**2. Add structured logging and basic monitoring — Effort: M, Risk: Low**
Replace `print()` with `logging.getLogger(__name__)` throughout `dags/`. Add log formatting. Connect `alert_state` table to Discord webhook for API errors, queue stagnation, and scan yield drops.

**3. Break up `database.py` (7 modules) — Effort: L, Risk: Medium**
Split into `schema.py`, `profile_sync.py`, `jobs_repo.py`, `jd_queue.py`, `fitting_queue.py`, `notifications_repo.py`, `scan_filters.py`. Keep `database.py` as backward-compatible re-export facade.

**4. Add database connection pooling and batch operations — Effort: M, Risk: Low**
Introduce `psycopg_pool.ConnectionPool`. Convert row-by-row patterns in `save_jobs()` and `enqueue_fitting_requests()` to `executemany()` or bulk UPDATE.

**5. Add test coverage for critical paths — Effort: M, Risk: Low**
Add tests for: Discord notification flow, Django web portal views, HMAC token verification, scan logic with mock HTTP, `enrich_job` with malformed LLM JSON, and one integration test for the full scan-JD-fit cycle.

### Recommended implementation order

1. CI pipeline + structured logging (quick wins, immediately reduce risk)
2. Connection pooling + batch operations (prepares for scale)
3. Test coverage (makes future refactoring safe)
4. Database.py split (largest ongoing cost, safer after tests are in place)
