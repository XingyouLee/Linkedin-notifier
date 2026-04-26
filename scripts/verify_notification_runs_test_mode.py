#!/usr/bin/env python3
"""Deterministic test-mode verification for durable notification runs."""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure test-mode profile gating is active before importing database helpers.
os.environ["LINKEDIN_TEST_MODE"] = "true"

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import psycopg

from dags import database

PROFILE_WITH_JOB = "test-notification-run"
PROFILE_ZERO = "test-notification-run-zero"
JOB_ID = "test-999001"
ZERO_JOB_ID = "test-999002"


def _cleanup(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT id FROM profiles WHERE profile_key = ANY(%s)",
            ([PROFILE_WITH_JOB, PROFILE_ZERO],),
        )
        profile_ids = [row[0] for row in cursor.fetchall()]
        if profile_ids:
            cursor.execute("DELETE FROM notification_runs WHERE profile_id = ANY(%s)", (profile_ids,))
            cursor.execute("DELETE FROM profile_jobs WHERE profile_id = ANY(%s)", (profile_ids,))
            cursor.execute("DELETE FROM search_configs WHERE profile_id = ANY(%s)", (profile_ids,))
            cursor.execute("DELETE FROM profiles WHERE id = ANY(%s)", (profile_ids,))
        cursor.execute("DELETE FROM jd_queue WHERE job_id = ANY(%s)", ([JOB_ID, ZERO_JOB_ID],))
        cursor.execute("DELETE FROM jobs WHERE id = ANY(%s)", ([JOB_ID, ZERO_JOB_ID],))


def _seed(conn):
    with conn.cursor() as cursor:
        for key, name in [(PROFILE_WITH_JOB, "Test Notification Run"), (PROFILE_ZERO, "Test Zero Run")]:
            cursor.execute(
                """
                INSERT INTO profiles (profile_key, display_name, is_active, is_test_profile)
                VALUES (%s, %s, TRUE, TRUE)
                ON CONFLICT(profile_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    is_active = TRUE,
                    is_test_profile = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (key, name),
            )
            profile_id = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO search_configs (profile_id, name, location, results_per_term, is_active)
                VALUES (%s, 'verify', 'Netherlands', 1, TRUE)
                ON CONFLICT(profile_id, name) DO UPDATE SET is_active = TRUE
                RETURNING id
                """,
                (profile_id,),
            )
            search_config_id = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO search_terms (search_config_id, term)
                VALUES (%s, 'data engineer')
                ON CONFLICT(search_config_id, term) DO NOTHING
                """,
                (search_config_id,),
            )
            if key == PROFILE_WITH_JOB:
                with_job_profile_id = profile_id
                with_job_config_id = search_config_id
            else:
                zero_profile_id = profile_id

        cursor.execute("INSERT INTO batches DEFAULT VALUES RETURNING id")
        batch_id = cursor.fetchone()[0]
        cursor.execute(
            """
            INSERT INTO jobs (id, site, job_url, title, company, source_job_id, batch_id, description)
            VALUES (%s, 'linkedin', 'https://example.com/test-999001', 'Junior Data Engineer', 'VerifyCo', '999001', %s, NULL)
            ON CONFLICT(id) DO UPDATE SET
                batch_id = EXCLUDED.batch_id,
                description = NULL
            """,
            (JOB_ID, batch_id),
        )
        cursor.execute(
            """
            INSERT INTO profile_jobs (
                profile_id, job_id, search_config_id, matched_term, fit_score, fit_decision,
                llm_match, llm_match_error, fit_status
            )
            VALUES (%s, %s, %s, 'data engineer', 82, 'Strong Fit',
                    '{"fit_score":82,"decision":"Strong Fit"}', NULL, 'fit_done')
            ON CONFLICT(profile_id, job_id) DO UPDATE SET
                fit_score = EXCLUDED.fit_score,
                fit_decision = EXCLUDED.fit_decision,
                llm_match = EXCLUDED.llm_match,
                llm_match_error = NULL,
                fit_status = 'fit_done',
                notified_at = NULL,
                notify_status = NULL,
                notify_error = NULL
            """,
            (with_job_profile_id, JOB_ID, with_job_config_id),
        )
        cursor.execute(
            """
            INSERT INTO jd_queue (job_id, job_url, status, updated_at)
            VALUES (%s, 'https://example.com/test-999001', 'pending', CURRENT_TIMESTAMP)
            ON CONFLICT(job_id) DO UPDATE SET status='pending', updated_at=CURRENT_TIMESTAMP
            """,
            (JOB_ID,),
        )
    return with_job_profile_id, zero_profile_id


def _assert(condition, message):
    if not condition:
        raise AssertionError(message)


def main() -> int:
    if not database.get_db_url():
        print("JOBS_DB_URL is required", file=sys.stderr)
        return 2
    database.init_db()
    with psycopg.connect(database.get_db_url()) as conn:
        _cleanup(conn)
        with_job_profile_id, zero_profile_id = _seed(conn)

    # Postgres must accept get_jobs_needing_jd after SELECT DISTINCT fix.
    jd_df = database.get_jobs_needing_jd(max_attempts=3)
    _assert(JOB_ID in set(jd_df.get("id", [])), "seeded JD candidate not returned")

    run_id = database.create_notification_run(
        with_job_profile_id, dag_id="verify", dag_run_id="verify-notification-runs"
    )
    database.add_notification_run_jobs(
        run_id,
        [
            {
                "profile_id": with_job_profile_id,
                "id": JOB_ID,
                "fit_score": 82,
                "fit_decision": "Strong Fit",
            }
        ],
    )
    _assert(database.claim_job_for_notification(with_job_profile_id, JOB_ID, run_id), "first claim failed")
    _assert(
        not database.claim_job_for_notification(with_job_profile_id, JOB_ID, run_id),
        "duplicate claim unexpectedly succeeded",
    )
    database.mark_job_notified(with_job_profile_id, JOB_ID, status="sent")
    database.mark_notification_run_job(run_id, with_job_profile_id, JOB_ID, status="sent")
    finalized = database.finalize_notification_run(run_id, summary_status="skipped")
    _assert(finalized["eligible_count"] == 1, "nonzero run eligible count mismatch")
    _assert(finalized["sent_count"] == 1, "nonzero run sent count mismatch")

    zero_run_id = database.create_notification_run(
        zero_profile_id, dag_id="verify", dag_run_id="verify-notification-runs"
    )
    zero_finalized = database.finalize_notification_run(zero_run_id, summary_status="skipped")
    _assert(zero_finalized["eligible_count"] == 0, "zero run should have no jobs")
    _assert(zero_finalized["status"] == "completed_zero_results", "zero run status mismatch")

    # Verify Django read path against the same DB if the nested web repo is present.
    web_path = REPO_ROOT / "linkedin-notifier-web"
    if web_path.exists():
        sys.path.insert(0, str(web_path))
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "notifier_web.settings")
        import django

        django.setup()
        from portal.models import NotifierProfile
        from portal.services import grouped_qualified_runs, qualified_jobs_for_run

        profile = NotifierProfile.objects.get(pk=with_job_profile_id)
        runs = grouped_qualified_runs(profile, limit=10)
        _assert(any(run["id"] == str(run_id) for run in runs), "Django run list missing run")
        jobs = qualified_jobs_for_run(profile, str(run_id))
        _assert(len(jobs) == 1 and jobs[0].job_id == JOB_ID, "Django run jobs mismatch")

    if os.getenv("KEEP_TEST_NOTIFICATION_RUN_ROWS") != "1":
        with psycopg.connect(database.get_db_url()) as conn:
            _cleanup(conn)

    print("notification run test-mode verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
