#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row

from dags import database
from dags.runtime_utils import load_env


def _connect():
    load_env()
    database.sync_profiles_from_source()
    return psycopg.connect(database.get_db_url(), row_factory=dict_row)


def _emit(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, default=str))


def _summary() -> dict:
    query = """
        WITH latest_batch AS (
            SELECT MAX(id) AS batch_id FROM batches
        ),
        profile_jobs_with_jd AS (
            SELECT pj.*, j.description, j.id AS job_id_ref
            FROM profile_jobs pj
            JOIN jobs j ON j.id = pj.job_id
            WHERE j.description IS NOT NULL
        ),
        latest_jobs AS (
            SELECT j.id
            FROM jobs j
            JOIN latest_batch lb ON lb.batch_id = j.batch_id
        ),
        latest_jd AS (
            SELECT q.status, COUNT(*) AS cnt
            FROM jd_queue q
            JOIN latest_jobs lj ON lj.id = q.job_id
            GROUP BY q.status
        ),
        latest_fit AS (
            SELECT pj.fit_status, COUNT(*) AS cnt
            FROM profile_jobs pj
            JOIN latest_jobs lj ON lj.id = pj.job_id
            GROUP BY pj.fit_status
        )
        SELECT json_build_object(
            'total_jobs', (SELECT COUNT(*) FROM jobs),
            'total_profile_jobs', (SELECT COUNT(*) FROM profile_jobs),
            'jd_pending', (SELECT COUNT(*) FROM jd_queue WHERE status = 'pending'),
            'jd_processing', (SELECT COUNT(*) FROM jd_queue WHERE status = 'processing'),
            'jd_failed', (SELECT COUNT(*) FROM jd_queue WHERE status = 'failed'),
            'jd_done', (SELECT COUNT(*) FROM jd_queue WHERE status = 'done'),
            'jd_global_total', (
                SELECT COUNT(*)
                FROM jd_queue
                WHERE status IN ('pending', 'processing', 'failed')
            ),
            'fit_pending', (SELECT COUNT(*) FROM profile_jobs_with_jd WHERE fit_status = 'pending_fit'),
            'fit_processing', (SELECT COUNT(*) FROM profile_jobs_with_jd WHERE fit_status = 'fitting'),
            'fit_failed', (SELECT COUNT(*) FROM profile_jobs_with_jd WHERE fit_status = 'fit_failed'),
            'fit_done', (SELECT COUNT(*) FROM profile_jobs_with_jd WHERE fit_status = 'fit_done'),
            'fit_terminal_done', (SELECT COUNT(*) FROM profile_jobs_with_jd WHERE fit_status IN ('fit_done', 'notified')),
            'fit_global_total', (
                SELECT COUNT(*)
                FROM profile_jobs_with_jd
                WHERE fit_status IN ('pending_fit', 'fitting', 'fit_failed')
            ),
            'notify_failed', (SELECT COUNT(*) FROM profile_jobs_with_jd WHERE notify_status = 'failed'),
            'latest_batch_id', (SELECT batch_id FROM latest_batch),
            'latest_batch_jobs_total', (SELECT COUNT(*) FROM latest_jobs),
            'latest_batch_jd_done', COALESCE((SELECT cnt FROM latest_jd WHERE status = 'done'), 0),
            'latest_batch_jd_processing', COALESCE((SELECT cnt FROM latest_jd WHERE status = 'processing'), 0),
            'latest_batch_jd_pending', COALESCE((SELECT cnt FROM latest_jd WHERE status = 'pending'), 0),
            'latest_batch_jd_failed', COALESCE((SELECT cnt FROM latest_jd WHERE status = 'failed'), 0),
            'latest_batch_fit_total', (
                SELECT COUNT(*)
                FROM profile_jobs pj
                JOIN latest_jobs lj ON lj.id = pj.job_id
            ),
            'latest_batch_fit_done', COALESCE((SELECT SUM(cnt) FROM latest_fit WHERE fit_status IN ('fit_done', 'notified')), 0),
            'latest_batch_fit_processing', COALESCE((SELECT cnt FROM latest_fit WHERE fit_status = 'fitting'), 0),
            'latest_batch_fit_pending', COALESCE((SELECT cnt FROM latest_fit WHERE fit_status = 'pending_fit'), 0),
            'latest_batch_fit_failed', COALESCE((SELECT SUM(cnt) FROM latest_fit WHERE fit_status IN ('fit_failed', 'notify_failed')), 0),
            'latest_batch_scan_progress', 100
        ) AS payload
    """
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
    return row["payload"] if row else {}


def _list_jobs(query_text: str | None, limit: int) -> list[dict]:
    normalized_query = (query_text or "").strip() or None
    pattern = f"%{normalized_query}%" if normalized_query else None
    base_query = """
        WITH filtered_jobs AS (
            SELECT DISTINCT j.id, j.batch_id
            FROM jobs j
            LEFT JOIN profile_jobs pj ON pj.job_id = j.id
            LEFT JOIN profiles p ON p.id = pj.profile_id
            {where_clause}
            ORDER BY j.batch_id DESC NULLS LAST, j.id DESC
            LIMIT %(limit)s
        )
        SELECT
            j.id AS job_id,
            j.title,
            j.company,
            j.job_url,
            j.batch_id,
            (j.description IS NOT NULL) AS has_description,
            q.status AS jd_status,
            COALESCE(q.attempts, 0) AS jd_attempts,
            COUNT(DISTINCT pj.profile_id) AS profile_match_count,
            MAX(pj.fit_score) AS best_fit_score,
            COALESCE(
                json_agg(
                    json_build_object(
                        'profile_id', p.id,
                        'profile_key', p.profile_key,
                        'display_name', p.display_name,
                        'matched_term', pj.matched_term,
                        'fit_status', pj.fit_status,
                        'fit_score', pj.fit_score,
                        'fit_decision', pj.fit_decision,
                        'notify_status', pj.notify_status
                    )
                    ORDER BY p.id
                ) FILTER (WHERE p.id IS NOT NULL),
                '[]'::json
            ) AS profile_matches
        FROM filtered_jobs fj
        JOIN jobs j ON j.id = fj.id
        LEFT JOIN jd_queue q ON q.job_id = j.id
        LEFT JOIN profile_jobs pj ON pj.job_id = j.id
        LEFT JOIN profiles p ON p.id = pj.profile_id
        GROUP BY
            j.id,
            j.title,
            j.company,
            j.job_url,
            j.batch_id,
            j.description,
            q.status,
            q.attempts
        ORDER BY COALESCE(j.batch_id, 0) DESC, j.id DESC
    """
    where_clause = ""
    params = {"limit": max(1, limit)}
    if normalized_query:
        where_clause = """
            WHERE (
                j.id = %(query)s
                OR COALESCE(j.title, '') ILIKE %(pattern)s
                OR COALESCE(j.company, '') ILIKE %(pattern)s
                OR COALESCE(p.profile_key, '') ILIKE %(pattern)s
                OR COALESCE(p.display_name, '') ILIKE %(pattern)s
                OR COALESCE(pj.matched_term, '') ILIKE %(pattern)s
            )
        """
        params.update({"query": normalized_query, "pattern": pattern})
    query = base_query.format(where_clause=where_clause)
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return list(rows)


def _job_detail(job_id: str) -> dict:
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    j.id AS job_id,
                    j.site,
                    j.title,
                    j.company,
                    j.job_url,
                    j.batch_id,
                    j.description,
                    j.description_error,
                    q.status AS jd_status,
                    COALESCE(q.attempts, 0) AS jd_attempts,
                    q.error AS jd_error
                FROM jobs j
                LEFT JOIN jd_queue q ON q.job_id = j.id
                WHERE j.id = %s
                """,
                (job_id,),
            )
            job_row = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    pj.profile_id,
                    p.profile_key,
                    p.display_name,
                    pj.search_config_id,
                    pj.matched_term,
                    pj.fit_status,
                    pj.fit_score,
                    pj.fit_decision,
                    pj.llm_match,
                    pj.llm_match_error,
                    pj.notify_status,
                    pj.notify_error,
                    pj.notified_at,
                    pj.discovered_at,
                    pj.last_seen_at
                FROM profile_jobs pj
                JOIN profiles p ON p.id = pj.profile_id
                WHERE pj.job_id = %s
                ORDER BY pj.profile_id ASC
                """,
                (job_id,),
            )
            profile_rows = cursor.fetchall()

    if not job_row:
        return {}

    return {
        "job": job_row,
        "profiles": list(profile_rows),
    }


def _profile_dashboards() -> dict:
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    p.id AS profile_id,
                    p.profile_key,
                    p.display_name,
                    p.is_active,
                    p.model_name,
                    p.discord_channel_id,
                    (NULLIF(BTRIM(p.discord_webhook_url), '') IS NOT NULL) AS has_discord_webhook,
                    COUNT(*) FILTER (WHERE j.description IS NOT NULL AND pj.job_id IS NOT NULL) AS total_jobs,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.notify_status = 'sent'
                    ) AS notified_jobs,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.fit_status = 'pending_fit'
                    ) AS fit_pending,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.fit_status = 'fitting'
                    ) AS fit_processing,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.fit_status = 'fit_done'
                    ) AS fit_done,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.fit_status = 'notified'
                    ) AS fit_notified,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.fit_status = 'fit_failed'
                    ) AS fit_failed,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL
                          AND (pj.notify_status = 'failed' OR pj.fit_status = 'notify_failed')
                    ) AS notify_failed,
                    COUNT(*) FILTER (
                        WHERE j.description IS NOT NULL AND pj.fit_score IS NOT NULL
                    ) AS scored_jobs,
                    ROUND(
                        AVG(pj.fit_score) FILTER (WHERE j.description IS NOT NULL)::numeric,
                        1
                    )::double precision AS avg_fit_score,
                    MAX(pj.fit_score) FILTER (WHERE j.description IS NOT NULL) AS max_fit_score,
                    MAX(pj.last_seen_at) AS last_seen_at,
                    MAX(pj.notified_at) AS last_notified_at
                FROM profiles p
                LEFT JOIN profile_jobs pj ON pj.profile_id = p.id
                LEFT JOIN jobs j ON j.id = pj.job_id
                GROUP BY
                    p.id,
                    p.profile_key,
                    p.display_name,
                    p.is_active,
                    p.model_name,
                    p.discord_channel_id,
                    p.discord_webhook_url
                ORDER BY p.is_active DESC, COUNT(pj.job_id) DESC, p.id ASC
                """
            )
            profile_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    pj.profile_id,
                    LEAST(GREATEST(FLOOR(pj.fit_score / 10.0)::int, 0), 9) AS bucket_index,
                    COUNT(*) AS count
                FROM profile_jobs pj
                JOIN jobs j ON j.id = pj.job_id
                WHERE pj.fit_score IS NOT NULL
                  AND j.description IS NOT NULL
                GROUP BY pj.profile_id, bucket_index
                """
            )
            score_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    pj.profile_id,
                    pj.fit_decision,
                    COUNT(*) AS count
                FROM profile_jobs pj
                JOIN jobs j ON j.id = pj.job_id
                WHERE pj.fit_decision IS NOT NULL
                  AND j.description IS NOT NULL
                GROUP BY pj.profile_id, pj.fit_decision
                """
            )
            decision_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    pj.profile_id,
                    pj.matched_term AS term,
                    COUNT(*) AS count
                FROM profile_jobs pj
                JOIN jobs j ON j.id = pj.job_id
                WHERE NULLIF(BTRIM(pj.matched_term), '') IS NOT NULL
                  AND j.description IS NOT NULL
                GROUP BY pj.profile_id, pj.matched_term
                ORDER BY pj.profile_id ASC, COUNT(*) DESC, pj.matched_term ASC
                """
            )
            term_rows = cursor.fetchall()

    score_counts_by_profile: dict[int, dict[int, int]] = defaultdict(dict)
    for row in score_rows:
        score_counts_by_profile[row["profile_id"]][row["bucket_index"]] = row["count"]

    decision_counts_by_profile: dict[int, dict[str, int]] = defaultdict(dict)
    for row in decision_rows:
        decision = str(row["fit_decision"]).strip()
        if decision:
            decision_counts_by_profile[row["profile_id"]][decision] = row["count"]

    top_terms_by_profile: dict[int, list[dict]] = defaultdict(list)
    for row in term_rows:
        profile_id = row["profile_id"]
        if len(top_terms_by_profile[profile_id]) >= 5:
            continue
        top_terms_by_profile[profile_id].append(
            {
                "term": row["term"],
                "count": row["count"],
            }
        )

    profiles: list[dict] = []
    decision_order = [
        "Strong Fit",
        "Moderate Fit",
        "Weak Fit",
        "Not Recommended",
    ]
    for row in profile_rows:
        profile_id = row["profile_id"]
        score_buckets = []
        for bucket_index in range(10):
            lower_bound = bucket_index * 10
            upper_bound = lower_bound + 9 if bucket_index < 9 else 100
            score_buckets.append(
                {
                    "bucket_index": bucket_index,
                    "label": f"{lower_bound}-{upper_bound}",
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "count": score_counts_by_profile[profile_id].get(bucket_index, 0),
                }
            )

        decision_breakdown = [
            {
                "decision": decision,
                "count": decision_counts_by_profile[profile_id].get(decision, 0),
            }
            for decision in decision_order
        ]

        profiles.append(
            {
                "profile_id": profile_id,
                "profile_key": row["profile_key"],
                "display_name": row["display_name"],
                "is_active": row["is_active"],
                "model_name": row["model_name"],
                "discord_channel_id": row["discord_channel_id"],
                "has_discord_webhook": row["has_discord_webhook"],
                "total_jobs": row["total_jobs"],
                "notified_jobs": row["notified_jobs"],
                "fit_pending": row["fit_pending"],
                "fit_processing": row["fit_processing"],
                "fit_done": row["fit_done"],
                "fit_notified": row["fit_notified"],
                "fit_failed": row["fit_failed"],
                "notify_failed": row["notify_failed"],
                "scored_jobs": row["scored_jobs"],
                "avg_fit_score": row["avg_fit_score"],
                "max_fit_score": row["max_fit_score"],
                "last_seen_at": row["last_seen_at"],
                "last_notified_at": row["last_notified_at"],
                "score_buckets": score_buckets,
                "decision_breakdown": decision_breakdown,
                "top_terms": top_terms_by_profile.get(profile_id, []),
            }
        )

    return {"profiles": profiles}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Frontend data bridge for the local Linkedin notifier app."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary_parser = subparsers.add_parser("summary")
    summary_parser.set_defaults(handler=lambda _args: _summary())

    jobs_parser = subparsers.add_parser("jobs")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command", required=True)

    list_parser = jobs_subparsers.add_parser("list")
    list_parser.add_argument("--query", default=None)
    list_parser.add_argument("--limit", type=int, default=100)
    list_parser.set_defaults(handler=lambda args: _list_jobs(args.query, args.limit))

    get_parser = jobs_subparsers.add_parser("get")
    get_parser.add_argument("job_id")
    get_parser.set_defaults(handler=lambda args: _job_detail(args.job_id))

    profiles_parser = subparsers.add_parser("profiles")
    profiles_subparsers = profiles_parser.add_subparsers(
        dest="profiles_command",
        required=True,
    )

    dashboard_parser = profiles_subparsers.add_parser("dashboard")
    dashboard_parser.set_defaults(handler=lambda _args: _profile_dashboards())

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    payload = args.handler(args)
    _emit(payload)


if __name__ == "__main__":
    main()
