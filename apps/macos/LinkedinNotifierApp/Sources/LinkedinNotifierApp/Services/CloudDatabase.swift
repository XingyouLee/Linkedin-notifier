import Foundation
import Logging
import PostgresNIO

enum CloudDatabaseError: LocalizedError {
    case missingPayload
    case missingJob(String)

    var errorDescription: String? {
        switch self {
        case .missingPayload:
            return "The cloud database query returned no payload."
        case let .missingJob(jobID):
            return "Could not find job \(jobID) in the cloud database."
        }
    }
}

actor CloudDatabase {
    private let config: CloudConfig
    private let logger = Logger(label: "local.levi.LinkedinNotifierApp.CloudDatabase")
    private var nextConnectionID = 0

    init(config: CloudConfig) {
        self.config = config
    }

    func loadSummary() async throws -> SummaryPayload {
        try await fetchPayload(summaryQuery(), as: SummaryPayload.self)
    }

    func loadJobs(searchText: String, limit: Int = 50) async throws -> [JobListItem] {
        let trimmedQuery = searchText.trimmingCharacters(in: .whitespacesAndNewlines)
        return try await fetchPayload(jobsQuery(searchText: trimmedQuery, limit: limit), as: [JobListItem].self)
    }

    func loadJobDetail(jobID: String) async throws -> JobDetailResponse {
        struct NullablePayload: Decodable {
            let job: JobCanonicalRecord?
            let profiles: [JobProfileDetail]
        }

        let payload = try await fetchPayload(jobDetailQuery(jobID: jobID), as: NullablePayload.self)
        guard let job = payload.job else {
            throw CloudDatabaseError.missingJob(jobID)
        }
        return JobDetailResponse(job: job, profiles: payload.profiles)
    }

    func loadProfileDashboards() async throws -> ProfileDashboardResponse {
        try await fetchPayload(profileDashboardQuery(), as: ProfileDashboardResponse.self)
    }

    private func fetchPayload<T: Decodable>(_ query: PostgresQuery, as type: T.Type) async throws -> T {
        let payloadData = try await withConnection { connection in
            let rows = try await connection.query(query, logger: logger)
            let collectedRows = try await rows.collect()
            guard let firstRow = collectedRows.first else {
                throw CloudDatabaseError.missingPayload
            }

            let payload = firstRow.makeRandomAccess()[data: "payload"]
            guard let jsonData = payload.json else {
                throw CloudDatabaseError.missingPayload
            }
            return jsonData
        }

        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return try decoder.decode(type, from: payloadData)
    }

    private func withConnection<T>(_ operation: @Sendable (PostgresConnection) async throws -> T) async throws -> T {
        let connectionID = nextConnectionID
        nextConnectionID += 1

        let connection = try await PostgresConnection.connect(
            configuration: config.postgresConnectionConfiguration,
            id: connectionID,
            logger: logger
        )

        do {
            let result = try await operation(connection)
            try await connection.closeGracefully()
            return result
        } catch {
            try? await connection.closeGracefully()
            throw error
        }
    }

    private func summaryQuery() -> PostgresQuery {
        """
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
    }

    private func jobsQuery(searchText: String, limit: Int) -> PostgresQuery {
        let safeLimit = max(1, limit)

        if searchText.isEmpty {
            return """
            SELECT COALESCE(json_agg(row_to_json(job_rows) ORDER BY COALESCE(job_rows.batch_id, 0) DESC, job_rows.job_id DESC), '[]'::json) AS payload
            FROM (
                WITH filtered_jobs AS (
                    SELECT DISTINCT j.id, j.batch_id
                    FROM jobs j
                    LEFT JOIN profile_jobs pj ON pj.job_id = j.id
                    LEFT JOIN profiles p ON p.id = pj.profile_id
                    ORDER BY j.batch_id DESC NULLS LAST, j.id DESC
                    LIMIT \(safeLimit)
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
            ) AS job_rows
            """
        }

        let pattern = "%\(searchText)%"
        return """
        SELECT COALESCE(json_agg(row_to_json(job_rows) ORDER BY COALESCE(job_rows.batch_id, 0) DESC, job_rows.job_id DESC), '[]'::json) AS payload
        FROM (
            WITH filtered_jobs AS (
                SELECT DISTINCT j.id, j.batch_id
                FROM jobs j
                LEFT JOIN profile_jobs pj ON pj.job_id = j.id
                LEFT JOIN profiles p ON p.id = pj.profile_id
                WHERE (
                    j.id = \(searchText)
                    OR COALESCE(j.title, '') ILIKE \(pattern)
                    OR COALESCE(j.company, '') ILIKE \(pattern)
                    OR COALESCE(p.profile_key, '') ILIKE \(pattern)
                    OR COALESCE(p.display_name, '') ILIKE \(pattern)
                    OR COALESCE(pj.matched_term, '') ILIKE \(pattern)
                )
                ORDER BY j.batch_id DESC NULLS LAST, j.id DESC
                LIMIT \(safeLimit)
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
        ) AS job_rows
        """
    }

    private func jobDetailQuery(jobID: String) -> PostgresQuery {
        """
        WITH job_row AS (
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
            WHERE j.id = \(jobID)
        ),
        profile_rows AS (
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
            WHERE pj.job_id = \(jobID)
            ORDER BY pj.profile_id ASC
        )
        SELECT json_build_object(
            'job', (SELECT row_to_json(job_row) FROM job_row),
            'profiles', COALESCE((SELECT json_agg(row_to_json(profile_rows) ORDER BY profile_id ASC) FROM profile_rows), '[]'::json)
        ) AS payload
        """
    }

    private func profileDashboardQuery() -> PostgresQuery {
        """
        WITH profile_rows AS (
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
        ),
        score_rows AS (
            SELECT
                pj.profile_id,
                LEAST(GREATEST(FLOOR(pj.fit_score / 10.0)::int, 0), 9) AS bucket_index,
                COUNT(*) AS count
            FROM profile_jobs pj
            JOIN jobs j ON j.id = pj.job_id
            WHERE pj.fit_score IS NOT NULL
              AND j.description IS NOT NULL
            GROUP BY pj.profile_id, bucket_index
        ),
        decision_rows AS (
            SELECT
                pj.profile_id,
                pj.fit_decision,
                COUNT(*) AS count
            FROM profile_jobs pj
            JOIN jobs j ON j.id = pj.job_id
            WHERE pj.fit_decision IS NOT NULL
              AND j.description IS NOT NULL
            GROUP BY pj.profile_id, pj.fit_decision
        ),
        term_rows AS (
            SELECT
                pj.profile_id,
                pj.matched_term AS term,
                COUNT(*) AS count
            FROM profile_jobs pj
            JOIN jobs j ON j.id = pj.job_id
            WHERE NULLIF(BTRIM(pj.matched_term), '') IS NOT NULL
              AND j.description IS NOT NULL
            GROUP BY pj.profile_id, pj.matched_term
        ),
        ordered_profiles AS (
            SELECT
                pr.profile_id,
                pr.is_active,
                pr.total_jobs,
                json_build_object(
                    'profile_id', pr.profile_id,
                    'profile_key', pr.profile_key,
                    'display_name', pr.display_name,
                    'is_active', pr.is_active,
                    'model_name', pr.model_name,
                    'discord_channel_id', pr.discord_channel_id,
                    'has_discord_webhook', pr.has_discord_webhook,
                    'total_jobs', pr.total_jobs,
                    'notified_jobs', pr.notified_jobs,
                    'fit_pending', pr.fit_pending,
                    'fit_processing', pr.fit_processing,
                    'fit_done', pr.fit_done,
                    'fit_notified', pr.fit_notified,
                    'fit_failed', pr.fit_failed,
                    'notify_failed', pr.notify_failed,
                    'scored_jobs', pr.scored_jobs,
                    'avg_fit_score', pr.avg_fit_score,
                    'max_fit_score', pr.max_fit_score,
                    'last_seen_at', pr.last_seen_at,
                    'last_notified_at', pr.last_notified_at,
                    'score_buckets', (
                        SELECT json_agg(
                            json_build_object(
                                'bucket_index', bucket_series.bucket_index,
                                'label', CASE WHEN bucket_series.bucket_index < 9 THEN CONCAT(bucket_series.bucket_index * 10, '-', bucket_series.bucket_index * 10 + 9) ELSE '90-100' END,
                                'lower_bound', bucket_series.bucket_index * 10,
                                'upper_bound', CASE WHEN bucket_series.bucket_index < 9 THEN bucket_series.bucket_index * 10 + 9 ELSE 100 END,
                                'count', COALESCE(sr.count, 0)
                            )
                            ORDER BY bucket_series.bucket_index
                        )
                        FROM generate_series(0, 9) AS bucket_series(bucket_index)
                        LEFT JOIN score_rows sr
                            ON sr.profile_id = pr.profile_id
                           AND sr.bucket_index = bucket_series.bucket_index
                    ),
                    'decision_breakdown', (
                        SELECT json_agg(
                            json_build_object(
                                'decision', ordered_decisions.decision,
                                'count', COALESCE(dr.count, 0)
                            )
                            ORDER BY ordered_decisions.position
                        )
                        FROM (
                            VALUES
                                (1, 'Strong Fit'),
                                (2, 'Moderate Fit'),
                                (3, 'Weak Fit'),
                                (4, 'Not Recommended')
                        ) AS ordered_decisions(position, decision)
                        LEFT JOIN decision_rows dr
                            ON dr.profile_id = pr.profile_id
                           AND dr.fit_decision = ordered_decisions.decision
                    ),
                    'top_terms', (
                        SELECT COALESCE(
                            json_agg(
                                json_build_object(
                                    'term', limited_terms.term,
                                    'count', limited_terms.count
                                )
                                ORDER BY limited_terms.count DESC, limited_terms.term ASC
                            ),
                            '[]'::json
                        )
                        FROM (
                            SELECT term, count
                            FROM term_rows
                            WHERE profile_id = pr.profile_id
                            ORDER BY count DESC, term ASC
                            LIMIT 5
                        ) AS limited_terms
                    )
                ) AS payload
            FROM profile_rows pr
        )
        SELECT json_build_object(
            'profiles', COALESCE(
                json_agg(ordered_profiles.payload ORDER BY ordered_profiles.is_active DESC, ordered_profiles.total_jobs DESC, ordered_profiles.profile_id ASC),
                '[]'::json
            )
        ) AS payload
        FROM ordered_profiles
        """
    }
}
