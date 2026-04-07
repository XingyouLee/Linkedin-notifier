import Foundation
import Testing

@testable import LinkedinNotifierApp

@Test
func cloudConfigParsesDatabaseURLAndBuildsAirflowLinks() throws {

    let config = try CloudConfig(
        jobsDatabaseURL: "postgresql://demo:secret@db.example.com:5432/jobsdb?sslmode=require",
        airflowWebURL: URL(string: "https://airflow.example.com"),
        databaseEndpoint: .parse(urlString: "postgresql://demo:secret@db.example.com:5432/jobsdb?sslmode=require")
    )

    #expect(config.databaseEndpoint.host == "db.example.com")
    #expect(config.databaseEndpoint.port == 5432)
    #expect(config.databaseEndpoint.username == "demo")
    #expect(config.databaseEndpoint.password == "secret")
    #expect(config.databaseEndpoint.database == "jobsdb")
    #expect(config.databaseEndpoint.tlsMode == .require)
    #expect(config.redactedDatabaseDescription == "demo@db.example.com:5432/jobsdb")
    #expect(config.postgresConnectionConfiguration.host == "db.example.com")
    #expect(config.postgresConnectionConfiguration.port == 5432)
    #expect(config.postgresConnectionConfiguration.username == "demo")
    #expect(config.postgresConnectionConfiguration.database == "jobsdb")
    #expect(config.postgresConnectionConfiguration.options.tlsServerName == nil)
    #expect(config.airflowHomeURL()?.absoluteString == "https://airflow.example.com")
    #expect(config.airflowDagURL(dagID: "linkedin_notifier")?.absoluteString == "https://airflow.example.com/dags/linkedin_notifier/grid")
    #expect(config.airflowDagRunURL(dagID: "linkedin_notifier", runID: "manual__123")?.absoluteString == "https://airflow.example.com/dags/linkedin_notifier/runs/manual__123")
}

@Test
func cloudConfigRequiresDatabaseURL() {
    #expect(throws: CloudConfigError.missingJobsDatabaseURL) {
        _ = try CloudConfig.load(bundle: .main, environment: [:])
    }
}

@Test
func cloudConfigLoadsFromEnvironment() throws {
    let config = try CloudConfig.load(environment: [
        CloudBuildConfig.jobsDatabaseURLEnvironmentKey: "postgresql://worker:pw@remote.example.com:5433/cloudjobs",
        CloudBuildConfig.airflowWebURLEnvironmentKey: "https://airflow.example.com"
    ])

    #expect(config.databaseEndpoint.host == "remote.example.com")
    #expect(config.databaseEndpoint.port == 5433)
    #expect(config.databaseEndpoint.username == "worker")
    #expect(config.databaseEndpoint.database == "cloudjobs")
    #expect(config.airflowWebURLString == "https://airflow.example.com")
}

@Test
func localProfileSummaryDecodesProfilesJSONShape() throws {
    let data = Data(
        """
        [
          {
            "profile_key": "Xingyou Li",
            "display_name": "Xingyou Li",
            "active": true,
            "resume_path": "resume/xingyouli.md",
            "discord_channel_id": "1476129860450779147",
            "model_name": "gpt-5.4",
            "candidate_summary": {
              "summary": "Early-career data engineer.",
              "target_roles": ["Data Engineer", "Analytics Engineer"],
              "candidate_years": 1.5,
              "candidate_seniority": "junior",
              "core_skills": ["Python", "SQL"],
              "obvious_gaps": ["Fluent Dutch"],
              "language_signals": {
                "dutch_level": "basic",
                "english_level": "fluent",
                "notes": "English is fluent."
              }
            },
            "search_configs": [
              {
                "name": "data",
                "location": "Netherlands",
                "distance": 50,
                "hours_old": 120,
                "results_per_term": 300,
                "terms": ["python", "data engineer"]
              }
            ]
          }
        ]
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let profiles = try decoder.decode([LocalProfileSummary].self, from: data)
    let profile = try #require(profiles.first)

    #expect(profile.profileKey == "Xingyou Li")
    #expect(profile.displayName == "Xingyou Li")
    #expect(profile.active == true)
    #expect(profile.resumePath == "resume/xingyouli.md")
    #expect(profile.modelName == "gpt-5.4")
    #expect(profile.candidateSummary?.candidateYears == 1.5)
    #expect(profile.candidateSummary?.targetRoles == ["Data Engineer", "Analytics Engineer"])
    #expect(profile.searchConfigs.first?.terms == ["python", "data engineer"])
}

@Test
func localProfileMatchesRemoteDashboardByKeyOrDisplayName() {
    let localProfile = LocalProfileSummary(
        profileKey: "Xingyou Li",
        displayName: "Xingyou Li",
        active: true,
        resumePath: nil,
        discordChannelId: nil,
        modelName: nil,
        candidateSummary: nil,
        searchConfigs: []
    )

    let matchingDashboard = ProfileDashboard(
        profileId: 2,
        profileKey: "Xingyou Li",
        displayName: "Xingyou Li",
        isActive: true,
        modelName: nil,
        discordChannelId: nil,
        hasDiscordWebhook: nil,
        totalJobs: 10,
        notifiedJobs: 2,
        fitPending: 1,
        fitProcessing: 0,
        fitDone: 5,
        fitNotified: 2,
        fitFailed: 0,
        notifyFailed: 0,
        scoredJobs: 8,
        avgFitScore: 51,
        maxFitScore: 72,
        lastSeenAt: nil,
        lastNotifiedAt: nil,
        scoreBuckets: [],
        decisionBreakdown: [],
        topTerms: []
    )

    let nonMatchingDashboard = ProfileDashboard(
        profileId: 3,
        profileKey: "Other",
        displayName: "Other",
        isActive: true,
        modelName: nil,
        discordChannelId: nil,
        hasDiscordWebhook: nil,
        totalJobs: 4,
        notifiedJobs: 0,
        fitPending: 0,
        fitProcessing: 0,
        fitDone: 1,
        fitNotified: 0,
        fitFailed: 0,
        notifyFailed: 0,
        scoredJobs: 1,
        avgFitScore: 10,
        maxFitScore: 10,
        lastSeenAt: nil,
        lastNotifiedAt: nil,
        scoreBuckets: [],
        decisionBreakdown: [],
        topTerms: []
    )

    let matched = [matchingDashboard, nonMatchingDashboard].first { dashboard in
        dashboard.matches(localProfile: localProfile)
    }

    #expect(matched?.profileId == 2)
}

@Test
func summaryPayloadDecodesGlobalProgressFields() throws {
    let data = Data(
        """
        {
          "jd_pending": 3,
          "jd_processing": 2,
          "jd_failed": 1,
          "jd_done": 4,
          "jd_global_total": 10,
          "fit_pending": 5,
          "fit_processing": 2,
          "fit_failed": 1,
          "fit_done": 6,
          "fit_terminal_done": 7,
          "fit_global_total": 15
        }
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let summary = try decoder.decode(SummaryPayload.self, from: data)

    #expect(summary.jdPending == 3)
    #expect(summary.jdProcessing == 2)
    #expect(summary.jdFailed == 1)
    #expect(summary.jdDone == 4)
    #expect(summary.jdGlobalTotal == 10)
    #expect(summary.fitPending == 5)
    #expect(summary.fitProcessing == 2)
    #expect(summary.fitFailed == 1)
    #expect(summary.fitDone == 6)
    #expect(summary.fitTerminalDone == 7)
    #expect(summary.fitGlobalTotal == 15)
}

@Test
func summaryPayloadComputesGlobalQueueProgressFromConsistentMetrics() {
    let summary = SummaryPayload(
        totalJobs: nil,
        totalProfileJobs: nil,
        jdPending: 11,
        jdProcessing: 7,
        jdFailed: 2,
        jdDone: 70,
        jdGlobalTotal: 20,
        fitPending: 20,
        fitProcessing: 53,
        fitFailed: 4,
        fitDone: 59,
        fitTerminalDone: 59,
        fitGlobalTotal: 77,
        notifyFailed: 3,
        latestBatchId: 123,
        latestBatchJobsTotal: 70,
        latestBatchJdDone: 70,
        latestBatchJdProcessing: 0,
        latestBatchJdPending: 0,
        latestBatchJdFailed: 1,
        latestBatchFitTotal: 143,
        latestBatchFitDone: 59,
        latestBatchFitProcessing: 53,
        latestBatchFitPending: 20,
        latestBatchFitFailed: 0,
        latestBatchScanProgress: 100
    )

    #expect(summary.globalJdCompletedCount == 72)
    #expect(summary.globalJdTotalCount == 90)
    #expect(summary.globalFitCompletedCount == 66)
    #expect(summary.globalFitTotalCount == 139)
}

@Test
func profileDashboardResponseDecodesDashboardPayload() throws {
    let data = Data(
        """
        {
          "profiles": [
            {
              "profile_id": 2,
              "profile_key": "Xingyou Li",
              "display_name": "Xingyou Li",
              "is_active": true,
              "model_name": "gpt-5.4",
              "discord_channel_id": "123456",
              "has_discord_webhook": true,
              "total_jobs": 100,
              "notified_jobs": 12,
              "fit_pending": 3,
              "fit_processing": 1,
              "fit_done": 60,
              "fit_notified": 10,
              "fit_failed": 2,
              "notify_failed": 1,
              "scored_jobs": 73,
              "avg_fit_score": 31.4,
              "max_fit_score": 88,
              "last_seen_at": "2026-03-31 19:14:09",
              "last_notified_at": "2026-03-31 12:00:00",
              "score_buckets": [
                {
                  "bucket_index": 0,
                  "label": "0-9",
                  "lower_bound": 0,
                  "upper_bound": 9,
                  "count": 4
                },
                {
                  "bucket_index": 1,
                  "label": "10-19",
                  "lower_bound": 10,
                  "upper_bound": 19,
                  "count": 6
                }
              ],
              "decision_breakdown": [
                { "decision": "Strong Fit", "count": 1 },
                { "decision": "Moderate Fit", "count": 4 },
                { "decision": "Weak Fit", "count": 12 },
                { "decision": "Not Recommended", "count": 40 }
              ],
              "top_terms": [
                { "term": "Data Engineer", "count": 44 },
                { "term": "Python Developer", "count": 10 }
              ]
            }
          ]
        }
        """.utf8
    )

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let response = try decoder.decode(ProfileDashboardResponse.self, from: data)
    let profile = try #require(response.profiles.first)

    #expect(profile.profileId == 2)
    #expect(profile.name == "Xingyou Li")
    #expect(profile.totalJobsValue == 100)
    #expect(profile.notifiedJobsValue == 12)
    #expect(profile.scoredJobsValue == 73)
    #expect(profile.terminalCompletedJobs == 70)
    #expect(profile.totalFailures == 3)
    #expect(profile.strongOrModerateCount == 5)
    #expect(profile.scoreBuckets.count == 2)
    #expect(profile.topTerms.count == 2)
}

@Test
func profileDashboardComputedMetricsRemainConsistent() {
    let profile = ProfileDashboard(
        profileId: 7,
        profileKey: "demo",
        displayName: "Demo",
        isActive: true,
        modelName: nil,
        discordChannelId: nil,
        hasDiscordWebhook: nil,
        totalJobs: 40,
        notifiedJobs: 8,
        fitPending: 3,
        fitProcessing: 2,
        fitDone: 10,
        fitNotified: 5,
        fitFailed: 4,
        notifyFailed: 1,
        scoredJobs: 22,
        avgFitScore: 48.5,
        maxFitScore: 88,
        lastSeenAt: nil,
        lastNotifiedAt: nil,
        scoreBuckets: [],
        decisionBreakdown: [
            ProfileDecisionCount(decision: "Strong Fit", count: 2),
            ProfileDecisionCount(decision: "Moderate Fit", count: 3),
            ProfileDecisionCount(decision: "Weak Fit", count: 5)
        ],
        topTerms: []
    )

    #expect(profile.totalJobsValue == 40)
    #expect(profile.notifiedJobsValue == 8)
    #expect(profile.terminalCompletedJobs == 15)
    #expect(profile.totalFailures == 5)
    #expect(profile.strongOrModerateCount == 5)
    #expect(profile.completionFraction == 0.375)
    #expect(profile.notifiedFraction == 0.2)
}
