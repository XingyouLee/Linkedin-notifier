import Foundation

struct ContainerInfo: Identifiable, Hashable {
    let name: String
    let status: String

    var id: String { name }
}

struct SummaryPayload: Codable, Hashable {
    let totalJobs: Int?
    let totalProfileJobs: Int?
    let jdPending: Int?
    let jdProcessing: Int?
    let jdFailed: Int?
    let jdDone: Int?
    let jdGlobalTotal: Int?
    let fitPending: Int?
    let fitProcessing: Int?
    let fitFailed: Int?
    let fitDone: Int?
    let fitTerminalDone: Int?
    let fitGlobalTotal: Int?
    let notifyFailed: Int?
    let latestBatchId: Int?
    let latestBatchJobsTotal: Int?
    let latestBatchJdDone: Int?
    let latestBatchJdProcessing: Int?
    let latestBatchJdPending: Int?
    let latestBatchJdFailed: Int?
    let latestBatchFitTotal: Int?
    let latestBatchFitDone: Int?
    let latestBatchFitProcessing: Int?
    let latestBatchFitPending: Int?
    let latestBatchFitFailed: Int?
    let latestBatchScanProgress: Int?
}

extension SummaryPayload {
    var globalJdCompletedCount: Int {
        max(0, (jdDone ?? 0) + (jdFailed ?? 0))
    }

    var globalJdTotalCount: Int {
        max(0, (jdDone ?? 0) + (jdPending ?? 0) + (jdProcessing ?? 0) + (jdFailed ?? 0))
    }

    var globalFitCompletedCount: Int {
        max(0, (fitTerminalDone ?? 0) + (fitFailed ?? 0) + (notifyFailed ?? 0))
    }

    var globalFitTotalCount: Int {
        max(
            0,
            (fitTerminalDone ?? 0)
                + (fitPending ?? 0)
                + (fitProcessing ?? 0)
                + (fitFailed ?? 0)
                + (notifyFailed ?? 0)
        )
    }
}

struct EnvironmentSnapshot: Hashable {
    let dockerRunning: Bool
    let astroRunning: Bool
    let schedulerContainer: String?
    let apiServerContainer: String?
    let postgresContainer: String?
    let containers: [ContainerInfo]
    let summary: SummaryPayload?
    let lastError: String?

    static let empty = EnvironmentSnapshot(
        dockerRunning: false,
        astroRunning: false,
        schedulerContainer: nil,
        apiServerContainer: nil,
        postgresContainer: nil,
        containers: [],
        summary: nil,
        lastError: nil
    )

    func withStatus(
        dockerRunning: Bool,
        astroRunning: Bool,
        schedulerContainer: String?,
        apiServerContainer: String?,
        postgresContainer: String?,
        containers: [ContainerInfo]
    ) -> EnvironmentSnapshot {
        EnvironmentSnapshot(
            dockerRunning: dockerRunning,
            astroRunning: astroRunning,
            schedulerContainer: schedulerContainer,
            apiServerContainer: apiServerContainer,
            postgresContainer: postgresContainer,
            containers: containers,
            summary: astroRunning ? summary : nil,
            lastError: astroRunning ? lastError : nil
        )
    }

    func withSummary(_ summary: SummaryPayload?, lastError: String?) -> EnvironmentSnapshot {
        EnvironmentSnapshot(
            dockerRunning: dockerRunning,
            astroRunning: astroRunning,
            schedulerContainer: schedulerContainer,
            apiServerContainer: apiServerContainer,
            postgresContainer: postgresContainer,
            containers: containers,
            summary: summary,
            lastError: lastError
        )
    }
}

struct DagRunRecord: Decodable, Hashable, Identifiable {
    let dagId: String
    let runId: String
    let state: String?
    let logicalDate: String?
    let startDate: String?
    let endDate: String?
    let runAfter: String?

    var id: String { "\(dagId):\(runId)" }

    enum CodingKeys: String, CodingKey {
        case dagId
        case runId
        case dagRunId
        case state
        case logicalDate
        case startDate
        case endDate
        case runAfter
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        dagId = try container.decode(String.self, forKey: .dagId)
        if let runIdValue = try container.decodeIfPresent(String.self, forKey: .runId) {
            runId = runIdValue
        } else {
            runId = try container.decode(String.self, forKey: .dagRunId)
        }
        state = try container.decodeIfPresent(String.self, forKey: .state)
        logicalDate = try container.decodeIfPresent(String.self, forKey: .logicalDate)
        startDate = try container.decodeIfPresent(String.self, forKey: .startDate)
        endDate = try container.decodeIfPresent(String.self, forKey: .endDate)
        runAfter = try container.decodeIfPresent(String.self, forKey: .runAfter)
    }
}

struct ProfileMatchSummary: Codable, Hashable, Identifiable {
    let profileId: Int
    let profileKey: String?
    let displayName: String?
    let matchedTerm: String?
    let fitStatus: String?
    let fitScore: Int?
    let fitDecision: String?
    let notifyStatus: String?

    var id: Int { profileId }
}

struct JobListItem: Codable, Hashable, Identifiable {
    let jobId: String
    let title: String?
    let company: String?
    let jobUrl: String?
    let batchId: Int?
    let hasDescription: Bool?
    let jdStatus: String?
    let jdAttempts: Int?
    let profileMatchCount: Int?
    let bestFitScore: Int?
    let profileMatches: [ProfileMatchSummary]

    var id: String { jobId }
}

struct JobCanonicalRecord: Codable, Hashable {
    let jobId: String
    let site: String?
    let title: String?
    let company: String?
    let jobUrl: String?
    let batchId: Int?
    let description: String?
    let descriptionError: String?
    let jdStatus: String?
    let jdAttempts: Int?
    let jdError: String?
}

struct JobProfileDetail: Codable, Hashable, Identifiable {
    let profileId: Int
    let profileKey: String?
    let displayName: String?
    let searchConfigId: Int?
    let matchedTerm: String?
    let fitStatus: String?
    let fitScore: Int?
    let fitDecision: String?
    let llmMatch: String?
    let llmMatchError: String?
    let notifyStatus: String?
    let notifyError: String?
    let notifiedAt: String?
    let discoveredAt: String?
    let lastSeenAt: String?

    var id: Int { profileId }
}

struct JobDetailResponse: Codable, Hashable {
    let job: JobCanonicalRecord
    let profiles: [JobProfileDetail]
}

struct ProfileDashboardResponse: Codable, Hashable {
    let profiles: [ProfileDashboard]
}

struct ProfileScoreBucket: Codable, Hashable, Identifiable {
    let bucketIndex: Int
    let label: String
    let lowerBound: Int
    let upperBound: Int
    let count: Int

    var id: Int { bucketIndex }
}

struct ProfileDecisionCount: Codable, Hashable, Identifiable {
    let decision: String
    let count: Int

    var id: String { decision }
}

struct ProfileTermCount: Codable, Hashable, Identifiable {
    let term: String
    let count: Int

    var id: String { term }
}

struct ProfileDashboard: Codable, Hashable, Identifiable {
    let profileId: Int
    let profileKey: String?
    let displayName: String?
    let isActive: Bool?
    let modelName: String?
    let discordChannelId: String?
    let hasDiscordWebhook: Bool?
    let totalJobs: Int?
    let notifiedJobs: Int?
    let fitPending: Int?
    let fitProcessing: Int?
    let fitDone: Int?
    let fitNotified: Int?
    let fitFailed: Int?
    let notifyFailed: Int?
    let scoredJobs: Int?
    let avgFitScore: Double?
    let maxFitScore: Int?
    let lastSeenAt: String?
    let lastNotifiedAt: String?
    let scoreBuckets: [ProfileScoreBucket]
    let decisionBreakdown: [ProfileDecisionCount]
    let topTerms: [ProfileTermCount]

    var id: Int { profileId }
}

extension ProfileDashboard {
    var name: String {
        displayName ?? profileKey ?? "Profile \(profileId)"
    }

    var totalJobsValue: Int {
        max(0, totalJobs ?? 0)
    }

    var notifiedJobsValue: Int {
        max(0, notifiedJobs ?? 0)
    }

    var scoredJobsValue: Int {
        max(0, scoredJobs ?? 0)
    }

    var fitPendingValue: Int {
        max(0, fitPending ?? 0)
    }

    var fitProcessingValue: Int {
        max(0, fitProcessing ?? 0)
    }

    var fitDoneValue: Int {
        max(0, fitDone ?? 0)
    }

    var fitNotifiedValue: Int {
        max(0, fitNotified ?? 0)
    }

    var fitFailedValue: Int {
        max(0, fitFailed ?? 0)
    }

    var notifyFailedValue: Int {
        max(0, notifyFailed ?? 0)
    }

    var terminalCompletedJobs: Int {
        fitDoneValue + fitNotifiedValue
    }

    var totalFailures: Int {
        fitFailedValue + notifyFailedValue
    }

    var strongOrModerateCount: Int {
        decisionBreakdown
            .filter { $0.decision == "Strong Fit" || $0.decision == "Moderate Fit" }
            .map(\.count)
            .reduce(0, +)
    }

    var completionFraction: Double {
        guard totalJobsValue > 0 else { return 0 }
        return min(Double(terminalCompletedJobs) / Double(totalJobsValue), 1.0)
    }

    var notifiedFraction: Double {
        guard totalJobsValue > 0 else { return 0 }
        return min(Double(notifiedJobsValue) / Double(totalJobsValue), 1.0)
    }
}
