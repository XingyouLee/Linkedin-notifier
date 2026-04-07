import Foundation

final class ProjectService: Sendable {
    let cloudConfig: CloudConfig
    private let database: CloudDatabase
    private let localProfilesStore: LocalProfilesStore

    init(cloudConfig: CloudConfig) {
        self.cloudConfig = cloudConfig
        self.database = CloudDatabase(config: cloudConfig)
        self.localProfilesStore = LocalProfilesStore()
    }

    static func loadDefault() throws -> ProjectService {
        try ProjectService(cloudConfig: CloudConfig.load())
    }

    func loadSummary() async throws -> SummaryPayload {
        try await database.loadSummary()
    }

    func loadJobs(searchText: String, limit: Int = 50) async throws -> [JobListItem] {
        try await database.loadJobs(searchText: searchText, limit: limit)
    }

    func loadJobDetail(jobID: String) async throws -> JobDetailResponse {
        try await database.loadJobDetail(jobID: jobID)
    }

    func loadProfileDashboards() async throws -> ProfileDashboardResponse {
        try await database.loadProfileDashboards()
    }

    func loadLocalProfiles() throws -> [LocalProfileSummary] {
        try localProfilesStore.loadProfiles()
    }

    func airflowHomeURL() -> URL? {
        cloudConfig.airflowHomeURL()
    }

    func airflowDagURL(dagID: String) -> URL? {
        cloudConfig.airflowDagURL(dagID: dagID)
    }

    func airflowDagRunURL(dagID: String, runID: String) -> URL? {
        cloudConfig.airflowDagRunURL(dagID: dagID, runID: runID)
    }
}
