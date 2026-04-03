import Foundation

@MainActor
final class OverviewViewModel: ObservableObject {
    @Published private(set) var snapshot: EnvironmentSnapshot = .empty
    @Published private(set) var activeRunsByDAG: [String: [DagRunRecord]] = [:]
    @Published private(set) var activeRunsErrorMessage: String?
    @Published private(set) var isLoading = false
    @Published private(set) var actionOutput = ""

    private var isRefreshingStatus = false
    private var isRefreshingSummary = false
    private var isRefreshingActiveRuns = false
    private var pendingStatusProjectPath: String?
    private var pendingSummaryProjectPath: String?
    private var pendingActiveRunsProjectPath: String?

    func refresh(projectPath: String, service: ProjectService) async {
        isLoading = true
        defer { isLoading = false }
        await refreshStatus(projectPath: projectPath, service: service)
        await refreshSummary(projectPath: projectPath, service: service)
        await refreshActiveRuns(projectPath: projectPath, service: service)
    }

    func refreshStatus(projectPath: String, service: ProjectService) async {
        if isRefreshingStatus {
            pendingStatusProjectPath = projectPath
            return
        }

        var nextProjectPath: String? = projectPath
        while let currentProjectPath = nextProjectPath {
            nextProjectPath = nil
            isRefreshingStatus = true
            let status = await service.refreshEnvironmentStatus(projectPath: currentProjectPath)

            if let pendingStatusProjectPath {
                nextProjectPath = pendingStatusProjectPath
                self.pendingStatusProjectPath = nil
            } else {
                snapshot = snapshot.withStatus(
                    dockerRunning: status.dockerRunning,
                    astroRunning: status.astroRunning,
                    schedulerContainer: status.schedulerContainer,
                    apiServerContainer: status.apiServerContainer,
                    postgresContainer: status.postgresContainer,
                    containers: status.containers
                )
                if !status.astroRunning {
                    activeRunsByDAG = [:]
                    activeRunsErrorMessage = nil
                }
            }

            isRefreshingStatus = false
        }
    }

    func refreshSummary(projectPath: String, service: ProjectService) async {
        guard snapshot.astroRunning else {
            snapshot = snapshot.withSummary(nil, lastError: nil)
            pendingSummaryProjectPath = nil
            return
        }
        if isRefreshingSummary {
            pendingSummaryProjectPath = projectPath
            return
        }

        var nextProjectPath: String? = projectPath
        while let currentProjectPath = nextProjectPath {
            nextProjectPath = nil
            isRefreshingSummary = true
            let result = await service.refreshEnvironmentSummary(projectPath: currentProjectPath)

            if let pendingSummaryProjectPath {
                nextProjectPath = pendingSummaryProjectPath
                self.pendingSummaryProjectPath = nil
            } else if snapshot.astroRunning {
                snapshot = snapshot.withSummary(result.summary, lastError: result.lastError)
            } else {
                snapshot = snapshot.withSummary(nil, lastError: nil)
            }

            isRefreshingSummary = false
        }
    }

    func refreshActiveRuns(projectPath: String, service: ProjectService) async {
        guard snapshot.astroRunning else {
            activeRunsByDAG = [:]
            activeRunsErrorMessage = nil
            pendingActiveRunsProjectPath = nil
            return
        }
        if isRefreshingActiveRuns {
            pendingActiveRunsProjectPath = projectPath
            return
        }

        var nextProjectPath: String? = projectPath
        while let currentProjectPath = nextProjectPath {
            nextProjectPath = nil
            isRefreshingActiveRuns = true
            var nextActiveRunsByDAG: [String: [DagRunRecord]] = [:]
            var successfulFetchCount = 0
            var firstErrorMessage: String?

            for dagID in RunsViewModel.dagChoices {
                do {
                    let runs = try await service.listDagRuns(projectPath: currentProjectPath, dagID: dagID)
                    successfulFetchCount += 1
                    let activeRuns = runs.filter { run in
                        guard let state = run.state?.lowercased() else {
                            return false
                        }
                        return state == "running" || state == "queued"
                    }
                    if !activeRuns.isEmpty {
                        nextActiveRunsByDAG[dagID] = activeRuns
                    }
                } catch {
                    if firstErrorMessage == nil {
                        firstErrorMessage = error.localizedDescription
                    }
                }
            }

            if let pendingActiveRunsProjectPath {
                nextProjectPath = pendingActiveRunsProjectPath
                self.pendingActiveRunsProjectPath = nil
            } else if !snapshot.astroRunning {
                activeRunsByDAG = [:]
                activeRunsErrorMessage = nil
            } else if successfulFetchCount > 0 {
                activeRunsByDAG = nextActiveRunsByDAG
                activeRunsErrorMessage = nil
            } else {
                activeRunsErrorMessage = firstErrorMessage ?? "Could not load active DAG runs."
            }

            isRefreshingActiveRuns = false
        }
    }

    func startAstro(projectPath: String, service: ProjectService) async {
        isLoading = true
        defer { isLoading = false }
        do {
            actionOutput = try await service.startAstro(projectPath: projectPath)
            await refreshStatus(projectPath: projectPath, service: service)
            await refreshSummary(projectPath: projectPath, service: service)
            await refreshActiveRuns(projectPath: projectPath, service: service)
        } catch {
            actionOutput = error.localizedDescription
        }
    }

    func triggerDag(projectPath: String, dagID: String, service: ProjectService) async {
        isLoading = true
        defer { isLoading = false }
        do {
            if let run = try await service.triggerDag(projectPath: projectPath, dagID: dagID) {
                actionOutput = "Triggered \(dagID): \(run.runId)"
            } else {
                actionOutput = "Triggered \(dagID)."
            }
            await refreshStatus(projectPath: projectPath, service: service)
            await refreshActiveRuns(projectPath: projectPath, service: service)
        } catch {
            actionOutput = error.localizedDescription
        }
    }
}
