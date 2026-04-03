import Foundation

@MainActor
final class RunsViewModel: ObservableObject {
    static let dagChoices = ["linkedin_notifier", "linkedin_fitting_notifier"]

    @Published var selectedDAG = RunsViewModel.dagChoices[0]
    @Published private(set) var runs: [DagRunRecord] = []
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?

    private var isRefreshingRuns = false
    private var pendingRunsRequest: (projectPath: String, dagID: String, showLoading: Bool)?
    private var latestRequestedProjectPath: String?

    func loadRuns(projectPath: String, service: ProjectService, showLoading: Bool = true) async {
        let requested = (projectPath: projectPath, dagID: selectedDAG, showLoading: showLoading)
        latestRequestedProjectPath = projectPath
        if isRefreshingRuns {
            pendingRunsRequest = requested
            return
        }

        var nextRequest: (projectPath: String, dagID: String, showLoading: Bool)? = requested
        while let request = nextRequest {
            nextRequest = nil
            isRefreshingRuns = true
            if request.showLoading {
                isLoading = true
            }
            errorMessage = nil

            do {
                let loadedRuns = try await service.listDagRuns(projectPath: request.projectPath, dagID: request.dagID)
                if let pendingRunsRequest {
                    nextRequest = pendingRunsRequest
                    self.pendingRunsRequest = nil
                } else if request.projectPath == latestRequestedProjectPath, request.dagID == selectedDAG {
                    runs = loadedRuns
                }
            } catch {
                if let pendingRunsRequest {
                    nextRequest = pendingRunsRequest
                    self.pendingRunsRequest = nil
                } else if request.projectPath == latestRequestedProjectPath, request.dagID == selectedDAG {
                    runs = []
                    errorMessage = error.localizedDescription
                }
            }

            isRefreshingRuns = false
            if request.showLoading {
                isLoading = false
            }
        }
    }
}
