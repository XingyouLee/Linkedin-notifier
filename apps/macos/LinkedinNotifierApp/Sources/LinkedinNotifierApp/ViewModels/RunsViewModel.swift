import Foundation

@MainActor
final class RunsViewModel: ObservableObject {
    static let dagChoices = ["linkedin_notifier", "linkedin_fitting_notifier"]

    @Published var selectedDAG = RunsViewModel.dagChoices[0]
    @Published private(set) var dagURLs: [String: URL] = [:]
    @Published private(set) var homeURL: URL?
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?

    func loadRuns(service: ProjectService, showLoading: Bool = true) async {
        if showLoading {
            isLoading = true
        }
        defer {
            if showLoading {
                isLoading = false
            }
        }

        homeURL = service.airflowHomeURL()
        dagURLs = Dictionary(uniqueKeysWithValues: Self.dagChoices.compactMap { dagID in
            service.airflowDagURL(dagID: dagID).map { (dagID, $0) }
        })
        errorMessage = homeURL == nil ? "No external Airflow web URL is configured in this build." : nil
    }
}
