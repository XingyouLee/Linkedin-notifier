import Foundation

@MainActor
final class OverviewViewModel: ObservableObject {
    @Published private(set) var snapshot: CloudStatusSnapshot = .empty
    @Published private(set) var isLoading = false

    func refresh(service: ProjectService) async {
        isLoading = true
        defer { isLoading = false }
        await refreshSummary(service: service)
    }

    func refreshSummary(service: ProjectService) async {
        do {
            let summary = try await service.loadSummary()
            snapshot = CloudStatusSnapshot(
                summary: summary,
                databaseDescription: service.cloudConfig.redactedDatabaseDescription,
                airflowWebURL: service.cloudConfig.airflowWebURLString,
                lastError: nil
            )
        } catch {
            snapshot = CloudStatusSnapshot(
                summary: nil,
                databaseDescription: service.cloudConfig.redactedDatabaseDescription,
                airflowWebURL: service.cloudConfig.airflowWebURLString,
                lastError: error.localizedDescription
            )
        }
    }
}
