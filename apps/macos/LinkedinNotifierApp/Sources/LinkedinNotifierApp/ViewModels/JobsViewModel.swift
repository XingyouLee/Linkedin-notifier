import Foundation

@MainActor
final class JobsViewModel: ObservableObject {
    @Published private(set) var searchText = ""
    @Published private(set) var jobs: [JobListItem] = []
    @Published private(set) var selectedJobID: String?
    @Published private(set) var selectedJobDetail: JobDetailResponse?
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?

    func loadJobs(
        service: ProjectService,
        searchText overrideSearchText: String? = nil
    ) async {
        isLoading = true
        errorMessage = nil
        defer { isLoading = false }

        let activeSearchText = overrideSearchText ?? searchText
        searchText = activeSearchText

        do {
            jobs = try await service.loadJobs(searchText: activeSearchText)
            if let selectedJobID, jobs.contains(where: { $0.jobId == selectedJobID }) {
                try await loadDetail(jobID: selectedJobID, service: service)
            } else if let firstJobID = jobs.first?.jobId {
                selectedJobID = firstJobID
                try await loadDetail(jobID: firstJobID, service: service)
            } else {
                selectedJobID = nil
                selectedJobDetail = nil
            }
        } catch {
            jobs = []
            selectedJobID = nil
            selectedJobDetail = nil
            errorMessage = error.localizedDescription
        }
    }

    func select(jobID: String?, service: ProjectService) async {
        guard let jobID else {
            selectedJobID = nil
            selectedJobDetail = nil
            return
        }

        selectedJobID = jobID
        do {
            try await loadDetail(jobID: jobID, service: service)
        } catch {
            selectedJobDetail = nil
            errorMessage = error.localizedDescription
        }
    }

    private func loadDetail(jobID: String, service: ProjectService) async throws {
        selectedJobDetail = try await service.loadJobDetail(jobID: jobID)
    }
}
