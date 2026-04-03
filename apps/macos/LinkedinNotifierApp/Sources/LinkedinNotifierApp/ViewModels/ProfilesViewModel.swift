import Foundation

@MainActor
final class ProfilesViewModel: ObservableObject {
    @Published private(set) var profiles: [ProfileDashboard] = []
    @Published var selectedProfileID: Int?
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?

    var selectedProfile: ProfileDashboard? {
        guard let selectedProfileID else {
            return profiles.first
        }
        return profiles.first(where: { $0.profileId == selectedProfileID }) ?? profiles.first
    }

    func loadProfiles(projectPath: String, service: ProjectService, showLoading: Bool = true) async {
        if showLoading {
            isLoading = true
        }
        errorMessage = nil
        defer {
            if showLoading {
                isLoading = false
            }
        }

        do {
            let response = try await service.loadProfileDashboards(projectPath: projectPath)
            profiles = response.profiles

            if let selectedProfileID,
               response.profiles.contains(where: { $0.profileId == selectedProfileID }) {
                self.selectedProfileID = selectedProfileID
            } else {
                self.selectedProfileID = response.profiles.first?.profileId
            }
        } catch {
            profiles = []
            selectedProfileID = nil
            errorMessage = error.localizedDescription
        }
    }
}
