import Foundation

@MainActor
final class ProfilesViewModel: ObservableObject {
    @Published private(set) var profiles: [LocalProfileSummary] = []
    @Published private(set) var dashboardsByProfileID: [String: ProfileDashboard] = [:]
    @Published var selectedProfileID: String?
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?
    @Published private(set) var dashboardErrorMessage: String?

    var selectedProfile: LocalProfileSummary? {
        guard let selectedProfileID else {
            return profiles.first
        }
        return profiles.first(where: { $0.id == selectedProfileID }) ?? profiles.first
    }

    var selectedDashboard: ProfileDashboard? {
        guard let selectedProfile else {
            return nil
        }
        return dashboardsByProfileID[selectedProfile.id]
    }

    func loadProfiles(service: ProjectService, showLoading: Bool = true) async {
        if showLoading {
            isLoading = true
        }
        errorMessage = nil
        dashboardErrorMessage = nil
        defer {
            if showLoading {
                isLoading = false
            }
        }

        do {
            let loadedProfiles = try service.loadLocalProfiles()
            profiles = loadedProfiles

            if let selectedProfileID,
               loadedProfiles.contains(where: { $0.id == selectedProfileID }) {
                self.selectedProfileID = selectedProfileID
            } else {
                self.selectedProfileID = loadedProfiles.first?.id
            }

            await loadDashboards(service: service, profiles: loadedProfiles)
        } catch {
            profiles = []
            dashboardsByProfileID = [:]
            selectedProfileID = nil
            errorMessage = error.localizedDescription
        }
    }

    private func loadDashboards(service: ProjectService, profiles: [LocalProfileSummary]) async {
        do {
            let response = try await service.loadProfileDashboards()
            let matches = profiles.compactMap { profile -> (String, ProfileDashboard)? in
                guard let dashboard = response.profiles.first(where: { $0.matches(localProfile: profile) }) else {
                    return nil
                }
                return (profile.id, dashboard)
            }
            dashboardsByProfileID = Dictionary(uniqueKeysWithValues: matches)
        } catch {
            dashboardsByProfileID = [:]
            dashboardErrorMessage = error.localizedDescription
        }
    }
}
