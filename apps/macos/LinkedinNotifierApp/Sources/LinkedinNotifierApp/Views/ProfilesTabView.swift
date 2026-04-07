import SwiftUI

struct ProfilesTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = ProfilesViewModel()

    var body: some View {
        HSplitView {
            ProfilesSidebarView(viewModel: viewModel) {
                Task {
                    await viewModel.loadProfiles(service: context.projectService)
                }
            }

            Group {
                if let profile = viewModel.selectedProfile {
                    ProfileDetailView(
                        profile: profile,
                        dashboard: viewModel.selectedDashboard,
                        dashboardErrorMessage: viewModel.dashboardErrorMessage
                    )
                    .padding(20)
                } else {
                    ContentUnavailableView(
                        "Select a Profile",
                        systemImage: "person.text.rectangle",
                        description: Text("Pick a local user profile to inspect its summary and search terms.")
                    )
                }
            }
            .frame(minWidth: 720, maxWidth: .infinity, maxHeight: .infinity)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .task {
            await viewModel.loadProfiles(service: context.projectService)
        }
        .task(id: isVisible) {
            guard isVisible else {
                return
            }
            await viewModel.loadProfiles(service: context.projectService, showLoading: false)
        }
    }
}
