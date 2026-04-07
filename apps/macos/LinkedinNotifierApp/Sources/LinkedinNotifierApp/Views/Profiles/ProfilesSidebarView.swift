import SwiftUI

struct ProfilesSidebarView: View {
    @ObservedObject var viewModel: ProfilesViewModel
    let refresh: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Profiles")
                    .font(.title2.bold())
                Spacer()
                Button("Refresh", action: refresh)
            }

            if let errorMessage = viewModel.errorMessage {
                InlineMessageText(errorMessage)
            }

            List(viewModel.profiles, selection: $viewModel.selectedProfileID) { profile in
                ProfileSidebarRowView(
                    profile: profile,
                    dashboard: viewModel.dashboardsByProfileID[profile.id]
                )
                .padding(.vertical, 4)
            }
            .listStyle(.sidebar)
            .overlay {
                if viewModel.isLoading {
                    ProgressView()
                } else if viewModel.profiles.isEmpty {
                    ContentUnavailableView(
                        "No Profiles",
                        systemImage: "person.crop.square.filled.and.at.rectangle",
                        description: Text("The packaged profiles.json file could not be loaded.")
                    )
                }
            }
        }
        .padding(16)
        .frame(minWidth: 280, idealWidth: 320, maxWidth: 360, maxHeight: .infinity)
    }
}
