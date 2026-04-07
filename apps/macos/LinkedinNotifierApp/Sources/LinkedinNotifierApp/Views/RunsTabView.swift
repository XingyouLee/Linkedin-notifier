import AppKit
import SwiftUI

struct RunsTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = RunsViewModel()
    @State private var navigationError: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Airflow")
                    .font(.title2.bold())
                Spacer()
                Button("Refresh") {
                    Task {
                        await viewModel.loadRuns(service: context.projectService)
                    }
                }
            }

            Text("This standalone build does not query live DAG runs directly. Use these shortcuts to open the external Airflow UI.")
                .foregroundStyle(.secondary)

            if let errorMessage = navigationError ?? viewModel.errorMessage {
                InlineMessageText(errorMessage)
            }

            RunsQuickLinksView(homeURL: viewModel.homeURL, dagURLs: viewModel.dagURLs, open: open)

            Spacer()
        }
        .padding(20)
        .overlay {
            if viewModel.isLoading {
                ProgressView()
            }
        }
        .task {
            await viewModel.loadRuns(service: context.projectService)
        }
        .task(id: isVisible) {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.loadRuns(service: context.projectService, showLoading: false)
                try? await Task.sleep(for: .seconds(30))
            }
        }
    }

    private func open(_ url: URL?, errorMessage: String) {
        navigationError = nil
        guard let url else {
            navigationError = "No external Airflow URL is configured in this build."
            return
        }

        if !NSWorkspace.shared.open(url) {
            navigationError = errorMessage
        }
    }
}
