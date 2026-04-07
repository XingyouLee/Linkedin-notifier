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
                Text(errorMessage)
                    .foregroundStyle(.red)
            }

            GroupBox("Quick Links") {
                VStack(alignment: .leading, spacing: 12) {
                    actionButton(
                        title: "Open Airflow Home",
                        subtitle: viewModel.homeURL?.absoluteString ?? "Not configured"
                    ) {
                        open(viewModel.homeURL, errorMessage: "Could not open the configured Airflow home page.")
                    }

                    ForEach(RunsViewModel.dagChoices, id: \.self) { dagID in
                        actionButton(
                            title: dagID,
                            subtitle: viewModel.dagURLs[dagID]?.absoluteString ?? "Not configured"
                        ) {
                            open(viewModel.dagURLs[dagID], errorMessage: "Could not open the DAG page for \(dagID).")
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }

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

    private func actionButton(title: String, subtitle: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            VStack(alignment: .leading, spacing: 6) {
                Text(title)
                    .font(.headline)
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(12)
            .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 12))
        }
        .buttonStyle(.plain)
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
