import AppKit
import SwiftUI

struct RunsTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = RunsViewModel()
    @State private var navigationError: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Picker("DAG", selection: $viewModel.selectedDAG) {
                    ForEach(RunsViewModel.dagChoices, id: \.self) { dagID in
                        Text(dagID).tag(dagID)
                    }
                }
                .pickerStyle(.segmented)
                .frame(maxWidth: 420)

                Button("Refresh") {
                    Task {
                        await viewModel.loadRuns(projectPath: context.projectPath, service: context.projectService)
                    }
                }
            }

            if let errorMessage = navigationError ?? viewModel.errorMessage {
                Text(errorMessage)
                    .foregroundStyle(.red)
            }

            List(viewModel.runs) { run in
                Button {
                    Task {
                        await openRun(run)
                    }
                } label: {
                    VStack(alignment: .leading, spacing: 6) {
                        HStack {
                            Text(run.runId)
                                .font(.system(.body, design: .monospaced))
                            Spacer()
                            Text(run.state ?? "unknown")
                                .foregroundStyle(color(for: run.state))
                        }
                        HStack(spacing: 12) {
                            label("Run After", run.runAfter)
                            label("Start", run.startDate)
                            label("End", run.endDate)
                        }
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    }
                    .padding(.vertical, 6)
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .help("Open this DAG run in the Airflow web UI")
            }
            .overlay {
                if viewModel.isLoading {
                    ProgressView()
                } else if viewModel.runs.isEmpty {
                    ContentUnavailableView("No Runs", systemImage: "clock.arrow.circlepath", description: Text("Refresh after the local Astro stack is up."))
                }
            }
        }
        .padding(20)
        .task(id: "initial|\(context.projectPath)|\(viewModel.selectedDAG)") {
            await viewModel.loadRuns(projectPath: context.projectPath, service: context.projectService)
        }
        .task(id: "poll|\(context.projectPath)|\(viewModel.selectedDAG)|\(isVisible)") {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.loadRuns(projectPath: context.projectPath, service: context.projectService, showLoading: false)
                try? await Task.sleep(for: .seconds(5))
            }
        }
    }

    private func label(_ title: String, _ value: String?) -> some View {
        Text("\(title): \(value ?? "—")")
    }

    @MainActor
    private func openRun(_ run: DagRunRecord) async {
        navigationError = nil
        guard let url = await context.projectService.airflowDagRunURL(
            projectPath: context.projectPath,
            dagID: run.dagId,
            runID: run.runId
        ) else {
            navigationError = "Could not resolve the local Airflow web URL."
            return
        }

        if !NSWorkspace.shared.open(url) {
            navigationError = "Could not open the selected Airflow run in the browser."
        }
    }

    private func color(for state: String?) -> Color {
        switch state?.lowercased() {
        case "success":
            return .green
        case "failed":
            return .red
        case "running", "queued":
            return .orange
        default:
            return .secondary
        }
    }
}
