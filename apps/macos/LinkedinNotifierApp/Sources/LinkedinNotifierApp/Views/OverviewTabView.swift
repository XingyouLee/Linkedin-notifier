import AppKit
import SwiftUI

struct OverviewTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = OverviewViewModel()
    @State private var activeRunNavigationError: String?

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                HStack(alignment: .bottom, spacing: 12) {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Astro Project")
                            .font(.headline)
                        TextField("Project path", text: $context.projectPath)
                            .textFieldStyle(.roundedBorder)
                            .font(.system(.body, design: .monospaced))
                    }

                    Button("Refresh") {
                        Task {
                            await viewModel.refresh(projectPath: context.projectPath, service: context.projectService)
                        }
                    }

                    Button("Start Astro") {
                        Task {
                            await viewModel.startAstro(projectPath: context.projectPath, service: context.projectService)
                        }
                    }

                    Button("Run Scan DAG") {
                        Task {
                            await viewModel.triggerDag(
                                projectPath: context.projectPath,
                                dagID: "linkedin_notifier",
                                service: context.projectService
                            )
                        }
                    }

                    Button("Run Fitting DAG") {
                        Task {
                            await viewModel.triggerDag(
                                projectPath: context.projectPath,
                                dagID: "linkedin_fitting_notifier",
                                service: context.projectService
                            )
                        }
                    }
                }

                HStack(spacing: 16) {
                    statusCard(title: "Docker", isActive: viewModel.snapshot.dockerRunning, detail: viewModel.snapshot.dockerRunning ? "Daemon reachable" : "Not running")
                    statusCard(title: "Astro", isActive: viewModel.snapshot.astroRunning, detail: viewModel.snapshot.astroRunning ? "Containers detected" : "Project is down")
                    statusCard(title: "Postgres", isActive: viewModel.snapshot.postgresContainer != nil, detail: viewModel.snapshot.postgresContainer ?? "No container")
                    statusCard(title: "Scheduler", isActive: viewModel.snapshot.schedulerContainer != nil, detail: viewModel.snapshot.schedulerContainer ?? "No container")
                }

                GroupBox("Active DAG Runs") {
                    if let activeRunsMessage = viewModel.activeRunsErrorMessage ?? activeRunNavigationError {
                        Text(activeRunsMessage)
                            .foregroundStyle(.red)
                    } else if viewModel.activeRunsByDAG.isEmpty {
                        Text("No DAGs currently running.")
                            .foregroundStyle(.secondary)
                    } else {
                        let dagIDs = Array(viewModel.activeRunsByDAG.keys).sorted()
                        VStack(alignment: .leading, spacing: 10) {
                            ForEach(dagIDs, id: \.self) { dagID in
                                if let runs = viewModel.activeRunsByDAG[dagID], let run = latestActiveRun(from: runs) {
                                    Button {
                                        Task {
                                            await openActiveRun(run)
                                        }
                                    } label: {
                                        VStack(alignment: .leading, spacing: 6) {
                                            Text(dagID)
                                                .font(.headline)
                                            HStack(spacing: 12) {
                                                Text(run.runId)
                                                    .font(.system(.body, design: .monospaced))
                                                Text(run.state ?? "unknown")
                                                    .foregroundStyle(color(for: run.state))
                                                if let duration = durationText(for: run) {
                                                    Text(duration)
                                                        .foregroundStyle(.secondary)
                                                }
                                                Spacer()
                                                Image(systemName: "arrow.up.right.square")
                                                    .foregroundStyle(.secondary)
                                            }
                                            .font(.caption)
                                        }
                                        .contentShape(Rectangle())
                                    }
                                    .buttonStyle(.plain)
                                    .help("Open this DAG run in the Airflow web UI")

                                    if dagID != dagIDs.last {
                                        Divider()
                                    }
                                }
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                if let summary = viewModel.snapshot.summary {
                    GroupBox("Queue Summary") {
                        LazyVGrid(columns: [GridItem(.adaptive(minimum: 180), spacing: 12)], spacing: 12) {
                            summaryTile("Jobs", value: summary.totalJobs)
                            summaryTile("Profile Jobs", value: summary.totalProfileJobs)
                            summaryTile("JD Pending", value: summary.jdPending)
                            summaryTile("JD Failed", value: summary.jdFailed)
                            summaryTile("Fit Pending", value: summary.fitPending)
                            summaryTile("Fit Running", value: summary.fitProcessing)
                            summaryTile("Fit Failed", value: summary.fitFailed)
                            summaryTile("Fit Done/Notified", value: summary.fitTerminalDone ?? summary.fitDone)
                            summaryTile("Notify Failed", value: summary.notifyFailed)
                            summaryTile("Latest Batch", value: summary.latestBatchId)
                        }
                    }

                    GroupBox("Latest Scan Batch") {
                        metricSection(
                            title: "Scan",
                            subtitle: summary.latestBatchId.map { "Batch \($0)" } ?? "No batch",
                            primaryValue: summary.latestBatchJobsTotal.map(String.init) ?? "—",
                            secondaryValue: "jobs discovered",
                            detail: "Scan writes jobs into batches, so this card stays batch-scoped while the queue bars below show global backlog progress."
                        )
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }

                    GroupBox("Global Queue Progress") {
                        VStack(alignment: .leading, spacing: 16) {
                            progressSection(
                                title: "JD Fetch",
                                subtitle: summary.globalJdTotalCount > 0
                                    ? "\(summary.globalJdTotalCount) tracked jobs"
                                    : "No jobs",
                                completed: summary.globalJdCompletedCount,
                                total: summary.globalJdTotalCount,
                                detail: "done \(summary.jdDone ?? 0)  processing \(summary.jdProcessing ?? 0)  pending \(summary.jdPending ?? 0)  failed \(summary.jdFailed ?? 0)"
                            )

                            progressSection(
                                title: "LLM Fitting",
                                subtitle: summary.globalFitTotalCount > 0
                                    ? "\(summary.globalFitTotalCount) tracked profile jobs"
                                    : "No profile jobs",
                                completed: summary.globalFitCompletedCount,
                                total: summary.globalFitTotalCount,
                                detail: "done/notified \(summary.fitTerminalDone ?? 0)  processing \(summary.fitProcessing ?? 0)  pending \(summary.fitPending ?? 0)  fit failed \(summary.fitFailed ?? 0)  notify failed \(summary.notifyFailed ?? 0)"
                            )
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                GroupBox("Containers") {
                    if viewModel.snapshot.containers.isEmpty {
                        Text("No containers found for this project.")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(viewModel.snapshot.containers) { container in
                            HStack {
                                Text(container.name)
                                    .font(.system(.body, design: .monospaced))
                                Spacer()
                                Text(container.status)
                                    .foregroundStyle(.secondary)
                            }
                            .padding(.vertical, 4)
                        }
                    }
                }

                if !viewModel.actionOutput.isEmpty || viewModel.snapshot.lastError != nil {
                    GroupBox("Output") {
                        VStack(alignment: .leading, spacing: 10) {
                            if let lastError = viewModel.snapshot.lastError {
                                Text(lastError)
                                    .foregroundStyle(.red)
                            }
                            if !viewModel.actionOutput.isEmpty {
                                Text(viewModel.actionOutput)
                                    .font(.system(.body, design: .monospaced))
                                    .textSelection(.enabled)
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }
            }
            .padding(20)
        }
        .overlay(alignment: .topTrailing) {
            if viewModel.isLoading {
                ProgressView()
                    .padding()
            }
        }
        .task(id: context.projectPath) {
            await viewModel.refresh(projectPath: context.projectPath, service: context.projectService)
        }
        .task(id: "status|\(context.projectPath)|\(isVisible)") {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.refreshStatus(projectPath: context.projectPath, service: context.projectService)
                try? await Task.sleep(for: .seconds(1))
            }
        }
        .task(id: "summary|\(context.projectPath)|\(isVisible)") {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.refreshSummary(projectPath: context.projectPath, service: context.projectService)
                try? await Task.sleep(for: .seconds(5))
            }
        }
        .task(id: "active-runs|\(context.projectPath)|\(isVisible)") {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.refreshActiveRuns(projectPath: context.projectPath, service: context.projectService)
                try? await Task.sleep(for: .seconds(5))
            }
        }
    }

    private func statusCard(title: String, isActive: Bool, detail: String) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 6) {
                Label(isActive ? "Healthy" : "Inactive", systemImage: isActive ? "checkmark.circle.fill" : "xmark.circle")
                    .foregroundStyle(isActive ? .green : .red)
                    .font(.headline)
                Text(title)
                    .font(.title3.bold())
                Text(detail)
                    .foregroundStyle(.secondary)
                    .font(.callout)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, 8)
        }
        .frame(maxWidth: .infinity)
    }

    private func summaryTile(_ title: String, value: Int?) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text(value.map(String.init) ?? "—")
                    .font(.title2.bold())
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func progressSection(
        title: String,
        subtitle: String,
        completed: Int,
        total: Int,
        detail: String
    ) -> some View {
        let safeCompleted = max(0, completed)
        let safeTotal = max(0, total)
        let fraction = safeTotal > 0 ? min(Double(safeCompleted) / Double(safeTotal), 1.0) : 0

        return VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.headline)
                    Text(subtitle)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Text(safeTotal > 0 ? "\(safeCompleted) / \(safeTotal)" : "—")
                    .font(.system(.body, design: .monospaced))
            }

            ProgressView(value: fraction)
                .controlSize(.large)

            Text(detail)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func metricSection(
        title: String,
        subtitle: String,
        primaryValue: String,
        secondaryValue: String,
        detail: String
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.headline)
                    Text(subtitle)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }

            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Text(primaryValue)
                    .font(.system(size: 28, weight: .bold, design: .rounded))
                Text(secondaryValue)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            Text(detail)
                .font(.caption)
                .foregroundStyle(.secondary)
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

    private func latestActiveRun(from runs: [DagRunRecord]) -> DagRunRecord? {
        runs.max { lhs, rhs in
            let lhsDate = runDate(for: lhs) ?? .distantPast
            let rhsDate = runDate(for: rhs) ?? .distantPast
            return lhsDate < rhsDate
        }
    }

    private func durationText(for run: DagRunRecord) -> String? {
        guard let startedAt = runDate(for: run) else {
            return nil
        }

        let elapsed = max(0, Int(Date().timeIntervalSince(startedAt)))
        let hours = elapsed / 3600
        let minutes = (elapsed % 3600) / 60
        let seconds = elapsed % 60

        if hours > 0 {
            return String(format: "%dh %02dm", hours, minutes)
        }
        if minutes > 0 {
            return String(format: "%dm %02ds", minutes, seconds)
        }
        return String(format: "%ds", seconds)
    }

    private func runDate(for run: DagRunRecord) -> Date? {
        parseDate(run.startDate) ?? parseDate(run.runAfter) ?? parseDate(run.logicalDate)
    }

    private func parseDate(_ value: String?) -> Date? {
        guard let value else {
            return nil
        }

        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = fractional.date(from: value) {
            return date
        }

        let plain = ISO8601DateFormatter()
        plain.formatOptions = [.withInternetDateTime]
        return plain.date(from: value)
    }

    @MainActor
    private func openActiveRun(_ run: DagRunRecord) async {
        activeRunNavigationError = nil
        guard let url = await context.projectService.airflowDagRunURL(
            projectPath: context.projectPath,
            dagID: run.dagId,
            runID: run.runId
        ) else {
            activeRunNavigationError = "Could not resolve the local Airflow web URL."
            return
        }

        if !NSWorkspace.shared.open(url) {
            activeRunNavigationError = "Could not open the selected Airflow run in the browser."
        }
    }
}
