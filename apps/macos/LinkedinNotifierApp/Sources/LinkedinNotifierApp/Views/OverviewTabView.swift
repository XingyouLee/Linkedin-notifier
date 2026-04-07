import AppKit
import SwiftUI

struct OverviewTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = OverviewViewModel()
    @State private var navigationError: String?

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text("Cloud Dashboard")
                            .font(.largeTitle.bold())
                        Text("Standalone macOS client reading the remote jobs database.")
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button("Refresh") {
                        Task {
                            await viewModel.refresh(service: context.projectService)
                        }
                    }
                }

                statusCard(
                    title: "Database",
                    isActive: true,
                    detail: viewModel.snapshot.databaseDescription
                )

                HStack(spacing: 12) {
                    Button("Open Airflow") {
                        open(url: context.projectService.airflowHomeURL(), errorMessage: "Could not open the configured Airflow URL.")
                    }
                    .disabled(context.projectService.airflowHomeURL() == nil)

                    Button("Open Scan DAG") {
                        open(
                            url: context.projectService.airflowDagURL(dagID: "linkedin_notifier"),
                            errorMessage: "Could not open the scan DAG page."
                        )
                    }
                    .disabled(context.projectService.airflowDagURL(dagID: "linkedin_notifier") == nil)

                    Button("Open Fitting DAG") {
                        open(
                            url: context.projectService.airflowDagURL(dagID: "linkedin_fitting_notifier"),
                            errorMessage: "Could not open the fitting DAG page."
                        )
                    }
                    .disabled(context.projectService.airflowDagURL(dagID: "linkedin_fitting_notifier") == nil)
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
                            detail: "Batch-scoped scan visibility, with the queue bars below showing global backlog progress."
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

                if let message = navigationError ?? viewModel.snapshot.lastError {
                    GroupBox("Status") {
                        Text(message)
                            .foregroundStyle(.red)
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
        .task {
            await viewModel.refresh(service: context.projectService)
        }
        .task(id: isVisible) {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.refreshSummary(service: context.projectService)
                try? await Task.sleep(for: .seconds(10))
            }
        }
    }

    private func open(url: URL?, errorMessage: String) {
        navigationError = nil
        guard let url else {
            navigationError = "No external Airflow URL is configured in this build."
            return
        }

        if !NSWorkspace.shared.open(url) {
            navigationError = errorMessage
        }
    }

    private func statusCard(title: String, isActive: Bool, detail: String) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 6) {
                Label(isActive ? "Configured" : "Unavailable", systemImage: isActive ? "checkmark.circle.fill" : "xmark.circle")
                    .foregroundStyle(isActive ? .green : .red)
                    .font(.headline)
                Text(title)
                    .font(.title3.bold())
                Text(detail)
                    .foregroundStyle(.secondary)
                    .font(.callout)
                    .textSelection(.enabled)
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
}
