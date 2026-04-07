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

                OverviewDatabaseStatusCard(detail: viewModel.snapshot.databaseDescription)

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
                            OverviewSummaryTile(title: "Profile Jobs", value: summary.totalProfileJobs)
                            OverviewSummaryTile(title: "JD Pending", value: summary.jdPending)
                            OverviewSummaryTile(title: "JD Failed", value: summary.jdFailed)
                            OverviewSummaryTile(title: "Fit Pending", value: summary.fitPending)
                            OverviewSummaryTile(title: "Fit Running", value: summary.fitProcessing)
                            OverviewSummaryTile(title: "Fit Failed", value: summary.fitFailed)
                            OverviewSummaryTile(title: "Fit Done/Notified", value: summary.fitTerminalDone ?? summary.fitDone)
                            OverviewSummaryTile(title: "Notify Failed", value: summary.notifyFailed)
                            OverviewSummaryTile(title: "Latest Batch", value: summary.latestBatchId)
                        }
                    }

                    GroupBox("Latest Scan Batch") {
                        OverviewMetricSection(
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
                            OverviewProgressSection(
                                title: "JD Fetch",
                                subtitle: summary.globalJdTotalCount > 0
                                    ? "\(summary.globalJdTotalCount) tracked jobs"
                                    : "No jobs",
                                completed: summary.globalJdCompletedCount,
                                total: summary.globalJdTotalCount,
                                detail: "done \(summary.jdDone ?? 0)  processing \(summary.jdProcessing ?? 0)  pending \(summary.jdPending ?? 0)  failed \(summary.jdFailed ?? 0)"
                            )

                            OverviewProgressSection(
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
                    InlineStatusView(message: message)
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

    private func summaryTile(_ title: String, value: Int?) -> some View {
        OverviewSummaryTile(title: title, value: value)
    }
}
