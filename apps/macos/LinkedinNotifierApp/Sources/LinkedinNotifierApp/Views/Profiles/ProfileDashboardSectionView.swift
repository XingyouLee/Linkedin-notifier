import Charts
import SwiftUI

struct ProfileDashboardSectionView: View {
    let dashboard: ProfileDashboard?
    let dashboardErrorMessage: String?

    private var detailColumns: [GridItem] {
        [GridItem(.adaptive(minimum: 280), spacing: 16, alignment: .top)]
    }

    var body: some View {
        Group {
            if let dashboard {
                content(dashboard)
            } else if let dashboardErrorMessage {
                InlineStatusView(title: "Remote dashboard stats are unavailable right now.", message: dashboardErrorMessage)
            } else {
                InlineStatusView(title: "Cloud Dashboard", message: "No remote dashboard stats matched this local profile yet.", isError: false)
            }
        }
    }

    @ViewBuilder
    private func content(_ dashboard: ProfileDashboard) -> some View {
        LazyVGrid(columns: [GridItem(.adaptive(minimum: 170), spacing: 12)], spacing: 12) {
            MetricCardView(title: "Jobs", value: "\(dashboard.totalJobsValue)", detail: "tracked profile-job pairs")
            MetricCardView(title: "Notified", value: "\(dashboard.notifiedJobsValue)", detail: profilePercentageText(fraction: dashboard.notifiedFraction))
            MetricCardView(title: "Avg Score", value: dashboard.avgFitScore.map { String(format: "%.1f", $0) } ?? "—", detail: "\(dashboard.scoredJobsValue) scored jobs")
            MetricCardView(title: "Strong + Moderate", value: "\(dashboard.strongOrModerateCount)", detail: "worth a closer look")
        }

        GroupBox("Queue State") {
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 150), spacing: 10)], spacing: 10) {
                MetricCardView(title: "Pending Fit", value: "\(dashboard.fitPendingValue)", tint: .orange)
                MetricCardView(title: "Processing", value: "\(dashboard.fitProcessingValue)", tint: .yellow)
                MetricCardView(title: "Completed", value: "\(dashboard.terminalCompletedJobs)", tint: .green)
                MetricCardView(title: "Failures", value: "\(dashboard.totalFailures)", tint: .red)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }

        GroupBox("Score Distribution") {
            if dashboard.scoreBuckets.isEmpty {
                Text("No scored jobs recorded yet.")
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
            } else {
                VStack(alignment: .leading, spacing: 12) {
                    Chart(dashboard.scoreBuckets) { bucket in
                        BarMark(
                            x: .value("Score Band", bucket.label),
                            y: .value("Jobs", bucket.count)
                        )
                        .foregroundStyle(profileBucketColor(for: bucket).gradient)
                        .clipShape(RoundedRectangle(cornerRadius: 4))
                    }
                    .frame(height: 240)
                    .chartYAxis {
                        AxisMarks(position: .leading)
                    }

                    HStack(spacing: 18) {
                        ProfileLegendChipView(title: "0-39", color: .blue)
                        ProfileLegendChipView(title: "40-69", color: .orange)
                        ProfileLegendChipView(title: "70-100", color: .green)
                    }
                    .font(.caption)
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }

        LazyVGrid(columns: detailColumns, alignment: .leading, spacing: 16) {
            GroupBox("Fit Decisions") {
                if dashboard.decisionBreakdown.isEmpty {
                    Text("No fit decisions recorded yet.")
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                } else {
                    VStack(alignment: .leading, spacing: 12) {
                        let maxCount = max(dashboard.decisionBreakdown.map(\.count).max() ?? 0, 1)
                        ForEach(dashboard.decisionBreakdown) { item in
                            VStack(alignment: .leading, spacing: 6) {
                                HStack {
                                    Text(item.decision)
                                    Spacer()
                                    Text("\(item.count)")
                                        .font(.system(.caption, design: .monospaced))
                                }
                                ProgressView(value: Double(item.count), total: Double(maxCount))
                                    .tint(profileDecisionColor(for: item.decision))
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Top Matched Terms") {
                if dashboard.topTerms.isEmpty {
                    Text("No matched terms recorded yet.")
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                } else {
                    VStack(alignment: .leading, spacing: 10) {
                        ForEach(Array(dashboard.topTerms.enumerated()), id: \.element.id) { index, term in
                            HStack(alignment: .firstTextBaseline) {
                                Text(term.term)
                                Spacer()
                                Text("\(term.count)")
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(.secondary)
                            }
                            if index < dashboard.topTerms.count - 1 {
                                Divider()
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

private struct ProfileLegendChipView: View {
    let title: String
    let color: Color

    var body: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)
            Text(title)
        }
    }
}
