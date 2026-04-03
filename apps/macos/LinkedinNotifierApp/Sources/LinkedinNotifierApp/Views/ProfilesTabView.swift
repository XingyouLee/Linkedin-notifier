import Charts
import SwiftUI

struct ProfilesTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = ProfilesViewModel()

    var body: some View {
        HSplitView {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Text("Profiles")
                        .font(.title2.bold())
                    Spacer()
                    Button("Refresh") {
                        Task {
                            await viewModel.loadProfiles(
                                projectPath: context.projectPath,
                                service: context.projectService
                            )
                        }
                    }
                }

                if let errorMessage = viewModel.errorMessage {
                    Text(errorMessage)
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                List(viewModel.profiles, selection: $viewModel.selectedProfileID) { profile in
                    ProfileSidebarRow(profile: profile)
                        .padding(.vertical, 4)
                }
                .overlay {
                    if viewModel.isLoading {
                        ProgressView()
                    } else if viewModel.profiles.isEmpty {
                        ContentUnavailableView(
                            "No Profiles",
                            systemImage: "person.crop.square.filled.and.at.rectangle",
                            description: Text("Refresh after the local Astro stack is up.")
                        )
                    }
                }
            }
            .padding(16)
            .frame(minWidth: 280, idealWidth: 320, maxWidth: 360, maxHeight: .infinity)

            Group {
                if let profile = viewModel.selectedProfile {
                    ProfileDashboardDetailView(profile: profile)
                        .padding(20)
                } else {
                    ContentUnavailableView(
                        "Select a Profile",
                        systemImage: "chart.bar.doc.horizontal",
                        description: Text("Pick a username to inspect dashboard metrics.")
                    )
                }
            }
            .frame(minWidth: 720, maxWidth: .infinity, maxHeight: .infinity)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .task(id: context.projectPath) {
            await viewModel.loadProfiles(
                projectPath: context.projectPath,
                service: context.projectService
            )
        }
        .task(id: "poll|\(context.projectPath)|\(isVisible)") {
            guard isVisible else {
                return
            }
            while !Task.isCancelled, isVisible {
                await viewModel.loadProfiles(
                    projectPath: context.projectPath,
                    service: context.projectService,
                    showLoading: false
                )
                try? await Task.sleep(for: .seconds(10))
            }
        }
    }
}

private struct ProfileSidebarRow: View {
    let profile: ProfileDashboard

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                Text(profile.name)
                    .font(.headline)
                Spacer()
                statusBadge(profile.isActive == true ? "active" : "inactive")
            }

            if let profileKey = profile.profileKey, profileKey != profile.name {
                Text(profileKey)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 8) {
                miniMetric(label: "jobs", value: profile.totalJobsValue)
                miniMetric(label: "notified", value: profile.notifiedJobsValue)
                miniMetric(label: "avg", value: profile.avgFitScore.map { String(format: "%.1f", $0) } ?? "—")
            }

            ProgressView(value: profile.completionFraction)
                .tint(.blue)

            Text("\(profile.terminalCompletedJobs) completed")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func miniMetric(label: String, value: Int) -> some View {
        miniMetric(label: label, value: String(value))
    }

    private func miniMetric(label: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label.uppercased())
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.system(.caption, design: .monospaced).weight(.semibold))
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func statusBadge(_ text: String) -> some View {
        Text(text)
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 3)
            .background(.quaternary.opacity(0.65), in: Capsule())
    }
}

private struct ProfileDashboardDetailView: View {
    let profile: ProfileDashboard

    private let scoreBarGradient = LinearGradient(
        colors: [Color.blue, Color.teal],
        startPoint: .bottom,
        endPoint: .top
    )

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                headerCard

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 170), spacing: 12)], spacing: 12) {
                    metricCard(
                        title: "Jobs",
                        value: "\(profile.totalJobsValue)",
                        detail: "tracked profile-job pairs"
                    )
                    metricCard(
                        title: "Notified",
                        value: "\(profile.notifiedJobsValue)",
                        detail: percentageText(fraction: profile.notifiedFraction)
                    )
                    metricCard(
                        title: "Avg Score",
                        value: profile.avgFitScore.map { String(format: "%.1f", $0) } ?? "—",
                        detail: "\(profile.scoredJobsValue) scored jobs"
                    )
                    metricCard(
                        title: "Strong + Moderate",
                        value: "\(profile.strongOrModerateCount)",
                        detail: "worth a closer look"
                    )
                }

                GroupBox("Queue State") {
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 150), spacing: 10)], spacing: 10) {
                        statusCard("Pending Fit", value: profile.fitPendingValue, tint: .orange)
                        statusCard("Processing", value: profile.fitProcessingValue, tint: .yellow)
                        statusCard("Completed", value: profile.terminalCompletedJobs, tint: .green)
                        statusCard("Failures", value: profile.totalFailures, tint: .red)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }

                GroupBox("Score Distribution") {
                    VStack(alignment: .leading, spacing: 12) {
                        Chart(profile.scoreBuckets) { bucket in
                            BarMark(
                                x: .value("Score Band", bucket.label),
                                y: .value("Jobs", bucket.count)
                            )
                            .foregroundStyle(color(for: bucket).gradient)
                            .clipShape(RoundedRectangle(cornerRadius: 4))
                        }
                        .frame(height: 240)
                        .chartYAxis {
                            AxisMarks(position: .leading)
                        }

                        HStack(spacing: 18) {
                            legendChip("0-39", color: .blue)
                            legendChip("40-69", color: .orange)
                            legendChip("70-100", color: .green)
                        }
                        .font(.caption)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }

                HStack(alignment: .top, spacing: 20) {
                    GroupBox("Fit Decisions") {
                        VStack(alignment: .leading, spacing: 12) {
                            let maxCount = max(profile.decisionBreakdown.map(\.count).max() ?? 0, 1)
                            ForEach(profile.decisionBreakdown) { item in
                                VStack(alignment: .leading, spacing: 6) {
                                    HStack {
                                        Text(item.decision)
                                        Spacer()
                                        Text("\(item.count)")
                                            .font(.system(.caption, design: .monospaced))
                                    }
                                    ProgressView(value: Double(item.count), total: Double(maxCount))
                                        .tint(decisionColor(for: item.decision))
                                }
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .frame(maxWidth: .infinity)

                    GroupBox("Top Matched Terms") {
                        VStack(alignment: .leading, spacing: 10) {
                            if profile.topTerms.isEmpty {
                                Text("No matched terms recorded yet.")
                                    .foregroundStyle(.secondary)
                            } else {
                                ForEach(profile.topTerms) { term in
                                    HStack(alignment: .firstTextBaseline) {
                                        Text(term.term)
                                        Spacer()
                                        Text("\(term.count)")
                                            .font(.system(.caption, design: .monospaced))
                                            .foregroundStyle(.secondary)
                                    }
                                    if term.id != profile.topTerms.last?.id {
                                        Divider()
                                    }
                                }
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .frame(maxWidth: .infinity)
                }
            }
        }
    }

    private var headerCard: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 16) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text(profile.name)
                            .font(.system(size: 28, weight: .bold, design: .rounded))
                        Text(profile.profileKey ?? "No profile key")
                            .font(.callout)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    statusPill(
                        text: profile.isActive == true ? "Active" : "Inactive",
                        tint: profile.isActive == true ? .green : .secondary
                    )
                }

                HStack(spacing: 12) {
                    infoPill("Model", profile.modelName ?? "—")
                    infoPill(
                        "Discord",
                        discordStatusText(
                            channelID: profile.discordChannelId,
                            hasWebhook: profile.hasDiscordWebhook == true
                        )
                    )
                    infoPill("Last Seen", profile.lastSeenAt ?? "—")
                    infoPill("Last Notified", profile.lastNotifiedAt ?? "—")
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, 4)
        }
    }

    private func metricCard(title: String, value: String, detail: String) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 6) {
                Text(title)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text(value)
                    .font(.system(size: 28, weight: .bold, design: .rounded))
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func statusCard(_ title: String, value: Int, tint: Color) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 6) {
                Text(title)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text("\(value)")
                    .font(.system(size: 24, weight: .bold, design: .rounded))
                    .foregroundStyle(tint)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func legendChip(_ title: String, color: Color) -> some View {
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)
            Text(title)
        }
    }

    private func statusPill(text: String, tint: Color) -> some View {
        Text(text)
            .font(.subheadline.weight(.semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 5)
            .background(tint.opacity(0.12), in: Capsule())
            .foregroundStyle(tint)
    }

    private func infoPill(_ title: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(title.uppercased())
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.system(.caption, design: .monospaced))
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(.quaternary.opacity(0.45), in: RoundedRectangle(cornerRadius: 10))
    }

    private func discordStatusText(channelID: String?, hasWebhook: Bool) -> String {
        if let channelID, !channelID.isEmpty, hasWebhook {
            return "channel + webhook"
        }
        if let channelID, !channelID.isEmpty {
            return "channel"
        }
        if hasWebhook {
            return "webhook"
        }
        return "not configured"
    }

    private func percentageText(fraction: Double) -> String {
        "\(Int((fraction * 100).rounded()))% of tracked jobs"
    }

    private func color(for bucket: ProfileScoreBucket) -> Color {
        switch bucket.upperBound {
        case ...39:
            return .blue
        case ...69:
            return .orange
        default:
            return .green
        }
    }

    private func decisionColor(for decision: String) -> Color {
        switch decision {
        case "Strong Fit":
            return .green
        case "Moderate Fit":
            return .mint
        case "Weak Fit":
            return .orange
        case "Not Recommended":
            return .red
        default:
            return .secondary
        }
    }
}
