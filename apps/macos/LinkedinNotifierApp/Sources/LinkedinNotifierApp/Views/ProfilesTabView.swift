import Charts
import SwiftUI

struct ProfilesTabView: View {
    let isVisible: Bool

    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = ProfilesViewModel()

    var body: some View {
        HSplitView {
            sidebar

            Group {
                if let profile = viewModel.selectedProfile {
                    LocalProfileDetailView(
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

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Profiles")
                    .font(.title2.bold())
                Spacer()
                Button("Refresh") {
                    Task {
                        await viewModel.loadProfiles(service: context.projectService)
                    }
                }
            }

            if let errorMessage = viewModel.errorMessage {
                Text(errorMessage)
                    .font(.callout)
                    .foregroundStyle(.red)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }

            List(viewModel.profiles, selection: $viewModel.selectedProfileID) { profile in
                LocalProfileSidebarRow(
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

private struct LocalProfileSidebarRow: View {
    let profile: LocalProfileSummary
    let dashboard: ProfileDashboard?

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                Text(profile.displayName ?? profile.profileKey)
                    .font(.headline)
                Spacer()
                statusBadge(profile.active == true ? "active" : "inactive")
            }

            Text(profile.modelName ?? "No model configured")
                .font(.caption)
                .foregroundStyle(.secondary)

            HStack(spacing: 8) {
                if let dashboard {
                    miniMetric(label: "jobs", value: "\(dashboard.totalJobsValue)")
                    miniMetric(label: "notified", value: "\(dashboard.notifiedJobsValue)")
                    miniMetric(label: "avg", value: dashboard.avgFitScore.map { String(format: "%.1f", $0) } ?? "—")
                } else {
                    miniMetric(label: "roles", value: "\(profile.candidateSummary?.targetRoles.count ?? 0)")
                    miniMetric(label: "skills", value: "\(profile.candidateSummary?.coreSkills.count ?? 0)")
                    miniMetric(label: "configs", value: "\(profile.searchConfigs.count)")
                }
            }

            if let dashboard {
                ProgressView(value: dashboard.completionFraction)
                    .tint(.blue)
                Text("\(dashboard.terminalCompletedJobs) completed")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
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

private struct LocalProfileDetailView: View {
    let profile: LocalProfileSummary
    let dashboard: ProfileDashboard?
    let dashboardErrorMessage: String?

    private var totalSearchTermCount: Int {
        profile.searchConfigs.reduce(0) { partial, config in
            partial + config.terms.count
        }
    }

    private var detailColumns: [GridItem] {
        [GridItem(.adaptive(minimum: 280), spacing: 16, alignment: .top)]
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                headerCard
                dashboardSection
                candidateSummarySection
                localProfileSections
                searchConfigsSection
            }
        }
    }

    private var headerCard: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 16) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text(profile.displayName ?? profile.profileKey)
                            .font(.system(size: 28, weight: .bold, design: .rounded))
                        Text(profile.profileKey)
                            .font(.callout)
                            .foregroundStyle(.secondary)
                            .textSelection(.enabled)
                    }
                    Spacer()
                    statusPill(
                        text: profile.active == true ? "Active" : "Inactive",
                        tint: profile.active == true ? .green : .secondary
                    )
                }

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 220), spacing: 12)], alignment: .leading, spacing: 12) {
                    infoPill("Model", profile.modelName ?? "—")
                    infoPill("Discord", profile.discordChannelId ?? "not configured")
                    infoPill("Resume", profile.resumePath ?? "—")

                    if let dashboard {
                        infoPill("Last Seen", dashboard.lastSeenAt ?? "—")
                        infoPill("Last Notified", dashboard.lastNotifiedAt ?? "—")
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, 4)
        }
    }

    @ViewBuilder
    private var dashboardSection: some View {
        if let dashboard {
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 170), spacing: 12)], spacing: 12) {
                metricCard(
                    title: "Jobs",
                    value: "\(dashboard.totalJobsValue)",
                    detail: "tracked profile-job pairs"
                )
                metricCard(
                    title: "Notified",
                    value: "\(dashboard.notifiedJobsValue)",
                    detail: percentageText(fraction: dashboard.notifiedFraction)
                )
                metricCard(
                    title: "Avg Score",
                    value: dashboard.avgFitScore.map { String(format: "%.1f", $0) } ?? "—",
                    detail: "\(dashboard.scoredJobsValue) scored jobs"
                )
                metricCard(
                    title: "Strong + Moderate",
                    value: "\(dashboard.strongOrModerateCount)",
                    detail: "worth a closer look"
                )
            }

            GroupBox("Queue State") {
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 150), spacing: 10)], spacing: 10) {
                    statusCard("Pending Fit", value: dashboard.fitPendingValue, tint: .orange)
                    statusCard("Processing", value: dashboard.fitProcessingValue, tint: .yellow)
                    statusCard("Completed", value: dashboard.terminalCompletedJobs, tint: .green)
                    statusCard("Failures", value: dashboard.totalFailures, tint: .red)
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
                                        .tint(decisionColor(for: item.decision))
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
        } else if let dashboardErrorMessage {
            GroupBox("Cloud Dashboard") {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Remote dashboard stats are unavailable right now.")
                        .font(.headline)
                    Text(dashboardErrorMessage)
                        .foregroundStyle(.red)
                        .textSelection(.enabled)
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        } else {
            GroupBox("Cloud Dashboard") {
                Text("No remote dashboard stats matched this local profile yet.")
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    private var candidateSummarySection: some View {
        GroupBox("Candidate Summary") {
            Text(profile.candidateSummary?.summary ?? "No candidate summary available.")
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private var localProfileSections: some View {
        LazyVGrid(columns: detailColumns, alignment: .leading, spacing: 16) {
            GroupBox("Target Roles") {
                tagGrid(profile.candidateSummary?.targetRoles ?? [])
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Core Skills") {
                tagGrid(profile.candidateSummary?.coreSkills ?? [])
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Obvious Gaps") {
                bulletList(profile.candidateSummary?.obviousGaps ?? [])
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Language Signals") {
                VStack(alignment: .leading, spacing: 10) {
                    infoRow("Dutch", profile.candidateSummary?.languageSignals?.dutchLevel)
                    infoRow("English", profile.candidateSummary?.languageSignals?.englishLevel)

                    if let notes = profile.candidateSummary?.languageSignals?.notes {
                        Text(notes)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private var searchConfigsSection: some View {
        GroupBox("Search Configs") {
            VStack(alignment: .leading, spacing: 14) {
                metricCard(
                    title: "Configs",
                    value: "\(profile.searchConfigs.count)",
                    detail: "saved search configs"
                )
                metricCard(
                    title: "Search Terms",
                    value: "\(totalSearchTermCount)",
                    detail: "terms across configs"
                )

                ForEach(Array(profile.searchConfigs.enumerated()), id: \.element.id) { index, config in
                    VStack(alignment: .leading, spacing: 10) {
                        HStack(alignment: .firstTextBaseline) {
                            Text(config.name)
                                .font(.headline)
                            Spacer()
                            Text(config.location ?? "No location")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }

                        HStack(spacing: 12) {
                            chip("distance \(config.distance.map(String.init) ?? "—")")
                            chip("hours \(config.hoursOld.map(String.init) ?? "—")")
                            chip("limit \(config.resultsPerTerm.map(String.init) ?? "—")")
                        }

                        tagGrid(config.terms)
                    }

                    if index < profile.searchConfigs.count - 1 {
                        Divider()
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
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

    @ViewBuilder
    private func bulletList(_ items: [String]) -> some View {
        if items.isEmpty {
            Text("No items available.")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            VStack(alignment: .leading, spacing: 8) {
                ForEach(items, id: \.self) { item in
                    HStack(alignment: .top, spacing: 8) {
                        Text("•")
                        Text(item)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    @ViewBuilder
    private func tagGrid(_ items: [String]) -> some View {
        if items.isEmpty {
            Text("No items available.")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 140), spacing: 8)], alignment: .leading, spacing: 8) {
                ForEach(items, id: \.self) { item in
                    chip(item)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func chip(_ text: String) -> some View {
        Text(text)
            .font(.caption)
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(.quaternary.opacity(0.65), in: Capsule())
            .textSelection(.enabled)
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
                .lineLimit(2)
                .truncationMode(.middle)
                .textSelection(.enabled)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(.quaternary.opacity(0.45), in: RoundedRectangle(cornerRadius: 10))
    }

    private func infoRow(_ label: String, _ value: String?) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Text(label)
                .foregroundStyle(.secondary)
                .frame(width: 80, alignment: .leading)
            Text(value ?? "—")
                .frame(maxWidth: .infinity, alignment: .leading)
        }
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
