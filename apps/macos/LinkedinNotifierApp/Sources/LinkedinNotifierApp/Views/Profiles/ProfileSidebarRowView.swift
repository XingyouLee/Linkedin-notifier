import SwiftUI

struct ProfileSidebarRowView: View {
    let profile: LocalProfileSummary
    let dashboard: ProfileDashboard?

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                Text(profile.displayName ?? profile.profileKey)
                    .font(.headline)
                Spacer()
                profileStatusBadge(profile.active == true ? "active" : "inactive")
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
}
