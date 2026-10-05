import SwiftUI

struct ProfileDetailView: View {
    let profile: LocalProfileSummary
    let dashboard: ProfileDashboard?
    let dashboardErrorMessage: String?

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                ProfileHeaderCardView(profile: profile, dashboard: dashboard)
                ProfileDashboardSectionView(dashboard: dashboard, dashboardErrorMessage: dashboardErrorMessage)
                ProfileCandidateSummarySectionView(profile: profile)
                ProfileLocalDetailsSectionView(profile: profile)
                ProfileSearchConfigsSectionView(profile: profile)
            }
        }
    }
}

private struct ProfileHeaderCardView: View {
    let profile: LocalProfileSummary
    let dashboard: ProfileDashboard?

    var body: some View {
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
                    profileStatusPill(
                        text: profile.active == true ? "Active" : "Inactive",
                        tint: profile.active == true ? .green : .secondary
                    )
                }

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 220), spacing: 12)], alignment: .leading, spacing: 12) {
                    profileInfoPill("Model", profile.modelName ?? "—")
                    profileInfoPill("Discord", profile.discordChannelId ?? "not configured")
                    profileInfoPill("Resume", profile.resumePath ?? "—")

                    if let dashboard {
                        profileInfoPill("Last Seen", dashboard.lastSeenAt ?? "—")
                        profileInfoPill("Last Notified", dashboard.lastNotifiedAt ?? "—")
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, 4)
        }
    }
}
