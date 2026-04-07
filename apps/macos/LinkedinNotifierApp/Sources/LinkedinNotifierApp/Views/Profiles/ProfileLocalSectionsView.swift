import SwiftUI

struct ProfileCandidateSummarySectionView: View {
    let profile: LocalProfileSummary

    var body: some View {
        GroupBox("Candidate Summary") {
            Text(profile.candidateSummary?.summary ?? "No candidate summary available.")
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

struct ProfileLocalDetailsSectionView: View {
    let profile: LocalProfileSummary

    private var detailColumns: [GridItem] {
        [GridItem(.adaptive(minimum: 280), spacing: 16, alignment: .top)]
    }

    var body: some View {
        LazyVGrid(columns: detailColumns, alignment: .leading, spacing: 16) {
            GroupBox("Target Roles") {
                ProfileTagGridView(items: profile.candidateSummary?.targetRoles ?? [])
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Core Skills") {
                ProfileTagGridView(items: profile.candidateSummary?.coreSkills ?? [])
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Obvious Gaps") {
                ProfileBulletListView(items: profile.candidateSummary?.obviousGaps ?? [])
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            GroupBox("Language Signals") {
                VStack(alignment: .leading, spacing: 10) {
                    InfoRowView(label: "Dutch", value: profile.candidateSummary?.languageSignals?.dutchLevel, labelWidth: 80)
                    InfoRowView(label: "English", value: profile.candidateSummary?.languageSignals?.englishLevel, labelWidth: 80)

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
}

struct ProfileSearchConfigsSectionView: View {
    let profile: LocalProfileSummary

    private var totalSearchTermCount: Int {
        profile.searchConfigs.reduce(0) { partial, config in
            partial + config.terms.count
        }
    }

    var body: some View {
        GroupBox("Search Configs") {
            VStack(alignment: .leading, spacing: 14) {
                MetricCardView(title: "Configs", value: "\(profile.searchConfigs.count)", detail: "saved search configs")
                MetricCardView(title: "Search Terms", value: "\(totalSearchTermCount)", detail: "terms across configs")

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
                            TagChipView("distance \(config.distance.map(String.init) ?? "—")", prominent: true)
                            TagChipView("hours \(config.hoursOld.map(String.init) ?? "—")", prominent: true)
                            TagChipView("limit \(config.resultsPerTerm.map(String.init) ?? "—")", prominent: true)
                        }

                        ProfileTagGridView(items: config.terms)
                    }

                    if index < profile.searchConfigs.count - 1 {
                        Divider()
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

private struct ProfileBulletListView: View {
    let items: [String]

    var body: some View {
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
}

private struct ProfileTagGridView: View {
    let items: [String]

    var body: some View {
        if items.isEmpty {
            Text("No items available.")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 140), spacing: 8)], alignment: .leading, spacing: 8) {
                ForEach(items, id: \.self) { item in
                    TagChipView(item, prominent: true)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
