import SwiftUI

struct OverviewDatabaseStatusCard: View {
    let detail: String

    var body: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 6) {
                Label("Configured", systemImage: "checkmark.circle.fill")
                    .foregroundStyle(.green)
                    .font(.headline)
                Text("Database")
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
}

struct OverviewSummaryTile: View {
    let title: String
    let value: Int?

    var body: some View {
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
}

struct OverviewProgressSection: View {
    let title: String
    let subtitle: String
    let completed: Int
    let total: Int
    let detail: String

    var body: some View {
        let safeCompleted = max(0, completed)
        let safeTotal = max(0, total)
        let fraction = safeTotal > 0 ? min(Double(safeCompleted) / Double(safeTotal), 1.0) : 0

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
}

struct OverviewMetricSection: View {
    let title: String
    let subtitle: String
    let primaryValue: String
    let secondaryValue: String
    let detail: String

    var body: some View {
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
