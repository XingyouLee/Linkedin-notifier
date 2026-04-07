import SwiftUI

struct MetricCardView: View {
    let title: String
    let value: String
    let detail: String?
    let tint: Color?

    init(title: String, value: String, detail: String? = nil, tint: Color? = nil) {
        self.title = title
        self.value = value
        self.detail = detail
        self.tint = tint
    }

    var body: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 6) {
                Text(title)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text(value)
                    .font(.system(size: 28, weight: .bold, design: .rounded))
                    .foregroundStyle(tint ?? .primary)
                if let detail {
                    Text(detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
