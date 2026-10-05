import SwiftUI

struct InfoRowView: View {
    let label: String
    let value: String?
    let labelWidth: CGFloat
    let monospace: Bool

    init(label: String, value: String?, labelWidth: CGFloat = 120, monospace: Bool = false) {
        self.label = label
        self.value = value
        self.labelWidth = labelWidth
        self.monospace = monospace
    }

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Text(label)
                .foregroundStyle(.secondary)
                .frame(width: labelWidth, alignment: .leading)
            Text(value ?? "—")
                .font(monospace ? .system(.body, design: .monospaced) : .body)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
