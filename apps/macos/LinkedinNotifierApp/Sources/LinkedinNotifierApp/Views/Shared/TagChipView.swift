import SwiftUI

struct TagChipView: View {
    let text: String
    let prominent: Bool

    init(_ text: String, prominent: Bool = false) {
        self.text = text
        self.prominent = prominent
    }

    var body: some View {
        Text(text)
            .font(.caption)
            .padding(.horizontal, prominent ? 10 : 8)
            .padding(.vertical, prominent ? 6 : 3)
            .background((prominent ? AnyShapeStyle(.quaternary.opacity(0.65)) : AnyShapeStyle(.quaternary.opacity(0.5))), in: Capsule())
            .textSelection(.enabled)
    }
}
