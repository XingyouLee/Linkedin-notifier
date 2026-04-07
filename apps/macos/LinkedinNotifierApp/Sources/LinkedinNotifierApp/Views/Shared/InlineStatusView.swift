import SwiftUI

struct InlineStatusView: View {
    let title: String?
    let message: String
    let isError: Bool

    init(title: String? = nil, message: String, isError: Bool = true) {
        self.title = title
        self.message = message
        self.isError = isError
    }

    var body: some View {
        GroupBox(title ?? (isError ? "Status" : "Info")) {
            VStack(alignment: .leading, spacing: 8) {
                if let title {
                    Text(title)
                        .font(.headline)
                }
                Text(message)
                    .foregroundStyle(isError ? .red : .secondary)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
