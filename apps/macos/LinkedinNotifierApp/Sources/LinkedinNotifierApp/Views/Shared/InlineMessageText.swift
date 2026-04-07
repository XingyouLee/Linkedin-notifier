import SwiftUI

struct InlineMessageText: View {
    let message: String
    let isError: Bool

    init(_ message: String, isError: Bool = true) {
        self.message = message
        self.isError = isError
    }

    var body: some View {
        Text(message)
            .foregroundStyle(isError ? .red : .secondary)
            .frame(maxWidth: .infinity, alignment: .leading)
    }
}
