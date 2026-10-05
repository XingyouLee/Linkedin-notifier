import SwiftUI

func profileStatusBadge(_ text: String) -> some View {
    Text(text)
        .font(.caption.weight(.semibold))
        .padding(.horizontal, 8)
        .padding(.vertical, 3)
        .background(.quaternary.opacity(0.65), in: Capsule())
}

func profileStatusPill(text: String, tint: Color) -> some View {
    Text(text)
        .font(.subheadline.weight(.semibold))
        .padding(.horizontal, 10)
        .padding(.vertical, 5)
        .background(tint.opacity(0.12), in: Capsule())
        .foregroundStyle(tint)
}

func profileInfoPill(_ title: String, _ value: String) -> some View {
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

func profilePercentageText(fraction: Double) -> String {
    "\(Int((fraction * 100).rounded()))% of tracked jobs"
}

func profileBucketColor(for bucket: ProfileScoreBucket) -> Color {
    switch bucket.upperBound {
    case ...39:
        return .blue
    case ...69:
        return .orange
    default:
        return .green
    }
}

func profileDecisionColor(for decision: String) -> Color {
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
