import SwiftUI

struct JobRowView: View {
    let job: JobListItem

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(job.title ?? "Untitled job")
                    .font(.headline)
                Spacer()
                Text(job.jobId)
                    .font(.system(.caption, design: .monospaced))
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 10) {
                Text(job.company ?? "Unknown company")
                if let batchId = job.batchId {
                    TagChipView("batch \(batchId)")
                }
                if let jdStatus = job.jdStatus {
                    TagChipView("jd \(jdStatus)")
                }
                if let profileMatchCount = job.profileMatchCount {
                    TagChipView("\(profileMatchCount) profile(s)")
                }
                if let bestFitScore = job.bestFitScore {
                    TagChipView("best \(bestFitScore)")
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(.vertical, 6)
    }
}
