import SwiftUI

struct JobDetailView: View {
    let detail: JobDetailResponse

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                GroupBox("Canonical Job") {
                    VStack(alignment: .leading, spacing: 10) {
                        InfoRowView(label: "Job ID", value: detail.job.jobId, monospace: true)
                        InfoRowView(label: "Title", value: detail.job.title)
                        InfoRowView(label: "Company", value: detail.job.company)
                        InfoRowView(label: "Site", value: detail.job.site)
                        InfoRowView(label: "Batch", value: detail.job.batchId.map(String.init))
                        InfoRowView(label: "JD Status", value: detail.job.jdStatus)
                        InfoRowView(label: "JD Attempts", value: detail.job.jdAttempts.map(String.init))
                        InfoRowView(label: "Job URL", value: detail.job.jobUrl, monospace: true)
                        if let descriptionError = detail.job.descriptionError {
                            InfoRowView(label: "Description Error", value: descriptionError)
                        }
                        if let jdError = detail.job.jdError {
                            InfoRowView(label: "JD Queue Error", value: jdError)
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }

                GroupBox("Job Description") {
                    Text(detail.job.description ?? "No description stored.")
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                GroupBox("Profile Matches") {
                    VStack(alignment: .leading, spacing: 14) {
                        if detail.profiles.isEmpty {
                            Text("No profile-specific records found for this job.")
                                .foregroundStyle(.secondary)
                        } else {
                            ForEach(Array(detail.profiles.enumerated()), id: \.element.id) { index, profile in
                                VStack(alignment: .leading, spacing: 8) {
                                    HStack {
                                        Text(profile.displayName ?? profile.profileKey ?? "Profile \(profile.profileId)")
                                            .font(.headline)
                                        Spacer()
                                        Text("profile_id=\(profile.profileId)")
                                            .font(.system(.caption, design: .monospaced))
                                            .foregroundStyle(.secondary)
                                    }
                                    InfoRowView(label: "Matched Term", value: profile.matchedTerm)
                                    InfoRowView(label: "Fit Status", value: profile.fitStatus)
                                    InfoRowView(label: "Fit Score", value: profile.fitScore.map(String.init))
                                    InfoRowView(label: "Fit Decision", value: profile.fitDecision)
                                    InfoRowView(label: "Notify Status", value: profile.notifyStatus)
                                    InfoRowView(label: "Notify Error", value: profile.notifyError)
                                    InfoRowView(label: "LLM Error", value: profile.llmMatchError)
                                    if let llmMatch = profile.llmMatch {
                                        Text(llmMatch)
                                            .font(.system(.caption, design: .monospaced))
                                            .textSelection(.enabled)
                                            .padding(10)
                                            .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 10))
                                    }
                                }
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .padding(.vertical, 4)
                                if index < detail.profiles.count - 1 {
                                    Divider()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
