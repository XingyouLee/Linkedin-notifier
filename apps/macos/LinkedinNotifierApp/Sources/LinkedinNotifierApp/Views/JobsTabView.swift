import AppKit
import SwiftUI

struct JobsTabView: View {
    @EnvironmentObject private var context: AppContext
    @StateObject private var viewModel = JobsViewModel()
    @State private var searchDraft = ""

    var body: some View {
        HSplitView {
            VStack(spacing: 12) {
                HStack {
                    NativeSearchField(
                        placeholder: "Search job id, title, company, profile, or matched term",
                        text: $searchDraft
                    ) {
                        Task {
                            await runSearch()
                        }
                    }
                    .frame(minWidth: 280)
                    Button("Search") {
                        Task {
                            await runSearch()
                        }
                    }
                    Button("Refresh") {
                        Task {
                            await runSearch()
                        }
                    }
                }

                if let errorMessage = viewModel.errorMessage {
                    Text(errorMessage)
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                List(viewModel.jobs, selection: Binding(
                    get: { viewModel.selectedJobID },
                    set: { newValue in
                        Task {
                            await viewModel.select(jobID: newValue, service: context.projectService)
                        }
                    }
                )) { job in
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
                                badge("batch \(batchId)")
                            }
                            if let jdStatus = job.jdStatus {
                                badge("jd \(jdStatus)")
                            }
                            if let profileMatchCount = job.profileMatchCount {
                                badge("\(profileMatchCount) profile(s)")
                            }
                            if let bestFitScore = job.bestFitScore {
                                badge("best \(bestFitScore)")
                            }
                        }
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    }
                    .padding(.vertical, 6)
                }
                .overlay {
                    if viewModel.isLoading {
                        ProgressView()
                    } else if viewModel.jobs.isEmpty {
                        ContentUnavailableView("No Jobs", systemImage: "magnifyingglass", description: Text("Refresh the data or adjust the search query."))
                    }
                }
            }
            .padding(16)
            .frame(minWidth: 420, idealWidth: 480, maxWidth: 620, maxHeight: .infinity)

            Group {
                if let detail = viewModel.selectedJobDetail {
                    JobDetailView(detail: detail)
                        .padding(20)
                } else {
                    ContentUnavailableView("Select a Job", systemImage: "tray", description: Text("Pick a row to inspect the canonical job and per-profile fit state."))
                }
            }
            .frame(minWidth: 640, maxWidth: .infinity, maxHeight: .infinity)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .task {
            searchDraft = viewModel.searchText
            await runSearch()
        }
    }

    private func runSearch() async {
        await viewModel.loadJobs(
            service: context.projectService,
            searchText: searchDraft
        )
    }

    private func badge(_ text: String) -> some View {
        Text(text)
            .padding(.horizontal, 8)
            .padding(.vertical, 3)
            .background(.quaternary.opacity(0.5), in: Capsule())
    }
}

private struct NativeSearchField: NSViewRepresentable {
    let placeholder: String
    @Binding var text: String
    let onSubmit: () -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(text: $text, onSubmit: onSubmit)
    }

    func makeNSView(context: Context) -> NSSearchField {
        let field = NSSearchField(frame: .zero)
        field.placeholderString = placeholder
        field.delegate = context.coordinator
        field.sendsSearchStringImmediately = false
        field.sendsWholeSearchString = true
        field.target = context.coordinator
        field.action = #selector(Coordinator.submit)
        return field
    }

    func updateNSView(_ nsView: NSSearchField, context: Context) {
        if nsView.stringValue != text {
            nsView.stringValue = text
        }
        nsView.placeholderString = placeholder
    }

    final class Coordinator: NSObject, NSSearchFieldDelegate {
        @Binding private var text: String
        private let onSubmit: () -> Void

        init(text: Binding<String>, onSubmit: @escaping () -> Void) {
            _text = text
            self.onSubmit = onSubmit
        }

        func controlTextDidChange(_ notification: Notification) {
            guard let field = notification.object as? NSSearchField else {
                return
            }
            text = field.stringValue
        }

        @objc func submit() {
            onSubmit()
        }
    }
}

private struct JobDetailView: View {
    let detail: JobDetailResponse

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                GroupBox("Canonical Job") {
                    VStack(alignment: .leading, spacing: 10) {
                        infoRow("Job ID", detail.job.jobId, monospace: true)
                        infoRow("Title", detail.job.title)
                        infoRow("Company", detail.job.company)
                        infoRow("Site", detail.job.site)
                        infoRow("Batch", detail.job.batchId.map(String.init))
                        infoRow("JD Status", detail.job.jdStatus)
                        infoRow("JD Attempts", detail.job.jdAttempts.map(String.init))
                        infoRow("Job URL", detail.job.jobUrl, monospace: true)
                        if let descriptionError = detail.job.descriptionError {
                            infoRow("Description Error", descriptionError)
                        }
                        if let jdError = detail.job.jdError {
                            infoRow("JD Queue Error", jdError)
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
                            ForEach(detail.profiles) { profile in
                                VStack(alignment: .leading, spacing: 8) {
                                    HStack {
                                        Text(profile.displayName ?? profile.profileKey ?? "Profile \(profile.profileId)")
                                            .font(.headline)
                                        Spacer()
                                        Text("profile_id=\(profile.profileId)")
                                            .font(.system(.caption, design: .monospaced))
                                            .foregroundStyle(.secondary)
                                    }
                                    infoRow("Matched Term", profile.matchedTerm)
                                    infoRow("Fit Status", profile.fitStatus)
                                    infoRow("Fit Score", profile.fitScore.map(String.init))
                                    infoRow("Fit Decision", profile.fitDecision)
                                    infoRow("Notify Status", profile.notifyStatus)
                                    infoRow("Notify Error", profile.notifyError)
                                    infoRow("LLM Error", profile.llmMatchError)
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
                                Divider()
                            }
                        }
                    }
                }
            }
        }
    }

    private func infoRow(_ label: String, _ value: String?, monospace: Bool = false) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Text(label)
                .foregroundStyle(.secondary)
                .frame(width: 120, alignment: .leading)
            Text(value ?? "—")
                .font(monospace ? .system(.body, design: .monospaced) : .body)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
