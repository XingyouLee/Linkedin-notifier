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
                    InlineMessageText(errorMessage)
                }

                List(viewModel.jobs, selection: Binding(
                    get: { viewModel.selectedJobID },
                    set: { newValue in
                        Task {
                            await viewModel.select(jobID: newValue, service: context.projectService)
                        }
                    }
                )) { job in
                    JobRowView(job: job)
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
