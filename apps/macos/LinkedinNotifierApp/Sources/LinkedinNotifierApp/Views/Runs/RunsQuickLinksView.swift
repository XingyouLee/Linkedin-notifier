import SwiftUI

struct RunsQuickLinksView: View {
    let homeURL: URL?
    let dagURLs: [String: URL]
    let open: (URL?, String) -> Void

    var body: some View {
        GroupBox("Quick Links") {
            VStack(alignment: .leading, spacing: 12) {
                LinkCardButton(
                    title: "Open Airflow Home",
                    subtitle: homeURL?.absoluteString ?? "Not configured"
                ) {
                    open(homeURL, "Could not open the configured Airflow home page.")
                }

                ForEach(RunsViewModel.dagChoices, id: \.self) { dagID in
                    LinkCardButton(
                        title: dagID,
                        subtitle: dagURLs[dagID]?.absoluteString ?? "Not configured"
                    ) {
                        open(dagURLs[dagID], "Could not open the DAG page for \(dagID).")
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}
